#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/backtest_replay_support.h"

namespace {

using quant_hft::apps::BacktestCliResult;
using quant_hft::apps::BacktestCliSpec;
using quant_hft::apps::BacktestSummary;
using quant_hft::apps::JsonEscape;
using quant_hft::apps::ParseArgs;
using quant_hft::apps::ParseBacktestCliSpec;
using quant_hft::apps::RenderBacktestJson;
using quant_hft::apps::RunBacktestSpec;
using quant_hft::apps::SummarizeBacktest;
using quant_hft::apps::WriteTextFile;

struct BaselineExpectation {
    std::int64_t intents_emitted{0};
    std::int64_t order_events{0};
    double total_pnl{0.0};
    double max_drawdown{0.0};
    std::int64_t rollover_events{0};
    std::int64_t rollover_actions{0};
    double rollover_slippage_cost{0.0};
    std::int64_t rollover_canceled_orders{0};
};

struct CheckResult {
    std::string metric;
    std::string expected;
    std::string actual;
    double abs_diff{0.0};
    double rel_diff{0.0};
    bool pass{false};
    std::string note;
};

std::string FormatDouble(double value) {
    std::ostringstream oss;
    oss << std::setprecision(12) << value;
    return oss.str();
}

bool ReadTextFile(const std::string& path, std::string* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "output buffer is null";
        }
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open file: " + path;
        }
        return false;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    *out = buffer.str();
    return true;
}

bool ExtractJsonValueStart(const std::string& json,
                           const std::string& key,
                           std::size_t* value_start) {
    if (value_start == nullptr) {
        return false;
    }
    const std::string quoted_key = "\"" + key + "\"";
    std::size_t key_pos = json.find(quoted_key);
    std::size_t colon_pos = std::string::npos;
    while (key_pos != std::string::npos) {
        std::size_t key_end = key_pos + quoted_key.size();
        while (key_end < json.size() &&
               std::isspace(static_cast<unsigned char>(json[key_end])) != 0) {
            ++key_end;
        }
        if (key_end < json.size() && json[key_end] == ':') {
            colon_pos = key_end;
            break;
        }
        key_pos = json.find(quoted_key, key_pos + quoted_key.size());
    }
    if (colon_pos == std::string::npos) {
        return false;
    }
    std::size_t cursor = colon_pos + 1;
    while (cursor < json.size() && std::isspace(static_cast<unsigned char>(json[cursor])) != 0) {
        ++cursor;
    }
    if (cursor >= json.size()) {
        return false;
    }
    *value_start = cursor;
    return true;
}

bool ExtractJsonObjectByKey(const std::string& json,
                            const std::string& key,
                            std::string* out_object) {
    if (out_object == nullptr) {
        return false;
    }
    std::size_t start = 0;
    if (!ExtractJsonValueStart(json, key, &start) || json[start] != '{') {
        return false;
    }

    int depth = 0;
    bool in_string = false;
    bool escaped = false;
    for (std::size_t i = start; i < json.size(); ++i) {
        const char ch = json[i];
        if (in_string) {
            if (escaped) {
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                in_string = false;
            }
            continue;
        }
        if (ch == '"') {
            in_string = true;
            continue;
        }
        if (ch == '{') {
            ++depth;
            continue;
        }
        if (ch == '}') {
            --depth;
            if (depth == 0) {
                *out_object = json.substr(start, i - start + 1);
                return true;
            }
        }
    }
    return false;
}

bool ExtractJsonArrayByKey(const std::string& json, const std::string& key, std::string* out_array) {
    if (out_array == nullptr) {
        return false;
    }
    std::size_t start = 0;
    if (!ExtractJsonValueStart(json, key, &start) || json[start] != '[') {
        return false;
    }

    int depth = 0;
    bool in_string = false;
    bool escaped = false;
    for (std::size_t i = start; i < json.size(); ++i) {
        const char ch = json[i];
        if (in_string) {
            if (escaped) {
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                in_string = false;
            }
            continue;
        }
        if (ch == '"') {
            in_string = true;
            continue;
        }
        if (ch == '[') {
            ++depth;
            continue;
        }
        if (ch == ']') {
            --depth;
            if (depth == 0) {
                *out_array = json.substr(start, i - start + 1);
                return true;
            }
        }
    }
    return false;
}

std::int64_t CountJsonArrayElements(const std::string& array_json) {
    if (array_json.size() < 2 || array_json.front() != '[' || array_json.back() != ']') {
        return 0;
    }

    bool in_string = false;
    bool escaped = false;
    int brace_depth = 0;
    int bracket_depth = 0;
    bool has_token = false;
    std::int64_t commas = 0;

    for (std::size_t i = 1; i + 1 < array_json.size(); ++i) {
        const char ch = array_json[i];
        if (in_string) {
            if (escaped) {
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                in_string = false;
            }
            has_token = true;
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch)) != 0) {
            continue;
        }
        if (ch == '"') {
            in_string = true;
            has_token = true;
            continue;
        }
        if (ch == '{') {
            ++brace_depth;
            has_token = true;
            continue;
        }
        if (ch == '}') {
            --brace_depth;
            continue;
        }
        if (ch == '[') {
            ++bracket_depth;
            has_token = true;
            continue;
        }
        if (ch == ']') {
            --bracket_depth;
            continue;
        }
        if (ch == ',' && brace_depth == 0 && bracket_depth == 0) {
            ++commas;
            continue;
        }
        has_token = true;
    }

    if (!has_token) {
        return 0;
    }
    return commas + 1;
}

bool ExtractRequiredNumber(const std::string& json,
                           const std::string& key,
                           double* out,
                           std::string* error) {
    if (!quant_hft::apps::detail::ExtractJsonNumber(json, key, out)) {
        if (error != nullptr) {
            *error = "missing numeric key: " + key;
        }
        return false;
    }
    return true;
}

bool ParseBaselineExpectation(const std::string& baseline_json,
                              BaselineExpectation* out,
                              std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "baseline expectation output is null";
        }
        return false;
    }

    for (const std::string& key :
         {"run_id", "mode", "spec", "replay", "deterministic", "summary"}) {
        if (baseline_json.find("\"" + key + "\"") == std::string::npos) {
            if (error != nullptr) {
                *error = "baseline missing required key: " + key;
            }
            return false;
        }
    }

    BaselineExpectation parsed;
    std::string summary_object;
    std::string deterministic_object;
    if (!ExtractJsonObjectByKey(baseline_json, "summary", &summary_object)) {
        if (error != nullptr) {
            *error = "baseline missing object: summary";
        }
        return false;
    }
    if (!ExtractJsonObjectByKey(baseline_json, "deterministic", &deterministic_object)) {
        if (error != nullptr) {
            *error = "baseline missing object: deterministic";
        }
        return false;
    }

    double numeric = 0.0;
    if (!ExtractRequiredNumber(summary_object, "intents_emitted", &numeric, error)) {
        return false;
    }
    parsed.intents_emitted = static_cast<std::int64_t>(std::llround(numeric));

    if (!ExtractRequiredNumber(summary_object, "order_events", &numeric, error)) {
        return false;
    }
    parsed.order_events = static_cast<std::int64_t>(std::llround(numeric));

    if (!ExtractRequiredNumber(summary_object, "total_pnl", &parsed.total_pnl, error)) {
        return false;
    }

    if (!ExtractRequiredNumber(summary_object, "max_drawdown", &parsed.max_drawdown, error)) {
        return false;
    }

    if (!ExtractRequiredNumber(
            deterministic_object, "rollover_slippage_cost", &parsed.rollover_slippage_cost, error)) {
        return false;
    }

    if (!ExtractRequiredNumber(deterministic_object, "rollover_canceled_orders", &numeric, error)) {
        return false;
    }
    parsed.rollover_canceled_orders = static_cast<std::int64_t>(std::llround(numeric));

    std::string rollover_events;
    std::string rollover_actions;
    if (!ExtractJsonArrayByKey(deterministic_object, "rollover_events", &rollover_events)) {
        if (error != nullptr) {
            *error = "baseline missing array: deterministic.rollover_events";
        }
        return false;
    }
    if (!ExtractJsonArrayByKey(deterministic_object, "rollover_actions", &rollover_actions)) {
        if (error != nullptr) {
            *error = "baseline missing array: deterministic.rollover_actions";
        }
        return false;
    }
    parsed.rollover_events = CountJsonArrayElements(rollover_events);
    parsed.rollover_actions = CountJsonArrayElements(rollover_actions);

    *out = parsed;
    return true;
}

void AppendIntCheck(const std::string& metric,
                    std::int64_t expected,
                    std::int64_t actual,
                    std::vector<CheckResult>* checks) {
    CheckResult check;
    check.metric = metric;
    check.expected = std::to_string(expected);
    check.actual = std::to_string(actual);
    check.abs_diff = std::fabs(static_cast<double>(actual - expected));
    check.rel_diff = expected == 0 ? check.abs_diff : check.abs_diff / std::fabs(expected);
    check.pass = expected == actual;
    checks->push_back(std::move(check));
}

void AppendFloatCheck(const std::string& metric,
                      double expected,
                      double actual,
                      double abs_tol,
                      double rel_tol,
                      std::vector<CheckResult>* checks) {
    CheckResult check;
    check.metric = metric;
    check.expected = FormatDouble(expected);
    check.actual = FormatDouble(actual);
    check.abs_diff = std::fabs(actual - expected);
    const double scale = std::max(std::fabs(actual), std::fabs(expected));
    check.rel_diff = scale > 0.0 ? (check.abs_diff / scale) : check.abs_diff;
    check.pass = quant_hft::apps::IsApproxEqual(actual, expected, abs_tol, rel_tol);
    checks->push_back(std::move(check));
}

std::string RenderCheckArray(const std::vector<CheckResult>& checks) {
    std::ostringstream oss;
    oss << "[";
    for (std::size_t i = 0; i < checks.size(); ++i) {
        if (i > 0) {
            oss << ", ";
        }
        const auto& check = checks[i];
        oss << "{"
            << "\"metric\": \"" << JsonEscape(check.metric) << "\", "
            << "\"expected\": \"" << JsonEscape(check.expected) << "\", "
            << "\"actual\": \"" << JsonEscape(check.actual) << "\", "
            << "\"abs_diff\": " << FormatDouble(check.abs_diff) << ", "
            << "\"rel_diff\": " << FormatDouble(check.rel_diff) << ", "
            << "\"pass\": " << (check.pass ? "true" : "false");
        if (!check.note.empty()) {
            oss << ", \"note\": \"" << JsonEscape(check.note) << "\"";
        }
        oss << "}";
    }
    oss << "]";
    return oss.str();
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft::apps;

    const auto args = ParseArgs(argc, argv);
    const std::string baseline_json_path =
        quant_hft::apps::detail::GetArgAny(args,
                                           {"baseline_json", "baseline-json"},
                                           "tests/regression/backtest_consistency/baseline/legacy_python/backtest_baseline.json");
    const std::string output_json =
        quant_hft::apps::detail::GetArgAny(args,
                                           {"output_json", "output-json"},
                                           "docs/results/backtest_consistency_report.json");

    double abs_tol = 1e-8;
    double rel_tol = 1e-6;
    {
        std::string parse_error;
        const std::string abs_raw =
            quant_hft::apps::detail::GetArgAny(args, {"abs_tol", "abs-tol"}, "1e-8");
        const std::string rel_raw =
            quant_hft::apps::detail::GetArgAny(args, {"rel_tol", "rel-tol"}, "1e-6");
        if (!quant_hft::apps::detail::ParseDouble(abs_raw, &abs_tol)) {
            parse_error = "invalid abs_tol: " + abs_raw;
        } else if (!quant_hft::apps::detail::ParseDouble(rel_raw, &rel_tol)) {
            parse_error = "invalid rel_tol: " + rel_raw;
        }
        if (!parse_error.empty()) {
            std::cerr << "backtest_consistency_cli: " << parse_error << '\n';
            return 2;
        }
    }

    BacktestCliSpec spec;
    std::string error;
    if (!ParseBacktestCliSpec(args, &spec, &error)) {
        std::cerr << "backtest_consistency_cli: " << error << '\n';
        return 2;
    }

    std::string baseline_json;
    if (!ReadTextFile(baseline_json_path, &baseline_json, &error)) {
        std::cerr << "backtest_consistency_cli: " << error << '\n';
        return 1;
    }

    BaselineExpectation baseline;
    if (!ParseBaselineExpectation(baseline_json, &baseline, &error)) {
        std::cerr << "backtest_consistency_cli: " << error << '\n';
        return 1;
    }

    BacktestCliResult result;
    if (!RunBacktestSpec(spec, &result, &error)) {
        std::cerr << "backtest_consistency_cli: " << error << '\n';
        return 1;
    }

    const BacktestSummary summary = SummarizeBacktest(result);
    const std::int64_t actual_rollover_events =
        result.has_deterministic ? static_cast<std::int64_t>(result.deterministic.rollover_events.size()) : 0;
    const std::int64_t actual_rollover_actions =
        result.has_deterministic ? static_cast<std::int64_t>(result.deterministic.rollover_actions.size()) : 0;
    const double actual_rollover_slippage_cost =
        result.has_deterministic ? result.deterministic.rollover_slippage_cost : 0.0;
    const std::int64_t actual_rollover_canceled_orders =
        result.has_deterministic ? result.deterministic.rollover_canceled_orders : 0;

    std::vector<CheckResult> checks;
    checks.reserve(8);
    AppendIntCheck("summary.intents_emitted", baseline.intents_emitted, summary.intents_emitted, &checks);
    AppendIntCheck("summary.order_events", baseline.order_events, summary.order_events, &checks);
    AppendFloatCheck("summary.total_pnl", baseline.total_pnl, summary.total_pnl, abs_tol, rel_tol, &checks);
    AppendFloatCheck("summary.max_drawdown",
                     baseline.max_drawdown,
                     summary.max_drawdown,
                     abs_tol,
                     rel_tol,
                     &checks);
    AppendIntCheck("deterministic.rollover_events",
                   baseline.rollover_events,
                   actual_rollover_events,
                   &checks);
    AppendIntCheck("deterministic.rollover_actions",
                   baseline.rollover_actions,
                   actual_rollover_actions,
                   &checks);
    AppendFloatCheck("deterministic.rollover_slippage_cost",
                     baseline.rollover_slippage_cost,
                     actual_rollover_slippage_cost,
                     abs_tol,
                     rel_tol,
                     &checks);
    AppendIntCheck("deterministic.rollover_canceled_orders",
                   baseline.rollover_canceled_orders,
                   actual_rollover_canceled_orders,
                   &checks);

    bool passed = result.has_deterministic;
    for (const auto& check : checks) {
        if (!check.pass) {
            passed = false;
            break;
        }
    }
    std::string reason = "within_tolerance";
    if (!result.has_deterministic) {
        reason = "missing_deterministic_mode";
    } else if (!passed) {
        reason = "difference_exceeds_tolerance";
    }

    const std::string actual_result_json = RenderBacktestJson(result);
    std::ostringstream report;
    report << "{\n"
           << "  \"status\": \"" << (passed ? "pass" : "fail") << "\",\n"
           << "  \"reason\": \"" << reason << "\",\n"
           << "  \"baseline_json\": \"" << JsonEscape(baseline_json_path) << "\",\n"
           << "  \"run_id\": \"" << JsonEscape(result.run_id) << "\",\n"
           << "  \"abs_tol\": " << FormatDouble(abs_tol) << ",\n"
           << "  \"rel_tol\": " << FormatDouble(rel_tol) << ",\n"
           << "  \"checks\": " << RenderCheckArray(checks) << ",\n"
           << "  \"actual_result_json\": \"" << JsonEscape(actual_result_json) << "\"\n"
           << "}\n";

    if (!WriteTextFile(output_json, report.str(), &error)) {
        std::cerr << "backtest_consistency_cli: " << error << '\n';
        return 1;
    }

    std::cout << report.str();
    return passed ? 0 : 1;
}
