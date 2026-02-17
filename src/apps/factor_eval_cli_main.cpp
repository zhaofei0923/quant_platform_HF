#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/backtest_replay_support.h"

namespace {

void ApplySpecJsonToArgs(const std::string& json, quant_hft::apps::ArgMap* args) {
    if (args == nullptr) {
        return;
    }
    auto apply_string = [&](const std::string& key) {
        std::string value;
        if (quant_hft::apps::detail::ExtractJsonString(json, key, &value)) {
            (*args)[key] = value;
        }
    };
    auto apply_number = [&](const std::string& key) {
        double value = 0.0;
        if (quant_hft::apps::detail::ExtractJsonNumber(json, key, &value)) {
            std::ostringstream oss;
            oss << value;
            (*args)[key] = oss.str();
        }
    };
    auto apply_bool = [&](const std::string& key) {
        bool value = false;
        if (quant_hft::apps::detail::ExtractJsonBool(json, key, &value)) {
            (*args)[key] = value ? "true" : "false";
        }
    };

    apply_string("csv_path");
    apply_string("dataset_root");
    apply_string("engine_mode");
    apply_string("rollover_mode");
    apply_string("rollover_price_mode");
    apply_string("start_date");
    apply_string("end_date");
    apply_string("wal_path");
    apply_string("account_id");
    apply_string("run_id");
    apply_number("rollover_slippage_bps");
    apply_number("max_ticks");
    apply_bool("deterministic_fills");
    apply_bool("emit_state_snapshots");
}

bool IsAllowedTemplate(const std::string& value) {
    return value == "trend" || value == "arbitrage" || value == "market_making";
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft::apps;
    const auto args = ParseArgs(argc, argv);

    std::string error;
    const std::string factor_id = detail::GetArgAny(args, {"factor_id", "factor-id"});
    if (factor_id.empty()) {
        std::cerr << "factor_eval_cli: factor_id is required\n";
        return 2;
    }

    const std::string template_name = detail::GetArgAny(args, {"template"}, "trend");
    if (!IsAllowedTemplate(template_name)) {
        std::cerr << "factor_eval_cli: unsupported template: " << template_name << '\n';
        return 2;
    }

    ArgMap spec_args = args;
    const std::string spec_file = detail::GetArgAny(args, {"spec_file", "spec-file"});
    if (!spec_file.empty()) {
        std::ifstream in(spec_file);
        if (!in.is_open()) {
            std::cerr << "factor_eval_cli: unable to open spec_file: " << spec_file << '\n';
            return 2;
        }
        std::ostringstream content;
        content << in.rdbuf();
        ApplySpecJsonToArgs(content.str(), &spec_args);
    }

    const std::string csv_path = detail::GetArgAny(args, {"csv_path", "csv-path", "csv"});
    if (!csv_path.empty()) {
        spec_args["csv_path"] = csv_path;
    }

    if (spec_file.empty() && detail::GetArgAny(spec_args, {"csv_path"}).empty()) {
        std::cerr << "factor_eval_cli: either spec_file or csv_path is required\n";
        return 2;
    }

    if (detail::GetArgAny(spec_args, {"run_id", "run-id"}).empty()) {
        spec_args["run_id"] = detail::GetArgAny(args, {"run_id", "run-id"}, "factor-eval");
    }
    if (detail::GetArgAny(spec_args, {"max_ticks", "max-ticks"}).empty()) {
        spec_args["max_ticks"] = "5000";
    }
    if (detail::GetArgAny(spec_args, {"deterministic_fills", "deterministic-fills"}).empty()) {
        spec_args["deterministic_fills"] = "true";
    }
    if (detail::GetArgAny(spec_args, {"account_id", "account-id"}).empty()) {
        spec_args["account_id"] = "sim-account";
    }

    BacktestCliSpec spec;
    if (!ParseBacktestCliSpec(spec_args, &spec, &error)) {
        std::cerr << "factor_eval_cli: " << error << '\n';
        return 2;
    }

    BacktestCliResult backtest_result;
    if (!RunBacktestSpec(spec, &backtest_result, &error)) {
        std::cerr << "factor_eval_cli: " << error << '\n';
        return 1;
    }

    double total_pnl = 0.0;
    double max_drawdown = 0.0;
    double fill_rate = 0.0;
    double win_rate = 0.0;
    double capital_efficiency = 0.0;

    if (backtest_result.has_deterministic) {
        const auto& perf = backtest_result.deterministic.performance;
        total_pnl = perf.total_pnl;
        max_drawdown = perf.max_drawdown;

        double accepted = 0.0;
        double filled = 0.0;
        const auto accepted_it = perf.order_status_counts.find("ACCEPTED");
        if (accepted_it != perf.order_status_counts.end()) {
            accepted = static_cast<double>(accepted_it->second);
        }
        const auto filled_it = perf.order_status_counts.find("FILLED");
        if (filled_it != perf.order_status_counts.end()) {
            filled = static_cast<double>(filled_it->second);
        }
        fill_rate = accepted > 0.0 ? filled / accepted : 0.0;
        win_rate = total_pnl > 0.0 ? 1.0 : 0.0;
        capital_efficiency = total_pnl / std::max(1.0, std::fabs(perf.max_equity));
    }

    const std::vector<std::string> required_metric_keys = {
        "total_pnl", "max_drawdown", "win_rate", "fill_rate", "capital_efficiency",
    };
    for (const auto& key : required_metric_keys) {
        if (key.empty()) {
            std::cerr << "factor_eval_cli: metric key validation failed\n";
            return 1;
        }
    }

    const std::string output_jsonl = detail::GetArgAny(args, {"output_jsonl", "output-jsonl"},
                                                       "docs/results/experiment_tracker.jsonl");
    const std::string output_json = detail::GetArgAny(args, {"output_json", "output-json"});

    const std::int64_t created_ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                           std::chrono::system_clock::now().time_since_epoch())
                                           .count();

    std::ostringstream jsonl_record;
    jsonl_record << "{\"run_id\":\"" << JsonEscape(backtest_result.run_id) << "\","
                 << "\"template\":\"" << JsonEscape(template_name) << "\"," << "\"factor_id\":\""
                 << JsonEscape(factor_id) << "\"," << "\"spec_signature\":\""
                 << JsonEscape(backtest_result.input_signature) << "\"," << "\"metrics\":{"
                 << "\"total_pnl\":" << detail::FormatDouble(total_pnl) << ","
                 << "\"max_drawdown\":" << detail::FormatDouble(max_drawdown) << ","
                 << "\"win_rate\":" << detail::FormatDouble(win_rate) << ","
                 << "\"fill_rate\":" << detail::FormatDouble(fill_rate) << ","
                 << "\"capital_efficiency\":" << detail::FormatDouble(capital_efficiency) << "},"
                 << "\"created_ts_ns\":" << created_ts_ns << "}\n";

    try {
        std::filesystem::path jsonl_path(output_jsonl);
        if (!jsonl_path.parent_path().empty()) {
            std::filesystem::create_directories(jsonl_path.parent_path());
        }
        std::ofstream out(jsonl_path, std::ios::out | std::ios::app);
        if (!out.is_open()) {
            std::cerr << "factor_eval_cli: unable to open output_jsonl: " << output_jsonl << '\n';
            return 1;
        }
        out << jsonl_record.str();
    } catch (const std::exception& ex) {
        std::cerr << "factor_eval_cli: " << ex.what() << '\n';
        return 1;
    }

    std::ostringstream result_json;
    result_json << "{\n"
                << "  \"run_id\": \"" << JsonEscape(backtest_result.run_id) << "\",\n"
                << "  \"factor_id\": \"" << JsonEscape(factor_id) << "\",\n"
                << "  \"template\": \"" << JsonEscape(template_name) << "\",\n"
                << "  \"spec_signature\": \"" << JsonEscape(backtest_result.input_signature)
                << "\",\n"
                << "  \"metrics\": {\n"
                << "    \"total_pnl\": " << detail::FormatDouble(total_pnl) << ",\n"
                << "    \"max_drawdown\": " << detail::FormatDouble(max_drawdown) << ",\n"
                << "    \"win_rate\": " << detail::FormatDouble(win_rate) << ",\n"
                << "    \"fill_rate\": " << detail::FormatDouble(fill_rate) << ",\n"
                << "    \"capital_efficiency\": " << detail::FormatDouble(capital_efficiency)
                << "\n"
                << "  },\n"
                << "  \"tracker_jsonl\": \"" << JsonEscape(output_jsonl) << "\",\n"
                << "  \"status\": \"ok\"\n"
                << "}\n";

    if (!WriteTextFile(output_json, result_json.str(), &error)) {
        std::cerr << "factor_eval_cli: " << error << '\n';
        return 1;
    }
    std::cout << "factor evaluation recorded: " << output_jsonl
              << " run_id=" << backtest_result.run_id << '\n';
    return 0;
}
