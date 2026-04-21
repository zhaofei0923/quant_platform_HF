#include "quant_hft/rolling/oos_top10_validation.h"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/core/simple_json.h"
#include "quant_hft/optim/result_analyzer.h"

namespace quant_hft::rolling {
namespace {

using quant_hft::apps::BacktestCliResult;
using quant_hft::apps::BacktestCliSpec;
using quant_hft::optim::ParamValue;
using quant_hft::optim::ParamValueMap;
using quant_hft::optim::ResultAnalyzer;
using quant_hft::optim::TrialMetricsSnapshot;
using quant_hft::simple_json::Value;

struct RankedTrial {
    std::string trial_id;
    ParamValueMap params;
    TrialMetricsSnapshot metrics;
    double objective{0.0};
    std::size_t original_index{0};
};

struct CsvValidationCandidate {
    int rank{0};
    std::string trial_id;
    std::optional<double> oos_calmar;
    std::optional<double> oos_max_drawdown_pct;
    std::optional<double> oos_sharpe;
    std::string status;
};

std::string Trim(std::string text) {
    const auto not_space = [](unsigned char ch) { return std::isspace(ch) == 0; };
    text.erase(text.begin(),
               std::find_if(text.begin(), text.end(), [&](char ch) {
                   return not_space(static_cast<unsigned char>(ch));
               }));
    text.erase(
        std::find_if(text.rbegin(), text.rend(), [&](char ch) {
            return not_space(static_cast<unsigned char>(ch));
        }).base(),
        text.end());
    return text;
}

bool ReadTextFile(const std::filesystem::path& path, std::string* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "text output is null";
        }
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open file: " + path.string();
        }
        return false;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed to read file: " + path.string();
        }
        return false;
    }
    *out = buffer.str();
    return true;
}

std::string NormalizeTradingDayToken(std::string_view raw) {
    std::string digits;
    digits.reserve(raw.size());
    for (const unsigned char ch : raw) {
        if (std::isdigit(ch) != 0) {
            digits.push_back(static_cast<char>(ch));
        }
    }
    return digits.size() == 8 ? digits : "";
}

bool ParseBoolValue(const Value* value, bool fallback) {
    if (value == nullptr || !value->IsBool()) {
        return fallback;
    }
    return value->bool_value;
}

std::string ParseStringValue(const Value* value, const std::string& fallback = "") {
    if (value == nullptr || !value->IsString()) {
        return fallback;
    }
    return value->string_value;
}

double ParseNumberValue(const Value* value, double fallback = 0.0) {
    if (value == nullptr || !value->IsNumber()) {
        return fallback;
    }
    return value->number_value;
}

std::optional<double> ParseOptionalNumberValue(const Value* value) {
    if (value == nullptr || !value->IsNumber()) {
        return std::nullopt;
    }
    return value->number_value;
}

std::optional<int> ParseOptionalIntValue(const Value* value) {
    if (value == nullptr || !value->IsNumber()) {
        return std::nullopt;
    }
    const double number = value->number_value;
    if (!std::isfinite(number)) {
        return std::nullopt;
    }
    const double rounded = std::round(number);
    if (std::fabs(number - rounded) > 1e-9) {
        return std::nullopt;
    }
    if (rounded < static_cast<double>(std::numeric_limits<int>::min()) ||
        rounded > static_cast<double>(std::numeric_limits<int>::max())) {
        return std::nullopt;
    }
    return static_cast<int>(rounded);
}

std::optional<ParamValue> ParseParamValue(const Value& value) {
    if (value.IsString()) {
        return ParamValue(value.string_value);
    }
    if (!value.IsNumber()) {
        return std::nullopt;
    }
    const double number = value.number_value;
    if (!std::isfinite(number)) {
        return std::nullopt;
    }
    const double rounded = std::round(number);
    if (std::fabs(number - rounded) <= 1e-9 &&
        rounded >= static_cast<double>(std::numeric_limits<int>::min()) &&
        rounded <= static_cast<double>(std::numeric_limits<int>::max())) {
        return ParamValue(static_cast<int>(rounded));
    }
    return ParamValue(number);
}

std::string FormatDouble(double value) {
    std::ostringstream oss;
    oss << std::setprecision(10) << value;
    return oss.str();
}

std::string FormatOptionalDouble(const std::optional<double>& value) {
    return value.has_value() ? FormatDouble(*value) : "";
}

std::string FormatOptionalInt(const std::optional<int>& value) {
    return value.has_value() ? std::to_string(*value) : "";
}

std::string FormatFixedPrecision(double value, int precision) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(precision) << value;
    return oss.str();
}

std::optional<double> ParseOptionalDoubleText(const std::string& text) {
    const std::string trimmed = Trim(text);
    if (trimmed.empty()) {
        return std::nullopt;
    }
    try {
        std::size_t parsed = 0;
        const double value = std::stod(trimmed, &parsed);
        if (parsed != trimmed.size() || !std::isfinite(value)) {
            return std::nullopt;
        }
        return value;
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<int> ParseOptionalIntText(const std::string& text) {
    const std::string trimmed = Trim(text);
    if (trimmed.empty()) {
        return std::nullopt;
    }
    try {
        std::size_t parsed = 0;
        const int value = std::stoi(trimmed, &parsed);
        if (parsed != trimmed.size()) {
            return std::nullopt;
        }
        return value;
    } catch (...) {
        return std::nullopt;
    }
}

std::string CsvEscape(const std::string& text) {
    bool needs_quotes = false;
    std::string escaped;
    escaped.reserve(text.size() + 8);
    for (const char ch : text) {
        if (ch == '"') {
            escaped += "\"\"";
            needs_quotes = true;
            continue;
        }
        if (ch == ',' || ch == '\n' || ch == '\r') {
            needs_quotes = true;
        }
        escaped.push_back(ch);
    }
    if (!needs_quotes) {
        return escaped;
    }
    return '"' + escaped + '"';
}

double MetricSortValue(const std::optional<double>& value) {
    return value.has_value() ? *value : -std::numeric_limits<double>::infinity();
}

std::string SanitizeFileStem(std::string text) {
    for (char& ch : text) {
        const unsigned char uchar = static_cast<unsigned char>(ch);
        if (std::isalnum(uchar) == 0 && ch != '_' && ch != '-' && ch != '.') {
            ch = '_';
        }
    }
    text = Trim(std::move(text));
    return text.empty() ? "trial" : text;
}

std::string FormatRankPrefix(int rank) {
    std::ostringstream oss;
    oss << std::setw(2) << std::setfill('0') << rank;
    return oss.str();
}

std::filesystem::path ToAbsoluteNormalized(const std::filesystem::path& path) {
    if (path.empty()) {
        return {};
    }
    std::error_code ec;
    const std::filesystem::path absolute = std::filesystem::absolute(path, ec);
    if (ec) {
        return path.lexically_normal();
    }
    return absolute.lexically_normal();
}

bool EndsWithTrialId(const std::string& name, const std::string& trial_id) {
    if (name == trial_id) {
        return true;
    }
    if (name.size() <= trial_id.size() + 1) {
        return false;
    }
    const std::size_t offset = name.size() - trial_id.size();
    return name.compare(offset, trial_id.size(), trial_id) == 0 && name[offset - 1] == '_';
}

bool ParseCsvRecord(const std::string& line,
                    std::vector<std::string>* fields,
                    std::string* error) {
    if (fields == nullptr) {
        if (error != nullptr) {
            *error = "csv fields output is null";
        }
        return false;
    }

    fields->clear();
    std::string field;
    bool in_quotes = false;
    for (std::size_t index = 0; index < line.size(); ++index) {
        const char ch = line[index];
        if (in_quotes) {
            if (ch == '"') {
                if (index + 1 < line.size() && line[index + 1] == '"') {
                    field.push_back('"');
                    ++index;
                } else {
                    in_quotes = false;
                }
            } else {
                field.push_back(ch);
            }
            continue;
        }

        if (ch == ',') {
            fields->push_back(field);
            field.clear();
            continue;
        }
        if (ch == '"') {
            in_quotes = true;
            continue;
        }
        field.push_back(ch);
    }

    if (in_quotes) {
        if (error != nullptr) {
            *error = "unterminated quoted csv field";
        }
        return false;
    }

    fields->push_back(field);
    return true;
}

bool ParseCsvValidationCandidates(const std::string& csv_text,
                                  std::vector<CsvValidationCandidate>* candidates,
                                  std::string* error) {
    if (candidates == nullptr) {
        if (error != nullptr) {
            *error = "csv candidates output is null";
        }
        return false;
    }

    std::istringstream input(csv_text);
    std::string line;
    std::vector<std::string> fields;
    std::unordered_map<std::string, std::size_t> header_index;
    bool header_parsed = false;
    int line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }

        std::string parse_error;
        if (!ParseCsvRecord(line, &fields, &parse_error)) {
            if (error != nullptr) {
                *error = "failed parsing csv line " + std::to_string(line_number) + ": " +
                         parse_error;
            }
            return false;
        }

        if (!header_parsed) {
            for (std::size_t index = 0; index < fields.size(); ++index) {
                header_index.emplace(fields[index], index);
            }
            for (const std::string& required :
                 {std::string("Rank"), std::string("Trial ID"), std::string("OOS_Calmar"),
                  std::string("OOS_MaxDD"), std::string("OOS_Sharpe"),
                  std::string("OOS_Status")}) {
                if (header_index.find(required) == header_index.end()) {
                    if (error != nullptr) {
                        *error = "csv missing required column: " + required;
                    }
                    return false;
                }
            }
            header_parsed = true;
            continue;
        }

        const auto get_field = [&](const std::string& name) -> std::string {
            const auto it = header_index.find(name);
            if (it == header_index.end() || it->second >= fields.size()) {
                return "";
            }
            return fields[it->second];
        };

        CsvValidationCandidate candidate;
        candidate.rank = ParseOptionalIntText(get_field("Rank")).value_or(0);
        candidate.trial_id = get_field("Trial ID");
        candidate.oos_calmar = ParseOptionalDoubleText(get_field("OOS_Calmar"));
        candidate.oos_max_drawdown_pct = ParseOptionalDoubleText(get_field("OOS_MaxDD"));
        candidate.oos_sharpe = ParseOptionalDoubleText(get_field("OOS_Sharpe"));
        candidate.status = get_field("OOS_Status");
        if (!candidate.trial_id.empty()) {
            candidates->push_back(std::move(candidate));
        }
    }

    if (!header_parsed) {
        if (error != nullptr) {
            *error = "csv is empty";
        }
        return false;
    }

    return true;
}

bool IsSuccessfulValidationStatus(std::string_view status) {
    return status == "completed" || status == "cached";
}

bool IsBetterCsvCandidate(const CsvValidationCandidate& left,
                          const CsvValidationCandidate& right) {
    const double left_calmar = MetricSortValue(left.oos_calmar);
    const double right_calmar = MetricSortValue(right.oos_calmar);
    if (left_calmar != right_calmar) {
        return left_calmar > right_calmar;
    }

    const double left_sharpe = MetricSortValue(left.oos_sharpe);
    const double right_sharpe = MetricSortValue(right.oos_sharpe);
    if (left_sharpe != right_sharpe) {
        return left_sharpe > right_sharpe;
    }

    const double left_drawdown =
        left.oos_max_drawdown_pct.value_or(std::numeric_limits<double>::infinity());
    const double right_drawdown =
        right.oos_max_drawdown_pct.value_or(std::numeric_limits<double>::infinity());
    if (left_drawdown != right_drawdown) {
        return left_drawdown < right_drawdown;
    }

    return left.rank < right.rank;
}

bool SelectRecommendedCandidateFromCsv(const std::string& csv_path,
                                       CsvValidationCandidate* selected,
                                       std::string* error) {
    if (selected == nullptr) {
        if (error != nullptr) {
            *error = "selected csv candidate output is null";
        }
        return false;
    }

    std::string csv_text;
    if (!ReadTextFile(csv_path, &csv_text, error)) {
        return false;
    }

    std::vector<CsvValidationCandidate> candidates;
    if (!ParseCsvValidationCandidates(csv_text, &candidates, error)) {
        return false;
    }

    const CsvValidationCandidate* best = nullptr;
    for (const CsvValidationCandidate& candidate : candidates) {
        if (!IsSuccessfulValidationStatus(candidate.status) || !candidate.oos_calmar.has_value()) {
            continue;
        }
        if (best == nullptr || IsBetterCsvCandidate(candidate, *best)) {
            best = &candidate;
        }
    }

    if (best == nullptr) {
        if (error != nullptr) {
            *error = "no successful OOS candidate with Calmar ratio found in csv: " + csv_path;
        }
        return false;
    }

    *selected = *best;
    return true;
}

bool ParseTrainReport(const std::string& json_text,
                      std::vector<RankedTrial>* ranked_trials,
                      std::string* error) {
    if (ranked_trials == nullptr) {
        if (error != nullptr) {
            *error = "ranked trials output is null";
        }
        return false;
    }

    Value root;
    if (!quant_hft::simple_json::Parse(json_text, &root, error)) {
        return false;
    }
    const Value* trials_value = root.Find("trials");
    if (trials_value == nullptr || !trials_value->IsArray()) {
        if (error != nullptr) {
            *error = "train report missing trials array";
        }
        return false;
    }

    ranked_trials->clear();
    ranked_trials->reserve(trials_value->array_value.size());
    for (std::size_t index = 0; index < trials_value->array_value.size(); ++index) {
        const Value& trial_value = trials_value->array_value[index];
        if (!trial_value.IsObject()) {
            continue;
        }
        if (ParseStringValue(trial_value.Find("status")) != "completed") {
            continue;
        }

        RankedTrial trial;
        trial.trial_id = ParseStringValue(trial_value.Find("trial_id"));
        trial.objective = ParseNumberValue(trial_value.Find("objective"), 0.0);
        trial.original_index = index;
        if (trial.trial_id.empty()) {
            continue;
        }

        const Value* params_value = trial_value.Find("params");
        if (params_value != nullptr && params_value->IsObject()) {
            for (const auto& [key, param_value] : params_value->object_value) {
                const std::optional<ParamValue> parsed = ParseParamValue(param_value);
                if (parsed.has_value()) {
                    trial.params.values[key] = *parsed;
                }
            }
        }

        const Value* metrics_value = trial_value.Find("metrics");
        if (metrics_value != nullptr && metrics_value->IsObject()) {
            trial.metrics.total_pnl = ParseOptionalNumberValue(metrics_value->Find("total_pnl"));
            trial.metrics.max_drawdown =
                ParseOptionalNumberValue(metrics_value->Find("max_drawdown"));
            trial.metrics.max_drawdown_pct =
                ParseOptionalNumberValue(metrics_value->Find("max_drawdown_pct"));
            trial.metrics.annualized_return_pct =
                ParseOptionalNumberValue(metrics_value->Find("annualized_return_pct"));
            trial.metrics.sharpe_ratio =
                ParseOptionalNumberValue(metrics_value->Find("sharpe_ratio"));
            trial.metrics.calmar_ratio =
                ParseOptionalNumberValue(metrics_value->Find("calmar_ratio"));
            trial.metrics.profit_factor =
                ParseOptionalNumberValue(metrics_value->Find("profit_factor"));
            trial.metrics.win_rate_pct =
                ParseOptionalNumberValue(metrics_value->Find("win_rate_pct"));
            trial.metrics.total_trades = ParseOptionalIntValue(metrics_value->Find("total_trades"));
            trial.metrics.expectancy_r =
                ParseOptionalNumberValue(metrics_value->Find("expectancy_r"));
        }

        if (!trial.metrics.calmar_ratio.has_value()) {
            trial.metrics.calmar_ratio = trial.objective;
        }
        ranked_trials->push_back(std::move(trial));
    }

    std::stable_sort(ranked_trials->begin(), ranked_trials->end(),
                     [](const RankedTrial& left, const RankedTrial& right) {
                         const double left_calmar = MetricSortValue(left.metrics.calmar_ratio);
                         const double right_calmar = MetricSortValue(right.metrics.calmar_ratio);
                         if (left_calmar != right_calmar) {
                             return left_calmar > right_calmar;
                         }
                         const double left_sharpe = MetricSortValue(left.metrics.sharpe_ratio);
                         const double right_sharpe = MetricSortValue(right.metrics.sharpe_ratio);
                         if (left_sharpe != right_sharpe) {
                             return left_sharpe > right_sharpe;
                         }
                         if (left.objective != right.objective) {
                             return left.objective > right.objective;
                         }
                         return left.original_index < right.original_index;
                     });
    return true;
}

bool ParseDetectorConfig(const Value* detector_value, quant_hft::MarketStateDetectorConfig* out) {
    if (detector_value == nullptr || out == nullptr || !detector_value->IsObject()) {
        return false;
    }
    out->adx_period = static_cast<int>(ParseNumberValue(detector_value->Find("adx_period"), out->adx_period));
    out->adx_strong_threshold =
        ParseNumberValue(detector_value->Find("adx_strong_threshold"), out->adx_strong_threshold);
    out->adx_weak_lower =
        ParseNumberValue(detector_value->Find("adx_weak_lower"), out->adx_weak_lower);
    out->adx_weak_upper =
        ParseNumberValue(detector_value->Find("adx_weak_upper"), out->adx_weak_upper);
    out->kama_er_period =
        static_cast<int>(ParseNumberValue(detector_value->Find("kama_er_period"), out->kama_er_period));
    out->kama_fast_period = static_cast<int>(
        ParseNumberValue(detector_value->Find("kama_fast_period"), out->kama_fast_period));
    out->kama_slow_period = static_cast<int>(
        ParseNumberValue(detector_value->Find("kama_slow_period"), out->kama_slow_period));
    out->kama_er_strong =
        ParseNumberValue(detector_value->Find("kama_er_strong"), out->kama_er_strong);
    out->kama_er_weak_lower =
        ParseNumberValue(detector_value->Find("kama_er_weak_lower"), out->kama_er_weak_lower);
    out->atr_period =
        static_cast<int>(ParseNumberValue(detector_value->Find("atr_period"), out->atr_period));
    out->atr_flat_ratio =
        ParseNumberValue(detector_value->Find("atr_flat_ratio"), out->atr_flat_ratio);
    out->require_adx_for_trend =
        ParseBoolValue(detector_value->Find("require_adx_for_trend"), out->require_adx_for_trend);
    out->use_kama_er = ParseBoolValue(detector_value->Find("use_kama_er"), out->use_kama_er);
    out->min_bars_for_flat = static_cast<int>(
        ParseNumberValue(detector_value->Find("min_bars_for_flat"), out->min_bars_for_flat));
    return true;
}

bool LoadArchivedBacktestSpec(const std::filesystem::path& archived_result_json,
                              BacktestCliSpec* spec,
                              std::string* error) {
    if (spec == nullptr) {
        if (error != nullptr) {
            *error = "backtest spec output is null";
        }
        return false;
    }

    std::string json_text;
    if (!ReadTextFile(archived_result_json, &json_text, error)) {
        return false;
    }

    Value root;
    if (!quant_hft::simple_json::Parse(json_text, &root, error)) {
        return false;
    }
    const Value* spec_value = root.Find("spec");
    if (spec_value == nullptr || !spec_value->IsObject()) {
        if (error != nullptr) {
            *error = "archived result missing spec object: " + archived_result_json.string();
        }
        return false;
    }

    BacktestCliSpec parsed;
    parsed.csv_path = ParseStringValue(spec_value->Find("csv_path"));
    parsed.dataset_root = ParseStringValue(spec_value->Find("dataset_root"));
    parsed.dataset_manifest = ParseStringValue(spec_value->Find("dataset_manifest"));
    parsed.detector_config_path = ParseStringValue(spec_value->Find("detector_config"));
    parsed.engine_mode = ParseStringValue(spec_value->Find("engine_mode"), parsed.engine_mode);
    parsed.rollover_mode =
        ParseStringValue(spec_value->Find("rollover_mode"), parsed.rollover_mode);
    parsed.product_series_mode =
        ParseStringValue(spec_value->Find("product_series_mode"), parsed.product_series_mode);
    parsed.rollover_price_mode =
        ParseStringValue(spec_value->Find("rollover_price_mode"), parsed.rollover_price_mode);
    parsed.rollover_slippage_bps =
        ParseNumberValue(spec_value->Find("rollover_slippage_bps"), parsed.rollover_slippage_bps);
    parsed.start_date = ParseStringValue(spec_value->Find("start_date"));
    parsed.end_date = ParseStringValue(spec_value->Find("end_date"));

    const Value* max_ticks_value = spec_value->Find("max_ticks");
    if (max_ticks_value != nullptr && max_ticks_value->IsNumber()) {
        parsed.max_ticks = static_cast<std::int64_t>(std::llround(max_ticks_value->number_value));
    } else {
        parsed.max_ticks.reset();
    }

    parsed.symbols.clear();
    const Value* symbols_value = spec_value->Find("symbols");
    if (symbols_value != nullptr && symbols_value->IsArray()) {
        for (const Value& symbol_value : symbols_value->array_value) {
            if (symbol_value.IsString()) {
                parsed.symbols.push_back(symbol_value.string_value);
            }
        }
    }

    parsed.deterministic_fills =
        ParseBoolValue(spec_value->Find("deterministic_fills"), parsed.deterministic_fills);
    parsed.streaming = ParseBoolValue(spec_value->Find("streaming"), parsed.streaming);
    parsed.strict_parquet =
        ParseBoolValue(spec_value->Find("strict_parquet"), parsed.strict_parquet);
    parsed.wal_path = ParseStringValue(spec_value->Find("wal_path"));
    parsed.account_id = ParseStringValue(spec_value->Find("account_id"), parsed.account_id);
    parsed.run_id = ParseStringValue(spec_value->Find("run_id"));
    parsed.initial_equity =
        ParseNumberValue(spec_value->Find("initial_equity"), parsed.initial_equity);
    parsed.product_config_path = ParseStringValue(spec_value->Find("product_config_path"));
    parsed.contract_expiry_calendar_path =
        ParseStringValue(spec_value->Find("contract_expiry_calendar_path"));
    parsed.strategy_main_config_path =
        ParseStringValue(spec_value->Find("strategy_main_config_path"));
    parsed.strategy_factory =
        ParseStringValue(spec_value->Find("strategy_factory"), parsed.strategy_factory);
    parsed.strategy_composite_config =
        ParseStringValue(spec_value->Find("strategy_composite_config"));
    parsed.trace_output_format =
        ParseStringValue(spec_value->Find("trace_output_format"), parsed.trace_output_format);
    ParseDetectorConfig(spec_value->Find("market_state_detector"), &parsed.detector_config);
    parsed.emit_state_snapshots =
        ParseBoolValue(spec_value->Find("emit_state_snapshots"), parsed.emit_state_snapshots);
    parsed.emit_indicator_trace =
        ParseBoolValue(spec_value->Find("emit_indicator_trace"), parsed.emit_indicator_trace);
    parsed.indicator_trace_path = ParseStringValue(spec_value->Find("indicator_trace_path"));
    parsed.emit_sub_strategy_indicator_trace = ParseBoolValue(
        spec_value->Find("emit_sub_strategy_indicator_trace"),
        parsed.emit_sub_strategy_indicator_trace);
    parsed.sub_strategy_indicator_trace_path =
        ParseStringValue(spec_value->Find("sub_strategy_indicator_trace_path"));
    parsed.emit_trades = ParseBoolValue(spec_value->Find("emit_trades"), parsed.emit_trades);
    parsed.emit_orders = ParseBoolValue(spec_value->Find("emit_orders"), parsed.emit_orders);
    parsed.emit_position_history =
        ParseBoolValue(spec_value->Find("emit_position_history"), parsed.emit_position_history);

    if (parsed.dataset_root.empty() || parsed.dataset_manifest.empty() || parsed.symbols.empty()) {
        if (error != nullptr) {
            *error = "archived result spec is incomplete: " + archived_result_json.string();
        }
        return false;
    }

    *spec = std::move(parsed);
    return true;
}

bool RewriteCompositeConfig(const std::filesystem::path& archived_dir,
                           const std::filesystem::path& run_dir,
                           std::filesystem::path* composite_path,
                           std::string* error) {
    if (composite_path == nullptr) {
        if (error != nullptr) {
            *error = "composite path output is null";
        }
        return false;
    }

    const std::filesystem::path archived_composite = archived_dir / "composite.yaml";
    const std::filesystem::path archived_target = archived_dir / "target_sub_strategy.yaml";
    if (!std::filesystem::exists(archived_composite) || !std::filesystem::exists(archived_target)) {
        if (error != nullptr) {
            *error = "archived trial artifacts are incomplete: " + archived_dir.string();
        }
        return false;
    }

    std::string composite_text;
    if (!ReadTextFile(archived_composite, &composite_text, error)) {
        return false;
    }

    std::error_code ec;
    std::filesystem::create_directories(run_dir, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create run directory: " + run_dir.string() + ": " + ec.message();
        }
        return false;
    }

    const std::filesystem::path absolute_run_dir = ToAbsoluteNormalized(run_dir);
    const std::filesystem::path local_target = absolute_run_dir / "target_sub_strategy.yaml";
    std::filesystem::copy_file(archived_target, local_target,
                               std::filesystem::copy_options::overwrite_existing, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to copy archived target config: " + ec.message();
        }
        return false;
    }

    std::ostringstream rewritten;
    std::istringstream lines(composite_text);
    std::string line;
    bool replaced_target = false;
    while (std::getline(lines, line)) {
        const std::size_t config_pos = line.find("config_path:");
        if (!replaced_target && config_pos != std::string::npos &&
            line.find("target_sub_strategy.yaml") != std::string::npos) {
            rewritten << line.substr(0, config_pos + std::string("config_path:").size()) << ' '
                      << local_target.string() << '\n';
            replaced_target = true;
            continue;
        }
        rewritten << line << '\n';
    }

    if (!replaced_target) {
        if (error != nullptr) {
            *error = "failed to locate target_sub_strategy config_path in archived composite: " +
                     archived_composite.string();
        }
        return false;
    }

    const std::filesystem::path local_composite = absolute_run_dir / "composite.yaml";
    if (!quant_hft::apps::WriteTextFile(local_composite.string(), rewritten.str(), error)) {
        return false;
    }
    *composite_path = local_composite;
    return true;
}

std::filesystem::path ResolveRuntimeRoot(const std::filesystem::path& train_report_json,
                                         std::string* error) {
    const std::filesystem::path window_dir = train_report_json.parent_path();
    const std::filesystem::path train_reports_dir = window_dir.parent_path();
    if (window_dir.empty() || train_reports_dir.filename() != "train_reports") {
        if (error != nullptr) {
            *error = "train_report_json must follow runtime/.../train_reports/window_xxxx/report.json layout";
        }
        return {};
    }
    return train_reports_dir.parent_path();
}

std::filesystem::path ResolveTopTrialsDir(const std::filesystem::path& train_report_json,
                                          std::string* error) {
    const std::filesystem::path runtime_root = ResolveRuntimeRoot(train_report_json, error);
    if (runtime_root.empty()) {
        return {};
    }
    const std::filesystem::path window_dir = train_report_json.parent_path();
    return runtime_root / "top_trials" / window_dir.filename();
}

std::filesystem::path ResolveOutputDir(const OosTop10ValidationRequest& request,
                                       const std::filesystem::path& train_report_json,
                                       std::string* error) {
    if (!request.output_dir.empty()) {
        return request.output_dir;
    }
    const std::filesystem::path runtime_root = ResolveRuntimeRoot(train_report_json, error);
    if (runtime_root.empty()) {
        return {};
    }
    return runtime_root / "oos_validation" / train_report_json.parent_path().filename();
}

std::filesystem::path FindArchivedTrialDir(const std::filesystem::path& top_trials_dir,
                                           const std::string& trial_id) {
    std::error_code ec;
    if (!std::filesystem::exists(top_trials_dir, ec)) {
        return {};
    }
    for (const auto& entry : std::filesystem::directory_iterator(top_trials_dir, ec)) {
        if (ec) {
            return {};
        }
        if (!entry.is_directory()) {
            continue;
        }
        const std::string name = entry.path().filename().string();
        if (EndsWithTrialId(name, trial_id)) {
            return entry.path();
        }
    }
    return {};
}

std::string FormatParamsJson(const ParamValueMap& params) {
    std::set<std::string> keys;
    for (const auto& [key, value] : params.values) {
        (void)value;
        keys.insert(key);
    }
    std::ostringstream oss;
    oss << '{';
    bool first = true;
    for (const std::string& key : keys) {
        if (!first) {
            oss << ',';
        }
        first = false;
        oss << '"' << quant_hft::apps::JsonEscape(key) << '"' << ':';
        const ParamValue& value = params.values.at(key);
        std::visit(
            [&](const auto& actual) {
                using T = std::decay_t<decltype(actual)>;
                if constexpr (std::is_same_v<T, std::string>) {
                    oss << '"' << quant_hft::apps::JsonEscape(actual) << '"';
                } else if constexpr (std::is_same_v<T, double>) {
                    oss << FormatDouble(actual);
                } else {
                    oss << actual;
                }
            },
            value);
    }
    oss << '}';
    return oss.str();
}

const OosTop10ValidationRow* FindRowByTrialId(const OosTop10ValidationReport& report,
                                              const std::string& trial_id) {
    for (const OosTop10ValidationRow& row : report.rows) {
        if (row.trial_id == trial_id) {
            return &row;
        }
    }
    return nullptr;
}

bool WriteFinalRecommendedParamsYaml(const OosTop10ValidationRow& row,
                                     const std::string& output_path,
                                     std::string* error) {
    if (!row.oos_calmar.has_value() || !row.oos_sharpe.has_value() ||
        !row.oos_max_drawdown_pct.has_value()) {
        if (error != nullptr) {
            *error = "selected OOS row is missing metrics required for recommendation comment";
        }
        return false;
    }

    if (!ResultAnalyzer::WriteBestParamsYaml(row.params, output_path, error)) {
        return false;
    }

    std::string yaml_body;
    if (!ReadTextFile(output_path, &yaml_body, error)) {
        return false;
    }

    std::ostringstream yaml;
    yaml << "# Selected based on highest Out-of-Sample Calmar Ratio among Top10 in-sample parameters.\n";
    yaml << "# OOS Calmar: " << FormatFixedPrecision(*row.oos_calmar, 2)
         << ", OOS Sharpe: " << FormatFixedPrecision(*row.oos_sharpe, 2)
         << ", OOS MaxDD: " << FormatFixedPrecision(*row.oos_max_drawdown_pct, 2) << "%\n";
    yaml << yaml_body;
    return quant_hft::apps::WriteTextFile(output_path, yaml.str(), error);
}

void PopulateOosMetrics(const TrialMetricsSnapshot& metrics, OosTop10ValidationRow* row) {
    if (row == nullptr) {
        return;
    }
    row->oos_calmar = metrics.calmar_ratio;
    row->oos_max_drawdown_pct = metrics.max_drawdown_pct;
    row->oos_sharpe = metrics.sharpe_ratio;
    row->oos_profit_factor = metrics.profit_factor;
    row->oos_total_pnl = metrics.total_pnl;
    row->oos_win_rate_pct = metrics.win_rate_pct;
    row->oos_trades = metrics.total_trades;
}

std::string RenderCsv(const OosTop10ValidationReport& report) {
    std::ostringstream csv;
    csv << "Rank,Trial ID,Params,InSample_Calmar,InSample_MaxDD,InSample_Sharpe,"
           "OOS_Calmar,OOS_MaxDD,OOS_Sharpe,OOS_ProfitFactor,OOS_TotalPnL,OOS_WinRate,"
           "OOS_Trades,OOS_Status,OOS_Error\n";
    for (const OosTop10ValidationRow& row : report.rows) {
        csv << row.rank << ',' << CsvEscape(row.trial_id) << ','
            << CsvEscape(FormatParamsJson(row.params)) << ','
            << FormatOptionalDouble(row.in_sample_calmar) << ','
            << FormatOptionalDouble(row.in_sample_max_drawdown_pct) << ','
            << FormatOptionalDouble(row.in_sample_sharpe) << ','
            << FormatOptionalDouble(row.oos_calmar) << ','
            << FormatOptionalDouble(row.oos_max_drawdown_pct) << ','
            << FormatOptionalDouble(row.oos_sharpe) << ','
            << FormatOptionalDouble(row.oos_profit_factor) << ','
            << FormatOptionalDouble(row.oos_total_pnl) << ','
            << FormatOptionalDouble(row.oos_win_rate_pct) << ','
            << FormatOptionalInt(row.oos_trades) << ',' << CsvEscape(row.status) << ','
            << CsvEscape(row.error_msg) << '\n';
    }
    return csv.str();
}

BacktestCliResult BuildResultForWrite(const BacktestCliResult& source, const BacktestCliSpec& spec) {
    BacktestCliResult result = source;
    result.spec = spec;
    if (result.run_id.empty()) {
        result.run_id = spec.run_id;
    }
    if (result.engine_mode.empty()) {
        result.engine_mode = spec.engine_mode;
    }
    if (result.rollover_mode.empty()) {
        result.rollover_mode = spec.rollover_mode;
    }
    if (result.initial_equity == 0.0) {
        result.initial_equity = spec.initial_equity;
    }
    return result;
}

}  // namespace

bool RunOosTop10Validation(const OosTop10ValidationRequest& request,
                           OosTop10ValidationReport* report,
                           std::string* error,
                           OosBacktestRunFn run_fn) {
    if (report == nullptr) {
        if (error != nullptr) {
            *error = "validation report output is null";
        }
        return false;
    }

    const std::string normalized_start = NormalizeTradingDayToken(request.oos_start_date);
    const std::string normalized_end = NormalizeTradingDayToken(request.oos_end_date);
    if (request.train_report_json.empty() || normalized_start.empty() || normalized_end.empty()) {
        if (error != nullptr) {
            *error = "train_report_json, oos_start_date, and oos_end_date are required";
        }
        return false;
    }
    if (request.top_n <= 0) {
        if (error != nullptr) {
            *error = "top_n must be positive";
        }
        return false;
    }

    const std::filesystem::path train_report_json =
        ToAbsoluteNormalized(std::filesystem::path(request.train_report_json));
    const std::filesystem::path top_trials_dir = ResolveTopTrialsDir(train_report_json, error);
    if (top_trials_dir.empty()) {
        return false;
    }
    const std::filesystem::path output_dir =
        ToAbsoluteNormalized(ResolveOutputDir(request, train_report_json, error));
    if (output_dir.empty()) {
        return false;
    }

    std::string train_report_text;
    if (!ReadTextFile(train_report_json, &train_report_text, error)) {
        return false;
    }

    std::vector<RankedTrial> ranked_trials;
    if (!ParseTrainReport(train_report_text, &ranked_trials, error)) {
        return false;
    }
    if (ranked_trials.empty()) {
        if (error != nullptr) {
            *error = "no completed trials found in train report: " + train_report_json.string();
        }
        return false;
    }

    std::error_code ec;
    std::filesystem::create_directories(output_dir, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create output directory: " + output_dir.string() + ": " +
                     ec.message();
        }
        return false;
    }

    OosBacktestRunFn effective_run_fn = std::move(run_fn);
    if (!effective_run_fn) {
        effective_run_fn = [](const BacktestCliSpec& spec, BacktestCliResult* out,
                              std::string* run_error) {
            return quant_hft::apps::RunBacktestSpec(spec, out, run_error);
        };
    }

    report->train_report_json = train_report_json.string();
    report->top_trials_dir = top_trials_dir.string();
    report->output_dir = output_dir.string();
    report->output_csv = (output_dir / "oos_top10_validation.csv").string();
    report->final_recommended_params_yaml.clear();
    report->recommended_trial_id.clear();
    report->requested_top_n = request.top_n;
    report->selected_count = std::min<int>(request.top_n, static_cast<int>(ranked_trials.size()));
    report->success_count = 0;
    report->failed_count = 0;
    report->rows.clear();
    report->rows.reserve(static_cast<std::size_t>(report->selected_count));

    for (int index = 0; index < report->selected_count; ++index) {
        const RankedTrial& trial = ranked_trials[static_cast<std::size_t>(index)];
        OosTop10ValidationRow row;
        row.rank = index + 1;
        row.trial_id = trial.trial_id;
        row.params = trial.params;
        row.in_sample_calmar = trial.metrics.calmar_ratio;
        row.in_sample_max_drawdown_pct = trial.metrics.max_drawdown_pct;
        row.in_sample_sharpe = trial.metrics.sharpe_ratio;
        row.status = "pending";

        const std::filesystem::path archived_dir = FindArchivedTrialDir(top_trials_dir, trial.trial_id);
        if (archived_dir.empty()) {
            row.status = "failed";
            row.error_msg = "missing archived trial directory for " + trial.trial_id;
            ++report->failed_count;
            report->rows.push_back(std::move(row));
            continue;
        }

        const std::filesystem::path row_dir = ToAbsoluteNormalized(
            output_dir / (FormatRankPrefix(row.rank) + "_" + SanitizeFileStem(trial.trial_id)));
        const std::filesystem::path result_json_path = row_dir / "result.json";
        row.result_json_path = result_json_path.string();

        TrialMetricsSnapshot extracted_metrics;
        std::string metrics_error;
        if (!request.overwrite && std::filesystem::exists(result_json_path) &&
            ResultAnalyzer::ExtractTrialMetricsFromJson(result_json_path.string(), &extracted_metrics,
                                                        &metrics_error)) {
            PopulateOosMetrics(extracted_metrics, &row);
            row.success = true;
            row.reused_existing = true;
            row.status = "cached";
            ++report->success_count;
            report->rows.push_back(std::move(row));
            continue;
        }

        std::filesystem::path composite_path;
        std::string row_error;
        if (!RewriteCompositeConfig(archived_dir, row_dir, &composite_path, &row_error)) {
            row.status = "failed";
            row.error_msg = row_error;
            ++report->failed_count;
            report->rows.push_back(std::move(row));
            continue;
        }

        BacktestCliSpec spec;
        if (!LoadArchivedBacktestSpec(archived_dir / "result.json", &spec, &row_error)) {
            row.status = "failed";
            row.error_msg = row_error;
            ++report->failed_count;
            report->rows.push_back(std::move(row));
            continue;
        }

        spec.start_date = normalized_start;
        spec.end_date = normalized_end;
        spec.strategy_composite_config = composite_path.string();
        spec.run_id = "oos-top10-" + train_report_json.parent_path().filename().string() + "-" +
                      SanitizeFileStem(trial.trial_id);
        if (spec.emit_indicator_trace && !spec.indicator_trace_path.empty()) {
            spec.indicator_trace_path = (row_dir / "indicator_trace.csv").string();
        }
        if (spec.emit_sub_strategy_indicator_trace && !spec.sub_strategy_indicator_trace_path.empty()) {
            spec.sub_strategy_indicator_trace_path =
                (row_dir / "sub_strategy_indicator_trace.csv").string();
        }

        BacktestCliResult run_result;
        if (!effective_run_fn(spec, &run_result, &row_error)) {
            row.status = "failed";
            row.error_msg = row_error;
            (void)quant_hft::apps::WriteTextFile((row_dir / "error.txt").string(), row_error,
                                                 nullptr);
            ++report->failed_count;
            report->rows.push_back(std::move(row));
            continue;
        }

        const BacktestCliResult persisted_result = BuildResultForWrite(run_result, spec);
        const std::string result_json_text = quant_hft::apps::RenderBacktestJson(persisted_result);
        if (!quant_hft::apps::WriteTextFile(result_json_path.string(), result_json_text, &row_error)) {
            row.status = "failed";
            row.error_msg = row_error;
            ++report->failed_count;
            report->rows.push_back(std::move(row));
            continue;
        }

        if (!ResultAnalyzer::ExtractTrialMetricsFromJson(result_json_path.string(), &extracted_metrics,
                                                         &row_error)) {
            row.status = "failed";
            row.error_msg = row_error;
            ++report->failed_count;
            report->rows.push_back(std::move(row));
            continue;
        }

        PopulateOosMetrics(extracted_metrics, &row);
        row.success = true;
        row.status = "completed";
        ++report->success_count;
        report->rows.push_back(std::move(row));
    }

    const std::string csv_text = RenderCsv(*report);
    if (!quant_hft::apps::WriteTextFile(report->output_csv, csv_text, error)) {
        return false;
    }

    CsvValidationCandidate recommended_candidate;
    if (!SelectRecommendedCandidateFromCsv(report->output_csv, &recommended_candidate, error)) {
        return false;
    }

    const OosTop10ValidationRow* recommended_row =
        FindRowByTrialId(*report, recommended_candidate.trial_id);
    if (recommended_row == nullptr) {
        if (error != nullptr) {
            *error = "recommended trial from csv not found in report rows: " +
                     recommended_candidate.trial_id;
        }
        return false;
    }

    const std::filesystem::path recommended_yaml = output_dir / "final_recommended_params.yaml";
    if (!WriteFinalRecommendedParamsYaml(*recommended_row, recommended_yaml.string(), error)) {
        return false;
    }

    report->final_recommended_params_yaml = recommended_yaml.string();
    report->recommended_trial_id = recommended_row->trial_id;
    return true;
}

}  // namespace quant_hft::rolling