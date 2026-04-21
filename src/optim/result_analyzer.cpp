#include "quant_hft/optim/result_analyzer.h"

#include <algorithm>
#include <cmath>
#include <cctype>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/core/simple_json.h"
#include "quant_hft/optim/parameter_space.h"

namespace quant_hft::optim {
namespace {

using quant_hft::apps::JsonEscape;
using quant_hft::apps::WriteTextFile;
using quant_hft::simple_json::Value;

struct OpenLotRecord {
    std::string trade_id;
    std::string symbol;
    std::string side;
    int volume{0};
    int remaining_volume{0};
    double commission{0.0};
    double risk_budget_r{0.0};
};

struct CompletedRoundTrip {
    int entry_volume{0};
    int matched_volume{0};
    double risk_budget_r{0.0};
    double allocated_entry_commission{0.0};
    double allocated_exit_commission{0.0};
    double gross_realized_pnl{0.0};
};

struct JsonTradeRecord {
    int fill_seq{0};
    std::string trade_id;
    std::string symbol;
    std::string side;
    std::string offset;
    int volume{0};
    double commission{0.0};
    double realized_pnl{0.0};
    double risk_budget_r{0.0};
    std::string signal_type;
};

struct RoundTripStats {
    int total_trades{0};
    double win_rate_pct{0.0};
    double expectancy_r{0.0};
    double profit_factor{0.0};
};

bool TryExtractOptionalMetric(const Value& root,
                              const std::string& metric_path,
                              std::optional<double>* out,
                              std::vector<std::string>* warnings);

bool ExtractDailyDerivedMetrics(const Value& root,
                                TrialMetricsSnapshot* metrics,
                                std::vector<std::string>* warnings);

bool ExtractTradeDerivedMetrics(const Value& root,
                                TrialMetricsSnapshot* metrics,
                                std::vector<std::string>* warnings);

std::string FormatDouble(double value) {
    std::ostringstream oss;
    oss << std::setprecision(12) << value;
    return oss.str();
}

std::string FormatFixed(double value, int precision) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(precision) << value;
    return oss.str();
}

std::string ParamValueToString(const ParamValue& value) {
    if (std::holds_alternative<int>(value)) {
        return std::to_string(std::get<int>(value));
    }
    if (std::holds_alternative<double>(value)) {
        return FormatDouble(std::get<double>(value));
    }
    return std::get<std::string>(value);
}

std::string ParamValueToJson(const ParamValue& value) {
    if (std::holds_alternative<int>(value)) {
        return std::to_string(std::get<int>(value));
    }
    if (std::holds_alternative<double>(value)) {
        return FormatDouble(std::get<double>(value));
    }
    return "\"" + JsonEscape(std::get<std::string>(value)) + "\"";
}

bool IsNumericParameter(const ParameterDef& param) {
    return param.type == ParameterType::kInt || param.type == ParameterType::kDouble;
}

std::vector<ParamValue> BuildGridValuesForParam(const ParameterDef& param) {
    if (!param.values.empty()) {
        return param.values;
    }

    std::vector<ParamValue> values;
    if (!param.min.has_value() || !param.max.has_value()) {
        return values;
    }

    const double step = param.step.value_or(1.0);
    if (!(step > 0.0)) {
        return values;
    }

    if (param.type == ParameterType::kInt) {
        const int min_value = std::get<int>(param.min.value());
        const int max_value = std::get<int>(param.max.value());
        const int step_int = static_cast<int>(std::round(step));
        if (step_int <= 0) {
            return values;
        }
        for (int value = min_value; value <= max_value; value += step_int) {
            values.emplace_back(value);
            if (value > max_value - step_int) {
                break;
            }
        }
        if (values.empty() || std::get<int>(values.back()) != max_value) {
            values.emplace_back(max_value);
        }
        return values;
    }

    if (param.type == ParameterType::kDouble) {
        const double min_value = std::get<double>(param.min.value());
        const double max_value = std::get<double>(param.max.value());
        constexpr double kEps = 1e-9;
        for (double value = min_value; value <= max_value + kEps; value += step) {
            const double clamped = (value > max_value && value < max_value + kEps) ? max_value : value;
            values.emplace_back(clamped);
            if (value > max_value - step) {
                break;
            }
        }
        if (values.empty()) {
            values.emplace_back(max_value);
        } else {
            const double last_value = std::get<double>(values.back());
            if (std::fabs(last_value - max_value) > kEps) {
                values.emplace_back(max_value);
            }
        }
    }

    return values;
}

struct HeatmapPair {
    const ParameterDef* x_param{nullptr};
    const ParameterDef* y_param{nullptr};
};

std::vector<HeatmapPair> BuildHeatmapPairs(const ParameterSpace& space) {
    std::vector<const ParameterDef*> numeric_params;
    numeric_params.reserve(space.parameters.size());
    for (const ParameterDef& param : space.parameters) {
        if (!IsNumericParameter(param)) {
            continue;
        }
        if (BuildGridValuesForParam(param).empty()) {
            continue;
        }
        numeric_params.push_back(&param);
    }

    std::vector<HeatmapPair> pairs;
    if (numeric_params.size() < 2U) {
        return pairs;
    }

    const auto find_named_param = [&](const std::string& name) {
        return std::find_if(numeric_params.begin(), numeric_params.end(), [&name](const ParameterDef* param) {
            return param != nullptr && param->name == name;
        });
    };

    const auto kama_it = find_named_param("kama_filter");
    const auto stop_loss_it = find_named_param("stop_loss_atr_multiplier");
    if (kama_it != numeric_params.end() && stop_loss_it != numeric_params.end()) {
        pairs.push_back({*kama_it, *stop_loss_it});
    }

    for (std::size_t i = 0; i < numeric_params.size(); ++i) {
        for (std::size_t j = i + 1; j < numeric_params.size(); ++j) {
            const ParameterDef* x_param = numeric_params[i];
            const ParameterDef* y_param = numeric_params[j];
            if (!pairs.empty() && pairs.front().x_param == x_param && pairs.front().y_param == y_param) {
                continue;
            }
            pairs.push_back({x_param, y_param});
        }
    }

    return pairs;
}

std::filesystem::path ResolveOutputDirectory(const std::string& output_dir) {
    if (output_dir.empty()) {
        return std::filesystem::current_path();
    }
    return std::filesystem::path(output_dir);
}

bool EnsureDirectoryExists(const std::filesystem::path& path, std::string* error) {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create heatmap output dir: " + ec.message();
        }
        return false;
    }
    return true;
}

std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::string current;
    for (char ch : path) {
        if (ch == '.') {
            if (!current.empty()) {
                parts.push_back(current);
                current.clear();
            }
            continue;
        }
        current.push_back(ch);
    }
    if (!current.empty()) {
        parts.push_back(current);
    }
    return parts;
}

bool ParseDoubleStrict(const std::string& text, double* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const double value = std::stod(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = value;
        return true;
    } catch (...) {
        return false;
    }
}

std::string ToLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::string Trim(const std::string& value) {
    std::size_t begin = 0;
    while (begin < value.size() && std::isspace(static_cast<unsigned char>(value[begin])) != 0) {
        ++begin;
    }
    std::size_t end = value.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
        --end;
    }
    return value.substr(begin, end - begin);
}

double Mean(const std::vector<double>& values) {
    if (values.empty()) {
        return 0.0;
    }
    double total = 0.0;
    for (double value : values) {
        total += value;
    }
    return total / static_cast<double>(values.size());
}

double SampleStdDev(const std::vector<double>& values) {
    if (values.size() < 2) {
        return 0.0;
    }
    const double mean = Mean(values);
    double variance = 0.0;
    for (double value : values) {
        const double diff = value - mean;
        variance += diff * diff;
    }
    variance /= static_cast<double>(values.size() - 1);
    return std::sqrt(variance);
}

double ExpectancyFromRMultiples(const std::vector<double>& values) {
    if (values.empty()) {
        return 0.0;
    }
    std::vector<double> wins;
    std::vector<double> losses;
    wins.reserve(values.size());
    losses.reserve(values.size());
    for (double value : values) {
        if (value > 0.0) {
            wins.push_back(value);
        } else if (value < 0.0) {
            losses.push_back(std::fabs(value));
        }
    }
    const double win_rate = static_cast<double>(wins.size()) / static_cast<double>(values.size());
    const double lose_rate =
        static_cast<double>(losses.size()) / static_cast<double>(values.size());
    return win_rate * Mean(wins) - lose_rate * Mean(losses);
}

std::string JoinMessages(const std::vector<std::string>& messages) {
    std::ostringstream oss;
    for (std::size_t i = 0; i < messages.size(); ++i) {
        if (i > 0) {
            oss << "; ";
        }
        oss << messages[i];
    }
    return oss.str();
}

std::string OptionalDoubleToJson(const std::optional<double>& value) {
    return value.has_value() ? FormatDouble(*value) : "null";
}

std::string OptionalIntToJson(const std::optional<int>& value) {
    return value.has_value() ? std::to_string(*value) : "null";
}

std::string ConstraintOperatorToString(const ConstraintOperator op) {
    switch (op) {
        case ConstraintOperator::kLess:
            return "<";
        case ConstraintOperator::kLessEqual:
            return "<=";
        case ConstraintOperator::kGreater:
            return ">";
        case ConstraintOperator::kGreaterEqual:
            return ">=";
        case ConstraintOperator::kEqual:
            return "==";
        case ConstraintOperator::kNotEqual:
            return "!=";
    }
    return "?";
}

bool CompareConstraintValue(const double actual,
                           const ConstraintOperator op,
                           const double threshold) {
    constexpr double kEps = 1e-9;
    switch (op) {
        case ConstraintOperator::kLess:
            return actual < threshold;
        case ConstraintOperator::kLessEqual:
            return actual <= threshold;
        case ConstraintOperator::kGreater:
            return actual > threshold;
        case ConstraintOperator::kGreaterEqual:
            return actual >= threshold;
        case ConstraintOperator::kEqual:
            return std::fabs(actual - threshold) <= kEps;
        case ConstraintOperator::kNotEqual:
            return std::fabs(actual - threshold) > kEps;
    }
    return false;
}

bool ResolveConstraintMetricSpec(const std::string& metric_token,
                                 std::string* metric_name,
                                 std::string* metric_path) {
    if (metric_name == nullptr || metric_path == nullptr) {
        return false;
    }

    const std::string normalized = ToLower(Trim(metric_token));
    const std::string resolved = ResultAnalyzer::ResolveMetricPathAlias(normalized);

    if (resolved == "hf_standard.risk_metrics.max_drawdown_pct" ||
        normalized == "summary.max_drawdown") {
        *metric_name = "max_drawdown_pct";
        *metric_path = "hf_standard.risk_metrics.max_drawdown_pct";
        return true;
    }
    if (resolved == "hf_standard.trade_statistics.total_trades") {
        *metric_name = "total_trades";
        *metric_path = "hf_standard.trade_statistics.total_trades";
        return true;
    }
    if (resolved == "hf_standard.advanced_summary.profit_factor") {
        *metric_name = "profit_factor";
        *metric_path = "hf_standard.advanced_summary.profit_factor";
        return true;
    }
    if (resolved == "hf_standard.trade_statistics.expectancy_r") {
        *metric_name = "expectancy_r";
        *metric_path = "hf_standard.trade_statistics.expectancy_r";
        return true;
    }
    if (resolved == "hf_standard.risk_metrics.calmar_ratio") {
        *metric_name = "calmar_ratio";
        *metric_path = "hf_standard.risk_metrics.calmar_ratio";
        return true;
    }
    if (resolved == "hf_standard.risk_metrics.sharpe_ratio") {
        *metric_name = "sharpe_ratio";
        *metric_path = "hf_standard.risk_metrics.sharpe_ratio";
        return true;
    }

    return false;
}

std::string FormatOptionalMetric(const std::optional<double>& value, int precision) {
    return value.has_value() ? FormatFixed(*value, precision) : "-";
}

std::string FormatOptionalInteger(const std::optional<int>& value) {
    return value.has_value() ? std::to_string(*value) : "-";
}

std::string EscapeMarkdownCell(std::string value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (char ch : value) {
        if (ch == '|') {
            escaped += "\\|";
        } else if (ch == '\n' || ch == '\r') {
            escaped.push_back(' ');
        } else {
            escaped.push_back(ch);
        }
    }
    return escaped;
}

std::vector<std::pair<std::string, ParamValue>> SortedParams(const ParamValueMap& params) {
    std::vector<std::pair<std::string, ParamValue>> sorted;
    sorted.reserve(params.values.size());
    for (const auto& [key, value] : params.values) {
        sorted.emplace_back(key, value);
    }
    std::sort(sorted.begin(), sorted.end(), [](const auto& left, const auto& right) {
        return left.first < right.first;
    });
    return sorted;
}

std::string FlattenParams(const ParamValueMap& params) {
    std::ostringstream oss;
    const auto sorted = SortedParams(params);
    for (std::size_t i = 0; i < sorted.size(); ++i) {
        if (i > 0) {
            oss << ", ";
        }
        oss << sorted[i].first << '=' << ParamValueToString(sorted[i].second);
    }
    return oss.str();
}

const Value* ResolvePath(const Value& root, const std::string& metric_path, std::string* error) {
    const std::string resolved_path = ResultAnalyzer::ResolveMetricPathAlias(metric_path);
    const std::vector<std::string> segments = SplitPath(resolved_path);
    if (segments.empty()) {
        if (error != nullptr) {
            *error = "metric path is empty";
        }
        return nullptr;
    }

    const Value* current = &root;
    for (const std::string& segment : segments) {
        if (!current->IsObject()) {
            if (error != nullptr) {
                *error = "metric path segment `" + segment + "` is not an object parent";
            }
            return nullptr;
        }
        current = current->Find(segment);
        if (current == nullptr) {
            if (error != nullptr) {
                *error = "metric path segment not found: " + segment;
            }
            return nullptr;
        }
    }

    return current;
}

bool TryReadNumber(const Value& value, double* out) {
    if (out == nullptr) {
        return false;
    }
    if (value.IsNumber()) {
        *out = value.number_value;
        return true;
    }
    if (value.IsString()) {
        return ParseDoubleStrict(value.string_value, out);
    }
    return false;
}

bool TryReadInt(const Value& value, int* out) {
    if (out == nullptr) {
        return false;
    }
    double number = 0.0;
    if (!TryReadNumber(value, &number)) {
        return false;
    }
    *out = static_cast<int>(std::llround(number));
    return true;
}

bool TryExtractMetricFromSnapshot(const TrialMetricsSnapshot& metrics,
                                  const std::string& resolved_metric_path,
                                  double* out) {
    if (out == nullptr) {
        return false;
    }

    if (resolved_metric_path == "hf_standard.risk_metrics.max_drawdown_pct" &&
        metrics.max_drawdown_pct.has_value()) {
        *out = *metrics.max_drawdown_pct;
        return true;
    }
    if (resolved_metric_path == "hf_standard.trade_statistics.total_trades" &&
        metrics.total_trades.has_value()) {
        *out = static_cast<double>(*metrics.total_trades);
        return true;
    }
    if (resolved_metric_path == "hf_standard.advanced_summary.profit_factor" &&
        metrics.profit_factor.has_value()) {
        *out = *metrics.profit_factor;
        return true;
    }
    if (resolved_metric_path == "hf_standard.trade_statistics.expectancy_r" &&
        metrics.expectancy_r.has_value()) {
        *out = *metrics.expectancy_r;
        return true;
    }
    if (resolved_metric_path == "hf_standard.risk_metrics.calmar_ratio" &&
        metrics.calmar_ratio.has_value()) {
        *out = *metrics.calmar_ratio;
        return true;
    }
    if (resolved_metric_path == "hf_standard.risk_metrics.sharpe_ratio" &&
        metrics.sharpe_ratio.has_value()) {
        *out = *metrics.sharpe_ratio;
        return true;
    }
    return false;
}

bool ExtractTrialMetricsFromValueTree(const Value& root,
                                     TrialMetricsSnapshot* out_metrics,
                                     std::string* error) {
    if (out_metrics == nullptr) {
        if (error != nullptr) {
            *error = "trial metrics output is null";
        }
        return false;
    }

    TrialMetricsSnapshot metrics;
    std::vector<std::string> warnings;
    (void)TryExtractOptionalMetric(root, "summary.total_pnl", &metrics.total_pnl, &warnings);
    (void)TryExtractOptionalMetric(root, "summary.max_drawdown", &metrics.max_drawdown, &warnings);
    (void)TryExtractOptionalMetric(root, "hf_standard.advanced_summary.profit_factor",
                                   &metrics.profit_factor, &warnings);
    (void)ExtractDailyDerivedMetrics(root, &metrics, &warnings);
    (void)ExtractTradeDerivedMetrics(root, &metrics, &warnings);

    *out_metrics = std::move(metrics);
    if (error != nullptr) {
        *error = JoinMessages(warnings);
    }
    return true;
}

bool TryExtractDerivedMetric(const Value& root,
                             const std::string& metric_path,
                             double* out,
                             std::string* error) {
    TrialMetricsSnapshot metrics;
    std::string metrics_error;
    if (!ExtractTrialMetricsFromValueTree(root, &metrics, &metrics_error)) {
        if (error != nullptr) {
            *error = metrics_error;
        }
        return false;
    }

    if (TryExtractMetricFromSnapshot(metrics, metric_path, out)) {
        return true;
    }

    if (metric_path == "hf_standard.risk_metrics.max_drawdown_pct") {
        const Value* fallback = ResolvePath(root, "summary.max_drawdown", nullptr);
        if (fallback != nullptr && TryReadNumber(*fallback, out)) {
            return true;
        }
    }

    if (error != nullptr) {
        *error = "metric path unavailable: " + metric_path;
        if (!metrics_error.empty()) {
            *error += " (" + metrics_error + ")";
        }
    }
    return false;
}

double ExtractMetricFromValueTree(const Value& root,
                                  const std::string& metric_path,
                                  std::string* error) {
    std::string path_error;
    const Value* current = ResolvePath(root, metric_path, &path_error);
    if (current != nullptr) {
        double value = 0.0;
        if (TryReadNumber(*current, &value)) {
            if (error != nullptr) {
                error->clear();
            }
            return value;
        }
        if (error != nullptr) {
            *error = "metric path does not resolve to numeric value";
        }
        return 0.0;
    }

    const std::string resolved_metric_path = ResultAnalyzer::ResolveMetricPathAlias(metric_path);
    double derived_value = 0.0;
    std::string derived_error;
    if (TryExtractDerivedMetric(root, resolved_metric_path, &derived_value, &derived_error)) {
        if (error != nullptr) {
            error->clear();
        }
        return derived_value;
    }

    if (error != nullptr) {
        *error = path_error.empty() ? derived_error : path_error;
    }
    return 0.0;
}

bool TryExtractOptionalMetric(const Value& root,
                              const std::string& metric_path,
                              std::optional<double>* out,
                              std::vector<std::string>* warnings) {
    if (out == nullptr) {
        return false;
    }
    std::string error;
    const Value* value = ResolvePath(root, metric_path, &error);
    if (value == nullptr) {
        if (warnings != nullptr) {
            warnings->push_back(metric_path + ": " + error);
        }
        return true;
    }
    double parsed = 0.0;
    if (!TryReadNumber(*value, &parsed)) {
        if (warnings != nullptr) {
            warnings->push_back(metric_path + ": not a numeric value");
        }
        return true;
    }
    *out = parsed;
    return true;
}

double ComputeObjectiveFromValueTree(const Value& root,
                                     const OptimizationConfig& config,
                                     std::string* error) {
    if (config.objectives.empty()) {
        return ExtractMetricFromValueTree(root, config.metric_path, error);
    }

    const bool needs_initial_equity =
        std::any_of(config.objectives.begin(), config.objectives.end(), [](const auto& objective) {
            return objective.scale_by_initial_equity;
        });

    double initial_equity = 0.0;
    if (needs_initial_equity) {
        std::string initial_equity_error;
        initial_equity = ExtractMetricFromValueTree(root, "initial_equity", &initial_equity_error);
        if (!initial_equity_error.empty()) {
            if (error != nullptr) {
                *error = "failed to read initial_equity: " + initial_equity_error;
            }
            return 0.0;
        }
        if (!(initial_equity > 0.0)) {
            if (error != nullptr) {
                *error = "initial_equity must be > 0 when scale_by_initial_equity=true";
            }
            return 0.0;
        }
    }

    double score = 0.0;
    for (const OptimizationObjective& objective : config.objectives) {
        std::string metric_error;
        double value = ExtractMetricFromValueTree(root, objective.metric_path, &metric_error);
        if (!metric_error.empty()) {
            if (error != nullptr) {
                *error = "objective path `" + objective.metric_path + "`: " + metric_error;
            }
            return 0.0;
        }
        if (objective.scale_by_initial_equity) {
            value /= initial_equity;
        }
        if (!objective.maximize) {
            value = -value;
        }
        score += objective.weight * value;
    }

    return score;
}

std::string LedgerKey(const std::string& symbol, const std::string& side) {
    return symbol + "|" + side;
}

std::string SideBucketForOpen(const JsonTradeRecord& trade) {
    return ToLower(trade.side) == "buy" ? "long" : "short";
}

std::string SideBucketForClose(const JsonTradeRecord& trade) {
    return ToLower(trade.side) == "sell" ? "long" : "short";
}

bool ParseTradesArray(const Value& trades_value,
                      std::vector<JsonTradeRecord>* out_trades,
                      std::string* error) {
    if (out_trades == nullptr) {
        if (error != nullptr) {
            *error = "trade output is null";
        }
        return false;
    }
    out_trades->clear();
    if (!trades_value.IsArray()) {
        if (error != nullptr) {
            *error = "trades is not an array";
        }
        return false;
    }

    out_trades->reserve(trades_value.array_value.size());
    for (std::size_t index = 0; index < trades_value.array_value.size(); ++index) {
        const Value& row = trades_value.array_value[index];
        if (!row.IsObject()) {
            if (error != nullptr) {
                *error = "trade row is not an object at index " + std::to_string(index);
            }
            return false;
        }

        const Value* fill_seq = row.Find("fill_seq");
        const Value* trade_id = row.Find("trade_id");
        const Value* symbol = row.Find("symbol");
        const Value* side = row.Find("side");
        const Value* offset = row.Find("offset");
        const Value* volume = row.Find("volume");
        if (fill_seq == nullptr || trade_id == nullptr || symbol == nullptr || side == nullptr ||
            offset == nullptr || volume == nullptr) {
            if (error != nullptr) {
                *error = "trade row missing required fields at index " + std::to_string(index);
            }
            return false;
        }

        JsonTradeRecord trade;
        if (!TryReadInt(*fill_seq, &trade.fill_seq) || !trade_id->IsString() ||
            !symbol->IsString() || !side->IsString() || !offset->IsString() ||
            !TryReadInt(*volume, &trade.volume)) {
            if (error != nullptr) {
                *error = "trade row has invalid field types at index " + std::to_string(index);
            }
            return false;
        }

        trade.trade_id = trade_id->string_value;
        trade.symbol = symbol->string_value;
        trade.side = side->string_value;
        trade.offset = offset->string_value;

        if (const Value* commission = row.Find("commission"); commission != nullptr) {
            (void)TryReadNumber(*commission, &trade.commission);
        }
        if (const Value* realized_pnl = row.Find("realized_pnl"); realized_pnl != nullptr) {
            (void)TryReadNumber(*realized_pnl, &trade.realized_pnl);
        }
        if (const Value* risk_budget_r = row.Find("risk_budget_r"); risk_budget_r != nullptr) {
            (void)TryReadNumber(*risk_budget_r, &trade.risk_budget_r);
        }
        if (const Value* signal_type = row.Find("signal_type");
            signal_type != nullptr && signal_type->IsString()) {
            trade.signal_type = signal_type->string_value;
        }

        out_trades->push_back(std::move(trade));
    }

    std::stable_sort(out_trades->begin(), out_trades->end(), [](const JsonTradeRecord& left,
                                                                const JsonTradeRecord& right) {
        return left.fill_seq < right.fill_seq;
    });
    return true;
}

std::map<std::string, int> ExtractFinalPositions(const Value& root, bool* has_positions) {
    if (has_positions != nullptr) {
        *has_positions = false;
    }

    const Value* deterministic = root.Find("deterministic");
    if (deterministic == nullptr || !deterministic->IsObject()) {
        return {};
    }
    const Value* instrument_pnl = deterministic->Find("instrument_pnl");
    if (instrument_pnl == nullptr || !instrument_pnl->IsObject()) {
        return {};
    }

    if (has_positions != nullptr) {
        *has_positions = true;
    }

    std::map<std::string, int> positions;
    for (const auto& [symbol, snapshot] : instrument_pnl->object_value) {
        if (!snapshot.IsObject()) {
            continue;
        }
        const Value* net_position = snapshot.Find("net_position");
        int parsed = 0;
        if (net_position != nullptr && TryReadInt(*net_position, &parsed) && parsed != 0) {
            positions[symbol] = parsed;
        }
    }
    return positions;
}

bool BuildRoundTripStats(const std::vector<JsonTradeRecord>& trades,
                         const std::map<std::string, int>& final_positions,
                         bool has_final_positions,
                         RoundTripStats* out_stats,
                         std::string* error) {
    if (out_stats == nullptr) {
        if (error != nullptr) {
            *error = "round trip stats output is null";
        }
        return false;
    }

    std::map<std::string, std::deque<OpenLotRecord>> ledgers;
    std::map<std::string, CompletedRoundTrip> completed;

    for (const JsonTradeRecord& trade : trades) {
        const std::string offset = ToLower(trade.offset);
        if (offset == "open") {
            OpenLotRecord lot;
            lot.trade_id = trade.trade_id;
            lot.symbol = trade.symbol;
            lot.side = SideBucketForOpen(trade);
            lot.volume = trade.volume;
            lot.remaining_volume = trade.volume;
            lot.commission = trade.commission;
            lot.risk_budget_r = trade.risk_budget_r;
            ledgers[LedgerKey(trade.symbol, lot.side)].push_back(std::move(lot));
            continue;
        }

        std::string close_bucket;
        if (offset == "close" || offset == "closetoday" || offset == "closeyesterday") {
            close_bucket = SideBucketForClose(trade);
        } else if (ToLower(trade.signal_type).find("expiry") != std::string::npos) {
            close_bucket = SideBucketForClose(trade);
        } else {
            continue;
        }

        std::deque<OpenLotRecord>& queue = ledgers[LedgerKey(trade.symbol, close_bucket)];
        int remaining = trade.volume;
        if (remaining <= 0) {
            continue;
        }
        if (queue.empty()) {
            if (error != nullptr) {
                *error = "unmatched close trade: trade_id=" + trade.trade_id + ", symbol=" +
                         trade.symbol;
            }
            return false;
        }

        while (remaining > 0) {
            if (queue.empty()) {
                if (error != nullptr) {
                    *error = "close trade exceeds open inventory: trade_id=" + trade.trade_id +
                             ", symbol=" + trade.symbol;
                }
                return false;
            }
            OpenLotRecord& lot = queue.front();
            const int matched = std::min(remaining, lot.remaining_volume);
            remaining -= matched;
            lot.remaining_volume -= matched;

            CompletedRoundTrip& round_trip = completed[lot.trade_id];
            if (round_trip.entry_volume == 0) {
                round_trip.entry_volume = lot.volume;
                round_trip.risk_budget_r = lot.risk_budget_r;
            }

            const double ratio_open = matched / static_cast<double>(std::max(lot.volume, 1));
            const double ratio_close = matched / static_cast<double>(std::max(trade.volume, 1));
            round_trip.matched_volume += matched;
            round_trip.allocated_entry_commission += lot.commission * ratio_open;
            round_trip.allocated_exit_commission += trade.commission * ratio_close;
            round_trip.gross_realized_pnl += trade.realized_pnl * ratio_close;

            if (lot.remaining_volume == 0) {
                queue.pop_front();
            }
        }
    }

    if (has_final_positions) {
        std::map<std::string, int> expected_by_symbol;
        for (const auto& [key, queue] : ledgers) {
            for (const OpenLotRecord& lot : queue) {
                if (lot.remaining_volume <= 0) {
                    continue;
                }
                const int signed_position = lot.side == "long" ? lot.remaining_volume
                                                                 : -lot.remaining_volume;
                expected_by_symbol[lot.symbol] += signed_position;
            }
        }

        std::map<std::string, int> normalized_expected;
        for (const auto& [symbol, position] : expected_by_symbol) {
            if (position != 0) {
                normalized_expected[symbol] = position;
            }
        }

        if (normalized_expected != final_positions) {
            if (error != nullptr) {
                *error = "paired open inventory does not match final positions";
            }
            return false;
        }
    }

    std::vector<double> round_trip_net;
    std::vector<double> gross_r_values;
    double total_wins = 0.0;
    double total_losses = 0.0;

    for (const auto& [trade_id, trip] : completed) {
        if (trip.entry_volume <= 0 || trip.matched_volume != trip.entry_volume) {
            continue;
        }
        const double net_pnl =
            trip.gross_realized_pnl - trip.allocated_entry_commission - trip.allocated_exit_commission;
        round_trip_net.push_back(net_pnl);
        if (net_pnl > 0.0) {
            total_wins += net_pnl;
        } else if (net_pnl < 0.0) {
            total_losses += net_pnl;
        }
        if (trip.risk_budget_r > 0.0) {
            gross_r_values.push_back(trip.gross_realized_pnl / trip.risk_budget_r);
        }
    }

    out_stats->total_trades = static_cast<int>(round_trip_net.size());
    if (!round_trip_net.empty()) {
        const int wins = static_cast<int>(std::count_if(round_trip_net.begin(), round_trip_net.end(),
                                                        [](double value) { return value > 0.0; }));
        out_stats->win_rate_pct =
            100.0 * static_cast<double>(wins) / static_cast<double>(round_trip_net.size());
    }
    out_stats->expectancy_r = ExpectancyFromRMultiples(gross_r_values);
    if (total_losses < 0.0) {
        out_stats->profit_factor = total_wins / std::fabs(total_losses);
    }
    return true;
}

double ComputeAnnualizedSharpeRatio(const std::vector<double>& daily_returns_pct) {
    if (daily_returns_pct.size() < 2) {
        return 0.0;
    }
    std::vector<double> returns;
    returns.reserve(daily_returns_pct.size());
    for (double value : daily_returns_pct) {
        returns.push_back(value / 100.0);
    }
    const double volatility = SampleStdDev(returns);
    if (volatility <= 1e-12) {
        return 0.0;
    }
    return Mean(returns) / volatility * std::sqrt(252.0);
}

bool ExtractDailyDerivedMetrics(const Value& root,
                                TrialMetricsSnapshot* metrics,
                                std::vector<std::string>* warnings) {
    if (metrics == nullptr) {
        return false;
    }
    const Value* daily = ResolvePath(root, "hf_standard.daily", nullptr);
    if (daily == nullptr) {
        daily = ResolvePath(root, "daily", nullptr);
    }
    if (daily == nullptr) {
        if (warnings != nullptr) {
            warnings->push_back("daily: missing");
        }
        return true;
    }
    if (!daily->IsArray()) {
        if (warnings != nullptr) {
            warnings->push_back("daily: invalid");
        }
        return true;
    }
    if (daily->array_value.empty()) {
        return true;
    }

    std::vector<double> daily_returns_pct;
    daily_returns_pct.reserve(daily->array_value.size());
    double cumulative_return_pct = 0.0;
    bool has_cumulative_return = false;
    double max_drawdown_pct = 0.0;
    bool has_drawdown = false;

    for (std::size_t index = 0; index < daily->array_value.size(); ++index) {
        const Value& row = daily->array_value[index];
        if (!row.IsObject()) {
            if (warnings != nullptr) {
                warnings->push_back("daily: row " + std::to_string(index) + " is not an object");
            }
            continue;
        }
        if (const Value* daily_return = row.Find("daily_return_pct"); daily_return != nullptr) {
            double parsed = 0.0;
            if (TryReadNumber(*daily_return, &parsed)) {
                daily_returns_pct.push_back(parsed);
            }
        }
        if (const Value* cumulative = row.Find("cumulative_return_pct"); cumulative != nullptr) {
            double parsed = 0.0;
            if (TryReadNumber(*cumulative, &parsed)) {
                cumulative_return_pct = parsed;
                has_cumulative_return = true;
            }
        }
        if (const Value* drawdown = row.Find("drawdown_pct"); drawdown != nullptr) {
            double parsed = 0.0;
            if (TryReadNumber(*drawdown, &parsed)) {
                max_drawdown_pct = std::max(max_drawdown_pct, parsed);
                has_drawdown = true;
            }
        }
    }

    if (!has_cumulative_return) {
        if (warnings != nullptr) {
            warnings->push_back("daily: cumulative_return_pct not found");
        }
        return true;
    }

    metrics->max_drawdown_pct = has_drawdown ? std::optional<double>(max_drawdown_pct)
                                             : std::optional<double>{};
    const int trading_days = static_cast<int>(daily->array_value.size());
    const double cumulative_ratio = 1.0 + cumulative_return_pct / 100.0;
    if (trading_days > 0 && cumulative_ratio > 0.0) {
        metrics->annualized_return_pct =
            (std::pow(cumulative_ratio, 252.0 / static_cast<double>(trading_days)) - 1.0) * 100.0;
    } else if (warnings != nullptr) {
        warnings->push_back("daily: annualized return could not be derived");
    }

    if (!daily_returns_pct.empty()) {
        metrics->sharpe_ratio = ComputeAnnualizedSharpeRatio(daily_returns_pct);
    }
    if (metrics->annualized_return_pct.has_value() && metrics->max_drawdown_pct.has_value()) {
        if (*metrics->max_drawdown_pct > 1e-12) {
            metrics->calmar_ratio = *metrics->annualized_return_pct / *metrics->max_drawdown_pct;
        } else {
            metrics->calmar_ratio = 0.0;
        }
    }
    return true;
}

bool ExtractTradeDerivedMetrics(const Value& root,
                                TrialMetricsSnapshot* metrics,
                                std::vector<std::string>* warnings) {
    if (metrics == nullptr) {
        return false;
    }

    const Value* trades = ResolvePath(root, "hf_standard.trades", nullptr);
    if (trades == nullptr) {
        trades = ResolvePath(root, "trades", nullptr);
    }
    if (trades == nullptr) {
        if (warnings != nullptr) {
            warnings->push_back("trades: missing");
        }
        return true;
    }
    if (!trades->IsArray()) {
        if (warnings != nullptr) {
            warnings->push_back("trades: invalid");
        }
        return true;
    }
    if (trades->array_value.empty()) {
        return true;
    }

    std::vector<JsonTradeRecord> parsed_trades;
    std::string parse_error;
    if (!ParseTradesArray(*trades, &parsed_trades, &parse_error)) {
        if (warnings != nullptr) {
            warnings->push_back("trades: " + parse_error);
        }
        return true;
    }

    bool has_final_positions = false;
    const std::map<std::string, int> final_positions = ExtractFinalPositions(root, &has_final_positions);
    RoundTripStats stats;
    std::string stats_error;
    if (!BuildRoundTripStats(parsed_trades, final_positions, has_final_positions, &stats,
                             &stats_error)) {
        if (warnings != nullptr) {
            warnings->push_back("round_trip_metrics: " + stats_error);
        }
        return true;
    }

    metrics->total_trades = stats.total_trades;
    metrics->win_rate_pct = stats.win_rate_pct;
    metrics->expectancy_r = stats.expectancy_r;
    if (!metrics->profit_factor.has_value()) {
        metrics->profit_factor = stats.profit_factor;
    }
    return true;
}

std::vector<const Trial*> SortedCompletedTrials(const OptimizationReport& report) {
    std::vector<const Trial*> completed;
    completed.reserve(report.trials.size());
    for (const Trial& trial : report.trials) {
        if (trial.status == "completed") {
            completed.push_back(&trial);
        }
    }
    std::stable_sort(completed.begin(), completed.end(), [&](const Trial* left, const Trial* right) {
        return report.maximize ? (left->objective > right->objective)
                               : (left->objective < right->objective);
    });
    return completed;
}

void AppendTrialJsonObject(std::ostringstream& json, const Trial& trial, int indent_spaces) {
    const std::string indent(static_cast<std::size_t>(indent_spaces), ' ');
    const std::string inner(static_cast<std::size_t>(indent_spaces + 2), ' ');

    json << "{\n"
         << inner << "\"trial_id\": \"" << JsonEscape(trial.trial_id) << "\",\n"
         << inner << "\"status\": \"" << JsonEscape(trial.status) << "\",\n"
         << inner << "\"objective\": " << FormatDouble(trial.objective) << ",\n"
         << inner << "\"elapsed_sec\": " << FormatDouble(trial.elapsed_sec) << ",\n"
         << inner << "\"result_json_path\": \"" << JsonEscape(trial.result_json_path)
         << "\",\n"
         << inner << "\"stdout_log_path\": \"" << JsonEscape(trial.stdout_log_path)
         << "\",\n"
         << inner << "\"stderr_log_path\": \"" << JsonEscape(trial.stderr_log_path)
         << "\",\n"
         << inner << "\"working_dir\": \"" << JsonEscape(trial.working_dir) << "\",\n"
         << inner << "\"archived_artifact_dir\": \"" << JsonEscape(trial.archived_artifact_dir)
         << "\",\n"
         << inner << "\"error_msg\": \"" << JsonEscape(trial.error_msg) << "\",\n"
         << inner << "\"metrics_error\": \"" << JsonEscape(trial.metrics_error) << "\",\n"
         << inner << "\"params\": {";

    const auto params = SortedParams(trial.params);
    for (std::size_t i = 0; i < params.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << "\"" << JsonEscape(params[i].first) << "\": " << ParamValueToJson(params[i].second);
    }

    json << "},\n"
         << inner << "\"metrics\": {\n"
         << inner << "  \"total_pnl\": " << OptionalDoubleToJson(trial.metrics.total_pnl) << ",\n"
         << inner << "  \"max_drawdown\": " << OptionalDoubleToJson(trial.metrics.max_drawdown)
         << ",\n"
         << inner << "  \"max_drawdown_pct\": "
         << OptionalDoubleToJson(trial.metrics.max_drawdown_pct) << ",\n"
         << inner << "  \"annualized_return_pct\": "
         << OptionalDoubleToJson(trial.metrics.annualized_return_pct) << ",\n"
         << inner << "  \"sharpe_ratio\": "
         << OptionalDoubleToJson(trial.metrics.sharpe_ratio) << ",\n"
         << inner << "  \"calmar_ratio\": "
         << OptionalDoubleToJson(trial.metrics.calmar_ratio) << ",\n"
         << inner << "  \"profit_factor\": "
         << OptionalDoubleToJson(trial.metrics.profit_factor) << ",\n"
         << inner << "  \"win_rate_pct\": "
         << OptionalDoubleToJson(trial.metrics.win_rate_pct) << ",\n"
         << inner << "  \"total_trades\": "
         << OptionalIntToJson(trial.metrics.total_trades) << ",\n"
         << inner << "  \"expectancy_r\": "
         << OptionalDoubleToJson(trial.metrics.expectancy_r) << "\n"
         << inner << "}\n"
         << indent << "}";
}

}  // namespace

bool ResultAnalyzer::ParseOptimizationConstraint(const std::string& expression,
                                                 OptimizationConstraint* out_constraint,
                                                 std::string* error) {
    if (out_constraint == nullptr) {
        if (error != nullptr) {
            *error = "constraint output is null";
        }
        return false;
    }

    const std::string trimmed = Trim(expression);
    if (trimmed.empty()) {
        if (error != nullptr) {
            *error = "constraint expression must not be empty";
        }
        return false;
    }

    struct OperatorSpec {
        const char* text;
        ConstraintOperator op;
    };
    static const std::vector<OperatorSpec> kOperators = {
        {"<=", ConstraintOperator::kLessEqual},
        {">=", ConstraintOperator::kGreaterEqual},
        {"==", ConstraintOperator::kEqual},
        {"!=", ConstraintOperator::kNotEqual},
        {"<", ConstraintOperator::kLess},
        {">", ConstraintOperator::kGreater},
    };

    std::size_t operator_pos = std::string::npos;
    ConstraintOperator op = ConstraintOperator::kLess;
    std::string operator_text;
    for (const auto& candidate : kOperators) {
        const std::size_t pos = trimmed.find(candidate.text);
        if (pos == std::string::npos) {
            continue;
        }
        operator_pos = pos;
        op = candidate.op;
        operator_text = candidate.text;
        break;
    }

    if (operator_pos == std::string::npos) {
        if (error != nullptr) {
            *error = "unsupported constraint operator in expression: " + trimmed;
        }
        return false;
    }

    const std::string lhs = Trim(trimmed.substr(0, operator_pos));
    const std::string rhs = Trim(trimmed.substr(operator_pos + operator_text.size()));
    if (lhs.empty() || rhs.empty()) {
        if (error != nullptr) {
            *error = "constraint expression must match '<metric> <operator> <value>'";
        }
        return false;
    }

    double threshold = 0.0;
    if (!ParseDoubleStrict(rhs, &threshold)) {
        if (error != nullptr) {
            *error = "constraint value must be numeric: " + rhs;
        }
        return false;
    }

    OptimizationConstraint constraint;
    constraint.raw_expression = trimmed;
    if (!ResolveConstraintMetricSpec(lhs, &constraint.metric_name, &constraint.metric_path)) {
        if (error != nullptr) {
            *error = "unsupported constraint metric: " + lhs;
        }
        return false;
    }
    constraint.op = op;
    constraint.threshold = threshold;
    *out_constraint = std::move(constraint);
    return true;
}

std::string ResultAnalyzer::ResolveMetricPathAlias(const std::string& metric_path) {
    static const std::map<std::string, std::string> kAliases = {
        {"hf_standard.profit_factor", "hf_standard.advanced_summary.profit_factor"},
        {"profit_factor", "hf_standard.advanced_summary.profit_factor"},
        {"max_drawdown_pct", "hf_standard.risk_metrics.max_drawdown_pct"},
        {"total_trades", "hf_standard.trade_statistics.total_trades"},
        {"expectancy_r", "hf_standard.trade_statistics.expectancy_r"},
        {"calmar_ratio", "hf_standard.risk_metrics.calmar_ratio"},
        {"sharpe_ratio", "hf_standard.risk_metrics.sharpe_ratio"},
        {"summary.total_pnl", "summary.total_pnl"},
        {"total_pnl", "summary.total_pnl"},
        {"max_drawdown", "summary.max_drawdown"},
    };
    const auto it = kAliases.find(metric_path);
    return it == kAliases.end() ? metric_path : it->second;
}

double ResultAnalyzer::ExtractMetricFromJsonText(const std::string& json_text,
                                                 const std::string& metric_path,
                                                 std::string* error) {
    Value root;
    if (!quant_hft::simple_json::Parse(json_text, &root, error)) {
        return 0.0;
    }
    return ExtractMetricFromValueTree(root, metric_path, error);
}

double ResultAnalyzer::ExtractMetricFromJson(const std::string& json_path,
                                             const std::string& metric_path,
                                             std::string* error) {
    std::ifstream input(json_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open trial json: " + json_path;
        }
        return 0.0;
    }

    std::ostringstream buffer;
    buffer << input.rdbuf();

    return ExtractMetricFromJsonText(buffer.str(), metric_path, error);
}

double ResultAnalyzer::ComputeObjectiveFromJsonText(const std::string& json_text,
                                                    const OptimizationConfig& config,
                                                    std::string* error) {
    Value root;
    if (!quant_hft::simple_json::Parse(json_text, &root, error)) {
        return 0.0;
    }
    return ComputeObjectiveFromValueTree(root, config, error);
}

double ResultAnalyzer::ComputeObjectiveFromJson(const std::string& json_path,
                                                const OptimizationConfig& config,
                                                std::string* error) {
    std::ifstream input(json_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open trial json: " + json_path;
        }
        return 0.0;
    }

    std::ostringstream buffer;
    buffer << input.rdbuf();

    return ComputeObjectiveFromJsonText(buffer.str(), config, error);
}

bool ResultAnalyzer::ExtractTrialMetricsFromJsonText(const std::string& json_text,
                                                     TrialMetricsSnapshot* out_metrics,
                                                     std::string* error) {
    if (out_metrics == nullptr) {
        if (error != nullptr) {
            *error = "trial metrics output is null";
        }
        return false;
    }

    Value root;
    if (!quant_hft::simple_json::Parse(json_text, &root, error)) {
        return false;
    }

    return ExtractTrialMetricsFromValueTree(root, out_metrics, error);
}

bool ResultAnalyzer::ExtractTrialMetricsFromJson(const std::string& json_path,
                                                 TrialMetricsSnapshot* out_metrics,
                                                 std::string* error) {
    if (out_metrics == nullptr) {
        if (error != nullptr) {
            *error = "trial metrics output is null";
        }
        return false;
    }

    std::ifstream input(json_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open trial json: " + json_path;
        }
        return false;
    }

    std::ostringstream buffer;
    buffer << input.rdbuf();

    return ExtractTrialMetricsFromJsonText(buffer.str(), out_metrics, error);
}

bool ResultAnalyzer::EvaluateConstraintsFromJsonText(const std::string& json_text,
                                                     const OptimizationConfig& config,
                                                     std::vector<std::string>* violations,
                                                     std::string* error) {
    if (violations == nullptr) {
        if (error != nullptr) {
            *error = "constraint violations output is null";
        }
        return false;
    }
    violations->clear();

    if (config.constraints.empty()) {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    Value root;
    if (!quant_hft::simple_json::Parse(json_text, &root, error)) {
        return false;
    }

    TrialMetricsSnapshot metrics;
    std::string metrics_error;
    if (!ExtractTrialMetricsFromValueTree(root, &metrics, &metrics_error)) {
        if (error != nullptr) {
            *error = metrics_error;
        }
        return false;
    }

    for (const OptimizationConstraint& constraint : config.constraints) {
        double actual_value = 0.0;
        std::string value_error;
        if (!TryExtractMetricFromSnapshot(metrics, constraint.metric_path, &actual_value)) {
            const Value* direct_value = ResolvePath(root, constraint.metric_path, nullptr);
            if (direct_value != nullptr && TryReadNumber(*direct_value, &actual_value)) {
                // Use the directly recorded metric when present.
            } else if (constraint.metric_name == "max_drawdown_pct") {
                const Value* fallback = ResolvePath(root, "summary.max_drawdown", nullptr);
                if (fallback != nullptr && TryReadNumber(*fallback, &actual_value)) {
                    // Older result.json variants may only have summary.max_drawdown.
                } else {
                    value_error = "metric unavailable: " + constraint.metric_name;
                }
            } else {
                value_error = "metric unavailable: " + constraint.metric_name;
            }
        }

        if (!value_error.empty()) {
            if (error != nullptr) {
                *error = "constraint `" + constraint.raw_expression + "`: " + value_error;
                if (!metrics_error.empty()) {
                    *error += " (" + metrics_error + ")";
                }
            }
            return false;
        }

        if (!CompareConstraintValue(actual_value, constraint.op, constraint.threshold)) {
            violations->push_back(constraint.metric_name + " " +
                                  ConstraintOperatorToString(constraint.op) + " " +
                                  FormatDouble(constraint.threshold) + " (actual=" +
                                  FormatDouble(actual_value) + ")");
        }
    }

    if (error != nullptr) {
        error->clear();
    }
    return true;
}

bool ResultAnalyzer::EvaluateConstraintsFromJson(const std::string& json_path,
                                                 const OptimizationConfig& config,
                                                 std::vector<std::string>* violations,
                                                 std::string* error) {
    if (violations == nullptr) {
        if (error != nullptr) {
            *error = "constraint violations output is null";
        }
        return false;
    }

    std::ifstream input(json_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open trial json: " + json_path;
        }
        return false;
    }

    std::ostringstream buffer;
    buffer << input.rdbuf();
    return EvaluateConstraintsFromJsonText(buffer.str(), config, violations, error);
}

OptimizationReport ResultAnalyzer::Analyze(const std::vector<Trial>& trials,
                                           const OptimizationConfig& config,
                                           bool interrupted) {
    OptimizationReport report;
    report.algorithm = config.algorithm;
    if (config.objectives.empty()) {
        report.metric_path = ResolveMetricPathAlias(config.metric_path);
    } else {
        report.metric_path = "weighted_objective";
        report.objectives.reserve(config.objectives.size());
        for (const OptimizationObjective& objective : config.objectives) {
            OptimizationObjective resolved = objective;
            resolved.metric_path = ResolveMetricPathAlias(objective.metric_path);
            report.objectives.push_back(std::move(resolved));
        }
    }
    report.maximize = config.maximize;
    report.interrupted = interrupted;
    report.trials = trials;
    report.total_trials = static_cast<int>(trials.size());

    bool has_best = false;
    Trial best;
    double best_so_far = config.maximize ? -std::numeric_limits<double>::infinity()
                                         : std::numeric_limits<double>::infinity();

    for (const Trial& trial : trials) {
        if (trial.status == "completed") {
            ++report.completed_trials;
            report.all_objectives.push_back(trial.objective);

            const bool better = !has_best ||
                                (config.maximize ? (trial.objective > best.objective)
                                                 : (trial.objective < best.objective));
            if (better) {
                best = trial;
                has_best = true;
            }
            best_so_far = config.maximize ? std::max(best_so_far, trial.objective)
                                          : std::min(best_so_far, trial.objective);
        } else if (trial.status == "constraint_violated") {
            ++report.constraint_stats.total_violations;
            report.constraint_stats.violated_trials.push_back(trial.trial_id);
            report.all_objectives.push_back(0.0);
        } else {
            ++report.failed_trials;
            report.all_objectives.push_back(0.0);
        }

        if (has_best) {
            report.convergence_curve.push_back(best_so_far);
        } else {
            report.convergence_curve.push_back(0.0);
        }
    }

    if (has_best) {
        report.best_trial = best;
    } else {
        report.best_trial.status = "failed";
        report.best_trial.error_msg = "no completed trial";
    }

    return report;
}

std::string ResultAnalyzer::DefaultTop10InSamplePath(const std::string& json_path,
                                                     const std::string& md_path) {
    std::filesystem::path base_dir;
    if (!md_path.empty()) {
        base_dir = std::filesystem::path(md_path).parent_path();
    }
    if (base_dir.empty() && !json_path.empty()) {
        base_dir = std::filesystem::path(json_path).parent_path();
    }
    if (base_dir.empty()) {
        base_dir = std::filesystem::current_path();
    }
    return (base_dir / "top10_in_sample.md").string();
}

bool ResultAnalyzer::WriteReport(const OptimizationReport& report,
                                 const std::string& json_path,
                                 const std::string& md_path,
                                 std::string* error) {
    const std::string top10_path = DefaultTop10InSamplePath(json_path, md_path);

    std::ostringstream json;
    json << "{\n"
         << "  \"task_id\": \"" << JsonEscape(report.task_id) << "\",\n"
         << "  \"started_at\": \"" << JsonEscape(report.started_at) << "\",\n"
         << "  \"finished_at\": \"" << JsonEscape(report.finished_at) << "\",\n"
         << "  \"wall_clock_sec\": " << FormatDouble(report.wall_clock_sec) << ",\n"
         << "  \"top10_in_sample_md_path\": \"" << JsonEscape(top10_path) << "\",\n"
         << "  \"algorithm\": \"" << JsonEscape(report.algorithm) << "\",\n"
         << "  \"metric_path\": \"" << JsonEscape(report.metric_path) << "\",\n"
         << "  \"maximize\": " << (report.maximize ? "true" : "false") << ",\n"
         << "  \"interrupted\": " << (report.interrupted ? "true" : "false") << ",\n"
         << "  \"total_trials\": " << report.total_trials << ",\n"
         << "  \"completed_trials\": " << report.completed_trials << ",\n"
         << "  \"failed_trials\": " << report.failed_trials << ",\n"
         << "  \"constraint_stats\": {\n"
         << "    \"total_violations\": " << report.constraint_stats.total_violations << ",\n"
         << "    \"violated_trials\": [";

    for (std::size_t i = 0; i < report.constraint_stats.violated_trials.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << "\"" << JsonEscape(report.constraint_stats.violated_trials[i]) << "\"";
    }
    json << "]\n"
         << "  },\n";

    if (!report.objectives.empty()) {
        json << "  \"objectives\": [\n";
        for (std::size_t i = 0; i < report.objectives.size(); ++i) {
            const OptimizationObjective& objective = report.objectives[i];
            json << "    {\n"
                 << "      \"path\": \"" << JsonEscape(objective.metric_path) << "\",\n"
                 << "      \"weight\": " << FormatDouble(objective.weight) << ",\n"
                 << "      \"maximize\": " << (objective.maximize ? "true" : "false")
                 << ",\n"
                 << "      \"scale_by_initial_equity\": "
                 << (objective.scale_by_initial_equity ? "true" : "false") << "\n"
                 << "    }";
            if (i + 1 < report.objectives.size()) {
                json << ',';
            }
            json << "\n";
        }
        json << "  ],\n";
    }

    json << "  \"best_trial\": ";
    AppendTrialJsonObject(json, report.best_trial, 2);
    json << ",\n"
         << "  \"all_objectives\": [";
    for (std::size_t i = 0; i < report.all_objectives.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << FormatDouble(report.all_objectives[i]);
    }
    json << "],\n"
         << "  \"convergence_curve\": [";
    for (std::size_t i = 0; i < report.convergence_curve.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << FormatDouble(report.convergence_curve[i]);
    }
    json << "],\n"
         << "  \"trials\": [\n";

    for (std::size_t i = 0; i < report.trials.size(); ++i) {
        json << "    ";
        AppendTrialJsonObject(json, report.trials[i], 4);
        if (i + 1 < report.trials.size()) {
            json << ',';
        }
        json << "\n";
    }

    json << "  ]\n"
         << "}\n";

    if (!WriteTextFile(json_path, json.str(), error)) {
        return false;
    }

    std::ostringstream md;
    md << "# 参数优化报告\n\n";
    if (!report.task_id.empty()) {
        md << "## 任务元数据\n\n"
           << "- task_id: `" << report.task_id << "`\n"
           << "- started_at: `" << report.started_at << "`\n"
           << "- finished_at: `" << report.finished_at << "`\n"
           << "- wall_clock_sec: `" << FormatDouble(report.wall_clock_sec) << "`\n\n";
    }
    md << "- 算法: `" << report.algorithm << "`\n"
       << "- 指标: `" << report.metric_path << "`\n"
       << "- 优化方向: `" << (report.maximize ? "maximize" : "minimize") << "`\n"
       << "- 总试验数: `" << report.total_trials << "`\n"
       << "- 成功: `" << report.completed_trials << "`\n"
       << "- 失败: `" << report.failed_trials << "`\n"
       << "- 约束违反: `" << report.constraint_stats.total_violations << "`\n"
       << "- 中断: `" << (report.interrupted ? "true" : "false") << "`\n"
       << "- Top10 文件: `" << top10_path << "`\n\n";

    md << "## 约束统计\n\n"
       << "- total_violations: `" << report.constraint_stats.total_violations << "`\n";
    if (report.constraint_stats.violated_trials.empty()) {
        md << "- violated_trials: `-`\n\n";
    } else {
        md << "- violated_trials: `";
        for (std::size_t index = 0; index < report.constraint_stats.violated_trials.size(); ++index) {
            if (index > 0) {
                md << ", ";
            }
            md << report.constraint_stats.violated_trials[index];
        }
        md << "`\n\n";
    }

    if (!report.objectives.empty()) {
        md << "## 目标构成\n\n";
        for (const OptimizationObjective& objective : report.objectives) {
            md << "- path: `" << objective.metric_path << "`, weight: `"
               << FormatDouble(objective.weight) << "`, direction: `"
               << (objective.maximize ? "maximize" : "minimize")
               << "`, scale_by_initial_equity: `"
               << (objective.scale_by_initial_equity ? "true" : "false") << "`\n";
        }
        md << "\n";
    }

    md << "## 最优试验\n\n"
       << "- trial_id: `" << report.best_trial.trial_id << "`\n"
       << "- objective: `" << FormatDouble(report.best_trial.objective) << "`\n"
       << "- status: `" << report.best_trial.status << "`\n"
       << "- result_json_path: `" << report.best_trial.result_json_path << "`\n"
       << "- stdout_log_path: `" << report.best_trial.stdout_log_path << "`\n"
       << "- stderr_log_path: `" << report.best_trial.stderr_log_path << "`\n"
       << "- working_dir: `" << report.best_trial.working_dir << "`\n"
       << "- archived_artifact_dir: `" << report.best_trial.archived_artifact_dir << "`\n"
       << "- metrics_error: `" << report.best_trial.metrics_error << "`\n\n"
       << "## 最优参数\n\n";

    for (const auto& [key, value] : SortedParams(report.best_trial.params)) {
        md << "- " << key << ": `" << ParamValueToString(value) << "`\n";
    }

    md << "\n## 最优试验指标快照\n\n"
       << "- total_pnl: `" << FormatOptionalMetric(report.best_trial.metrics.total_pnl, 2) << "`\n"
       << "- max_drawdown: `" << FormatOptionalMetric(report.best_trial.metrics.max_drawdown, 2)
       << "`\n"
       << "- max_drawdown_pct: `"
       << FormatOptionalMetric(report.best_trial.metrics.max_drawdown_pct, 2) << "`\n"
       << "- annualized_return_pct: `"
       << FormatOptionalMetric(report.best_trial.metrics.annualized_return_pct, 2) << "`\n"
       << "- sharpe_ratio: `"
       << FormatOptionalMetric(report.best_trial.metrics.sharpe_ratio, 2) << "`\n"
       << "- calmar_ratio: `"
       << FormatOptionalMetric(report.best_trial.metrics.calmar_ratio, 2) << "`\n"
       << "- profit_factor: `"
       << FormatOptionalMetric(report.best_trial.metrics.profit_factor, 2) << "`\n"
       << "- win_rate_pct: `"
       << FormatOptionalMetric(report.best_trial.metrics.win_rate_pct, 2) << "`\n"
       << "- total_trades: `"
       << FormatOptionalInteger(report.best_trial.metrics.total_trades) << "`\n"
       << "- expectancy_r: `"
       << FormatOptionalMetric(report.best_trial.metrics.expectancy_r, 2) << "`\n";

    md << "\n## 失败样本\n\n";
    for (const Trial& trial : report.trials) {
        if (trial.status == "completed") {
            continue;
        }
        md << "- " << trial.trial_id << ": " << trial.error_msg;
        if (!trial.stderr_log_path.empty()) {
            md << " (`" << trial.stderr_log_path << "`)";
        } else if (!trial.working_dir.empty()) {
            md << " (`" << trial.working_dir << "`)";
        }
        md << "\n";
    }

    if (!WriteTextFile(md_path, md.str(), error)) {
        return false;
    }

    return WriteTop10InSampleMarkdown(report, top10_path, error);
}

bool ResultAnalyzer::WriteHeatmaps(const OptimizationReport& report,
                                   const ParameterSpace& space,
                                   const std::string& output_dir,
                                   std::string* error) {
    const std::vector<HeatmapPair> pairs = BuildHeatmapPairs(space);
    if (pairs.empty()) {
        return true;
    }

    const std::filesystem::path base_dir = ResolveOutputDirectory(output_dir);
    if (!EnsureDirectoryExists(base_dir, error)) {
        return false;
    }

    for (const HeatmapPair& pair : pairs) {
        if (pair.x_param == nullptr || pair.y_param == nullptr) {
            continue;
        }

        const std::vector<ParamValue> x_values = BuildGridValuesForParam(*pair.x_param);
        const std::vector<ParamValue> y_values = BuildGridValuesForParam(*pair.y_param);
        if (x_values.empty() || y_values.empty()) {
            continue;
        }

        std::map<std::string, std::size_t> x_index;
        std::map<std::string, std::size_t> y_index;
        for (std::size_t i = 0; i < x_values.size(); ++i) {
            x_index.emplace(ParamValueToString(x_values[i]), i);
        }
        for (std::size_t i = 0; i < y_values.size(); ++i) {
            y_index.emplace(ParamValueToString(y_values[i]), i);
        }

        std::map<std::pair<std::size_t, std::size_t>, std::vector<double>> buckets;
        for (const Trial& trial : report.trials) {
            if (trial.status != "completed") {
                continue;
            }

            const auto x_it = trial.params.values.find(pair.x_param->name);
            const auto y_it = trial.params.values.find(pair.y_param->name);
            if (x_it == trial.params.values.end() || y_it == trial.params.values.end()) {
                continue;
            }

            const auto x_axis_it = x_index.find(ParamValueToString(x_it->second));
            const auto y_axis_it = y_index.find(ParamValueToString(y_it->second));
            if (x_axis_it == x_index.end() || y_axis_it == y_index.end()) {
                continue;
            }

            buckets[{x_axis_it->second, y_axis_it->second}].push_back(trial.objective);
        }

        std::ostringstream json;
        json << "{\n"
             << "  \"x_param\": \"" << JsonEscape(pair.x_param->name) << "\",\n"
             << "  \"y_param\": \"" << JsonEscape(pair.y_param->name) << "\",\n"
             << "  \"x_values\": [";
        for (std::size_t i = 0; i < x_values.size(); ++i) {
            if (i > 0) {
                json << ", ";
            }
            json << ParamValueToJson(x_values[i]);
        }
        json << "],\n"
             << "  \"y_values\": [";
        for (std::size_t i = 0; i < y_values.size(); ++i) {
            if (i > 0) {
                json << ", ";
            }
            json << ParamValueToJson(y_values[i]);
        }
        json << "],\n"
             << "  \"z_values\": [\n";

        for (std::size_t y = 0; y < y_values.size(); ++y) {
            json << "    [";
            for (std::size_t x = 0; x < x_values.size(); ++x) {
                if (x > 0) {
                    json << ", ";
                }
                const auto bucket_it = buckets.find({x, y});
                if (bucket_it == buckets.end() || bucket_it->second.empty()) {
                    json << "null";
                } else {
                    json << FormatDouble(Mean(bucket_it->second));
                }
            }
            json << "]";
            if (y + 1 < y_values.size()) {
                json << ',';
            }
            json << "\n";
        }

        json << "  ],\n"
             << "  \"objective\": \"" << JsonEscape(report.metric_path) << "\"\n"
             << "}\n";

        const std::filesystem::path output_path =
            base_dir / ("heatmap_" + pair.x_param->name + "_vs_" + pair.y_param->name + ".json");
        if (!WriteTextFile(output_path.string(), json.str(), error)) {
            return false;
        }
    }

    return true;
}

bool ResultAnalyzer::WriteTop10InSampleMarkdown(const OptimizationReport& report,
                                                const std::string& output_path,
                                                std::string* error) {
    std::ostringstream md;
    md << "# In-Sample Top 10\n\n";
    if (!report.task_id.empty()) {
        md << "- task_id: `" << report.task_id << "`\n"
           << "- started_at: `" << report.started_at << "`\n"
           << "- finished_at: `" << report.finished_at << "`\n"
           << "- wall_clock_sec: `" << FormatDouble(report.wall_clock_sec) << "`\n\n";
    }

    const std::vector<const Trial*> completed = SortedCompletedTrials(report);
    if (completed.empty()) {
        md << "没有可用的成功 trial。\n";
        return WriteTextFile(output_path, md.str(), error);
    }

    md << "| Rank | Trial | Objective | Total PnL | MaxDD (%) | Annualized (%) | Sharpe | Calmar | Win Rate (%) | Profit Factor | Trades | Params |\n"
       << "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n";

    const std::size_t top_n = std::min<std::size_t>(10U, completed.size());
    for (std::size_t index = 0; index < top_n; ++index) {
        const Trial& trial = *completed[index];
        md << "| " << (index + 1) << " | " << EscapeMarkdownCell(trial.trial_id) << " | "
           << FormatFixed(trial.objective, 6) << " | "
           << FormatOptionalMetric(trial.metrics.total_pnl, 2) << " | "
           << FormatOptionalMetric(trial.metrics.max_drawdown_pct, 2) << " | "
           << FormatOptionalMetric(trial.metrics.annualized_return_pct, 2) << " | "
           << FormatOptionalMetric(trial.metrics.sharpe_ratio, 2) << " | "
           << FormatOptionalMetric(trial.metrics.calmar_ratio, 2) << " | "
           << FormatOptionalMetric(trial.metrics.win_rate_pct, 2) << " | "
           << FormatOptionalMetric(trial.metrics.profit_factor, 2) << " | "
           << FormatOptionalInteger(trial.metrics.total_trades) << " | "
           << EscapeMarkdownCell(FlattenParams(trial.params)) << " |\n";
    }

    return WriteTextFile(output_path, md.str(), error);
}

bool ResultAnalyzer::WriteBestParamsYaml(const ParamValueMap& best_params,
                                         const std::string& output_path,
                                         std::string* error) {
    std::ostringstream yaml;
    yaml << "params:\n";
    const auto sorted = SortedParams(best_params);
    for (const auto& [key, value] : sorted) {
        yaml << "  " << key << ": " << ParamValueToString(value) << "\n";
    }
    return WriteTextFile(output_path, yaml.str(), error);
}

}  // namespace quant_hft::optim