#include "quant_hft/rolling/metric_extractor.h"

#include <string>

#include "quant_hft/optim/result_analyzer.h"

namespace quant_hft::rolling {

bool ExtractMetricFromResult(const quant_hft::apps::BacktestCliResult& result,
                             const std::string& metric_path,
                             double* out,
                             std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "metric output is null";
        }
        return false;
    }

    const std::string resolved = quant_hft::optim::ResultAnalyzer::ResolveMetricPathAlias(metric_path);
    const quant_hft::apps::BacktestSummary summary = quant_hft::apps::SummarizeBacktest(result);

    if (resolved == "summary.total_pnl") {
        *out = summary.total_pnl;
        return true;
    }
    if (resolved == "summary.max_drawdown") {
        *out = summary.max_drawdown;
        return true;
    }
    if (resolved == "summary.order_events") {
        *out = static_cast<double>(summary.order_events);
        return true;
    }
    if (resolved == "summary.intents_emitted") {
        *out = static_cast<double>(summary.intents_emitted);
        return true;
    }
    if (resolved == "hf_standard.advanced_summary.profit_factor") {
        *out = result.advanced_summary.profit_factor;
        return true;
    }
    if (resolved == "hf_standard.risk_metrics.var_95") {
        *out = result.risk_metrics.var_95;
        return true;
    }
    if (resolved == "hf_standard.execution_quality.limit_order_fill_rate") {
        *out = result.execution_quality.limit_order_fill_rate;
        return true;
    }
    if (resolved == "final_equity") {
        *out = result.final_equity;
        return true;
    }
    if (resolved == "initial_equity") {
        *out = result.initial_equity;
        return true;
    }

    const std::string json = quant_hft::apps::RenderBacktestJson(result);
    std::string local_error;
    const double value =
        quant_hft::optim::ResultAnalyzer::ExtractMetricFromJsonText(json, resolved, &local_error);
    if (!local_error.empty()) {
        if (error != nullptr) {
            *error = local_error;
        }
        return false;
    }

    *out = value;
    return true;
}

}  // namespace quant_hft::rolling

