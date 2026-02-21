#include "quant_hft/rolling/metric_extractor.h"

#include <gtest/gtest.h>

namespace quant_hft::rolling {
namespace {

quant_hft::apps::BacktestCliResult BuildResult() {
    quant_hft::apps::BacktestCliResult result;
    result.run_id = "metric-test";
    result.mode = "backtest";
    result.engine_mode = "parquet";
    result.data_source = "parquet";
    result.initial_equity = 1000000.0;
    result.final_equity = 1010000.0;
    result.spec.initial_equity = 1000000.0;
    result.spec.emit_trades = false;
    result.spec.emit_orders = false;
    result.spec.emit_position_history = false;

    result.has_deterministic = true;
    result.deterministic.order_events_emitted = 12;
    result.deterministic.performance.total_pnl = 10000.0;
    result.deterministic.performance.max_drawdown = -1200.0;

    result.advanced_summary.profit_factor = 1.8;
    result.risk_metrics.var_95 = -2.3;
    result.execution_quality.limit_order_fill_rate = 0.75;
    return result;
}

TEST(MetricExtractorTest, ExtractsDirectPaths) {
    const auto result = BuildResult();

    double value = 0.0;
    std::string error;
    ASSERT_TRUE(ExtractMetricFromResult(result, "summary.total_pnl", &value, &error)) << error;
    EXPECT_DOUBLE_EQ(value, 10000.0);

    ASSERT_TRUE(ExtractMetricFromResult(result, "hf_standard.profit_factor", &value, &error))
        << error;
    EXPECT_DOUBLE_EQ(value, 1.8);

    ASSERT_TRUE(ExtractMetricFromResult(result, "hf_standard.risk_metrics.var_95", &value, &error))
        << error;
    EXPECT_DOUBLE_EQ(value, -2.3);
}

TEST(MetricExtractorTest, FallsBackToJsonPathForUnknownDirectMapping) {
    const auto result = BuildResult();

    double value = 0.0;
    std::string error;
    ASSERT_TRUE(ExtractMetricFromResult(result, "spec.initial_equity", &value, &error)) << error;
    EXPECT_DOUBLE_EQ(value, 1000000.0);
}

TEST(MetricExtractorTest, ReportsErrorForMissingMetricPath) {
    const auto result = BuildResult();

    double value = 0.0;
    std::string error;
    EXPECT_FALSE(ExtractMetricFromResult(result, "hf_standard.not_exists.field", &value, &error));
    EXPECT_FALSE(error.empty());
}

}  // namespace
}  // namespace quant_hft::rolling

