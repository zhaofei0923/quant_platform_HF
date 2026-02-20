#include "quant_hft/apps/backtest_metrics.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace quant_hft::apps {
namespace {

TEST(BacktestMetricsTest, ComputeDailyMetricsAggregatesByDayAndTracksDrawdown) {
    std::vector<EquitySample> equity_history;
    equity_history.push_back(
        EquitySample{1704067200000000000LL, "20240101", 100.0, 10.0, "kStrongTrend"});
    equity_history.push_back(
        EquitySample{1704067205000000000LL, "20240101", 110.0, 12.0, "kStrongTrend"});
    equity_history.push_back(
        EquitySample{1704153600000000000LL, "20240102", 90.0, 9.0, "kRanging"});

    std::vector<TradeRecord> trades;
    trades.push_back(
        TradeRecord{"t1", "o1", "rb", "", "Buy", "Open", 1, 100.0, 1704067205000000000LL});
    trades.push_back(
        TradeRecord{"t2", "o2", "rb", "", "Sell", "Close", 1, 90.0, 1704153600000000000LL});

    const std::vector<DailyPerformance> daily =
        ComputeDailyMetrics(equity_history, trades, 100.0);

    ASSERT_EQ(daily.size(), 2U);
    EXPECT_EQ(daily[0].date, "20240101");
    EXPECT_DOUBLE_EQ(daily[0].capital, 110.0);
    EXPECT_DOUBLE_EQ(daily[0].daily_return_pct, 10.0);
    EXPECT_DOUBLE_EQ(daily[0].drawdown_pct, 0.0);
    EXPECT_EQ(daily[0].trades_count, 1);

    EXPECT_EQ(daily[1].date, "20240102");
    EXPECT_DOUBLE_EQ(daily[1].capital, 90.0);
    EXPECT_NEAR(daily[1].daily_return_pct, -18.181818, 1e-5);
    EXPECT_NEAR(daily[1].cumulative_return_pct, -10.0, 1e-8);
    EXPECT_NEAR(daily[1].drawdown_pct, 18.181818, 1e-5);
    EXPECT_EQ(daily[1].trades_count, 1);
}

TEST(BacktestMetricsTest, ComputeRiskMetricsReturnsNonZeroForVolatileSeries) {
    std::vector<DailyPerformance> daily = {
        DailyPerformance{"20240101", 100.0, 0.0, 0.0, 0.0},
        DailyPerformance{"20240102", 110.0, 10.0, 10.0, 0.0},
        DailyPerformance{"20240103", 90.0, -18.181818, -10.0, 18.181818},
    };

    const RiskMetrics metrics = ComputeRiskMetrics(daily);
    EXPECT_GT(metrics.var_95, 0.0);
    EXPECT_GT(metrics.expected_shortfall_95, 0.0);
    EXPECT_GT(metrics.ulcer_index, 0.0);
    EXPECT_GT(metrics.tail_loss, 0.0);
}

TEST(BacktestMetricsTest, ComputeExecutionQualityTracksRatesAndSlippageStats) {
    std::vector<OrderRecord> orders = {
        OrderRecord{"o1", "c1", "rb", "Limit", "Buy", "Open", 100.0, 1, "Filled", 1, 100.0,
                    1704067200000000000LL, 1704067200100000000LL, "demo", ""},
        OrderRecord{"o2", "c2", "rb", "Limit", "Sell", "Close", 101.0, 1, "Canceled", 0, 0.0,
                    1704067201000000000LL, 1704067201100000000LL, "demo", ""},
    };
    std::vector<TradeRecord> trades = {
        TradeRecord{"t1", "o1", "rb", "", "Buy", "Open", 1, 100.0, 1704067200100000000LL, 0.0,
                    0.5, 1.0},
        TradeRecord{"t2", "o3", "rb", "", "Sell", "Close", 1, 101.0, 1704067200200000000LL, 0.0,
                    1.5, -1.0},
    };

    const ExecutionQuality quality = ComputeExecutionQuality(orders, trades);
    EXPECT_DOUBLE_EQ(quality.limit_order_fill_rate, 0.5);
    EXPECT_DOUBLE_EQ(quality.cancel_rate, 0.5);
    EXPECT_NEAR(quality.avg_wait_time_ms, 100.0, 1e-8);
    EXPECT_NEAR(quality.slippage_mean, 1.0, 1e-8);
    EXPECT_EQ(quality.slippage_percentiles.size(), 3U);
}

TEST(BacktestMetricsTest, ComputeRollingMetricsProducesSeriesWithInputLength) {
    std::vector<DailyPerformance> daily;
    for (int day = 1; day <= 8; ++day) {
        DailyPerformance perf;
        perf.date = "2024010" + std::to_string(day);
        perf.capital = 100.0 + static_cast<double>(day);
        perf.daily_return_pct = 1.0;
        daily.push_back(perf);
    }

    const RollingMetrics rolling = ComputeRollingMetrics(daily, 3);
    ASSERT_EQ(rolling.rolling_sharpe_3m.size(), daily.size());
    ASSERT_EQ(rolling.rolling_max_dd_3m.size(), daily.size());
}

}  // namespace
}  // namespace quant_hft::apps
