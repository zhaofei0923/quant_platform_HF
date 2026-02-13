#include <gtest/gtest.h>

#include "quant_hft/backtest/engine.h"
#include "quant_hft/backtest/performance.h"

namespace quant_hft::backtest {

TEST(PerformanceTest, AnalyzePerformanceComputesCoreMetrics) {
    BacktestResult result;

    EquityPoint p1;
    p1.time = Timestamp(1);
    p1.balance = 1000.0;
    result.equity_curve.push_back(p1);

    EquityPoint p2;
    p2.time = Timestamp(2);
    p2.balance = 1100.0;
    result.equity_curve.push_back(p2);

    EquityPoint p3;
    p3.time = Timestamp(3);
    p3.balance = 990.0;
    result.equity_curve.push_back(p3);

    EquityPoint p4;
    p4.time = Timestamp(4);
    p4.balance = 1210.0;
    result.equity_curve.push_back(p4);

    Order order;
    order.status = OrderStatus::kFilled;
    result.orders.push_back(order);

    Trade trade1;
    trade1.commission = 1.5;
    result.trades.push_back(trade1);

    Trade trade2;
    trade2.commission = 0.5;
    result.trades.push_back(trade2);

    const BacktestPerformanceSummary summary = AnalyzePerformance(result);

    EXPECT_DOUBLE_EQ(summary.initial_balance, 1000.0);
    EXPECT_DOUBLE_EQ(summary.final_balance, 1210.0);
    EXPECT_DOUBLE_EQ(summary.net_profit, 210.0);
    EXPECT_NEAR(summary.total_return, 0.21, 1e-12);
    EXPECT_DOUBLE_EQ(summary.max_drawdown, 110.0);
    EXPECT_NEAR(summary.max_drawdown_ratio, 0.1, 1e-12);
    EXPECT_EQ(summary.order_count, 1U);
    EXPECT_EQ(summary.trade_count, 2U);
    EXPECT_DOUBLE_EQ(summary.commission_paid, 2.0);
    EXPECT_GE(summary.return_volatility, 0.0);
}

TEST(PerformanceTest, AnalyzePerformanceHandlesEmptyResult) {
    const BacktestPerformanceSummary summary = AnalyzePerformance(BacktestResult{});

    EXPECT_DOUBLE_EQ(summary.initial_balance, 0.0);
    EXPECT_DOUBLE_EQ(summary.final_balance, 0.0);
    EXPECT_DOUBLE_EQ(summary.net_profit, 0.0);
    EXPECT_DOUBLE_EQ(summary.total_return, 0.0);
    EXPECT_EQ(summary.order_count, 0U);
    EXPECT_EQ(summary.trade_count, 0U);
    EXPECT_DOUBLE_EQ(summary.commission_paid, 0.0);
}

}  // namespace quant_hft::backtest
