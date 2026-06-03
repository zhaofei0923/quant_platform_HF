#include "quant_hft/services/execution_planner.h"

#include <gtest/gtest.h>

#include <vector>

namespace quant_hft {
namespace {

SignalIntent MakeSignal(std::int32_t volume, const std::string& trace_id = "trace-1") {
    SignalIntent signal;
    signal.strategy_id = "demo";
    signal.instrument_id = "SHFE.ag2406";
    signal.side = Side::kBuy;
    signal.offset = OffsetFlag::kOpen;
    signal.volume = volume;
    signal.limit_price = 4500.0;
    signal.ts_ns = 100;
    signal.trace_id = trace_id;
    return signal;
}

MarketSnapshot MakeMarket(double bid, double ask) {
    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.ag2406";
    snapshot.bid_price_1 = bid;
    snapshot.ask_price_1 = ask;
    snapshot.volume = 100;
    return snapshot;
}

TEST(ExecutionPlannerTest, BuildsDirectPlanAsSingleOrder) {
    ExecutionPlanner planner;
    ExecutionConfig cfg;
    cfg.algo = ExecutionAlgo::kDirect;

    const auto plan = planner.BuildPlan(MakeSignal(5), "acc-1", cfg, {});
    ASSERT_EQ(plan.size(), 1U);
    EXPECT_EQ(plan[0].intent.volume, 5);
    EXPECT_EQ(plan[0].execution_algo_id, "direct");
    EXPECT_EQ(plan[0].slice_index, 1);
    EXPECT_EQ(plan[0].slice_total, 1);
    EXPECT_EQ(plan[0].intent.client_order_id, "trace-1");
    EXPECT_EQ(plan[0].intent.trace_id, "trace-1");
    EXPECT_DOUBLE_EQ(plan[0].intent.price, 4500.0);
}

TEST(ExecutionPlannerTest, MarketableLimitUsesAskForBuyAndBidForSell) {
    ExecutionPlanner planner;
    ExecutionConfig cfg;
    cfg.price_mode = ExecutionPriceMode::kMarketableLimit;
    const std::vector<MarketSnapshot> recent_market{MakeMarket(4499.0, 4501.0)};

    SignalIntent buy = MakeSignal(5, "trace-buy");
    buy.side = Side::kBuy;
    buy.offset = OffsetFlag::kClose;
    auto buy_plan = planner.BuildPlan(buy, "acc-1", cfg, recent_market);
    ASSERT_EQ(buy_plan.size(), 1U);
    EXPECT_DOUBLE_EQ(buy_plan[0].intent.price, 4501.0);
    EXPECT_EQ(buy_plan[0].intent.offset, OffsetFlag::kClose);

    SignalIntent sell = MakeSignal(5, "trace-sell");
    sell.side = Side::kSell;
    sell.offset = OffsetFlag::kOpen;
    auto sell_plan = planner.BuildPlan(sell, "acc-1", cfg, recent_market);
    ASSERT_EQ(sell_plan.size(), 1U);
    EXPECT_DOUBLE_EQ(sell_plan[0].intent.price, 4499.0);
    EXPECT_EQ(sell_plan[0].intent.offset, OffsetFlag::kOpen);
}

TEST(ExecutionPlannerTest, MarketableLimitRequiresValidQuote) {
    ExecutionPlanner planner;
    ExecutionConfig cfg;
    cfg.price_mode = ExecutionPriceMode::kMarketableLimit;

    EXPECT_TRUE(planner.BuildPlan(MakeSignal(5), "acc-1", cfg, {}).empty());
    EXPECT_TRUE(planner.BuildPlan(MakeSignal(5), "acc-1", cfg, {MakeMarket(0.0, 0.0)}).empty());
}

TEST(ExecutionPlannerTest, ReturnsEmptyPlanWhenTraceIdIsMissing) {
    ExecutionPlanner planner;
    ExecutionConfig cfg;
    cfg.algo = ExecutionAlgo::kDirect;

    const auto plan = planner.BuildPlan(MakeSignal(5, ""), "acc-1", cfg, {});
    EXPECT_TRUE(plan.empty());
}

TEST(ExecutionPlannerTest, BuildsSlicedPlanWithDeterministicIds) {
    ExecutionPlanner planner;
    ExecutionConfig cfg;
    cfg.algo = ExecutionAlgo::kSliced;
    cfg.slice_size = 2;

    const auto plan = planner.BuildPlan(MakeSignal(5, "trace-xyz"), "acc-1", cfg, {});
    ASSERT_EQ(plan.size(), 3U);
    EXPECT_EQ(plan[0].intent.client_order_id, "trace-xyz#slice-1");
    EXPECT_EQ(plan[1].intent.client_order_id, "trace-xyz#slice-2");
    EXPECT_EQ(plan[2].intent.client_order_id, "trace-xyz#slice-3");
    EXPECT_EQ(plan[2].intent.volume, 1);
    EXPECT_EQ(plan[2].slice_total, 3);
}

TEST(ExecutionPlannerTest, FallsBackToUniformPlanWhenVwapInputIsMissing) {
    ExecutionPlanner planner;
    ExecutionConfig cfg;
    cfg.algo = ExecutionAlgo::kVwapLite;
    cfg.slice_size = 2;

    const auto plan = planner.BuildPlan(MakeSignal(4), "acc-1", cfg, {});
    ASSERT_EQ(plan.size(), 2U);
    EXPECT_EQ(plan[0].execution_algo_id, "vwap_lite");
    EXPECT_EQ(plan[0].intent.volume, 2);
    EXPECT_EQ(plan[1].intent.volume, 2);
}

TEST(ExecutionPlannerTest, UsesRejectRatioThresholdForThrottleDecision) {
    ExecutionPlanner planner(10);
    for (int i = 0; i < 10; ++i) {
        planner.RecordOrderResult(i < 6);
    }

    EXPECT_TRUE(planner.ShouldThrottle(0.5));
    EXPECT_FALSE(planner.ShouldThrottle(0.8));
}

}  // namespace
}  // namespace quant_hft
