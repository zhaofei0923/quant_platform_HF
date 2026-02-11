#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/services/execution_planner.h"

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
