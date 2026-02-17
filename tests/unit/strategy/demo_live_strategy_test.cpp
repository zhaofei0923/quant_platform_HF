#include "quant_hft/strategy/demo_live_strategy.h"

#include <gtest/gtest.h>

namespace quant_hft {

TEST(DemoLiveStrategyTest, EmitsBuyIntentWhenTrendScoreIsNonNegative) {
    DemoLiveStrategy strategy;
    StrategyContext context;
    context.strategy_id = "demo-alpha";
    strategy.Initialize(context);

    StateSnapshot7D state;
    state.instrument_id = "SHFE.ag2406";
    state.trend.score = 0.5;
    state.ts_ns = 101;

    const auto intents = strategy.OnState(state);
    ASSERT_EQ(intents.size(), 1U);

    const auto& intent = intents.front();
    EXPECT_EQ(intent.strategy_id, "demo-alpha");
    EXPECT_EQ(intent.instrument_id, "SHFE.ag2406");
    EXPECT_EQ(intent.side, Side::kBuy);
    EXPECT_EQ(intent.offset, OffsetFlag::kOpen);
    EXPECT_EQ(intent.volume, 1);
    EXPECT_DOUBLE_EQ(intent.limit_price, 4500.0);
    EXPECT_EQ(intent.ts_ns, 101);
    EXPECT_EQ(intent.trace_id, "demo-alpha-SHFE.ag2406-101-1");

    strategy.Shutdown();
}

TEST(DemoLiveStrategyTest, EmitsSellIntentWhenTrendScoreIsNegativeAndIncrementsTraceCounter) {
    DemoLiveStrategy strategy;
    StrategyContext context;
    context.strategy_id = "demo-beta";
    strategy.Initialize(context);

    StateSnapshot7D first_state;
    first_state.instrument_id = "SHFE.rb2405";
    first_state.trend.score = 1.0;
    first_state.ts_ns = 201;
    (void)strategy.OnState(first_state);

    StateSnapshot7D second_state;
    second_state.instrument_id = "SHFE.rb2405";
    second_state.trend.score = -0.1;
    second_state.ts_ns = 202;

    const auto intents = strategy.OnState(second_state);
    ASSERT_EQ(intents.size(), 1U);
    EXPECT_EQ(intents.front().side, Side::kSell);
    EXPECT_EQ(intents.front().trace_id, "demo-beta-SHFE.rb2405-202-2");

    strategy.Shutdown();
}

}  // namespace quant_hft
