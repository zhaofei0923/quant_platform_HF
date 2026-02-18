#include "quant_hft/apps/backtest_replay_support.h"

#include <gtest/gtest.h>

namespace quant_hft::apps {

TEST(BacktestReplaySupportTest, BuildStateSnapshotFromBarPopulatesBarFields) {
    ReplayTick first;
    first.instrument_id = "SHFE.ag2406";
    first.last_price = 100.0;
    first.volume = 100;
    first.bid_volume_1 = 20;
    first.ask_volume_1 = 18;

    ReplayTick last = first;
    last.last_price = 105.0;
    last.volume = 160;
    last.bid_volume_1 = 25;
    last.ask_volume_1 = 22;
    last.ts_ns = 456;

    const StateSnapshot7D state = BuildStateSnapshotFromBar(first, last, 106.0, 99.0, 60, 456);

    EXPECT_EQ(state.instrument_id, "SHFE.ag2406");
    EXPECT_DOUBLE_EQ(state.bar_open, 100.0);
    EXPECT_DOUBLE_EQ(state.bar_high, 106.0);
    EXPECT_DOUBLE_EQ(state.bar_low, 99.0);
    EXPECT_DOUBLE_EQ(state.bar_close, 105.0);
    EXPECT_DOUBLE_EQ(state.bar_volume, 60.0);
    EXPECT_TRUE(state.has_bar);
    EXPECT_EQ(state.ts_ns, 456);
}

}  // namespace quant_hft::apps
