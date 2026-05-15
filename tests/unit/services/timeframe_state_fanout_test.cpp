#include "quant_hft/services/timeframe_state_fanout.h"

#include <gtest/gtest.h>

#include <string>

namespace quant_hft {
namespace {

BarSnapshot MakeOneMinuteBar(const std::string& minute, double open, double high, double low,
                             double close, std::int64_t volume, EpochNanos ts_ns) {
    BarSnapshot bar;
    bar.instrument_id = "DCE.c2605";
    bar.exchange_id = "DCE";
    bar.trading_day = "20260515";
    bar.action_day = "20260515";
    bar.minute = minute;
    bar.open = open;
    bar.high = high;
    bar.low = low;
    bar.close = close;
    bar.analysis_open = open;
    bar.analysis_high = high;
    bar.analysis_low = low;
    bar.analysis_close = close;
    bar.volume = volume;
    bar.ts_ns = ts_ns;
    return bar;
}

}  // namespace

TEST(TimeframeStateFanoutTest, EmitsClosedFiveMinuteBucketOnNextBucket) {
    TimeframeStateFanout fanout({5});

    for (int i = 0; i < 5; ++i) {
        const std::string minute = "20260515 09:0" + std::to_string(i);
        const auto emissions = fanout.OnOneMinuteBar(
            MakeOneMinuteBar(minute, 100.0 + i, 101.0 + i, 99.0 + i, 100.5 + i, 10 + i, i + 1));
        EXPECT_TRUE(emissions.empty());
    }

    const auto emissions = fanout.OnOneMinuteBar(
        MakeOneMinuteBar("20260515 09:05", 105.0, 106.0, 104.0, 105.5, 20, 6));
    ASSERT_EQ(emissions.size(), 1U);
    const auto& emitted = emissions.front();
    EXPECT_EQ(emitted.timeframe_minutes, 5);
    EXPECT_EQ(emitted.bar.minute, "20260515 09:00");
    EXPECT_DOUBLE_EQ(emitted.bar.open, 100.0);
    EXPECT_DOUBLE_EQ(emitted.bar.high, 105.0);
    EXPECT_DOUBLE_EQ(emitted.bar.low, 99.0);
    EXPECT_DOUBLE_EQ(emitted.bar.close, 104.5);
    EXPECT_EQ(emitted.bar.volume, 60);
    EXPECT_EQ(emitted.state.timeframe_minutes, 5);
    EXPECT_TRUE(emitted.state.has_bar);
    EXPECT_DOUBLE_EQ(emitted.state.effective_bar_close(), 104.5);
}

TEST(TimeframeStateFanoutTest, FlushEmitsUnfinishedBucketOnce) {
    TimeframeStateFanout fanout({5});
    EXPECT_TRUE(
        fanout
            .OnOneMinuteBar(MakeOneMinuteBar("20260515 11:30", 200.0, 201.0, 199.0, 200.5, 12, 10))
            .empty());

    const auto first_flush = fanout.Flush();
    ASSERT_EQ(first_flush.size(), 1U);
    EXPECT_EQ(first_flush.front().bar.minute, "20260515 11:30");
    EXPECT_EQ(first_flush.front().state.timeframe_minutes, 5);

    const auto second_flush = fanout.Flush();
    EXPECT_TRUE(second_flush.empty());
}

TEST(TimeframeStateFanoutTest, RestoresPartialBucketFromState) {
    TimeframeStateFanout fanout({5});
    EXPECT_TRUE(
        fanout.OnOneMinuteBar(MakeOneMinuteBar("20260515 09:00", 100.0, 101.0, 99.0, 100.5, 10, 1))
            .empty());

    TimeframeStateFanout::PersistenceState state;
    std::string error;
    ASSERT_TRUE(fanout.SaveState(&state, &error)) << error;

    TimeframeStateFanout restored({5});
    ASSERT_TRUE(restored.LoadState(state, &error)) << error;
    EXPECT_TRUE(
        restored
            .OnOneMinuteBar(MakeOneMinuteBar("20260515 09:01", 100.5, 102.0, 100.0, 101.5, 11, 2))
            .empty());
    const auto flushed = restored.Flush();
    ASSERT_EQ(flushed.size(), 1U);
    EXPECT_EQ(flushed.front().bar.minute, "20260515 09:00");
    EXPECT_DOUBLE_EQ(flushed.front().bar.open, 100.0);
    EXPECT_DOUBLE_EQ(flushed.front().bar.high, 102.0);
    EXPECT_DOUBLE_EQ(flushed.front().bar.low, 99.0);
    EXPECT_DOUBLE_EQ(flushed.front().bar.close, 101.5);
    EXPECT_EQ(flushed.front().bar.volume, 21);
}

}  // namespace quant_hft
