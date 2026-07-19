#include "quant_hft/services/market_bar_pipeline.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <ctime>
#include <filesystem>
#include <string>
#include <vector>

namespace quant_hft {
namespace {

EpochNanos ShanghaiEpochNs(const std::string& day, int hour, int minute, int second = 0,
                           int millis = 0) {
    std::tm local_tm{};
    local_tm.tm_year = std::stoi(day.substr(0, 4)) - 1900;
    local_tm.tm_mon = std::stoi(day.substr(4, 2)) - 1;
    local_tm.tm_mday = std::stoi(day.substr(6, 2));
    local_tm.tm_hour = hour;
    local_tm.tm_min = minute;
    local_tm.tm_sec = second;
    return (static_cast<EpochNanos>(timegm(&local_tm)) - 8LL * 60LL * 60LL) * 1'000'000'000LL +
           static_cast<EpochNanos>(millis) * 1'000'000LL;
}

MarketSnapshot MakeTick(int minute, int second, std::int64_t volume, double price,
                        EpochNanos recv_ts_ns = 0) {
    char update_time[16];
    std::snprintf(update_time, sizeof(update_time), "09:%02d:%02d", minute, second);
    MarketSnapshot snapshot;
    snapshot.instrument_id = "DCE.c2609";
    snapshot.exchange_id = "DCE";
    snapshot.trading_day = "20260710";
    snapshot.action_day = "20260710";
    snapshot.update_time = update_time;
    snapshot.last_price = price;
    snapshot.bid_price_1 = price - 1.0;
    snapshot.ask_price_1 = price + 1.0;
    snapshot.volume = volume;
    snapshot.exchange_ts_ns = ShanghaiEpochNs("20260710", 9, minute, second);
    snapshot.recv_ts_ns = recv_ts_ns > 0 ? recv_ts_ns : snapshot.exchange_ts_ns;
    return snapshot;
}

MarketBarPipelineConfig MakeConfig() {
    MarketBarPipelineConfig config;
    config.bar_aggregator.allowed_lateness_ms = 3500;
    config.timeframes = {5};
    return config;
}

void FeedAndFinalizeMinute(MarketBarPipeline* pipeline, int minute, std::int64_t volume,
                           std::vector<TimeframeStateEmission>* emissions = nullptr) {
    ASSERT_NE(pipeline, nullptr);
    (void)pipeline->OnTick(MakeTick(minute, 10, volume, 100.0 + minute));
    const auto result = pipeline->AdvanceWatermark(ShanghaiEpochNs("20260710", 9, minute + 1, 4));
    if (emissions != nullptr) {
        emissions->insert(emissions->end(), result.timeframe_emissions.begin(),
                          result.timeframe_emissions.end());
    }
}

}  // namespace

TEST(MarketBarPipelineTest, ExactPayloadDuplicateIsIdempotent) {
    MarketBarPipeline pipeline(MakeConfig());
    const MarketSnapshot tick = MakeTick(0, 10, 100, 100.0);
    EXPECT_FALSE(pipeline.OnTick(tick).duplicate_tick);
    EXPECT_TRUE(pipeline.OnTick(tick).duplicate_tick);

    const auto result = pipeline.AdvanceWatermark(ShanghaiEpochNs("20260710", 9, 1, 4));
    ASSERT_EQ(result.one_minute_bars.size(), 1U);
    EXPECT_DOUBLE_EQ(result.one_minute_bars[0].open, 100.0);
    EXPECT_DOUBLE_EQ(result.one_minute_bars[0].close, 100.0);
    EXPECT_EQ(result.one_minute_bars[0].volume, 0);
}

TEST(MarketBarPipelineTest, CheckpointRestartMatchesContinuousPendingAggregation) {
    MarketBarPipeline continuous(MakeConfig());
    FeedAndFinalizeMinute(&continuous, 0, 100);
    (void)continuous.OnTick(MakeTick(1, 10, 110, 101.0));

    MarketBarPipeline::PersistenceState checkpoint;
    std::string error;
    ASSERT_TRUE(continuous.SaveState(&checkpoint, &error)) << error;

    MarketBarPipeline restored(MakeConfig());
    ASSERT_TRUE(restored.LoadState(checkpoint, &error)) << error;

    std::vector<TimeframeStateEmission> continuous_emissions;
    std::vector<TimeframeStateEmission> restored_emissions;
    for (int minute = 2; minute <= 4; ++minute) {
        (void)continuous.OnTick(MakeTick(minute, 10, 100 + minute * 10, 100.0 + minute));
        (void)restored.OnTick(MakeTick(minute, 10, 100 + minute * 10, 100.0 + minute));
    }
    const EpochNanos finalize_ts = ShanghaiEpochNs("20260710", 9, 5, 4);
    const auto continuous_result = continuous.AdvanceWatermark(finalize_ts);
    const auto restored_result = restored.AdvanceWatermark(finalize_ts);
    continuous_emissions = continuous_result.timeframe_emissions;
    restored_emissions = restored_result.timeframe_emissions;

    ASSERT_EQ(continuous_result.one_minute_bars.size(), restored_result.one_minute_bars.size());
    ASSERT_EQ(continuous_emissions.size(), 1U);
    ASSERT_EQ(restored_emissions.size(), 1U);
    const auto& lhs = continuous_emissions.front().bar;
    const auto& rhs = restored_emissions.front().bar;
    EXPECT_EQ(lhs.minute, rhs.minute);
    EXPECT_DOUBLE_EQ(lhs.open, rhs.open);
    EXPECT_DOUBLE_EQ(lhs.high, rhs.high);
    EXPECT_DOUBLE_EQ(lhs.low, rhs.low);
    EXPECT_DOUBLE_EQ(lhs.close, rhs.close);
    EXPECT_EQ(lhs.volume, rhs.volume);
    EXPECT_EQ(lhs.expected_source_bars, rhs.expected_source_bars);
    EXPECT_EQ(lhs.observed_source_bars, rhs.observed_source_bars);
}

TEST(MarketBarPipelineTest, RecentCanonicalFiveMinuteStatesSurviveCheckpoint) {
    MarketBarPipeline pipeline(MakeConfig());
    for (int minute = 0; minute <= 9; ++minute) {
        FeedAndFinalizeMinute(&pipeline, minute, 100 + minute * 10);
    }
    const auto recent = pipeline.RecentCompleteStates("DCE.c2609", 5, 30);
    ASSERT_EQ(recent.size(), 1U);
    EXPECT_TRUE(recent.front().has_bar);
    EXPECT_EQ(recent.front().timeframe_minutes, 5);

    MarketBarPipeline::PersistenceState checkpoint;
    std::string error;
    ASSERT_TRUE(pipeline.SaveState(&checkpoint, &error)) << error;
    MarketBarPipeline restored(MakeConfig());
    ASSERT_TRUE(restored.LoadState(checkpoint, &error)) << error;
    const auto restored_recent = restored.RecentCompleteStates("DCE.c2609", 5, 30);
    ASSERT_EQ(restored_recent.size(), 1U);
    EXPECT_EQ(restored_recent.front().instrument_id, "DCE.c2609");
    EXPECT_EQ(restored_recent.front().ts_ns, recent.front().ts_ns);
    EXPECT_DOUBLE_EQ(restored_recent.front().bar_close, recent.front().bar_close);
}

TEST(MarketBarPipelineTest, RecoveryTailNeverProducesTradableEmission) {
    MarketBarPipeline source(MakeConfig());
    FeedAndFinalizeMinute(&source, 0, 100);
    const MarketSnapshot already_seen = MakeTick(1, 10, 110, 101.0);
    (void)source.OnTick(already_seen);

    MarketBarPipeline::PersistenceState checkpoint;
    std::string error;
    ASSERT_TRUE(source.SaveState(&checkpoint, &error)) << error;

    MarketBarPipeline recovered(MakeConfig());
    MarketBarPipelineResult result;
    ASSERT_TRUE(
        recovered.Recover(checkpoint, {already_seen, MakeTick(2, 10, 120, 102.0)}, &result, &error))
        << error;
    EXPECT_TRUE(result.recovery_replay);
    ASSERT_FALSE(result.one_minute_bars.empty());
    for (const auto& bar : result.one_minute_bars) {
        EXPECT_TRUE(bar.is_recovery_replay);
        EXPECT_FALSE(bar.strategy_eligible);
    }
    for (const auto& emission : result.timeframe_emissions) {
        EXPECT_TRUE(emission.bar.is_recovery_replay);
        EXPECT_FALSE(emission.strategy_eligible);
        EXPECT_FALSE(emission.state.has_bar);
    }
}

TEST(MarketBarPipelineTest, LateTickSuppressesOpeningUntilTwoCompleteFiveMinuteBars) {
    MarketBarPipeline pipeline(MakeConfig());
    std::vector<TimeframeStateEmission> emissions;
    for (int minute = 0; minute <= 4; ++minute) {
        FeedAndFinalizeMinute(&pipeline, minute, 100 + minute * 10, &emissions);
    }
    ASSERT_EQ(emissions.size(), 1U);

    MarketSnapshot late = MakeTick(2, 30, 125, 88.0, ShanghaiEpochNs("20260710", 9, 5, 5));
    const auto late_result = pipeline.OnTick(late);
    EXPECT_TRUE(late_result.late_tick);
    EXPECT_TRUE(pipeline.IsOpeningSuppressed("DCE.c2609"));

    emissions.clear();
    for (int minute = 5; minute <= 9; ++minute) {
        FeedAndFinalizeMinute(&pipeline, minute, 200 + minute * 10, &emissions);
    }
    ASSERT_EQ(emissions.size(), 1U);
    EXPECT_TRUE(emissions[0].bar.is_complete);
    EXPECT_FALSE(emissions[0].strategy_eligible);
    EXPECT_TRUE(pipeline.IsOpeningSuppressed("DCE.c2609"));

    emissions.clear();
    for (int minute = 10; minute <= 14; ++minute) {
        FeedAndFinalizeMinute(&pipeline, minute, 300 + minute * 10, &emissions);
    }
    ASSERT_EQ(emissions.size(), 1U);
    EXPECT_TRUE(emissions[0].bar.is_complete);
    EXPECT_FALSE(emissions[0].strategy_eligible);
    EXPECT_FALSE(pipeline.IsOpeningSuppressed("DCE.c2609"));

    emissions.clear();
    for (int minute = 15; minute <= 19; ++minute) {
        FeedAndFinalizeMinute(&pipeline, minute, 400 + minute * 10, &emissions);
    }
    ASSERT_EQ(emissions.size(), 1U);
    EXPECT_TRUE(emissions[0].strategy_eligible);
}

TEST(MarketBarPipelineTest, AtomicCheckpointRoundTripPreservesPendingState) {
    MarketBarPipeline source(MakeConfig());
    (void)source.OnTick(MakeTick(0, 10, 100, 100.0));
    const auto path = std::filesystem::temp_directory_path() /
                      "quant_hft_market_bar_pipeline_checkpoint_v2.state";
    std::error_code ec;
    std::filesystem::remove(path, ec);
    std::string error;
    ASSERT_TRUE(source.SaveCheckpointAtomically(path.string(), &error)) << error;

    MarketBarPipeline restored(MakeConfig());
    ASSERT_TRUE(restored.LoadCheckpointFile(path.string(), &error)) << error;
    (void)restored.OnTick(MakeTick(0, 50, 110, 102.0));
    const auto result = restored.AdvanceWatermark(ShanghaiEpochNs("20260710", 9, 1, 4));
    ASSERT_EQ(result.one_minute_bars.size(), 1U);
    EXPECT_DOUBLE_EQ(result.one_minute_bars[0].open, 100.0);
    EXPECT_DOUBLE_EQ(result.one_minute_bars[0].close, 102.0);
    EXPECT_EQ(result.one_minute_bars[0].volume, 10);

    std::filesystem::remove(path, ec);
}

TEST(MarketBarPipelineTest, CorruptNestedCheckpointDoesNotPartiallyMutateLiveState) {
    MarketBarPipeline pipeline(MakeConfig());
    (void)pipeline.OnTick(MakeTick(0, 10, 100, 100.0));
    MarketBarPipeline::PersistenceState before;
    std::string error;
    ASSERT_TRUE(pipeline.SaveState(&before, &error)) << error;

    auto corrupt = before;
    ASSERT_TRUE(corrupt.find("fanout.version") != corrupt.end());
    corrupt["fanout.version"] = "corrupt";
    EXPECT_FALSE(pipeline.LoadState(corrupt, &error));

    MarketBarPipeline::PersistenceState after;
    ASSERT_TRUE(pipeline.SaveState(&after, &error)) << error;
    EXPECT_EQ(after, before);
}

}  // namespace quant_hft
