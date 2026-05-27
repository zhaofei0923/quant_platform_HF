#include "quant_hft/services/timeframe_state_fanout.h"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdio>
#include <string>
#include <vector>

namespace quant_hft {
namespace {

BarSnapshot MakeOneMinuteBar(const std::string& minute, double open, double high, double low,
                             double close, std::int64_t volume, EpochNanos ts_ns,
                             const std::string& instrument_id = "DCE.c2605") {
    BarSnapshot bar;
    bar.instrument_id = instrument_id;
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

std::string FormatMinute(int minute_index) {
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "20260515 %02d:%02d", 9 + minute_index / 60,
                  minute_index % 60);
    return std::string(buffer);
}

std::vector<TimeframeStateEmission> FeedTrendingMinutes(TimeframeStateFanout* fanout,
                                                        const std::string& instrument_id,
                                                        int total_minutes, double base_price) {
    std::vector<TimeframeStateEmission> emissions;
    for (int i = 0; i < total_minutes; ++i) {
        const double close = base_price + static_cast<double>(i);
        const auto batch = fanout->OnOneMinuteBar(MakeOneMinuteBar(FormatMinute(i), close - 0.5,
                                                                   close + 1.0, close - 1.0, close,
                                                                   10 + i, i + 1, instrument_id));
        emissions.insert(emissions.end(), batch.begin(), batch.end());
    }
    return emissions;
}

MarketStateDetectorConfig MakeCanaryDetectorConfig() {
    MarketStateDetectorConfig config;
    config.adx_period = 14;
    config.atr_period = 3;
    config.kama_er_period = 3;
    config.require_adx_for_trend = false;
    return config;
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

TEST(TimeframeStateFanoutTest, ResetInstrumentBucketsPreservesDetectorState) {
    const MarketStateDetectorConfig config = MakeCanaryDetectorConfig();
    const std::string instrument_id = "DCE.c2607";
    TimeframeStateFanout fanout({5}, config);

    const auto initial_emissions = FeedTrendingMinutes(&fanout, instrument_id, 20, 100.0);
    ASSERT_FALSE(initial_emissions.empty());
    const auto previous_bars_seen = initial_emissions.back().state.market_state_bars_seen;
    ASSERT_GT(previous_bars_seen, 0U);

    TimeframeStateFanout::PersistenceState state;
    std::string error;
    ASSERT_TRUE(fanout.SaveState(&state, &error)) << error;

    TimeframeStateFanout restored({5}, config);
    ASSERT_TRUE(restored.LoadState(state, &error)) << error;
    restored.ResetInstrumentBuckets(instrument_id);

    std::vector<TimeframeStateEmission> resumed_emissions;
    for (int i = 20; i <= 25; ++i) {
        const double close = 100.0 + static_cast<double>(i);
        const auto batch = restored.OnOneMinuteBar(MakeOneMinuteBar(FormatMinute(i), close - 0.5,
                                                                    close + 1.0, close - 1.0, close,
                                                                    10 + i, i + 1, instrument_id));
        resumed_emissions.insert(resumed_emissions.end(), batch.begin(), batch.end());
    }

    ASSERT_EQ(resumed_emissions.size(), 1U);
    EXPECT_EQ(resumed_emissions.front().bar.minute, "20260515 09:20");
    EXPECT_EQ(resumed_emissions.front().state.market_state_bars_seen, previous_bars_seen + 1);
}

TEST(TimeframeStateFanoutTest, UsesGlobalDetectorConfigForCAndHcWhenNoOverrideConfigured) {
    const MarketStateDetectorConfig config = MakeCanaryDetectorConfig();
    TimeframeStateFanout fanout({5}, config);

    const auto c_emissions = FeedTrendingMinutes(&fanout, "DCE.c2607", 45, 100.0);
    const auto hc_emissions = FeedTrendingMinutes(&fanout, "SHFE.hc2610", 45, 200.0);

    ASSERT_FALSE(c_emissions.empty());
    ASSERT_FALSE(hc_emissions.empty());
    EXPECT_EQ(c_emissions.back().state.market_regime, MarketRegime::kStrongTrend);
    EXPECT_EQ(hc_emissions.back().state.market_regime, MarketRegime::kStrongTrend);
    EXPECT_EQ(c_emissions.back().state.market_state_decision_reason, "kama_strong");
    EXPECT_EQ(hc_emissions.back().state.market_state_decision_reason, "kama_strong");
    EXPECT_EQ(c_emissions.back().state.market_state_bars_seen,
              hc_emissions.back().state.market_state_bars_seen);
}

TEST(TimeframeStateFanoutTest, SelectsDetectorConfigByInstrumentProductPrefix) {
    const MarketStateDetectorConfig global_config = MakeCanaryDetectorConfig();
    MarketStateDetectorConfig hc_config = global_config;
    hc_config.require_adx_for_trend = true;

    MarketStateDetectorConfigByProduct by_product;
    by_product.emplace("hc", hc_config);
    TimeframeStateFanout fanout({5}, global_config, by_product);

    const auto c_emissions = FeedTrendingMinutes(&fanout, "DCE.c2607", 45, 100.0);
    const auto hc_emissions = FeedTrendingMinutes(&fanout, "SHFE.hc2610", 45, 200.0);

    ASSERT_FALSE(c_emissions.empty());
    ASSERT_FALSE(hc_emissions.empty());
    EXPECT_EQ(c_emissions.back().state.market_regime, MarketRegime::kStrongTrend);
    EXPECT_EQ(c_emissions.back().state.market_state_decision_reason, "kama_strong");
    EXPECT_EQ(hc_emissions.back().state.market_regime, MarketRegime::kUnknown);
    EXPECT_EQ(hc_emissions.back().state.market_state_decision_reason, "adx_warmup");
    EXPECT_TRUE(std::isfinite(hc_emissions.back().state.market_state_atr_ratio));
}

}  // namespace quant_hft
