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

// Regression test for the cross-instrument premature flush bug:
// When two instruments share the same night session (e.g. 21:00-23:00), their
// last 1m bars ("22:59") may become ready at different times. Previously,
// flush_session_end_market_bars used a global Flush() — so when hc2610's "22:59"
// was ready first, c2609's incomplete "22:55" bucket (missing "22:59" data) was
// also emitted prematurely, causing a duplicate bar. FlushInstrument() must only
// emit the specified instrument's buckets and leave others intact.
TEST(TimeframeStateFanoutTest, FlushInstrumentDoesNotEmitOtherInstrumentPendingBuckets) {
    TimeframeStateFanout fanout({5});

    // Feed c2609: bars 22:55-22:58 (4 bars, "22:59" not yet arrived)
    auto make_night_bar = [](const std::string& inst, const std::string& minute, int vol,
                             EpochNanos ts) {
        BarSnapshot bar;
        bar.instrument_id = inst;
        bar.exchange_id = inst.rfind("DCE", 0) == 0 ? "DCE" : "SHFE";
        bar.trading_day = "20260710";
        bar.action_day = "20260709";
        bar.minute = "20260710 " + minute;
        bar.open = bar.high = bar.low = bar.close = 2310.0;
        bar.analysis_open = bar.analysis_high = bar.analysis_low = bar.analysis_close = 2310.0;
        bar.volume = vol;
        bar.ts_ns = ts;
        return bar;
    };

    // c2609: receives 22:55 through 22:58 (4 bars → "22:55" bucket incomplete)
    for (int m = 55; m <= 58; ++m) {
        char buf[8];
        std::snprintf(buf, sizeof(buf), "22:%02d", m);
        EXPECT_TRUE(fanout.OnOneMinuteBar(make_night_bar("DCE.c2609", buf, 100 * m, m)).empty());
    }

    // hc2610: receives all 5 bars 22:55-22:59 (bucket complete)
    for (int m = 55; m <= 59; ++m) {
        char buf[8];
        std::snprintf(buf, sizeof(buf), "22:%02d", m);
        EXPECT_TRUE(fanout.OnOneMinuteBar(make_night_bar("SHFE.hc2610", buf, 200 * m, m)).empty());
    }

    // Simulate: hc2610's "22:59" becomes ready first → FlushInstrument("SHFE.hc2610")
    const auto hc_emissions = fanout.FlushInstrument("SHFE.hc2610");
    ASSERT_EQ(hc_emissions.size(), 1U) << "hc2610 22:55 should be emitted";
    EXPECT_EQ(hc_emissions[0].bar.minute, "20260710 22:55");
    EXPECT_EQ(hc_emissions[0].bar.instrument_id, "SHFE.hc2610");
    EXPECT_EQ(hc_emissions[0].bar.volume, 200*55 + 200*56 + 200*57 + 200*58 + 200*59);

    // c2609's bucket must still be in the fanout (not affected by hc flush)
    const auto after_hc_flush = fanout.Flush();
    // Still has c2609's partial "22:55" bucket (22:55-22:58 only)
    ASSERT_EQ(after_hc_flush.size(), 1U) << "c2609 22:55 should still be pending after hc flush";
    EXPECT_EQ(after_hc_flush[0].bar.instrument_id, "DCE.c2609");
    EXPECT_EQ(after_hc_flush[0].bar.minute, "20260710 22:55");
    // Volume should be the SUM of 22:55-22:58 only (4 bars), not including 22:59
    const int64_t expected_c_vol = 100*55 + 100*56 + 100*57 + 100*58;
    EXPECT_EQ(after_hc_flush[0].bar.volume, expected_c_vol);
}

// Verify the complete correct flow: c2609's "22:59" arrives after hc2610's flush,
// then FlushInstrument("DCE.c2609") emits the COMPLETE "22:55" bar including 22:59.
TEST(TimeframeStateFanoutTest, FlushInstrumentEmitsCompleteBarAfterLastMinuteArrives) {
    TimeframeStateFanout fanout({5});

    auto make_bar = [](const std::string& inst, const std::string& td_minute, int vol,
                       EpochNanos ts) {
        BarSnapshot bar;
        bar.instrument_id = inst;
        bar.trading_day = "20260710";
        bar.action_day = "20260709";
        bar.minute = td_minute;
        bar.open = bar.high = bar.low = bar.close = 2310.0;
        bar.analysis_open = bar.analysis_high = bar.analysis_low = bar.analysis_close = 2310.0;
        bar.volume = vol;
        bar.ts_ns = ts;
        return bar;
    };

    // Both instruments: 22:55-22:58 in fanout
    for (int m = 55; m <= 58; ++m) {
        char key[32];
        std::snprintf(key, sizeof(key), "20260710 22:%02d", m);
        fanout.OnOneMinuteBar(make_bar("DCE.c2609", key, 100, m));
        fanout.OnOneMinuteBar(make_bar("SHFE.hc2610", key, 200, m));
    }

    // hc2610 gets 22:59 first → hc flush happens
    fanout.OnOneMinuteBar(make_bar("SHFE.hc2610", "20260710 22:59", 200, 59));
    const auto hc_flush = fanout.FlushInstrument("SHFE.hc2610");
    ASSERT_EQ(hc_flush.size(), 1U);
    EXPECT_EQ(hc_flush[0].bar.volume, 200 * 5);  // 22:55-22:59, 5 bars × 200

    // c2609 still has partial "22:55" (22:55-22:58 only)
    // Now c2609's "22:59" arrives
    fanout.OnOneMinuteBar(make_bar("DCE.c2609", "20260710 22:59", 100, 59));

    // c2609 flush → must include the 22:59 data (complete bar)
    const auto c_flush = fanout.FlushInstrument("DCE.c2609");
    ASSERT_EQ(c_flush.size(), 1U);
    EXPECT_EQ(c_flush[0].bar.minute, "20260710 22:55");
    EXPECT_EQ(c_flush[0].bar.volume, 100 * 5);   // 22:55-22:59, 5 bars × 100 (complete!)
    EXPECT_EQ(c_flush[0].bar.ts_ns, 59);          // ts_ns = last bar's ts_ns

    // Fanout now empty
    EXPECT_TRUE(fanout.Flush().empty());
}

}  // namespace quant_hft
