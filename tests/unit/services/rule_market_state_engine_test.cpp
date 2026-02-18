#include <gtest/gtest.h>

#include "quant_hft/services/rule_market_state_engine.h"

namespace quant_hft {

TEST(RuleMarketStateEngineTest, BuildsStateAfterSnapshots) {
    RuleMarketStateEngine engine(16);

    MarketSnapshot s1;
    s1.instrument_id = "SHFE.ag2406";
    s1.last_price = 100.0;
    s1.bid_price_1 = 99.0;
    s1.ask_price_1 = 101.0;
    s1.bid_volume_1 = 20;
    s1.ask_volume_1 = 10;
    s1.volume = 100;
    s1.recv_ts_ns = NowEpochNanos();

    auto s2 = s1;
    s2.last_price = 101.0;
    s2.volume = 120;

    engine.OnMarketSnapshot(s1);
    engine.OnMarketSnapshot(s2);

    const auto state = engine.GetCurrentState("SHFE.ag2406");
    EXPECT_EQ(state.instrument_id, "SHFE.ag2406");
    EXPECT_GT(state.trend.confidence, 0.0);
    EXPECT_GE(state.volatility.score, 0.0);
    EXPECT_LE(state.liquidity.score, 1.0);
    EXPECT_TRUE(state.has_bar);
    EXPECT_DOUBLE_EQ(state.bar_open, 100.0);
    EXPECT_DOUBLE_EQ(state.bar_high, 101.0);
    EXPECT_DOUBLE_EQ(state.bar_low, 100.0);
    EXPECT_DOUBLE_EQ(state.bar_close, 101.0);
    EXPECT_DOUBLE_EQ(state.bar_volume, 20.0);
}

TEST(RuleMarketStateEngineTest, ComputesMarketRegimePerInstrument) {
    MarketStateDetectorConfig detector_config;
    detector_config.adx_period = 3;
    detector_config.atr_period = 3;
    detector_config.kama_er_period = 3;
    detector_config.min_bars_for_flat = 1;
    RuleMarketStateEngine engine(16, detector_config);

    MarketSnapshot trend;
    trend.instrument_id = "SHFE.rb2405";
    trend.bid_price_1 = 99.0;
    trend.ask_price_1 = 101.0;
    trend.bid_volume_1 = 20;
    trend.ask_volume_1 = 10;
    trend.recv_ts_ns = 100;

    MarketSnapshot flat = trend;
    flat.instrument_id = "SHFE.ag2406";

    for (int i = 0; i < 8; ++i) {
        trend.last_price = 100.0 + static_cast<double>(i);
        trend.volume = 100 + i * 10;
        trend.recv_ts_ns += 100;
        engine.OnMarketSnapshot(trend);

        flat.last_price = 50.0;
        flat.volume = 200 + i * 3;
        flat.recv_ts_ns += 100;
        engine.OnMarketSnapshot(flat);
    }

    const auto trend_state = engine.GetCurrentState("SHFE.rb2405");
    const auto flat_state = engine.GetCurrentState("SHFE.ag2406");
    EXPECT_EQ(trend_state.market_regime, MarketRegime::kStrongTrend);
    EXPECT_EQ(flat_state.market_regime, MarketRegime::kFlat);
}

TEST(RuleMarketStateEngineTest, SkipsDetectorUpdateForOutOfOrderTimestamps) {
    MarketStateDetectorConfig detector_config;
    detector_config.adx_period = 3;
    detector_config.atr_period = 3;
    detector_config.kama_er_period = 3;
    detector_config.min_bars_for_flat = 1;
    RuleMarketStateEngine engine(16, detector_config);

    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.rb2405";
    snapshot.bid_price_1 = 99.0;
    snapshot.ask_price_1 = 101.0;
    snapshot.bid_volume_1 = 20;
    snapshot.ask_volume_1 = 10;
    snapshot.recv_ts_ns = 100;

    for (int i = 0; i < 8; ++i) {
        snapshot.last_price = 100.0 + static_cast<double>(i);
        snapshot.volume = 100 + i * 10;
        snapshot.recv_ts_ns += 100;
        engine.OnMarketSnapshot(snapshot);
    }

    const auto before = engine.GetCurrentState("SHFE.rb2405");
    EXPECT_EQ(before.market_regime, MarketRegime::kStrongTrend);

    snapshot.last_price = 70.0;
    snapshot.volume += 10;
    snapshot.recv_ts_ns = 50;
    engine.OnMarketSnapshot(snapshot);

    const auto after = engine.GetCurrentState("SHFE.rb2405");
    EXPECT_EQ(after.market_regime, before.market_regime);
}

}  // namespace quant_hft
