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
}

}  // namespace quant_hft
