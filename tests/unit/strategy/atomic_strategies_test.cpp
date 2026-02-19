#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>

#include "quant_hft/strategy/atomic/atr_stop_loss.h"
#include "quant_hft/strategy/atomic/atr_take_profit.h"
#include "quant_hft/strategy/atomic/max_position_risk_control.h"
#include "quant_hft/strategy/atomic/time_filter.h"
#include "quant_hft/strategy/atomic/trend_opening.h"

namespace quant_hft {
namespace {

StateSnapshot7D MakeBarState(const std::string& instrument, double high, double low, double close,
                             EpochNanos ts_ns = 0) {
    StateSnapshot7D state;
    state.instrument_id = instrument;
    state.has_bar = true;
    state.bar_high = high;
    state.bar_low = low;
    state.bar_close = close;
    state.ts_ns = ts_ns;
    return state;
}

AtomicStrategyContext MakeContext(const std::string& account_id) {
    AtomicStrategyContext ctx;
    ctx.account_id = account_id;
    return ctx;
}

EpochNanos UtcHourTs(int hour, int minute = 0) {
    constexpr std::int64_t kBaseTsSec = 1704067200;  // 2024-01-01 00:00:00 UTC
    return (kBaseTsSec + static_cast<std::int64_t>(hour) * 3600 + minute * 60) * 1000000000LL;
}

}  // namespace

TEST(AtomicStrategiesTest, TrendOpeningRequiresIndicatorReadinessAndResets) {
    TrendOpening opening;
    opening.Init({
        {"id", "trend_open"},
        {"instrument_id", "IF2406"},
        {"er_period", "2"},
        {"fast_period", "2"},
        {"slow_period", "4"},
        {"volume", "1"},
    });

    AtomicStrategyContext ctx = MakeContext("acct");
    EXPECT_TRUE(opening.OnState(MakeBarState("IF2406", 100.0, 99.0, 100.0), ctx).empty());
    EXPECT_TRUE(opening.OnState(MakeBarState("IF2406", 101.0, 100.0, 101.0), ctx).empty());

    const auto signals = opening.OnState(MakeBarState("IF2406", 102.0, 101.0, 103.0), ctx);
    ASSERT_EQ(signals.size(), 1u);
    EXPECT_EQ(signals[0].signal_type, SignalType::kOpen);
    EXPECT_EQ(signals[0].side, Side::kBuy);
    EXPECT_EQ(signals[0].instrument_id, "IF2406");
    EXPECT_EQ(signals[0].volume, 1);

    opening.Reset();
    EXPECT_TRUE(opening.OnState(MakeBarState("IF2406", 103.0, 102.0, 103.0), ctx).empty());
}

TEST(AtomicStrategiesTest, TrendOpeningSkipsWhenPositionExistsOrNonFiniteInput) {
    TrendOpening opening;
    opening.Init({
        {"id", "trend_open"},
        {"instrument_id", "IF2406"},
        {"er_period", "2"},
        {"fast_period", "2"},
        {"slow_period", "4"},
        {"volume", "2"},
    });

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.net_positions["IF2406"] = 1;
    opening.OnState(MakeBarState("IF2406", 100.0, 99.0, 100.0), ctx);
    opening.OnState(MakeBarState("IF2406", 101.0, 100.0, 101.0), ctx);
    EXPECT_TRUE(opening.OnState(MakeBarState("IF2406", 102.0, 101.0, 103.0), ctx).empty());

    StateSnapshot7D bad = MakeBarState("IF2406", 100.0, 99.0, 100.0);
    bad.bar_close = std::numeric_limits<double>::quiet_NaN();
    EXPECT_TRUE(opening.OnState(bad, ctx).empty());
}

TEST(AtomicStrategiesTest, ATRStopLossReadinessTriggerAndReset) {
    ATRStopLoss stop_loss;
    stop_loss.Init({
        {"id", "atr_sl"},
        {"atr_period", "3"},
        {"atr_multiplier", "2.0"},
    });

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.net_positions["IF2406"] = 3;
    ctx.avg_open_prices["IF2406"] = 105.0;

    EXPECT_TRUE(stop_loss.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0), ctx).empty());
    EXPECT_TRUE(stop_loss.OnState(MakeBarState("IF2406", 102.0, 98.0, 100.0), ctx).empty());

    const auto signals = stop_loss.OnState(MakeBarState("IF2406", 101.0, 97.0, 98.0), ctx);
    ASSERT_EQ(signals.size(), 1u);
    EXPECT_EQ(signals[0].signal_type, SignalType::kStopLoss);
    EXPECT_EQ(signals[0].side, Side::kSell);
    EXPECT_EQ(signals[0].offset, OffsetFlag::kClose);
    EXPECT_EQ(signals[0].volume, 3);

    stop_loss.Reset();
    EXPECT_TRUE(stop_loss.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0), ctx).empty());
}

TEST(AtomicStrategiesTest, ATRTakeProfitReadinessTriggerAndReset) {
    ATRTakeProfit take_profit;
    take_profit.Init({
        {"id", "atr_tp"},
        {"atr_period", "3"},
        {"atr_multiplier", "2.0"},
    });

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.net_positions["IF2406"] = 2;
    ctx.avg_open_prices["IF2406"] = 90.0;

    EXPECT_TRUE(take_profit.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0), ctx).empty());
    EXPECT_TRUE(take_profit.OnState(MakeBarState("IF2406", 102.0, 98.0, 100.0), ctx).empty());

    const auto signals = take_profit.OnState(MakeBarState("IF2406", 104.0, 100.0, 98.0), ctx);
    ASSERT_EQ(signals.size(), 1u);
    EXPECT_EQ(signals[0].signal_type, SignalType::kTakeProfit);
    EXPECT_EQ(signals[0].side, Side::kSell);
    EXPECT_EQ(signals[0].offset, OffsetFlag::kClose);
    EXPECT_EQ(signals[0].volume, 2);

    take_profit.Reset();
    EXPECT_TRUE(take_profit.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0), ctx).empty());
}

TEST(AtomicStrategiesTest, TrendOpeningExposesIndicatorSnapshotWhenReady) {
    TrendOpening opening;
    opening.Init({
        {"id", "trend_open"},
        {"instrument_id", "IF2406"},
        {"er_period", "2"},
        {"fast_period", "2"},
        {"slow_period", "4"},
        {"volume", "1"},
    });

    auto* provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(&opening);
    ASSERT_NE(provider, nullptr);
    EXPECT_FALSE(provider->IndicatorSnapshot().has_value());

    AtomicStrategyContext ctx = MakeContext("acct");
    opening.OnState(MakeBarState("IF2406", 100.0, 99.0, 100.0), ctx);
    opening.OnState(MakeBarState("IF2406", 101.0, 100.0, 101.0), ctx);
    opening.OnState(MakeBarState("IF2406", 102.0, 101.0, 103.0), ctx);

    const auto snapshot = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_TRUE(snapshot->kama.has_value());
    ASSERT_TRUE(snapshot->er.has_value());
    EXPECT_FALSE(snapshot->atr.has_value());
    EXPECT_FALSE(snapshot->adx.has_value());

    opening.Reset();
    EXPECT_FALSE(provider->IndicatorSnapshot().has_value());
}

TEST(AtomicStrategiesTest, ATRStopLossExposesIndicatorSnapshotWhenReady) {
    ATRStopLoss stop_loss;
    stop_loss.Init({
        {"id", "atr_sl"},
        {"atr_period", "3"},
        {"atr_multiplier", "2.0"},
    });

    auto* provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(&stop_loss);
    ASSERT_NE(provider, nullptr);
    EXPECT_FALSE(provider->IndicatorSnapshot().has_value());

    AtomicStrategyContext ctx = MakeContext("acct");
    stop_loss.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0), ctx);
    stop_loss.OnState(MakeBarState("IF2406", 102.0, 98.0, 100.0), ctx);
    stop_loss.OnState(MakeBarState("IF2406", 101.0, 97.0, 98.0), ctx);

    const auto snapshot = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_TRUE(snapshot->atr.has_value());
    EXPECT_FALSE(snapshot->kama.has_value());
    EXPECT_FALSE(snapshot->adx.has_value());
    EXPECT_FALSE(snapshot->er.has_value());

    stop_loss.Reset();
    EXPECT_FALSE(provider->IndicatorSnapshot().has_value());
}

TEST(AtomicStrategiesTest, ATRTakeProfitExposesIndicatorSnapshotWhenReady) {
    ATRTakeProfit take_profit;
    take_profit.Init({
        {"id", "atr_tp"},
        {"atr_period", "3"},
        {"atr_multiplier", "2.0"},
    });

    auto* provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(&take_profit);
    ASSERT_NE(provider, nullptr);
    EXPECT_FALSE(provider->IndicatorSnapshot().has_value());

    AtomicStrategyContext ctx = MakeContext("acct");
    take_profit.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0), ctx);
    take_profit.OnState(MakeBarState("IF2406", 102.0, 98.0, 100.0), ctx);
    take_profit.OnState(MakeBarState("IF2406", 104.0, 100.0, 98.0), ctx);

    const auto snapshot = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_TRUE(snapshot->atr.has_value());
    EXPECT_FALSE(snapshot->kama.has_value());
    EXPECT_FALSE(snapshot->adx.has_value());
    EXPECT_FALSE(snapshot->er.has_value());

    take_profit.Reset();
    EXPECT_FALSE(provider->IndicatorSnapshot().has_value());
}

TEST(AtomicStrategiesTest, TimeFilterCrossMidnightAndTimezone) {
    TimeFilter filter;
    filter.Init({
        {"id", "night_filter"},
        {"start_hour", "21"},
        {"end_hour", "2"},
        {"timezone", "UTC"},
    });

    EXPECT_TRUE(filter.AllowOpening(UtcHourTs(20, 59)));
    EXPECT_FALSE(filter.AllowOpening(UtcHourTs(21, 0)));
    EXPECT_FALSE(filter.AllowOpening(UtcHourTs(23, 30)));
    EXPECT_FALSE(filter.AllowOpening(UtcHourTs(1, 59)));
    EXPECT_TRUE(filter.AllowOpening(UtcHourTs(2, 0)));

    TimeFilter shanghai_filter;
    shanghai_filter.Init({
        {"id", "shanghai_filter"},
        {"start_hour", "9"},
        {"end_hour", "10"},
        {"timezone", "Asia/Shanghai"},
    });
    EXPECT_FALSE(shanghai_filter.AllowOpening(UtcHourTs(1, 30)));  // UTC 01:30 == CST 09:30
}

TEST(AtomicStrategiesTest, MaxPositionRiskControlTriggerAndReset) {
    MaxPositionRiskControl rc;
    rc.Init({
        {"id", "max_pos"},
        {"max_abs_position", "5"},
    });

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.net_positions["IF2406"] = 9;
    const auto long_signals = rc.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0), ctx);
    ASSERT_EQ(long_signals.size(), 1u);
    EXPECT_EQ(long_signals[0].signal_type, SignalType::kForceClose);
    EXPECT_EQ(long_signals[0].side, Side::kSell);
    EXPECT_EQ(long_signals[0].offset, OffsetFlag::kClose);
    EXPECT_EQ(long_signals[0].volume, 4);

    ctx.net_positions["IF2406"] = -8;
    const auto short_signals = rc.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0), ctx);
    ASSERT_EQ(short_signals.size(), 1u);
    EXPECT_EQ(short_signals[0].signal_type, SignalType::kForceClose);
    EXPECT_EQ(short_signals[0].side, Side::kBuy);
    EXPECT_EQ(short_signals[0].volume, 3);

    rc.Reset();
    ctx.net_positions["IF2406"] = 5;
    EXPECT_TRUE(rc.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0), ctx).empty());
}

}  // namespace quant_hft
