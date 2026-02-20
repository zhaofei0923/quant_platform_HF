#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "quant_hft/strategy/atomic/kama_trend_strategy.h"
#include "quant_hft/strategy/atomic/trend_strategy.h"

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

std::vector<SignalIntent> FeedCloses(ISubStrategy* strategy, const std::string& instrument,
                                     AtomicStrategyContext* ctx,
                                     const std::vector<double>& closes) {
    std::vector<SignalIntent> last;
    for (std::size_t i = 0; i < closes.size(); ++i) {
        const double close = closes[i];
        last = strategy->OnState(MakeBarState(instrument, close + 1.0, close - 1.0, close,
                                              static_cast<EpochNanos>(i + 1)),
                                 *ctx);
    }
    return last;
}

AtomicParams MakeKamaParams() {
    return {
        {"id", "kama_1"},
        {"er_period", "2"},
        {"fast_period", "2"},
        {"slow_period", "4"},
        {"std_period", "3"},
        {"kama_filter", "0.0"},
        {"risk_per_trade_pct", "0.01"},
        {"default_volume", "1"},
        {"stop_loss_mode", "trailing_atr"},
        {"stop_loss_atr_period", "2"},
        {"stop_loss_atr_multiplier", "2.0"},
        {"take_profit_mode", "atr_target"},
        {"take_profit_atr_period", "2"},
        {"take_profit_atr_multiplier", "3.0"},
    };
}

AtomicParams MakeTrendParams() {
    return {
        {"id", "trend_1"},
        {"er_period", "2"},
        {"fast_period", "2"},
        {"slow_period", "4"},
        {"kama_filter", "0.0"},
        {"risk_per_trade_pct", "0.01"},
        {"default_volume", "1"},
        {"stop_loss_mode", "trailing_atr"},
        {"stop_loss_atr_period", "2"},
        {"stop_loss_atr_multiplier", "2.0"},
        {"take_profit_mode", "atr_target"},
        {"take_profit_atr_period", "2"},
        {"take_profit_atr_multiplier", "1.0"},
    };
}

}  // namespace

TEST(AtomicStrategiesTest, KamaTrendStrategyEmitsOpenWithRiskSizingAndSnapshot) {
    KamaTrendStrategy strategy;
    strategy.Init(MakeKamaParams());

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.account_equity = 100000.0;
    ctx.contract_multipliers["IF2406"] = 10.0;

    const std::vector<SignalIntent> signals =
        FeedCloses(&strategy, "IF2406", &ctx, {100, 101, 102, 103, 104, 105, 106, 107});
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kOpen);
    EXPECT_EQ(signals.front().side, Side::kBuy);
    EXPECT_EQ(signals.front().volume, 25);

    auto* provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(&strategy);
    ASSERT_NE(provider, nullptr);
    const auto snapshot = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_TRUE(snapshot->kama.has_value());
    EXPECT_TRUE(snapshot->atr.has_value());
    EXPECT_TRUE(snapshot->er.has_value());
    EXPECT_FALSE(snapshot->stop_loss_price.has_value());
    EXPECT_FALSE(snapshot->take_profit_price.has_value());
}

TEST(AtomicStrategiesTest, KamaTrendStrategyFallsBackToDefaultVolumeWhenMultiplierMissing) {
    KamaTrendStrategy strategy;
    AtomicParams params = MakeKamaParams();
    params["default_volume"] = "3";
    strategy.Init(params);

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.account_equity = 100000.0;

    const std::vector<SignalIntent> signals =
        FeedCloses(&strategy, "IF2406", &ctx, {100, 101, 102, 103, 104, 105, 106, 107});
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kOpen);
    EXPECT_EQ(signals.front().volume, 3);
}

TEST(AtomicStrategiesTest, KamaTrendStrategySkipsOpenWhenRiskBudgetTooSmall) {
    KamaTrendStrategy strategy;
    strategy.Init(MakeKamaParams());

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.account_equity = 1000.0;
    ctx.contract_multipliers["IF2406"] = 10.0;

    const std::vector<SignalIntent> signals =
        FeedCloses(&strategy, "IF2406", &ctx, {100, 101, 102, 103, 104, 105, 106, 107});
    EXPECT_TRUE(signals.empty());
}

TEST(AtomicStrategiesTest, KamaTrendStrategyTrailingStopOnlyTightensAndThenTriggers) {
    KamaTrendStrategy strategy;
    AtomicParams params = MakeKamaParams();
    params["take_profit_mode"] = "none";
    strategy.Init(params);

    auto* provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(&strategy);
    ASSERT_NE(provider, nullptr);

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.net_positions["IF2406"] = 1;
    ctx.avg_open_prices["IF2406"] = 100.0;

    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0, 1), ctx).empty());
    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 102.0, 100.0, 101.0, 2), ctx).empty());

    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 104.0, 102.0, 103.0, 3), ctx).empty());
    const auto snapshot_after_tighten = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot_after_tighten.has_value());
    ASSERT_TRUE(snapshot_after_tighten->stop_loss_price.has_value());
    const double tightened_stop = snapshot_after_tighten->stop_loss_price.value();

    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 103.0, 101.0, 102.0, 4), ctx).empty());
    const auto snapshot_after_pullback = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot_after_pullback.has_value());
    ASSERT_TRUE(snapshot_after_pullback->stop_loss_price.has_value());
    EXPECT_DOUBLE_EQ(snapshot_after_pullback->stop_loss_price.value(), tightened_stop);

    const std::vector<SignalIntent> trigger =
        strategy.OnState(MakeBarState("IF2406", 91.0, 89.0, 90.0, 5), ctx);
    ASSERT_EQ(trigger.size(), 1U);
    EXPECT_EQ(trigger.front().signal_type, SignalType::kStopLoss);
    EXPECT_EQ(trigger.front().side, Side::kSell);
    EXPECT_EQ(trigger.front().offset, OffsetFlag::kClose);
    EXPECT_EQ(trigger.front().volume, 1);
}

TEST(AtomicStrategiesTest, TrendStrategyEmitsOpenAndTakeProfitSignals) {
    TrendStrategy strategy;
    strategy.Init(MakeTrendParams());

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.account_equity = 100000.0;
    ctx.contract_multipliers["rb2405"] = 10.0;

    const std::vector<SignalIntent> open_signals =
        FeedCloses(&strategy, "rb2405", &ctx, {100, 101, 102, 103, 104});
    ASSERT_EQ(open_signals.size(), 1U);
    EXPECT_EQ(open_signals.front().signal_type, SignalType::kOpen);

    ctx.net_positions["rb2405"] = 1;
    ctx.avg_open_prices["rb2405"] = 100.0;
    FeedCloses(&strategy, "rb2405", &ctx, {104, 105});

    const std::vector<SignalIntent> take_profit_signals =
        strategy.OnState(MakeBarState("rb2405", 108.0, 106.0, 107.0, 100), ctx);
    ASSERT_EQ(take_profit_signals.size(), 1U);
    EXPECT_EQ(take_profit_signals.front().signal_type, SignalType::kTakeProfit);
    EXPECT_EQ(take_profit_signals.front().side, Side::kSell);

    auto* provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(&strategy);
    ASSERT_NE(provider, nullptr);
    const auto snapshot = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_TRUE(snapshot->take_profit_price.has_value());
}

}  // namespace quant_hft
