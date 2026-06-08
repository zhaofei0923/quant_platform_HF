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
    state.analysis_bar_high = high;
    state.analysis_bar_low = low;
    state.analysis_bar_close = close;
    state.ts_ns = ts_ns;
    return state;
}

AtomicTickSnapshot MakeTick(const std::string& instrument, double last_price,
                            EpochNanos ts_ns = 0) {
    AtomicTickSnapshot tick;
    tick.instrument_id = instrument;
    tick.last_price = last_price;
    tick.ts_ns = ts_ns;
    return tick;
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
        {"adx_period", "2"},
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
    EXPECT_EQ(signals.front().trace_id, "kama_1-open-IF2406-8");

    auto* provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(&strategy);
    ASSERT_NE(provider, nullptr);
    const auto snapshot = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_TRUE(snapshot->kama.has_value());
    EXPECT_TRUE(snapshot->atr.has_value());
    EXPECT_TRUE(snapshot->adx.has_value());
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

TEST(AtomicStrategiesTest, KamaTrendStrategyStateRestoreContinuesIndicators) {
    KamaTrendStrategy continuous;
    continuous.Init(MakeKamaParams());

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.account_equity = 100000.0;
    ctx.contract_multipliers["IF2406"] = 10.0;

    for (double close : {100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0}) {
        (void)continuous.OnState(
            MakeBarState("IF2406", close + 1.0, close - 1.0, close, static_cast<EpochNanos>(close)),
            ctx);
    }

    AtomicState saved;
    std::string error;
    ASSERT_TRUE(continuous.SaveState(&saved, &error)) << error;

    KamaTrendStrategy restored;
    restored.Init(MakeKamaParams());
    ASSERT_TRUE(restored.LoadState(saved, &error)) << error;

    const StateSnapshot7D next = MakeBarState("IF2406", 109.0, 107.0, 108.0, 108);
    const std::vector<SignalIntent> continuous_signals = continuous.OnState(next, ctx);
    const std::vector<SignalIntent> restored_signals = restored.OnState(next, ctx);
    ASSERT_EQ(restored_signals.size(), continuous_signals.size());
    if (!continuous_signals.empty()) {
        EXPECT_EQ(restored_signals.front().signal_type, continuous_signals.front().signal_type);
        EXPECT_EQ(restored_signals.front().side, continuous_signals.front().side);
        EXPECT_EQ(restored_signals.front().volume, continuous_signals.front().volume);
    }

    const auto continuous_snapshot = continuous.IndicatorSnapshot();
    const auto restored_snapshot = restored.IndicatorSnapshot();
    ASSERT_TRUE(continuous_snapshot.has_value());
    ASSERT_TRUE(restored_snapshot.has_value());
    ASSERT_TRUE(continuous_snapshot->kama.has_value());
    ASSERT_TRUE(restored_snapshot->kama.has_value());
    ASSERT_TRUE(continuous_snapshot->adx.has_value());
    ASSERT_TRUE(restored_snapshot->adx.has_value());
    ASSERT_TRUE(continuous_snapshot->atr.has_value());
    ASSERT_TRUE(restored_snapshot->atr.has_value());
    EXPECT_NEAR(*restored_snapshot->kama, *continuous_snapshot->kama, 1e-12);
    EXPECT_NEAR(*restored_snapshot->adx, *continuous_snapshot->adx, 1e-12);
    EXPECT_NEAR(*restored_snapshot->atr, *continuous_snapshot->atr, 1e-12);
    EXPECT_EQ(restored_snapshot->trend_sum, continuous_snapshot->trend_sum);
}

TEST(AtomicStrategiesTest, KamaTrendStrategyUsesMinimumVolumeWhenRiskBudgetTooSmall) {
    KamaTrendStrategy strategy;
    strategy.Init(MakeKamaParams());

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.account_equity = 1000.0;
    ctx.contract_multipliers["IF2406"] = 10.0;

    const std::vector<SignalIntent> signals =
        FeedCloses(&strategy, "IF2406", &ctx, {100, 101, 102, 103, 104, 105, 106, 107});
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kOpen);
    EXPECT_EQ(signals.front().volume, 1);
}

TEST(AtomicStrategiesTest, KamaTrendStrategyProductionParamsRequireKamaSideAndMinimumVolume) {
    KamaTrendStrategy strategy;
    strategy.Init({
        {"id", "kama_trend_1"},
        {"er_period", "10"},
        {"fast_period", "2"},
        {"slow_period", "30"},
        {"std_period", "10"},
        {"kama_filter", "1.0"},
        {"risk_per_trade_pct", "0.005"},
        {"default_volume", "1"},
        {"stop_loss_mode", "trailing_atr"},
        {"stop_loss_atr_period", "14"},
        {"stop_loss_atr_multiplier", "3.5"},
        {"take_profit_mode", "atr_target"},
        {"take_profit_atr_period", "14"},
        {"take_profit_atr_multiplier", "30.0"},
        {"adx_period", "14"},
    });

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.account_equity = 1000.0;
    ctx.contract_multipliers["IF2406"] = 10.0;

    const std::vector<double> closes = {100.0, 100.0, 100.0, 100.0, 100.0,  100.0,  100.0,
                                        100.0, 100.0, 100.0, 100.0, 100.0,  100.0,  100.0,
                                        100.0, 140.0, 196.0, 274.4, 384.16, 537.824};
    const std::vector<SignalIntent> signals = FeedCloses(&strategy, "IF2406", &ctx, closes);
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kOpen);
    EXPECT_EQ(signals.front().side, Side::kBuy);
    EXPECT_EQ(signals.front().volume, 1);

    auto* provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(&strategy);
    ASSERT_NE(provider, nullptr);
    const auto snapshot = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_TRUE(snapshot->kama.has_value());
    EXPECT_GT(closes.back(), snapshot->kama.value());
}

TEST(AtomicStrategiesTest, KamaTrendStrategyTrailingStopRefreshesAndTriggersOnTick) {
    KamaTrendStrategy strategy;
    AtomicParams params = MakeKamaParams();
    params["take_profit_mode"] = "none";
    strategy.Init(params);

    auto* provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(&strategy);
    ASSERT_NE(provider, nullptr);
    auto* tick_aware = dynamic_cast<IAtomicBacktestTickAware*>(&strategy);
    ASSERT_NE(tick_aware, nullptr);

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.net_positions["IF2406"] = 1;
    ctx.avg_open_prices["IF2406"] = 100.0;

    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0, 1), ctx).empty());
    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 102.0, 100.0, 101.0, 2), ctx).empty());

    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 104.0, 102.0, 103.0, 3), ctx).empty());
    const auto snapshot_after_tighten = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot_after_tighten.has_value());
    ASSERT_TRUE(snapshot_after_tighten->stop_loss_price.has_value());
    const double bar_stop = snapshot_after_tighten->stop_loss_price.value();

    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 106.0, 104.0, 105.0, 4), ctx).empty());
    const auto snapshot_after_bar_rally = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot_after_bar_rally.has_value());
    ASSERT_TRUE(snapshot_after_bar_rally->stop_loss_price.has_value());
    EXPECT_DOUBLE_EQ(snapshot_after_bar_rally->stop_loss_price.value(), bar_stop);

    const double rally_tick_price = bar_stop + 25.0;
    EXPECT_TRUE(tick_aware->OnBacktestTick(MakeTick("IF2406", rally_tick_price, 5), ctx).empty());
    const auto snapshot_after_tick_refresh = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot_after_tick_refresh.has_value());
    ASSERT_TRUE(snapshot_after_tick_refresh->stop_loss_price.has_value());
    EXPECT_GT(snapshot_after_tick_refresh->stop_loss_price.value(), bar_stop);
    const double tick_refreshed_stop = snapshot_after_tick_refresh->stop_loss_price.value();

    const std::vector<SignalIntent> bar_cross_signals = strategy.OnState(
        MakeBarState("IF2406", tick_refreshed_stop + 1.0, tick_refreshed_stop - 1.0,
                     tick_refreshed_stop - 0.5, 6),
        ctx);
    for (const SignalIntent& signal : bar_cross_signals) {
        EXPECT_NE(signal.signal_type, SignalType::kStopLoss);
        EXPECT_NE(signal.signal_type, SignalType::kTakeProfit);
    }

    const std::vector<SignalIntent> trigger =
        tick_aware->OnBacktestTick(MakeTick("IF2406", tick_refreshed_stop - 0.5, 7), ctx);
    ASSERT_EQ(trigger.size(), 1U);
    EXPECT_EQ(trigger.front().signal_type, SignalType::kStopLoss);
    EXPECT_EQ(trigger.front().side, Side::kSell);
    EXPECT_EQ(trigger.front().offset, OffsetFlag::kClose);
    EXPECT_EQ(trigger.front().volume, 1);
    EXPECT_EQ(trigger.front().trace_id, "kama_1-stop_loss-IF2406-7");
}

TEST(AtomicStrategiesTest, KamaTrendStrategyExposesInitialAndTrailingRiskPrices) {
    KamaTrendStrategy strategy;
    strategy.Init(MakeKamaParams());

    auto* risk_provider = dynamic_cast<IAtomicRiskPriceProvider*>(&strategy);
    ASSERT_NE(risk_provider, nullptr);
    auto* tick_aware = dynamic_cast<IAtomicBacktestTickAware*>(&strategy);
    ASSERT_NE(tick_aware, nullptr);

    // No position yet: no risk prices reported.
    EXPECT_TRUE(risk_provider->RiskPricesByInstrument().empty());

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.net_positions["IF2406"] = 1;
    ctx.avg_open_prices["IF2406"] = 100.0;

    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 101.0, 99.0, 100.0, 1), ctx).empty());
    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 102.0, 100.0, 101.0, 2), ctx).empty());
    EXPECT_TRUE(strategy.OnState(MakeBarState("IF2406", 104.0, 102.0, 103.0, 3), ctx).empty());

    const auto prices_after_entry = risk_provider->RiskPricesByInstrument();
    ASSERT_EQ(prices_after_entry.count("IF2406"), 1U);
    const auto& entry_levels = prices_after_entry.at("IF2406");
    ASSERT_TRUE(entry_levels.initial_stop.has_value());
    ASSERT_TRUE(entry_levels.trailing_stop.has_value());
    ASSERT_TRUE(entry_levels.take_profit.has_value());
    const double initial_stop = entry_levels.initial_stop.value();
    const double trailing_stop_at_entry = entry_levels.trailing_stop.value();

    // A favorable tick ratchets the trailing stop up; the initial stop is fixed.
    // The tick may also emit a take-profit signal, which is irrelevant here.
    const double rally_tick_price = trailing_stop_at_entry + 25.0;
    (void)tick_aware->OnBacktestTick(MakeTick("IF2406", rally_tick_price, 4), ctx);

    const auto prices_after_rally = risk_provider->RiskPricesByInstrument();
    ASSERT_EQ(prices_after_rally.count("IF2406"), 1U);
    const auto& rally_levels = prices_after_rally.at("IF2406");
    ASSERT_TRUE(rally_levels.initial_stop.has_value());
    ASSERT_TRUE(rally_levels.trailing_stop.has_value());
    EXPECT_DOUBLE_EQ(rally_levels.initial_stop.value(), initial_stop);
    EXPECT_GT(rally_levels.trailing_stop.value(), trailing_stop_at_entry);

    // Flattening the position clears all reported risk levels.
    ctx.net_positions["IF2406"] = 0;
    (void)tick_aware->OnBacktestTick(MakeTick("IF2406", rally_tick_price, 5), ctx);
    EXPECT_TRUE(risk_provider->RiskPricesByInstrument().empty());
}

TEST(AtomicStrategiesTest, TrendStrategyEmitsOpenAndTakeProfitSignalsOnTick) {
    TrendStrategy strategy;
    strategy.Init(MakeTrendParams());
    auto* tick_aware = dynamic_cast<IAtomicBacktestTickAware*>(&strategy);
    ASSERT_NE(tick_aware, nullptr);

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

    const std::vector<SignalIntent> bar_take_signals =
        strategy.OnState(MakeBarState("rb2405", 108.0, 106.0, 107.0, 99), ctx);
    for (const SignalIntent& signal : bar_take_signals) {
        EXPECT_NE(signal.signal_type, SignalType::kStopLoss);
        EXPECT_NE(signal.signal_type, SignalType::kTakeProfit);
    }

    const std::vector<SignalIntent> take_profit_signals =
        tick_aware->OnBacktestTick(MakeTick("rb2405", 107.0, 100), ctx);
    ASSERT_EQ(take_profit_signals.size(), 1U);
    EXPECT_EQ(take_profit_signals.front().signal_type, SignalType::kTakeProfit);
    EXPECT_EQ(take_profit_signals.front().side, Side::kSell);

    auto* provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(&strategy);
    ASSERT_NE(provider, nullptr);
    const auto snapshot = provider->IndicatorSnapshot();
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_TRUE(snapshot->take_profit_price.has_value());
}

TEST(AtomicStrategiesTest, KamaTrendStrategyEmitsReverseOpenWhenTrendFlipsWhileHoldingPosition) {
    KamaTrendStrategy strategy;
    AtomicParams params = MakeKamaParams();
    params["stop_loss_mode"] = "none";
    params["take_profit_mode"] = "none";
    strategy.Init(params);

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.account_equity = 100000.0;
    ctx.contract_multipliers["IF2406"] = 10.0;

    const std::vector<SignalIntent> open_signals =
        FeedCloses(&strategy, "IF2406", &ctx, {100, 101, 102, 103, 104, 105, 106, 107});
    ASSERT_EQ(open_signals.size(), 1U);
    EXPECT_EQ(open_signals.front().signal_type, SignalType::kOpen);
    EXPECT_EQ(open_signals.front().side, Side::kBuy);

    ctx.net_positions["IF2406"] = 1;
    ctx.avg_open_prices["IF2406"] = 107.0;

    const std::vector<SignalIntent> reverse_signals =
        FeedCloses(&strategy, "IF2406", &ctx, {106, 105, 104, 103, 102, 101, 100, 99});
    ASSERT_EQ(reverse_signals.size(), 1U);
    EXPECT_EQ(reverse_signals.front().signal_type, SignalType::kOpen);
    EXPECT_EQ(reverse_signals.front().side, Side::kSell);
    EXPECT_EQ(reverse_signals.front().offset, OffsetFlag::kOpen);
}

TEST(AtomicStrategiesTest, TrendStrategyEmitsReverseOpenWhenTrendFlipsWhileHoldingPosition) {
    TrendStrategy strategy;
    AtomicParams params = MakeTrendParams();
    params["stop_loss_mode"] = "none";
    params["take_profit_mode"] = "none";
    strategy.Init(params);

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.account_equity = 0.0;
    ctx.contract_multipliers["rb2405"] = 10.0;

    const std::vector<SignalIntent> open_signals =
        FeedCloses(&strategy, "rb2405", &ctx, {100, 101, 102, 103, 104});
    ASSERT_EQ(open_signals.size(), 1U);
    EXPECT_EQ(open_signals.front().signal_type, SignalType::kOpen);
    EXPECT_EQ(open_signals.front().side, Side::kBuy);

    ctx.net_positions["rb2405"] = 1;
    ctx.avg_open_prices["rb2405"] = 104.0;

    const std::vector<SignalIntent> reverse_signals =
        FeedCloses(&strategy, "rb2405", &ctx, {103, 100, 97, 94, 91});
    ASSERT_EQ(reverse_signals.size(), 1U);
    EXPECT_EQ(reverse_signals.front().signal_type, SignalType::kOpen);
    EXPECT_EQ(reverse_signals.front().side, Side::kSell);
    EXPECT_EQ(reverse_signals.front().offset, OffsetFlag::kOpen);
}

TEST(AtomicStrategiesTest, TrendStrategyUsesAnalysisBarForSignalButKeepsRawLimitPrice) {
    TrendStrategy strategy;
    AtomicParams params = MakeTrendParams();
    params["stop_loss_mode"] = "none";
    params["take_profit_mode"] = "none";
    strategy.Init(params);

    AtomicStrategyContext ctx = MakeContext("acct");
    ctx.account_equity = 100000.0;
    ctx.contract_multipliers["rb2405"] = 10.0;

    for (std::size_t index = 0; index < 4; ++index) {
        const double close = 100.0 + static_cast<double>(index);
        (void)strategy.OnState(MakeBarState("rb2405", close + 1.0, close - 1.0, close, index + 1),
                               ctx);
    }

    StateSnapshot7D rollover_state;
    rollover_state.instrument_id = "rb2405";
    rollover_state.has_bar = true;
    rollover_state.bar_high = 201.0;
    rollover_state.bar_low = 199.0;
    rollover_state.bar_close = 200.0;
    rollover_state.analysis_bar_high = 100.0;
    rollover_state.analysis_bar_low = 98.0;
    rollover_state.analysis_bar_close = 99.0;
    rollover_state.ts_ns = 10;

    const std::vector<SignalIntent> signals = strategy.OnState(rollover_state, ctx);
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kOpen);
    EXPECT_EQ(signals.front().side, Side::kSell);
    EXPECT_DOUBLE_EQ(signals.front().limit_price, 200.0);
}

}  // namespace quant_hft
