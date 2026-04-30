#include "quant_hft/strategy/composite_strategy.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cctype>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {
namespace {

std::vector<std::string>* g_call_log = nullptr;
double* g_captured_account_equity = nullptr;
double* g_captured_total_pnl_after_cost = nullptr;
std::string* g_captured_run_type = nullptr;
std::unordered_map<std::string, double>* g_captured_contract_multipliers = nullptr;

bool ParseBool(const std::string& value) {
    return value == "1" || value == "true" || value == "TRUE";
}

int ParseInt(const std::string& value) { return std::stoi(value); }

double ParseDouble(const std::string& value) { return std::stod(value); }

std::string ToLower(std::string value) {
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value;
}

std::string GetOrDefault(const AtomicParams& params, const std::string& key,
                         const std::string& def) {
    const auto it = params.find(key);
    return it == params.end() ? def : it->second;
}

std::optional<double> ParseOptionalDouble(const AtomicParams& params, const std::string& key) {
    const auto it = params.find(key);
    if (it == params.end() || it->second.empty()) {
        return std::nullopt;
    }
    return ParseDouble(it->second);
}

class ScriptedSubStrategy final : public ISubStrategy,
                                  public IAtomicBacktestTickAware,
                                  public IAtomicOrderAware,
                                  public IAtomicIndicatorTraceProvider {
   public:
    void Init(const AtomicParams& params) override {
        id_ = GetOrDefault(params, "id", "scripted");
        emit_open_ = ParseBool(GetOrDefault(params, "emit_open", "0"));
        emit_close_ = ParseBool(GetOrDefault(params, "emit_close", "0"));
        emit_stop_loss_ = ParseBool(GetOrDefault(params, "emit_stop_loss", "0"));
        emit_take_profit_ = ParseBool(GetOrDefault(params, "emit_take_profit", "0"));
        emit_force_close_ = ParseBool(GetOrDefault(params, "emit_force_close", "0"));
        emit_tick_stop_loss_ = ParseBool(GetOrDefault(params, "emit_tick_stop_loss", "0"));
        emit_tick_take_profit_ = ParseBool(GetOrDefault(params, "emit_tick_take_profit", "0"));
        max_open_emits_ = ParseInt(GetOrDefault(params, "max_open_emits", "-1"));
        volume_ = ParseInt(GetOrDefault(params, "volume", "1"));
        signal_ts_ns_ = std::stoll(GetOrDefault(params, "signal_ts_ns", "0"));
        trace_base_ = GetOrDefault(params, "trace", id_);
        open_side_ =
            ToLower(GetOrDefault(params, "open_side", "buy")) == "sell" ? Side::kSell : Side::kBuy;
        close_side_ =
            ToLower(GetOrDefault(params, "close_side", "sell")) == "buy" ? Side::kBuy : Side::kSell;

        snapshot_kama_ = ParseOptionalDouble(params, "snapshot_kama");
        snapshot_atr_ = ParseOptionalDouble(params, "snapshot_atr");
        snapshot_er_ = ParseOptionalDouble(params, "snapshot_er");
        snapshot_stop_loss_price_ = ParseOptionalDouble(params, "snapshot_stop_loss_price");
        snapshot_take_profit_price_ = ParseOptionalDouble(params, "snapshot_take_profit_price");
    }

    std::string GetId() const override { return id_; }

    void Reset() override {}

    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        if (g_call_log != nullptr) {
            g_call_log->push_back(id_);
        }
        if (g_captured_account_equity != nullptr) {
            *g_captured_account_equity = ctx.account_equity;
        }
        if (g_captured_total_pnl_after_cost != nullptr) {
            *g_captured_total_pnl_after_cost = ctx.total_pnl_after_cost;
        }
        if (g_captured_run_type != nullptr) {
            *g_captured_run_type = ctx.run_type;
        }
        if (g_captured_contract_multipliers != nullptr) {
            *g_captured_contract_multipliers = ctx.contract_multipliers;
        }

        std::vector<SignalIntent> signals;
        auto append_signal = [&](SignalType type, Side side, OffsetFlag offset,
                                 const std::string& suffix) {
            SignalIntent signal;
            signal.strategy_id = id_;
            signal.instrument_id = state.instrument_id;
            signal.signal_type = type;
            signal.side = side;
            signal.offset = offset;
            signal.volume = volume_;
            signal.limit_price = state.bar_close;
            signal.ts_ns = signal_ts_ns_ == 0 ? state.ts_ns : signal_ts_ns_;
            signal.trace_id = trace_base_ + suffix;
            signals.push_back(signal);
        };

        if (emit_force_close_) {
            append_signal(SignalType::kForceClose, close_side_, OffsetFlag::kClose, "-force");
        }
        if (emit_stop_loss_) {
            append_signal(SignalType::kStopLoss, close_side_, OffsetFlag::kClose, "-stop");
        }
        if (emit_take_profit_) {
            append_signal(SignalType::kTakeProfit, close_side_, OffsetFlag::kClose, "-take");
        }
        if (emit_close_) {
            append_signal(SignalType::kClose, close_side_, OffsetFlag::kClose, "-close");
        }
        if (emit_open_ && max_open_emits_ != 0) {
            append_signal(SignalType::kOpen, open_side_, OffsetFlag::kOpen, "-open");
            if (max_open_emits_ > 0) {
                --max_open_emits_;
            }
        }
        return signals;
    }

    std::vector<SignalIntent> OnBacktestTick(const AtomicTickSnapshot& tick,
                                             const AtomicStrategyContext& ctx) override {
        (void)ctx;
        std::vector<SignalIntent> signals;
        auto append_signal = [&](SignalType type, Side side, OffsetFlag offset,
                                 const std::string& suffix) {
            SignalIntent signal;
            signal.strategy_id = id_;
            signal.instrument_id = tick.instrument_id;
            signal.signal_type = type;
            signal.side = side;
            signal.offset = offset;
            signal.volume = volume_;
            signal.limit_price = tick.last_price;
            signal.ts_ns = tick.ts_ns;
            signal.trace_id = trace_base_ + suffix;
            signals.push_back(signal);
        };
        if (emit_tick_stop_loss_) {
            append_signal(SignalType::kStopLoss, close_side_, OffsetFlag::kClose, "-tick-stop");
        }
        if (emit_tick_take_profit_) {
            append_signal(SignalType::kTakeProfit, close_side_, OffsetFlag::kClose, "-tick-take");
        }
        return signals;
    }

    void OnOrderEvent(const OrderEvent& event, const AtomicStrategyContext& ctx) override {
        (void)event;
        if (g_captured_account_equity != nullptr) {
            *g_captured_account_equity = ctx.account_equity;
        }
        if (g_captured_total_pnl_after_cost != nullptr) {
            *g_captured_total_pnl_after_cost = ctx.total_pnl_after_cost;
        }
    }

    std::optional<AtomicIndicatorSnapshot> IndicatorSnapshot() const override {
        if (!snapshot_kama_.has_value() && !snapshot_atr_.has_value() &&
            !snapshot_er_.has_value() && !snapshot_stop_loss_price_.has_value() &&
            !snapshot_take_profit_price_.has_value()) {
            return std::nullopt;
        }
        AtomicIndicatorSnapshot snapshot;
        snapshot.kama = snapshot_kama_;
        snapshot.atr = snapshot_atr_;
        snapshot.er = snapshot_er_;
        snapshot.stop_loss_price = snapshot_stop_loss_price_;
        snapshot.take_profit_price = snapshot_take_profit_price_;
        return snapshot;
    }

   private:
    std::string id_;
    bool emit_open_{false};
    bool emit_close_{false};
    bool emit_stop_loss_{false};
    bool emit_take_profit_{false};
    bool emit_force_close_{false};
    bool emit_tick_stop_loss_{false};
    bool emit_tick_take_profit_{false};
    int max_open_emits_{-1};
    int volume_{1};
    EpochNanos signal_ts_ns_{0};
    std::string trace_base_{"trace"};
    Side open_side_{Side::kBuy};
    Side close_side_{Side::kSell};
    std::optional<double> snapshot_kama_;
    std::optional<double> snapshot_atr_;
    std::optional<double> snapshot_er_;
    std::optional<double> snapshot_stop_loss_price_;
    std::optional<double> snapshot_take_profit_price_;
};

std::string UniqueType(const std::string& stem) {
    static std::atomic<int> seq{0};
    return "composite_strategy_test_" + stem + "_" + std::to_string(seq.fetch_add(1));
}

void RegisterScriptedType(const std::string& type) {
    AtomicFactory& factory = AtomicFactory::Instance();
    if (factory.Has(type)) {
        return;
    }
    std::string error;
    ASSERT_TRUE(factory.Register(
        type, []() { return std::make_unique<ScriptedSubStrategy>(); }, &error))
        << error;
}

SubStrategyDefinition MakeSubStrategy(const std::string& id, const std::string& type,
                                      const AtomicParams& params, bool enabled = true) {
    SubStrategyDefinition definition;
    definition.id = id;
    definition.type = type;
    definition.enabled = enabled;
    definition.params = params;
    return definition;
}

StateSnapshot7D MakeState(const std::string& instrument, EpochNanos ts_ns = 1,
                          MarketRegime market_regime = MarketRegime::kUnknown,
                          std::int32_t timeframe_minutes = 1) {
    StateSnapshot7D state;
    state.instrument_id = instrument;
    state.timeframe_minutes = timeframe_minutes;
    state.has_bar = true;
    state.bar_open = 100.0;
    state.bar_high = 101.0;
    state.bar_low = 99.0;
    state.bar_close = 100.5;
    state.bar_volume = 10.0;
    state.market_regime = market_regime;
    state.ts_ns = ts_ns;
    return state;
}

StateSnapshot7D MakeBarState(const std::string& instrument, double close, EpochNanos ts_ns,
                             MarketRegime market_regime = MarketRegime::kUnknown,
                             std::int32_t timeframe_minutes = 1) {
    StateSnapshot7D state = MakeState(instrument, ts_ns, market_regime, timeframe_minutes);
    state.bar_open = close;
    state.bar_high = close + 1.0;
    state.bar_low = close - 1.0;
    state.bar_close = close;
    return state;
}

EpochNanos PseudoLocalEpochNs(int hour, int minute, int second = 0) {
    return (static_cast<EpochNanos>(hour) * 60LL * 60LL + static_cast<EpochNanos>(minute) * 60LL +
            static_cast<EpochNanos>(second)) *
           1'000'000'000LL;
}

EpochNanos AddPseudoLocalDays(EpochNanos ts_ns, int days) {
    return ts_ns + static_cast<EpochNanos>(days) * 24LL * 60LL * 60LL * 1'000'000'000LL;
}

StrategyContext MakeStrategyContext(const std::string& run_type = "backtest") {
    StrategyContext ctx;
    ctx.strategy_id = "composite";
    ctx.account_id = "acct";
    ctx.metadata["run_type"] = run_type;
    return ctx;
}

OrderEvent MakeOrderEvent(const std::string& strategy_id, const std::string& instrument_id,
                          Side side, OffsetFlag offset, std::int32_t filled_volume,
                          double fill_price, const std::string& client_order_id,
                          const std::string& exchange_order_id = "") {
    OrderEvent event;
    event.strategy_id = strategy_id;
    event.instrument_id = instrument_id;
    event.side = side;
    event.offset = offset;
    event.filled_volume = filled_volume;
    event.avg_fill_price = fill_price;
    event.client_order_id = client_order_id;
    event.exchange_order_id = exchange_order_id;
    return event;
}

AtomicParams MakeTrendParams() {
    return {{"id", "trend_1"},       {"er_period", "2"},         {"fast_period", "2"},
            {"slow_period", "4"},    {"kama_filter", "0.0"},     {"risk_per_trade_pct", "0.01"},
            {"default_volume", "1"}, {"stop_loss_mode", "none"}, {"take_profit_mode", "none"}};
}

}  // namespace

TEST(CompositeStrategyTest, DispatchesOnlyEnabledSubStrategies) {
    const std::string enabled_type = UniqueType("enabled");
    const std::string disabled_type = UniqueType("disabled");
    RegisterScriptedType(enabled_type);
    RegisterScriptedType(disabled_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {
        MakeSubStrategy("s1", enabled_type,
                        {{"id", "s1"}, {"emit_open", "1"}, {"open_side", "buy"}}),
        MakeSubStrategy("s2", disabled_type,
                        {{"id", "s2"}, {"emit_open", "1"}, {"open_side", "buy"}}, false),
    };

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());

    std::vector<std::string> call_log;
    g_call_log = &call_log;
    const std::vector<SignalIntent> signals = strategy.OnState(MakeState("rb2405", 10));
    g_call_log = nullptr;

    ASSERT_EQ(call_log.size(), 1U);
    EXPECT_EQ(call_log.front(), "s1");
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().strategy_id, "s1");
    EXPECT_EQ(signals.front().signal_type, SignalType::kOpen);
}

TEST(CompositeStrategyTest, DispatchesSubStrategiesByTimeframeMinutes) {
    const std::string tf1_type = UniqueType("tf1");
    const std::string tf5_type = UniqueType("tf5");
    RegisterScriptedType(tf1_type);
    RegisterScriptedType(tf5_type);

    SubStrategyDefinition tf1 =
        MakeSubStrategy("tf1", tf1_type, {{"id", "tf1"}, {"emit_open", "1"}, {"open_side", "buy"}});
    tf1.timeframe_minutes = 1;
    SubStrategyDefinition tf5 =
        MakeSubStrategy("tf5", tf5_type, {{"id", "tf5"}, {"emit_open", "1"}, {"open_side", "buy"}});
    tf5.timeframe_minutes = 5;

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {tf1, tf5};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());

    std::vector<std::string> call_log;
    g_call_log = &call_log;
    const std::vector<SignalIntent> tf1_signals =
        strategy.OnState(MakeState("rb2405", 10, MarketRegime::kUnknown, 1));
    g_call_log = nullptr;
    ASSERT_EQ(call_log.size(), 1U);
    EXPECT_EQ(call_log.front(), "tf1");
    ASSERT_EQ(tf1_signals.size(), 1U);
    EXPECT_EQ(tf1_signals.front().strategy_id, "tf1");

    call_log.clear();
    g_call_log = &call_log;
    const std::vector<SignalIntent> tf5_signals =
        strategy.OnState(MakeState("rb2405", 11, MarketRegime::kUnknown, 5));
    g_call_log = nullptr;
    ASSERT_EQ(call_log.size(), 1U);
    EXPECT_EQ(call_log.front(), "tf5");
    ASSERT_EQ(tf5_signals.size(), 1U);
    EXPECT_EQ(tf5_signals.front().strategy_id, "tf5");
}

TEST(CompositeStrategyTest, MarketRegimeFilterAppliesOnlyToOpenSignals) {
    const std::string sub_type = UniqueType("regime");
    RegisterScriptedType(sub_type);

    SubStrategyDefinition sub = MakeSubStrategy(
        "s1", sub_type,
        {{"id", "s1"}, {"emit_open", "1"}, {"emit_stop_loss", "1"}, {"open_side", "buy"}});
    sub.entry_market_regimes = {MarketRegime::kStrongTrend};

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.market_state_mode = true;
    definition.sub_strategies = {sub};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.OnOrderEvent(
        MakeOrderEvent("s1", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0, "open-fill-1"));

    const std::vector<SignalIntent> signals =
        strategy.OnState(MakeState("rb2405", 20, MarketRegime::kRanging));
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kStopLoss);
    EXPECT_EQ(signals.front().offset, OffsetFlag::kClose);
}

TEST(CompositeStrategyTest, EmitsSameCycleCloseAndReverseOpenForOwnedPosition) {
    const std::string sell_type = UniqueType("sell");
    RegisterScriptedType(sell_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy(
        "s1", sell_type,
        {{"id", "s1"}, {"emit_open", "1"}, {"open_side", "sell"}, {"volume", "1"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.OnOrderEvent(
        MakeOrderEvent("s1", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0, "owner-open"));

    const std::vector<SignalIntent> first = strategy.OnState(MakeState("rb2405", 30));
    ASSERT_EQ(first.size(), 2U);
    EXPECT_EQ(first.front().signal_type, SignalType::kClose);
    EXPECT_EQ(first.front().offset, OffsetFlag::kClose);
    EXPECT_EQ(first.front().side, Side::kSell);
    EXPECT_EQ(first[1].signal_type, SignalType::kOpen);
    EXPECT_EQ(first[1].offset, OffsetFlag::kOpen);
    EXPECT_EQ(first[1].side, Side::kSell);

    strategy.OnOrderEvent(
        MakeOrderEvent("s1", "rb2405", Side::kSell, OffsetFlag::kClose, 1, 99.0, "owner-close"));
    strategy.OnOrderEvent(
        MakeOrderEvent("s1", "rb2405", Side::kSell, OffsetFlag::kOpen, 1, 99.0, "reverse-open"));

    const std::vector<SignalIntent> second = strategy.OnState(MakeState("rb2405", 31));
    EXPECT_TRUE(second.empty());
}

TEST(CompositeStrategyTest, ReverseOpenCanBeDisabledPerSubStrategy) {
    const std::string sell_type = UniqueType("sell_no_reverse");
    RegisterScriptedType(sell_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy("s1", sell_type,
                                                 {{"id", "s1"},
                                                  {"emit_open", "1"},
                                                  {"max_open_emits", "1"},
                                                  {"open_side", "sell"},
                                                  {"allow_reverse_open", "false"},
                                                  {"volume", "1"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.OnOrderEvent(
        MakeOrderEvent("s1", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0, "owner-open"));

    const std::vector<SignalIntent> first = strategy.OnState(MakeState("rb2405", 30));
    EXPECT_TRUE(first.empty());

    strategy.OnOrderEvent(
        MakeOrderEvent("s1", "rb2405", Side::kSell, OffsetFlag::kClose, 1, 99.0, "manual-close"));
    const std::vector<SignalIntent> second = strategy.OnState(MakeState("rb2405", 31));
    EXPECT_TRUE(second.empty());
}

TEST(CompositeStrategyTest, RepeatedReverseCloseTraceUsesUniqueExchangeOrderIdForPositionSync) {
    const std::string sell_type = UniqueType("reverse_close_order_identity");
    RegisterScriptedType(sell_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy(
        "s1", sell_type,
        {{"id", "s1"}, {"emit_open", "1"}, {"open_side", "sell"}, {"volume", "8"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());

    strategy.OnOrderEvent(MakeOrderEvent("s1", "rb2405", Side::kBuy, OffsetFlag::kOpen, 7, 100.0,
                                         "buy-open-1", "fill-1"));
    strategy.OnOrderEvent(MakeOrderEvent("s1", "rb2405", Side::kSell, OffsetFlag::kClose, 7, 99.0,
                                         "shared-reverse-close-trace", "fill-2"));
    strategy.OnOrderEvent(MakeOrderEvent("s1", "rb2405", Side::kSell, OffsetFlag::kOpen, 8, 99.0,
                                         "sell-open-1", "fill-3"));
    strategy.OnOrderEvent(MakeOrderEvent("s1", "rb2405", Side::kBuy, OffsetFlag::kClose, 8, 98.0,
                                         "shared-reverse-close-trace", "fill-4"));
    strategy.OnOrderEvent(MakeOrderEvent("s1", "rb2405", Side::kBuy, OffsetFlag::kOpen, 8, 98.0,
                                         "buy-open-2", "fill-5"));

    const std::vector<SignalIntent> signals = strategy.OnState(MakeState("rb2405", 32));
    ASSERT_EQ(signals.size(), 2U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kClose);
    EXPECT_EQ(signals.front().offset, OffsetFlag::kClose);
    EXPECT_EQ(signals.front().side, Side::kSell);
    EXPECT_EQ(signals.front().volume, 8);
    EXPECT_EQ(signals[1].signal_type, SignalType::kOpen);
    EXPECT_EQ(signals[1].offset, OffsetFlag::kOpen);
    EXPECT_EQ(signals[1].side, Side::kSell);
    EXPECT_EQ(signals[1].volume, 8);
}

TEST(CompositeStrategyTest, ReverseOpenDoesNotBypassHigherPriorityCloseSignals) {
    const std::string stop_and_sell_type = UniqueType("stop_and_sell");
    RegisterScriptedType(stop_and_sell_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy("s1", stop_and_sell_type,
                                                 {{"id", "s1"},
                                                  {"emit_open", "1"},
                                                  {"emit_stop_loss", "1"},
                                                  {"open_side", "sell"},
                                                  {"close_side", "sell"},
                                                  {"volume", "1"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.OnOrderEvent(
        MakeOrderEvent("s1", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0, "owner-open"));

    const std::vector<SignalIntent> signals = strategy.OnState(MakeState("rb2405", 30));
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kStopLoss);
    EXPECT_EQ(signals.front().offset, OffsetFlag::kClose);
    EXPECT_EQ(signals.front().side, Side::kSell);
}

TEST(CompositeStrategyTest, BuiltinTrendStrategyReverseOpenHonorsAllowReverseSetting) {
    auto run_until_reverse = [](bool allow_reverse_open) {
        CompositeStrategyDefinition definition;
        definition.run_type = "backtest";
        AtomicParams params = MakeTrendParams();
        params["allow_reverse_open"] = allow_reverse_open ? "true" : "false";
        definition.sub_strategies = {MakeSubStrategy("trend_1", "TrendStrategy", params)};

        CompositeStrategy strategy(definition, &AtomicFactory::Instance());
        StrategyContext ctx = MakeStrategyContext();
        ctx.metadata["run_type"] = "backtest";
        strategy.Initialize(ctx);
        strategy.SetBacktestAccountSnapshot(100000.0, 0.0);
        strategy.SetBacktestContractMultiplier("rb2405", 10.0);

        std::vector<SignalIntent> open_signals;
        const std::vector<double> rising = {100, 101, 102, 103, 104};
        for (std::size_t i = 0; i < rising.size(); ++i) {
            open_signals =
                strategy.OnState(MakeBarState("rb2405", rising[i], static_cast<EpochNanos>(i + 1)));
        }
        EXPECT_EQ(open_signals.size(), 1U);
        EXPECT_EQ(open_signals.front().signal_type, SignalType::kOpen);
        EXPECT_EQ(open_signals.front().side, Side::kBuy);

        strategy.OnOrderEvent(MakeOrderEvent("trend_1", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1,
                                             104.0, "open-fill"));

        std::vector<SignalIntent> reverse_signals;
        const std::vector<double> falling = {103, 100, 97, 94, 91};
        for (std::size_t i = 0; i < falling.size(); ++i) {
            reverse_signals = strategy.OnState(
                MakeBarState("rb2405", falling[i], static_cast<EpochNanos>(100 + i)));
        }
        return reverse_signals;
    };

    const std::vector<SignalIntent> disabled_signals = run_until_reverse(false);
    EXPECT_TRUE(disabled_signals.empty());

    const std::vector<SignalIntent> enabled_signals = run_until_reverse(true);
    ASSERT_EQ(enabled_signals.size(), 2U);
    EXPECT_EQ(enabled_signals.front().signal_type, SignalType::kClose);
    EXPECT_EQ(enabled_signals.front().offset, OffsetFlag::kClose);
    EXPECT_EQ(enabled_signals.front().side, Side::kSell);
    EXPECT_EQ(enabled_signals[1].signal_type, SignalType::kOpen);
    EXPECT_EQ(enabled_signals[1].offset, OffsetFlag::kOpen);
    EXPECT_EQ(enabled_signals[1].side, Side::kSell);
}

TEST(CompositeStrategyTest, TickAwareSubStrategySignalsExitThroughCompositeBacktestTick) {
    const std::string tick_type = UniqueType("tick_exit");
    RegisterScriptedType(tick_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy(
        "tick", tick_type, {{"id", "tick"}, {"emit_tick_take_profit", "1"}, {"volume", "1"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.OnOrderEvent(
        MakeOrderEvent("tick", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0, "tick-open"));

    const std::vector<SignalIntent> tick_signals = strategy.OnBacktestTick("rb2405", 1000, 105.0);
    ASSERT_EQ(tick_signals.size(), 1U);
    EXPECT_EQ(tick_signals.front().strategy_id, "tick");
    EXPECT_EQ(tick_signals.front().signal_type, SignalType::kTakeProfit);
    EXPECT_EQ(tick_signals.front().offset, OffsetFlag::kClose);
    EXPECT_EQ(tick_signals.front().side, Side::kSell);
}

TEST(CompositeStrategyTest, MergesByPriorityThenVolumeThenTimestampThenTraceId) {
    const std::string stop_type = UniqueType("stop");
    const std::string open_type_a = UniqueType("open_a");
    const std::string open_type_b = UniqueType("open_b");
    const std::string open_type_c = UniqueType("open_c");
    RegisterScriptedType(stop_type);
    RegisterScriptedType(open_type_a);
    RegisterScriptedType(open_type_b);
    RegisterScriptedType(open_type_c);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {
        MakeSubStrategy("stop", stop_type,
                        {{"id", "stop"}, {"emit_stop_loss", "1"}, {"trace", "a"}, {"volume", "1"}}),
        MakeSubStrategy("openA", open_type_a,
                        {{"id", "openA"},
                         {"emit_open", "1"},
                         {"trace", "b"},
                         {"signal_ts_ns", "100"},
                         {"volume", "2"}}),
        MakeSubStrategy("openB", open_type_b,
                        {{"id", "openB"},
                         {"emit_open", "1"},
                         {"trace", "a"},
                         {"signal_ts_ns", "100"},
                         {"volume", "2"}}),
        MakeSubStrategy("openC", open_type_c,
                        {{"id", "openC"},
                         {"emit_open", "1"},
                         {"trace", "z"},
                         {"signal_ts_ns", "200"},
                         {"volume", "1"}}),
    };

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.OnOrderEvent(
        MakeOrderEvent("stop", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0, "priority-open"));

    const std::vector<SignalIntent> signals = strategy.OnState(MakeState("rb2405", 40));
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kStopLoss);
    EXPECT_EQ(signals.front().strategy_id, "stop");
}

TEST(CompositeStrategyTest, RejectsNonBacktestRunType) {
    const std::string sub_type = UniqueType("run_type");
    RegisterScriptedType(sub_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "sim";
    definition.sub_strategies = {
        MakeSubStrategy("s1", sub_type, {{"id", "s1"}, {"emit_open", "1"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    StrategyContext ctx = MakeStrategyContext();
    ctx.metadata["run_type"] = "sim";
    EXPECT_THROW(strategy.Initialize(ctx), std::runtime_error);
}

TEST(CompositeStrategyTest, AllowsNonBacktestRunTypeWhenEnabled) {
    const std::string sub_type = UniqueType("run_type_enabled");
    RegisterScriptedType(sub_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "sim";
    definition.enable_non_backtest = true;
    definition.sub_strategies = {
        MakeSubStrategy("s1", sub_type, {{"id", "s1"}, {"emit_open", "1"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    StrategyContext ctx = MakeStrategyContext();
    ctx.metadata["run_type"] = "sim";
    EXPECT_NO_THROW(strategy.Initialize(ctx));
}

TEST(CompositeStrategyTest, AppliesRunModeOverridesBeforeAtomicInit) {
    const std::string sub_type = UniqueType("override_mode");
    RegisterScriptedType(sub_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "sim";
    definition.enable_non_backtest = true;
    SubStrategyDefinition sub =
        MakeSubStrategy("s1", sub_type, {{"id", "s1"}, {"emit_open", "1"}, {"volume", "1"}});
    sub.overrides.sim_params["volume"] = "5";
    definition.sub_strategies = {sub};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    StrategyContext ctx = MakeStrategyContext();
    ctx.metadata["run_type"] = "sim";
    strategy.Initialize(ctx);

    const std::vector<SignalIntent> signals = strategy.OnState(MakeState("rb2405", 70));
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().volume, 5);
}

TEST(CompositeStrategyTest, TimeFilterStrategiesCanBlockOpenSignals) {
    const std::string open_type = UniqueType("open_time_filter");
    RegisterScriptedType(open_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {
        MakeSubStrategy("open", open_type, {{"id", "open"}, {"emit_open", "1"}}),
        MakeSubStrategy(
            "time_filter", "TimeFilter",
            {{"id", "time_filter"}, {"start_hour", "0"}, {"end_hour", "23"}, {"timezone", "UTC"}}),
    };

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());

    const std::vector<SignalIntent> blocked = strategy.OnState(MakeState("rb2405", 0));
    EXPECT_TRUE(blocked.empty());
}

TEST(CompositeStrategyTest, SubStrategyForbidOpenWindowsBlockOnlyMatchingStrategy) {
    const std::string blocked_type = UniqueType("blocked_window");
    const std::string allowed_type = UniqueType("allowed_window");
    RegisterScriptedType(blocked_type);
    RegisterScriptedType(allowed_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "live";
    definition.enable_non_backtest = true;
    definition.sub_strategies = {
        MakeSubStrategy("blocked", blocked_type,
                        {{"id", "blocked"},
                         {"emit_open", "1"},
                         {"trace", "a"},
                         {"forbid_open_windows", "00:00-01:00"},
                         {"window_timezone", "UTC"}}),
        MakeSubStrategy("allowed", allowed_type,
                        {{"id", "allowed"}, {"emit_open", "1"}, {"trace", "z"}}),
    };

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext("live"));

    const EpochNanos blocked_ts_ns = 30LL * 60LL * 1'000'000'000LL;
    const std::vector<SignalIntent> signals = strategy.OnState(MakeState("rb2405", blocked_ts_ns));
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().strategy_id, "allowed");
    EXPECT_EQ(signals.front().signal_type, SignalType::kOpen);
}

TEST(CompositeStrategyTest, ForceCloseWindowsAlsoBlockOpeningSignals) {
    const std::string open_type = UniqueType("force_close_window");
    RegisterScriptedType(open_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "live";
    definition.enable_non_backtest = true;
    definition.sub_strategies = {MakeSubStrategy("open", open_type,
                                                 {{"id", "open"},
                                                  {"emit_open", "1"},
                                                  {"force_close_windows", "23:00-01:00"},
                                                  {"window_timezone", "UTC"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext("live"));

    const EpochNanos inside_window_ts_ns = 30LL * 60LL * 1'000'000'000LL;
    const std::vector<SignalIntent> blocked =
        strategy.OnState(MakeState("rb2405", inside_window_ts_ns));
    EXPECT_TRUE(blocked.empty());

    const EpochNanos outside_window_ts_ns = 2LL * 60LL * 60LL * 1'000'000'000LL;
    const std::vector<SignalIntent> allowed =
        strategy.OnState(MakeState("rb2405", outside_window_ts_ns));
    ASSERT_EQ(allowed.size(), 1U);
    EXPECT_EQ(allowed.front().strategy_id, "open");
    EXPECT_EQ(allowed.front().signal_type, SignalType::kOpen);
}

TEST(CompositeStrategyTest, ForbidOpenWindowsBlockOpenButAllowCloseSignals) {
    const std::string open_type = UniqueType("forbid_open_allows_close");
    RegisterScriptedType(open_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {
        MakeSubStrategy("open", open_type,
                        {{"id", "open"},
                         {"emit_open", "1"},
                         {"emit_stop_loss", "1"},
                         {"forbid_open_windows", "09:00-09:30,21:00-21:30"},
                         {"window_timezone", "Asia/Shanghai"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());

    EXPECT_TRUE(strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(9, 10))).empty());
    EXPECT_TRUE(strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(21, 10))).empty());

    const std::vector<SignalIntent> allowed =
        strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(9, 30)));
    ASSERT_EQ(allowed.size(), 1U);
    EXPECT_EQ(allowed.front().signal_type, SignalType::kOpen);

    strategy.OnOrderEvent(
        MakeOrderEvent("open", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0, "open-fill"));
    const std::vector<SignalIntent> close_allowed =
        strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(21, 10)));
    ASSERT_EQ(close_allowed.size(), 1U);
    EXPECT_EQ(close_allowed.front().signal_type, SignalType::kStopLoss);
    EXPECT_EQ(close_allowed.front().offset, OffsetFlag::kClose);
}

TEST(CompositeStrategyTest, DailyMaxLossGuardBlocksOnlySameDayOpenSignals) {
    const std::string open_type = UniqueType("daily_loss_guard");
    RegisterScriptedType(open_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy("open", open_type,
                                                 {{"id", "open"},
                                                  {"emit_open", "1"},
                                                  {"emit_stop_loss", "1"},
                                                  {"daily_max_loss_R", "3"},
                                                  {"risk_per_trade_pct", "0.01"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.SetBacktestAccountSnapshot(100000.0, 0.0);
    strategy.SetBacktestContractMultiplier("rb2405", 10.0);

    OrderEvent open_fill =
        MakeOrderEvent("open", "rb2405", Side::kBuy, OffsetFlag::kOpen, 30, 100.0, "open-1");
    open_fill.ts_ns = PseudoLocalEpochNs(10, 0);
    strategy.OnOrderEvent(open_fill);
    OrderEvent loss_fill =
        MakeOrderEvent("open", "rb2405", Side::kSell, OffsetFlag::kClose, 30, 90.0, "close-1");
    loss_fill.ts_ns = PseudoLocalEpochNs(10, 5);
    strategy.OnOrderEvent(loss_fill);

    EXPECT_TRUE(strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(10, 10))).empty());

    OrderEvent protective_open =
        MakeOrderEvent("open", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0, "open-2");
    protective_open.ts_ns = PseudoLocalEpochNs(10, 15);
    strategy.OnOrderEvent(protective_open);
    const std::vector<SignalIntent> close_allowed =
        strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(10, 20)));
    ASSERT_EQ(close_allowed.size(), 1U);
    EXPECT_EQ(close_allowed.front().signal_type, SignalType::kStopLoss);

    OrderEvent protective_close =
        MakeOrderEvent("open", "rb2405", Side::kSell, OffsetFlag::kClose, 1, 101.0, "close-2");
    protective_close.ts_ns = PseudoLocalEpochNs(10, 25);
    strategy.OnOrderEvent(protective_close);
    const std::vector<SignalIntent> next_day =
        strategy.OnState(MakeState("rb2405", AddPseudoLocalDays(PseudoLocalEpochNs(10, 10), 1)));
    ASSERT_EQ(next_day.size(), 1U);
    EXPECT_EQ(next_day.front().signal_type, SignalType::kOpen);
}

TEST(CompositeStrategyTest, DailyMaxLossGuardUsesDynamicRiskBudgetFromAccountEquity) {
    const std::string open_type = UniqueType("daily_loss_dynamic_r");
    RegisterScriptedType(open_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy("open", open_type,
                                                 {{"id", "open"},
                                                  {"emit_open", "1"},
                                                  {"daily_max_loss_R", "3"},
                                                  {"risk_per_trade_pct", "0.01"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.SetBacktestAccountSnapshot(200000.0, 0.0);
    strategy.SetBacktestContractMultiplier("rb2405", 10.0);

    OrderEvent open_fill =
        MakeOrderEvent("open", "rb2405", Side::kBuy, OffsetFlag::kOpen, 30, 100.0, "open-1");
    open_fill.ts_ns = PseudoLocalEpochNs(10, 0);
    strategy.OnOrderEvent(open_fill);
    OrderEvent loss_fill =
        MakeOrderEvent("open", "rb2405", Side::kSell, OffsetFlag::kClose, 30, 90.0, "close-1");
    loss_fill.ts_ns = PseudoLocalEpochNs(10, 5);
    strategy.OnOrderEvent(loss_fill);

    const std::vector<SignalIntent> allowed =
        strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(10, 10)));
    ASSERT_EQ(allowed.size(), 1U);
    EXPECT_EQ(allowed.front().signal_type, SignalType::kOpen);

    strategy.SetBacktestAccountSnapshot(100000.0, 0.0);
    EXPECT_TRUE(strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(10, 15))).empty());
}

TEST(CompositeStrategyTest, ConsecutiveLossGuardPausesOpenSignalsUntilNextDay) {
    const std::string open_type = UniqueType("consecutive_loss_guard");
    RegisterScriptedType(open_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy(
        "open", open_type, {{"id", "open"}, {"emit_open", "1"}, {"max_consecutive_losses", "3"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.SetBacktestContractMultiplier("rb2405", 10.0);

    for (int index = 0; index < 3; ++index) {
        const EpochNanos day_base = AddPseudoLocalDays(PseudoLocalEpochNs(10, 0), index);
        OrderEvent open_fill = MakeOrderEvent("open", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1,
                                              100.0, "open-" + std::to_string(index));
        open_fill.ts_ns = day_base;
        strategy.OnOrderEvent(open_fill);
        OrderEvent loss_fill = MakeOrderEvent("open", "rb2405", Side::kSell, OffsetFlag::kClose, 1,
                                              99.0, "close-" + std::to_string(index));
        loss_fill.ts_ns = day_base + 60LL * 1'000'000'000LL;
        strategy.OnOrderEvent(loss_fill);
    }

    const EpochNanos third_loss_day = AddPseudoLocalDays(PseudoLocalEpochNs(10, 10), 2);
    EXPECT_TRUE(strategy.OnState(MakeState("rb2405", third_loss_day)).empty());

    const std::vector<SignalIntent> next_day =
        strategy.OnState(MakeState("rb2405", AddPseudoLocalDays(third_loss_day, 1)));
    ASSERT_EQ(next_day.size(), 1U);
    EXPECT_EQ(next_day.front().signal_type, SignalType::kOpen);
}

TEST(CompositeStrategyTest, RejectsInvalidSubStrategyTimeWindows) {
    const std::string open_type = UniqueType("invalid_window");
    RegisterScriptedType(open_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {
        MakeSubStrategy("open", open_type,
                        {{"id", "open"},
                         {"emit_open", "1"},
                         {"forbid_open_windows", "10:00-10:00"},
                         {"window_timezone", "UTC"}}),
    };

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    EXPECT_THROW(strategy.Initialize(MakeStrategyContext()), std::runtime_error);
}

TEST(CompositeStrategyTest, BacktestForbidOpenWindowsUsePseudoLocalAsiaShanghaiTime) {
    const std::string open_type = UniqueType("backtest_asia_shanghai_window");
    RegisterScriptedType(open_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy("open", open_type,
                                                 {{"id", "open"},
                                                  {"emit_open", "1"},
                                                  {"forbid_open_windows", "09:00-09:30"},
                                                  {"window_timezone", "Asia/Shanghai"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());

    const std::vector<SignalIntent> blocked =
        strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(9, 10)));
    EXPECT_TRUE(blocked.empty());
}

TEST(CompositeStrategyTest, BacktestForbidOpenWindowsMapPseudoLocalTimeIntoUtcWindow) {
    const std::string open_type = UniqueType("backtest_utc_window");
    RegisterScriptedType(open_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy("open", open_type,
                                                 {{"id", "open"},
                                                  {"emit_open", "1"},
                                                  {"forbid_open_windows", "01:00-01:30"},
                                                  {"window_timezone", "UTC"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());

    const std::vector<SignalIntent> blocked =
        strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(9, 10)));
    EXPECT_TRUE(blocked.empty());
}

TEST(CompositeStrategyTest, BacktestForceCloseWindowsRespectCrossNightAsiaShanghaiWindow) {
    const std::string open_type = UniqueType("backtest_cross_night_window");
    RegisterScriptedType(open_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy("open", open_type,
                                                 {{"id", "open"},
                                                  {"emit_open", "1"},
                                                  {"force_close_windows", "23:00-01:00"},
                                                  {"window_timezone", "Asia/Shanghai"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());

    const std::vector<SignalIntent> blocked =
        strategy.OnState(MakeState("rb2405", PseudoLocalEpochNs(23, 30)));
    EXPECT_TRUE(blocked.empty());
}

TEST(CompositeStrategyTest, TimeFilterAppliesOnlyToMatchingTimeframe) {
    const std::string open_type = UniqueType("open_time_filter_tf");
    RegisterScriptedType(open_type);

    SubStrategyDefinition open_sub =
        MakeSubStrategy("open", open_type, {{"id", "open"}, {"emit_open", "1"}});
    open_sub.timeframe_minutes = 1;
    SubStrategyDefinition time_filter_sub = MakeSubStrategy(
        "time_filter", "TimeFilter",
        {{"id", "time_filter"}, {"start_hour", "0"}, {"end_hour", "23"}, {"timezone", "UTC"}});
    time_filter_sub.timeframe_minutes = 5;

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {open_sub, time_filter_sub};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());

    const std::vector<SignalIntent> signals =
        strategy.OnState(MakeState("rb2405", 0, MarketRegime::kUnknown, 1));
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().strategy_id, "open");
    EXPECT_EQ(signals.front().signal_type, SignalType::kOpen);
}

TEST(CompositeStrategyTest, SaveAndLoadStateRestoresPositionGate) {
    const std::string sub_type = UniqueType("state_round_trip");
    RegisterScriptedType(sub_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy(
        "capture", sub_type, {{"id", "capture"}, {"emit_close", "1"}, {"close_side", "sell"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.OnOrderEvent(
        MakeOrderEvent("capture", "rb2405", Side::kBuy, OffsetFlag::kOpen, 2, 100.0, "state-open"));

    StrategyState snapshot;
    std::string error;
    ASSERT_TRUE(strategy.SaveState(&snapshot, &error)) << error;

    CompositeStrategy restored(definition, &AtomicFactory::Instance());
    restored.Initialize(MakeStrategyContext());
    ASSERT_TRUE(restored.LoadState(snapshot, &error)) << error;
    const std::vector<SignalIntent> signals = restored.OnState(MakeState("rb2405", 80));
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kClose);
}

TEST(CompositeStrategyTest, PropagatesBacktestSnapshotIntoAtomicContext) {
    const std::string sub_type = UniqueType("ctx");
    RegisterScriptedType(sub_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {
        MakeSubStrategy("capture", sub_type, {{"id", "capture"}, {"emit_open", "0"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.SetBacktestAccountSnapshot(200000.0, 1234.5);
    strategy.SetBacktestContractMultiplier("rb2405", 10.0);

    double captured_equity = 0.0;
    double captured_pnl = 0.0;
    std::string captured_run_type;
    std::unordered_map<std::string, double> captured_multipliers;
    g_captured_account_equity = &captured_equity;
    g_captured_total_pnl_after_cost = &captured_pnl;
    g_captured_run_type = &captured_run_type;
    g_captured_contract_multipliers = &captured_multipliers;
    (void)strategy.OnState(MakeState("rb2405", 50));
    g_captured_account_equity = nullptr;
    g_captured_total_pnl_after_cost = nullptr;
    g_captured_run_type = nullptr;
    g_captured_contract_multipliers = nullptr;

    EXPECT_DOUBLE_EQ(captured_equity, 200000.0);
    EXPECT_DOUBLE_EQ(captured_pnl, 1234.5);
    EXPECT_EQ(captured_run_type, "backtest");
    ASSERT_EQ(captured_multipliers.count("rb2405"), 1U);
    EXPECT_DOUBLE_EQ(captured_multipliers["rb2405"], 10.0);
}

TEST(CompositeStrategyTest, CollectAtomicIndicatorTraceContainsStopAndTakePrices) {
    const std::string trace_type = UniqueType("trace");
    RegisterScriptedType(trace_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy("trace", trace_type,
                                                 {{"id", "trace"},
                                                  {"snapshot_kama", "101.1"},
                                                  {"snapshot_atr", "1.2"},
                                                  {"snapshot_er", "0.6"},
                                                  {"snapshot_stop_loss_price", "98.8"},
                                                  {"snapshot_take_profit_price", "106.6"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    (void)strategy.OnState(MakeState("rb2405", 60));

    const std::vector<CompositeAtomicTraceRow> rows = strategy.CollectAtomicIndicatorTrace();
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows.front().strategy_id, "trace");
    ASSERT_TRUE(rows.front().stop_loss_price.has_value());
    ASSERT_TRUE(rows.front().take_profit_price.has_value());
    EXPECT_DOUBLE_EQ(rows.front().stop_loss_price.value(), 98.8);
    EXPECT_DOUBLE_EQ(rows.front().take_profit_price.value(), 106.6);
}

}  // namespace quant_hft
