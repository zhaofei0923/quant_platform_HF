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
        volume_ = ParseInt(GetOrDefault(params, "volume", "1"));
        signal_ts_ns_ = std::stoll(GetOrDefault(params, "signal_ts_ns", "0"));
        trace_base_ = GetOrDefault(params, "trace", id_);
        open_side_ =
            ToLower(GetOrDefault(params, "open_side", "buy")) == "sell" ? Side::kSell : Side::kBuy;
        close_side_ = ToLower(GetOrDefault(params, "close_side", "sell")) == "buy" ? Side::kBuy
                                                                                     : Side::kSell;

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
        if (emit_open_) {
            append_signal(SignalType::kOpen, open_side_, OffsetFlag::kOpen, "-open");
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
        if (!snapshot_kama_.has_value() && !snapshot_atr_.has_value() && !snapshot_er_.has_value() &&
            !snapshot_stop_loss_price_.has_value() && !snapshot_take_profit_price_.has_value()) {
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
    ASSERT_TRUE(factory.Register(type, []() { return std::make_unique<ScriptedSubStrategy>(); }, &error))
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
                          MarketRegime market_regime = MarketRegime::kUnknown) {
    StateSnapshot7D state;
    state.instrument_id = instrument;
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

StrategyContext MakeStrategyContext() {
    StrategyContext ctx;
    ctx.strategy_id = "composite";
    ctx.account_id = "acct";
    ctx.metadata["run_type"] = "backtest";
    return ctx;
}

OrderEvent MakeOrderEvent(const std::string& strategy_id, const std::string& instrument_id,
                          Side side, OffsetFlag offset, std::int32_t filled_volume,
                          double fill_price, const std::string& order_id) {
    OrderEvent event;
    event.strategy_id = strategy_id;
    event.instrument_id = instrument_id;
    event.side = side;
    event.offset = offset;
    event.filled_volume = filled_volume;
    event.avg_fill_price = fill_price;
    event.client_order_id = order_id;
    return event;
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
    strategy.OnOrderEvent(MakeOrderEvent("s1", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0,
                                         "open-fill-1"));

    const std::vector<SignalIntent> signals =
        strategy.OnState(MakeState("rb2405", 20, MarketRegime::kRanging));
    ASSERT_EQ(signals.size(), 1U);
    EXPECT_EQ(signals.front().signal_type, SignalType::kStopLoss);
    EXPECT_EQ(signals.front().offset, OffsetFlag::kClose);
}

TEST(CompositeStrategyTest, KeepsOwnershipGateAndReverseTwoStep) {
    const std::string sell_type = UniqueType("sell");
    RegisterScriptedType(sell_type);

    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.sub_strategies = {MakeSubStrategy(
        "s1", sell_type, {{"id", "s1"}, {"emit_open", "1"}, {"open_side", "sell"}, {"volume", "1"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    strategy.Initialize(MakeStrategyContext());
    strategy.OnOrderEvent(MakeOrderEvent("s1", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0,
                                         "owner-open"));

    const std::vector<SignalIntent> first = strategy.OnState(MakeState("rb2405", 30));
    ASSERT_EQ(first.size(), 1U);
    EXPECT_EQ(first.front().signal_type, SignalType::kClose);
    EXPECT_EQ(first.front().offset, OffsetFlag::kClose);
    EXPECT_EQ(first.front().side, Side::kSell);

    strategy.OnOrderEvent(MakeOrderEvent("s1", "rb2405", Side::kSell, OffsetFlag::kClose, 1, 99.0,
                                         "owner-close"));
    const std::vector<SignalIntent> second = strategy.OnState(MakeState("rb2405", 31));
    ASSERT_EQ(second.size(), 1U);
    EXPECT_EQ(second.front().signal_type, SignalType::kOpen);
    EXPECT_EQ(second.front().offset, OffsetFlag::kOpen);
    EXPECT_EQ(second.front().side, Side::kSell);
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
    strategy.OnOrderEvent(MakeOrderEvent("stop", "rb2405", Side::kBuy, OffsetFlag::kOpen, 1, 100.0,
                                         "priority-open"));

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
    definition.sub_strategies = {MakeSubStrategy("s1", sub_type, {{"id", "s1"}, {"emit_open", "1"}})};

    CompositeStrategy strategy(definition, &AtomicFactory::Instance());
    StrategyContext ctx = MakeStrategyContext();
    ctx.metadata["run_type"] = "sim";
    EXPECT_THROW(strategy.Initialize(ctx), std::runtime_error);
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
    definition.sub_strategies = {MakeSubStrategy(
        "trace", trace_type,
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
