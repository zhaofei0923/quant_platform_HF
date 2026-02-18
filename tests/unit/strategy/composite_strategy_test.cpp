#include "quant_hft/strategy/composite_strategy.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {
namespace {

std::vector<std::string>* g_call_log = nullptr;
std::unordered_map<std::string, std::int32_t>* g_captured_positions = nullptr;
std::unordered_map<std::string, double>* g_captured_avg_prices = nullptr;

bool ParseBool(const std::string& value) {
    return value == "1" || value == "true" || value == "TRUE";
}

int ParseInt(const std::string& value) { return std::stoi(value); }

std::string GetOrDefault(const AtomicParams& params, const std::string& key,
                         const std::string& def) {
    const auto it = params.find(key);
    return it == params.end() ? def : it->second;
}

class TestRiskControl final : public IRiskControlStrategy {
   public:
    void Init(const AtomicParams& params) override {
        id_ = GetOrDefault(params, "id", "risk");
        emit_ = ParseBool(GetOrDefault(params, "emit", "0"));
        volume_ = ParseInt(GetOrDefault(params, "volume", "1"));
        trace_id_ = GetOrDefault(params, "trace_id", "risk");
    }
    std::string GetId() const override { return id_; }
    void Reset() override {}
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        (void)ctx;
        if (g_call_log != nullptr) {
            g_call_log->push_back("risk");
        }
        if (!emit_) {
            return {};
        }
        SignalIntent signal;
        signal.strategy_id = id_;
        signal.instrument_id = state.instrument_id;
        signal.signal_type = SignalType::kForceClose;
        signal.side = Side::kSell;
        signal.offset = OffsetFlag::kClose;
        signal.volume = volume_;
        signal.limit_price = state.bar_close;
        signal.ts_ns = state.ts_ns;
        signal.trace_id = trace_id_;
        return {signal};
    }

   private:
    std::string id_;
    bool emit_{false};
    int volume_{1};
    std::string trace_id_;
};

class TestStopLoss final : public IStopLossStrategy {
   public:
    void Init(const AtomicParams& params) override {
        id_ = GetOrDefault(params, "id", "stop");
        emit_ = ParseBool(GetOrDefault(params, "emit", "0"));
    }
    std::string GetId() const override { return id_; }
    void Reset() override {}
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        (void)ctx;
        if (g_call_log != nullptr) {
            g_call_log->push_back("stop");
        }
        if (!emit_) {
            return {};
        }
        SignalIntent signal;
        signal.strategy_id = id_;
        signal.instrument_id = state.instrument_id;
        signal.signal_type = SignalType::kStopLoss;
        signal.side = Side::kSell;
        signal.offset = OffsetFlag::kClose;
        signal.volume = 1;
        signal.limit_price = state.bar_close;
        signal.ts_ns = state.ts_ns;
        signal.trace_id = "stop";
        return {signal};
    }

   private:
    std::string id_;
    bool emit_{false};
};

class TestTakeProfit final : public ITakeProfitStrategy {
   public:
    void Init(const AtomicParams& params) override {
        id_ = GetOrDefault(params, "id", "take");
        emit_ = ParseBool(GetOrDefault(params, "emit", "0"));
    }
    std::string GetId() const override { return id_; }
    void Reset() override {}
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        (void)ctx;
        if (g_call_log != nullptr) {
            g_call_log->push_back("take");
        }
        if (!emit_) {
            return {};
        }
        SignalIntent signal;
        signal.strategy_id = id_;
        signal.instrument_id = state.instrument_id;
        signal.signal_type = SignalType::kTakeProfit;
        signal.side = Side::kSell;
        signal.offset = OffsetFlag::kClose;
        signal.volume = 1;
        signal.limit_price = state.bar_close;
        signal.ts_ns = state.ts_ns;
        signal.trace_id = "take";
        return {signal};
    }

   private:
    std::string id_;
    bool emit_{false};
};

class TestTimeFilter final : public ITimeFilterStrategy {
   public:
    void Init(const AtomicParams& params) override {
        id_ = GetOrDefault(params, "id", "time");
        allow_ = ParseBool(GetOrDefault(params, "allow", "1"));
    }
    std::string GetId() const override { return id_; }
    void Reset() override {}
    bool AllowOpening(EpochNanos now_ns) override {
        (void)now_ns;
        if (g_call_log != nullptr) {
            g_call_log->push_back("time");
        }
        return allow_;
    }

   private:
    std::string id_;
    bool allow_{true};
};

class TestOpening final : public IOpeningStrategy {
   public:
    void Init(const AtomicParams& params) override {
        id_ = GetOrDefault(params, "id", "open");
        emit_ = ParseBool(GetOrDefault(params, "emit", "1"));
        volume_ = ParseInt(GetOrDefault(params, "volume", "1"));
        ts_ns_ = std::stoll(GetOrDefault(params, "signal_ts_ns", "0"));
        trace_id_ = GetOrDefault(params, "trace_id", "open");
    }
    std::string GetId() const override { return id_; }
    void Reset() override {}
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        (void)ctx;
        if (g_call_log != nullptr) {
            g_call_log->push_back("open");
        }
        if (!emit_) {
            return {};
        }
        SignalIntent signal;
        signal.strategy_id = id_;
        signal.instrument_id = state.instrument_id;
        signal.signal_type = SignalType::kOpen;
        signal.side = Side::kBuy;
        signal.offset = OffsetFlag::kOpen;
        signal.volume = volume_;
        signal.limit_price = state.bar_close;
        signal.ts_ns = (ts_ns_ == 0 ? state.ts_ns : ts_ns_);
        signal.trace_id = trace_id_;
        return {signal};
    }

   private:
    std::string id_;
    bool emit_{true};
    int volume_{1};
    EpochNanos ts_ns_{0};
    std::string trace_id_;
};

class ContextCaptureOpening final : public IOpeningStrategy {
   public:
    void Init(const AtomicParams& params) override { id_ = GetOrDefault(params, "id", "capture"); }
    std::string GetId() const override { return id_; }
    void Reset() override {}
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        (void)state;
        if (g_captured_positions != nullptr) {
            *g_captured_positions = ctx.net_positions;
        }
        if (g_captured_avg_prices != nullptr) {
            *g_captured_avg_prices = ctx.avg_open_prices;
        }
        return {};
    }

   private:
    std::string id_;
};

std::string UniqueType(const std::string& prefix) {
    static std::atomic<int> seq{0};
    return prefix + "_" + std::to_string(seq.fetch_add(1));
}

CompositeStrategyDefinition MakeDefinition(const std::string& risk_type,
                                           const std::string& stop_type,
                                           const std::string& take_type,
                                           const std::string& time_type,
                                           const std::vector<AtomicStrategyDefinition>& openings) {
    CompositeStrategyDefinition definition;
    definition.merge_rule = SignalMergeRule::kPriority;

    AtomicStrategyDefinition risk;
    risk.id = "risk_1";
    risk.type = risk_type;
    risk.params = {{"id", "risk_1"}, {"emit", "0"}};
    definition.risk_control_strategies.push_back(risk);

    AtomicStrategyDefinition stop;
    stop.id = "stop_1";
    stop.type = stop_type;
    stop.params = {{"id", "stop_1"}, {"emit", "0"}};
    definition.stop_loss_strategies.push_back(stop);

    AtomicStrategyDefinition take;
    take.id = "take_1";
    take.type = take_type;
    take.params = {{"id", "take_1"}, {"emit", "0"}};
    definition.take_profit_strategies.push_back(take);

    AtomicStrategyDefinition time;
    time.id = "time_1";
    time.type = time_type;
    time.params = {{"id", "time_1"}, {"allow", "1"}};
    definition.time_filters.push_back(time);

    definition.opening_strategies = openings;
    return definition;
}

StateSnapshot7D MakeState(MarketRegime regime = MarketRegime::kStrongTrend) {
    StateSnapshot7D state;
    state.instrument_id = "IF2406";
    state.has_bar = true;
    state.bar_close = 100.0;
    state.ts_ns = 123456789;
    state.market_regime = regime;
    return state;
}

OrderEvent MakeOrderEvent(const std::string& order_id, const std::string& instrument_id, Side side,
                          OffsetFlag offset, std::int32_t filled_volume, double avg_fill_price) {
    OrderEvent event;
    event.account_id = "acct";
    event.strategy_id = "composite";
    event.client_order_id = order_id;
    event.instrument_id = instrument_id;
    event.side = side;
    event.offset = offset;
    event.status = OrderStatus::kPartiallyFilled;
    event.total_volume = 10;
    event.filled_volume = filled_volume;
    event.avg_fill_price = avg_fill_price;
    return event;
}

}  // namespace

TEST(CompositeStrategyTest, ExecutesInExpectedOrderAndPriorityMergeKeepsHighestPriority) {
    AtomicFactory factory;
    std::string error;
    const std::string risk_type = UniqueType("risk");
    const std::string stop_type = UniqueType("stop");
    const std::string take_type = UniqueType("take");
    const std::string time_type = UniqueType("time");
    const std::string open_type = UniqueType("open");

    ASSERT_TRUE(factory.Register(
        risk_type, []() { return std::make_unique<TestRiskControl>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        stop_type, []() { return std::make_unique<TestStopLoss>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        take_type, []() { return std::make_unique<TestTakeProfit>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        time_type, []() { return std::make_unique<TestTimeFilter>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        open_type, []() { return std::make_unique<TestOpening>(); }, &error))
        << error;

    AtomicStrategyDefinition opening;
    opening.id = "open_1";
    opening.type = open_type;
    opening.params = {{"id", "open_1"}, {"emit", "1"}};
    CompositeStrategyDefinition definition =
        MakeDefinition(risk_type, stop_type, take_type, time_type, {opening});
    definition.risk_control_strategies[0].params["emit"] = "1";
    definition.stop_loss_strategies[0].params["emit"] = "1";
    definition.take_profit_strategies[0].params["emit"] = "1";

    CompositeStrategy strategy(definition, &factory);
    StrategyContext ctx;
    ctx.strategy_id = "composite";
    ctx.account_id = "acct";
    strategy.Initialize(ctx);

    std::vector<std::string> call_log;
    g_call_log = &call_log;
    const auto signals = strategy.OnState(MakeState());
    g_call_log = nullptr;

    ASSERT_EQ(call_log.size(), 5u);
    EXPECT_EQ(call_log[0], "risk");
    EXPECT_EQ(call_log[1], "stop");
    EXPECT_EQ(call_log[2], "take");
    EXPECT_EQ(call_log[3], "time");
    EXPECT_EQ(call_log[4], "open");

    ASSERT_EQ(signals.size(), 1u);
    EXPECT_EQ(signals[0].signal_type, SignalType::kForceClose);
}

TEST(CompositeStrategyTest, TimeFilterBlocksOpeningSignals) {
    AtomicFactory factory;
    std::string error;
    const std::string risk_type = UniqueType("risk");
    const std::string stop_type = UniqueType("stop");
    const std::string take_type = UniqueType("take");
    const std::string time_type = UniqueType("time");
    const std::string open_type = UniqueType("open");

    ASSERT_TRUE(factory.Register(
        risk_type, []() { return std::make_unique<TestRiskControl>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        stop_type, []() { return std::make_unique<TestStopLoss>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        take_type, []() { return std::make_unique<TestTakeProfit>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        time_type, []() { return std::make_unique<TestTimeFilter>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        open_type, []() { return std::make_unique<TestOpening>(); }, &error))
        << error;

    AtomicStrategyDefinition opening;
    opening.id = "open_1";
    opening.type = open_type;
    opening.params = {{"id", "open_1"}, {"emit", "1"}};

    CompositeStrategyDefinition definition =
        MakeDefinition(risk_type, stop_type, take_type, time_type, {opening});
    definition.time_filters[0].params["allow"] = "0";

    CompositeStrategy strategy(definition, &factory);
    StrategyContext ctx;
    ctx.strategy_id = "composite";
    ctx.account_id = "acct";
    strategy.Initialize(ctx);

    std::vector<std::string> call_log;
    g_call_log = &call_log;
    const auto signals = strategy.OnState(MakeState());
    g_call_log = nullptr;

    EXPECT_TRUE(signals.empty());
    EXPECT_EQ(call_log.size(), 4u);
    EXPECT_EQ(call_log[3], "time");
}

TEST(CompositeStrategyTest, FiltersOpeningByMarketRegime) {
    AtomicFactory factory;
    std::string error;
    const std::string risk_type = UniqueType("risk");
    const std::string stop_type = UniqueType("stop");
    const std::string take_type = UniqueType("take");
    const std::string time_type = UniqueType("time");
    const std::string open_type = UniqueType("open");

    ASSERT_TRUE(factory.Register(
        risk_type, []() { return std::make_unique<TestRiskControl>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        stop_type, []() { return std::make_unique<TestStopLoss>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        take_type, []() { return std::make_unique<TestTakeProfit>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        time_type, []() { return std::make_unique<TestTimeFilter>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        open_type, []() { return std::make_unique<TestOpening>(); }, &error))
        << error;

    AtomicStrategyDefinition opening;
    opening.id = "open_1";
    opening.type = open_type;
    opening.params = {{"id", "open_1"}, {"emit", "1"}};
    opening.market_regimes = {MarketRegime::kStrongTrend};

    CompositeStrategyDefinition definition =
        MakeDefinition(risk_type, stop_type, take_type, time_type, {opening});
    CompositeStrategy strategy(definition, &factory);
    StrategyContext ctx;
    ctx.strategy_id = "composite";
    ctx.account_id = "acct";
    strategy.Initialize(ctx);

    EXPECT_TRUE(strategy.OnState(MakeState(MarketRegime::kRanging)).empty());
    const auto signals = strategy.OnState(MakeState(MarketRegime::kStrongTrend));
    ASSERT_EQ(signals.size(), 1u);
    EXPECT_EQ(signals[0].signal_type, SignalType::kOpen);
}

TEST(CompositeStrategyTest, TieBreakUsesVolumeThenTimestampThenTraceId) {
    AtomicFactory factory;
    std::string error;
    const std::string risk_type = UniqueType("risk");
    const std::string stop_type = UniqueType("stop");
    const std::string take_type = UniqueType("take");
    const std::string time_type = UniqueType("time");
    const std::string open_type = UniqueType("open");

    ASSERT_TRUE(factory.Register(
        risk_type, []() { return std::make_unique<TestRiskControl>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        stop_type, []() { return std::make_unique<TestStopLoss>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        take_type, []() { return std::make_unique<TestTakeProfit>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        time_type, []() { return std::make_unique<TestTimeFilter>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        open_type, []() { return std::make_unique<TestOpening>(); }, &error))
        << error;

    AtomicStrategyDefinition open_a;
    open_a.id = "open_a";
    open_a.type = open_type;
    open_a.params = {{"id", "open_a"},
                     {"emit", "1"},
                     {"volume", "2"},
                     {"signal_ts_ns", "200"},
                     {"trace_id", "c"}};

    AtomicStrategyDefinition open_b;
    open_b.id = "open_b";
    open_b.type = open_type;
    open_b.params = {{"id", "open_b"},
                     {"emit", "1"},
                     {"volume", "2"},
                     {"signal_ts_ns", "200"},
                     {"trace_id", "a"}};

    AtomicStrategyDefinition open_c;
    open_c.id = "open_c";
    open_c.type = open_type;
    open_c.params = {{"id", "open_c"},
                     {"emit", "1"},
                     {"volume", "1"},
                     {"signal_ts_ns", "300"},
                     {"trace_id", "z"}};

    CompositeStrategyDefinition definition =
        MakeDefinition(risk_type, stop_type, take_type, time_type, {open_a, open_b, open_c});

    CompositeStrategy strategy(definition, &factory);
    StrategyContext ctx;
    ctx.strategy_id = "composite";
    ctx.account_id = "acct";
    strategy.Initialize(ctx);

    const auto signals = strategy.OnState(MakeState());
    ASSERT_EQ(signals.size(), 1u);
    EXPECT_EQ(signals[0].signal_type, SignalType::kOpen);
    EXPECT_EQ(signals[0].trace_id, "a");
}

TEST(CompositeStrategyTest, OrderEventsUpdatePositionAverageAndDeltaFilledIdempotency) {
    AtomicFactory factory;
    std::string error;
    const std::string risk_type = UniqueType("risk");
    const std::string stop_type = UniqueType("stop");
    const std::string take_type = UniqueType("take");
    const std::string time_type = UniqueType("time");
    const std::string capture_type = UniqueType("capture");

    ASSERT_TRUE(factory.Register(
        risk_type, []() { return std::make_unique<TestRiskControl>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        stop_type, []() { return std::make_unique<TestStopLoss>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        take_type, []() { return std::make_unique<TestTakeProfit>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        time_type, []() { return std::make_unique<TestTimeFilter>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        capture_type, []() { return std::make_unique<ContextCaptureOpening>(); }, &error))
        << error;

    AtomicStrategyDefinition capture;
    capture.id = "capture";
    capture.type = capture_type;
    capture.params = {{"id", "capture"}};
    CompositeStrategyDefinition definition =
        MakeDefinition(risk_type, stop_type, take_type, time_type, {capture});

    CompositeStrategy strategy(definition, &factory);
    StrategyContext ctx;
    ctx.strategy_id = "composite";
    ctx.account_id = "acct";
    strategy.Initialize(ctx);

    strategy.OnOrderEvent(
        MakeOrderEvent("ord_open", "IF2406", Side::kBuy, OffsetFlag::kOpen, 1, 100.0));
    strategy.OnOrderEvent(
        MakeOrderEvent("ord_open", "IF2406", Side::kBuy, OffsetFlag::kOpen, 3, 102.0));
    strategy.OnOrderEvent(
        MakeOrderEvent("ord_open", "IF2406", Side::kBuy, OffsetFlag::kOpen, 3, 102.0));
    strategy.OnOrderEvent(
        MakeOrderEvent("ord_close", "IF2406", Side::kSell, OffsetFlag::kClose, 1, 110.0));

    std::unordered_map<std::string, std::int32_t> captured_positions;
    std::unordered_map<std::string, double> captured_avg_prices;
    g_captured_positions = &captured_positions;
    g_captured_avg_prices = &captured_avg_prices;
    strategy.OnState(MakeState());
    g_captured_positions = nullptr;
    g_captured_avg_prices = nullptr;

    ASSERT_EQ(captured_positions["IF2406"], 2);
    ASSERT_TRUE(captured_avg_prices.find("IF2406") != captured_avg_prices.end());
    EXPECT_NEAR(captured_avg_prices["IF2406"], (100.0 + 102.0 * 2.0) / 3.0, 1e-9);
}

TEST(CompositeStrategyTest, OrderEventsHandleReverseAndFlattingToZero) {
    AtomicFactory factory;
    std::string error;
    const std::string risk_type = UniqueType("risk");
    const std::string stop_type = UniqueType("stop");
    const std::string take_type = UniqueType("take");
    const std::string time_type = UniqueType("time");
    const std::string capture_type = UniqueType("capture");

    ASSERT_TRUE(factory.Register(
        risk_type, []() { return std::make_unique<TestRiskControl>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        stop_type, []() { return std::make_unique<TestStopLoss>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        take_type, []() { return std::make_unique<TestTakeProfit>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        time_type, []() { return std::make_unique<TestTimeFilter>(); }, &error))
        << error;
    ASSERT_TRUE(factory.Register(
        capture_type, []() { return std::make_unique<ContextCaptureOpening>(); }, &error))
        << error;

    AtomicStrategyDefinition capture;
    capture.id = "capture";
    capture.type = capture_type;
    capture.params = {{"id", "capture"}};
    CompositeStrategyDefinition definition =
        MakeDefinition(risk_type, stop_type, take_type, time_type, {capture});

    CompositeStrategy strategy(definition, &factory);
    StrategyContext ctx;
    ctx.strategy_id = "composite";
    ctx.account_id = "acct";
    strategy.Initialize(ctx);

    strategy.OnOrderEvent(
        MakeOrderEvent("ord_open", "IF2406", Side::kBuy, OffsetFlag::kOpen, 2, 100.0));
    strategy.OnOrderEvent(
        MakeOrderEvent("ord_reverse", "IF2406", Side::kSell, OffsetFlag::kClose, 5, 99.0));

    std::unordered_map<std::string, std::int32_t> captured_positions;
    std::unordered_map<std::string, double> captured_avg_prices;
    g_captured_positions = &captured_positions;
    g_captured_avg_prices = &captured_avg_prices;
    strategy.OnState(MakeState());
    g_captured_positions = nullptr;
    g_captured_avg_prices = nullptr;

    ASSERT_EQ(captured_positions["IF2406"], -3);
    ASSERT_TRUE(captured_avg_prices.find("IF2406") != captured_avg_prices.end());
    EXPECT_NEAR(captured_avg_prices["IF2406"], 99.0, 1e-9);

    strategy.OnOrderEvent(
        MakeOrderEvent("ord_flat", "IF2406", Side::kBuy, OffsetFlag::kClose, 3, 98.0));

    captured_positions.clear();
    captured_avg_prices.clear();
    g_captured_positions = &captured_positions;
    g_captured_avg_prices = &captured_avg_prices;
    strategy.OnState(MakeState());
    g_captured_positions = nullptr;
    g_captured_avg_prices = nullptr;

    ASSERT_EQ(captured_positions["IF2406"], 0);
    EXPECT_TRUE(captured_avg_prices.find("IF2406") == captured_avg_prices.end());
}

}  // namespace quant_hft
