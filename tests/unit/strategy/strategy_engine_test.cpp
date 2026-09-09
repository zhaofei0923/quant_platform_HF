#include "quant_hft/strategy/strategy_engine.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "quant_hft/monitoring/dashboard_snapshot.h"
#include "quant_hft/strategy/live_strategy.h"
#include "quant_hft/strategy/market_gap_recovery.h"
#include "quant_hft/strategy/strategy_registry.h"

namespace quant_hft {
namespace {

struct Probe {
    std::mutex mutex;
    std::vector<std::string> initialized_strategy_ids;
    std::vector<std::string> initialized_composite_paths;
    std::vector<EpochNanos> observed_state_ts;
    std::vector<std::string> observed_market_ticks;
    std::vector<std::string> observed_order_events;
    std::vector<std::string> observed_account_snapshots;
    std::vector<std::string> observed_timer_strategies;
    std::vector<std::string> contract_switches;
};

Probe* g_probe = nullptr;
std::atomic<int> g_state_delay_ms{0};
std::mutex g_behavior_mutex;
std::string g_throw_on_state_strategy;
std::string g_throw_on_order_strategy;
std::string g_throw_on_timer_strategy;

std::string UniqueFactoryName() {
    static std::atomic<int> seq{0};
    return "strategy_engine_test_factory_" + std::to_string(seq.fetch_add(1));
}

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return predicate();
}

void ResetThrowingBehavior() {
    std::lock_guard<std::mutex> lock(g_behavior_mutex);
    g_throw_on_state_strategy.clear();
    g_throw_on_order_strategy.clear();
    g_throw_on_timer_strategy.clear();
}

bool ShouldThrowOnState(const std::string& strategy_id) {
    std::lock_guard<std::mutex> lock(g_behavior_mutex);
    return strategy_id == g_throw_on_state_strategy;
}

bool ShouldThrowOnOrder(const std::string& strategy_id) {
    std::lock_guard<std::mutex> lock(g_behavior_mutex);
    return strategy_id == g_throw_on_order_strategy;
}

bool ShouldThrowOnTimer(const std::string& strategy_id) {
    std::lock_guard<std::mutex> lock(g_behavior_mutex);
    return strategy_id == g_throw_on_timer_strategy;
}

bool ContainsEvent(const std::vector<std::string>& events, const std::string& needle) {
    return std::find(events.begin(), events.end(), needle) != events.end();
}

class RecordingStrategy final : public ILiveStrategy {
   public:
    void Initialize(const StrategyContext& ctx) override {
        strategy_id_ = ctx.strategy_id;
        if (g_probe != nullptr) {
            std::lock_guard<std::mutex> lock(g_probe->mutex);
            g_probe->initialized_strategy_ids.push_back(strategy_id_);
            const auto config_it = ctx.metadata.find("composite_config_path");
            g_probe->initialized_composite_paths.push_back(
                config_it == ctx.metadata.end() ? std::string() : config_it->second);
        }
    }

    std::vector<SignalIntent> OnState(const StateSnapshot7D& state) override {
        if (ShouldThrowOnState(strategy_id_)) {
            throw std::runtime_error("state exception");
        }

        const int delay_ms = g_state_delay_ms.load();
        if (delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        }
        if (g_probe != nullptr) {
            std::lock_guard<std::mutex> lock(g_probe->mutex);
            g_probe->observed_state_ts.push_back(state.ts_ns);
        }

        SignalIntent intent;
        intent.strategy_id = strategy_id_;
        intent.instrument_id = state.instrument_id;
        intent.side = Side::kBuy;
        intent.offset = OffsetFlag::kOpen;
        intent.volume = 1;
        intent.limit_price = 1.0;
        intent.ts_ns = state.ts_ns;
        intent.trace_id = strategy_id_ + "-" + std::to_string(state.ts_ns) +
                          (loaded_from_state_ ? "-loaded" : "-fresh");
        return {intent};
    }

    std::vector<SignalIntent> OnMarketTick(const MarketSnapshot& snapshot) override {
        if (g_probe != nullptr) {
            std::lock_guard<std::mutex> lock(g_probe->mutex);
            g_probe->observed_market_ticks.push_back(strategy_id_ + ":" + snapshot.instrument_id);
        }

        SignalIntent intent;
        intent.strategy_id = strategy_id_;
        intent.instrument_id = snapshot.instrument_id;
        intent.signal_type = SignalType::kTakeProfit;
        intent.side = Side::kSell;
        intent.offset = OffsetFlag::kClose;
        intent.volume = 1;
        intent.limit_price = snapshot.last_price;
        intent.ts_ns = snapshot.recv_ts_ns;
        intent.trace_id = strategy_id_ + "-tick-" + std::to_string(snapshot.recv_ts_ns);
        return {intent};
    }

    void OnOrderEvent(const OrderEvent& event) override {
        if (ShouldThrowOnOrder(strategy_id_)) {
            throw std::runtime_error("order exception");
        }
        if (g_probe != nullptr) {
            std::lock_guard<std::mutex> lock(g_probe->mutex);
            g_probe->observed_order_events.push_back(strategy_id_ + ":" + event.client_order_id);
        }
    }

    void OnAccountSnapshot(const TradingAccountSnapshot& snapshot) override {
        if (g_probe != nullptr) {
            std::lock_guard<std::mutex> lock(g_probe->mutex);
            g_probe->observed_account_snapshots.push_back(strategy_id_ + ":" +
                                                          std::to_string(snapshot.balance));
        }
    }

    std::vector<SignalIntent> OnTimer(EpochNanos now_ns) override {
        if (ShouldThrowOnTimer(strategy_id_)) {
            throw std::runtime_error("timer exception");
        }
        if (g_probe != nullptr) {
            std::lock_guard<std::mutex> lock(g_probe->mutex);
            g_probe->observed_timer_strategies.push_back(strategy_id_);
        }

        (void)now_ns;
        return {};
    }

    std::vector<StrategyMetric> CollectMetrics() const override {
        return {StrategyMetric{"strategy_engine_test_metric",
                               loaded_from_state_ ? 1.0 : 0.0,
                               {{"strategy_id", strategy_id_}}}};
    }

    std::vector<StrategyRiskSnapshot> CollectRiskSnapshot(EpochNanos) const override {
        StrategyRiskSnapshot row;
        row.strategy_id = "implementation-cannot-spoof-outer-id";
        row.owner_strategy_id = strategy_id_ + "_owner";
        row.instrument_id = "hc2701";
        row.net = 1;
        row.avg_open = 3500.0;
        row.initial_stop = 3450.0;
        row.trailing_stop = 3475.0;
        row.effective_stop = 3475.0;
        row.stop_kind = StrategyStopKind::kTrailing;
        row.take_profit = 3600.0;
        row.as_of_ns = 77;
        return {row};
    }

    bool ResetForContractSwitch(const ContractSwitchContext& context, std::string* error) override {
        if (error != nullptr) {
            error->clear();
        }
        if (g_probe != nullptr) {
            std::lock_guard<std::mutex> lock(g_probe->mutex);
            g_probe->contract_switches.push_back(
                context.product_id + ":" + context.previous_instrument_id + "->" +
                context.current_instrument_id + ":" + std::to_string(context.generation));
        }
        return true;
    }

    std::int32_t RequiredContractWarmupBars(const ContractSwitchContext& context) const override {
        (void)context;
        return 2;
    }
    MarketWarmupRequirements RequiredMarketWarmupBars(const std::string&) const override {
        return {{5, 2}};
    }
    bool ResetForMarketGap(const MarketGapContext& context, std::string*) override {
        if (g_probe) {
            std::lock_guard<std::mutex> lock(g_probe->mutex);
            g_probe->contract_switches.push_back("gap:" + context.instrument_id);
        }
        return true;
    }

    bool SaveState(StrategyState* out, std::string* error) const override {
        (void)error;
        if (out == nullptr) {
            return false;
        }
        (*out)["loaded"] = loaded_from_state_ ? "1" : "0";
        return true;
    }

    bool LoadState(const StrategyState& state, std::string* error) override {
        (void)error;
        const auto it = state.find("loaded");
        loaded_from_state_ = (it != state.end() && it->second == "1");
        return true;
    }

    void Shutdown() override {}

   private:
    std::string strategy_id_;
    bool loaded_from_state_{false};
};

class TestStatePersistence final : public IStrategyStatePersistence {
   public:
    bool SaveStrategyState(const std::string& account_id, const std::string& strategy_id,
                           const StrategyState& state, std::string* error) override {
        (void)error;
        std::lock_guard<std::mutex> lock(mutex_);
        ++save_calls_;
        if (fail_save_) {
            if (error != nullptr) *error = "injected durable snapshot failure";
            return false;
        }
        storage_[account_id + ":" + strategy_id] = state;
        return true;
    }

    bool LoadStrategyState(const std::string& account_id, const std::string& strategy_id,
                           StrategyState* state, std::string* error) const override {
        if (state == nullptr) {
            if (error != nullptr) {
                *error = "state out is null";
            }
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        ++load_calls_;
        const auto it = storage_.find(account_id + ":" + strategy_id);
        if (it == storage_.end()) {
            if (error != nullptr) {
                *error = "not found";
            }
            return false;
        }
        *state = it->second;
        return true;
    }

    void Seed(const std::string& key, const StrategyState& state) {
        std::lock_guard<std::mutex> lock(mutex_);
        storage_[key] = state;
    }
    void FailSave(bool fail) {
        std::lock_guard<std::mutex> lock(mutex_);
        fail_save_ = fail;
    }

    std::uint64_t save_calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return save_calls_;
    }

    std::uint64_t load_calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return load_calls_;
    }

   private:
    mutable std::mutex mutex_;
    mutable std::uint64_t load_calls_{0};
    std::uint64_t save_calls_{0};
    bool fail_save_{false};
    std::unordered_map<std::string, StrategyState> storage_;
};

TEST(StrategyEngineTest, DispatchesStateAndOrderEventsToAllStrategies) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    std::mutex sink_mutex;
    std::vector<SignalIntent> emitted_intents;
    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 1000 * 1000 * 1000;
    StrategyEngine engine(cfg, [&](const SignalIntent& intent) {
        std::lock_guard<std::mutex> lock(sink_mutex);
        emitted_intents.push_back(intent);
    });

    StrategyContext base_context;
    base_context.account_id = "sim-account";
    ASSERT_TRUE(engine.Start({"alpha", "beta"}, factory_name, base_context, &error)) << error;

    StateSnapshot7D state;
    state.instrument_id = "SHFE.ag2406";
    state.ts_ns = 1001;
    engine.EnqueueState(state);

    OrderEvent event;
    event.client_order_id = "ord-1";
    event.ts_ns = 1002;
    engine.EnqueueOrderEvent(event);

    ASSERT_TRUE(WaitUntil(
        [&]() {
            std::lock_guard<std::mutex> sink_lock(sink_mutex);
            std::lock_guard<std::mutex> probe_lock(probe.mutex);
            return emitted_intents.size() >= 2 && probe.observed_order_events.size() >= 2;
        },
        std::chrono::milliseconds(500)));

    engine.Stop();
    g_probe = nullptr;

    std::vector<std::string> strategy_ids;
    {
        std::lock_guard<std::mutex> lock(sink_mutex);
        ASSERT_EQ(emitted_intents.size(), 2U);
        strategy_ids.push_back(emitted_intents[0].strategy_id);
        strategy_ids.push_back(emitted_intents[1].strategy_id);
    }
    std::sort(strategy_ids.begin(), strategy_ids.end());
    EXPECT_EQ(strategy_ids[0], "alpha");
    EXPECT_EQ(strategy_ids[1], "beta");

    std::lock_guard<std::mutex> probe_lock(probe.mutex);
    EXPECT_EQ(probe.initialized_strategy_ids.size(), 2U);
    EXPECT_TRUE(ContainsEvent(probe.observed_order_events, "alpha:ord-1"));
    EXPECT_TRUE(ContainsEvent(probe.observed_order_events, "beta:ord-1"));

    const auto stats = engine.GetStats();
    EXPECT_EQ(stats.broadcast_order_events, 1U);
    EXPECT_EQ(stats.unmatched_order_events, 0U);
}

TEST(StrategyEngineTest, DrainBarrierWaitsForCallbackCompletion) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();
    g_state_delay_ms.store(100);

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 1'000'000'000;
    StrategyEngine engine(cfg, nullptr);
    StrategyContext context;
    ASSERT_TRUE(engine.Start({"alpha"}, factory_name, context, &error)) << error;

    StateSnapshot7D state;
    state.instrument_id = "DCE.c2609";
    state.ts_ns = 42;
    engine.EnqueueState(state);
    EXPECT_FALSE(engine.WaitUntilDrained(10));
    EXPECT_TRUE(engine.WaitUntilDrained(500));

    engine.Stop();
    g_state_delay_ms.store(0);
    g_probe = nullptr;
    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_EQ(probe.observed_state_ts, std::vector<EpochNanos>{42});
}

TEST(StrategyEngineTest, DispatchesMarketTicksToAllStrategies) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    std::mutex sink_mutex;
    std::vector<SignalIntent> emitted_intents;
    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 1000 * 1000 * 1000;
    StrategyEngine engine(cfg, [&](const SignalIntent& intent) {
        std::lock_guard<std::mutex> lock(sink_mutex);
        emitted_intents.push_back(intent);
    });

    StrategyContext base_context;
    base_context.account_id = "sim-account";
    ASSERT_TRUE(engine.Start({"alpha", "beta"}, factory_name, base_context, &error)) << error;

    MarketSnapshot tick;
    tick.instrument_id = "rb2405";
    tick.last_price = 105.5;
    tick.recv_ts_ns = 2001;
    engine.EnqueueMarketTick(tick);

    ASSERT_TRUE(WaitUntil(
        [&]() {
            std::lock_guard<std::mutex> sink_lock(sink_mutex);
            std::lock_guard<std::mutex> probe_lock(probe.mutex);
            return emitted_intents.size() >= 2 && probe.observed_market_ticks.size() >= 2;
        },
        std::chrono::milliseconds(500)));

    engine.Stop();
    g_probe = nullptr;

    std::vector<std::string> strategy_ids;
    {
        std::lock_guard<std::mutex> lock(sink_mutex);
        ASSERT_EQ(emitted_intents.size(), 2U);
        strategy_ids.push_back(emitted_intents[0].strategy_id);
        strategy_ids.push_back(emitted_intents[1].strategy_id);
        EXPECT_EQ(emitted_intents[0].signal_type, SignalType::kTakeProfit);
        EXPECT_EQ(emitted_intents[1].signal_type, SignalType::kTakeProfit);
    }
    std::sort(strategy_ids.begin(), strategy_ids.end());
    EXPECT_EQ(strategy_ids[0], "alpha");
    EXPECT_EQ(strategy_ids[1], "beta");

    std::lock_guard<std::mutex> probe_lock(probe.mutex);
    EXPECT_TRUE(ContainsEvent(probe.observed_market_ticks, "alpha:rb2405"));
    EXPECT_TRUE(ContainsEvent(probe.observed_market_ticks, "beta:rb2405"));
}

TEST(StrategyEngineTest, StartsLaunchSpecsWithPerStrategyMetadata) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 1000 * 1000 * 1000;
    StrategyEngine engine(cfg, nullptr);

    StrategyContext c_context;
    c_context.metadata["composite_config_path"] = "configs/c.yaml";
    StrategyContext rb_context;
    rb_context.metadata["composite_config_path"] = "configs/rb.yaml";

    std::vector<StrategyEngine::StrategyLaunchSpec> specs = {
        StrategyEngine::StrategyLaunchSpec{"c_alpha", factory_name, c_context},
        StrategyEngine::StrategyLaunchSpec{"rb_alpha", factory_name, rb_context},
    };
    ASSERT_TRUE(engine.Start(specs, &error)) << error;
    engine.Stop();
    g_probe = nullptr;

    std::lock_guard<std::mutex> lock(probe.mutex);
    ASSERT_EQ(probe.initialized_strategy_ids.size(), 2U);
    EXPECT_TRUE(ContainsEvent(probe.initialized_strategy_ids, "c_alpha"));
    EXPECT_TRUE(ContainsEvent(probe.initialized_strategy_ids, "rb_alpha"));
    ASSERT_EQ(probe.initialized_composite_paths.size(), 2U);
    EXPECT_TRUE(ContainsEvent(probe.initialized_composite_paths, "configs/c.yaml"));
    EXPECT_TRUE(ContainsEvent(probe.initialized_composite_paths, "configs/rb.yaml"));
}

TEST(StrategyEngineTest, RoutesOrderEventByStrategyId) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 1000 * 1000 * 1000;
    StrategyEngine engine(cfg, nullptr);

    StrategyContext base_context;
    ASSERT_TRUE(engine.Start({"alpha", "beta"}, factory_name, base_context, &error)) << error;

    OrderEvent event;
    event.client_order_id = "ord-target";
    event.strategy_id = "beta";
    engine.EnqueueOrderEvent(event);

    ASSERT_TRUE(WaitUntil(
        [&]() {
            std::lock_guard<std::mutex> lock(probe.mutex);
            return ContainsEvent(probe.observed_order_events, "beta:ord-target");
        },
        std::chrono::milliseconds(500)));

    engine.Stop();
    g_probe = nullptr;

    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_FALSE(ContainsEvent(probe.observed_order_events, "alpha:ord-target"));
    EXPECT_TRUE(ContainsEvent(probe.observed_order_events, "beta:ord-target"));

    const auto stats = engine.GetStats();
    EXPECT_EQ(stats.broadcast_order_events, 0U);
    EXPECT_EQ(stats.unmatched_order_events, 0U);
}

TEST(StrategyEngineTest, CountsUnmatchedOrderEvents) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 1000 * 1000 * 1000;
    StrategyEngine engine(cfg, nullptr);

    StrategyContext base_context;
    ASSERT_TRUE(engine.Start({"alpha"}, factory_name, base_context, &error)) << error;

    OrderEvent event;
    event.client_order_id = "ord-unknown";
    event.strategy_id = "ghost";
    engine.EnqueueOrderEvent(event);

    ASSERT_TRUE(WaitUntil(
        [&]() {
            const auto stats = engine.GetStats();
            return stats.unmatched_order_events > 0;
        },
        std::chrono::milliseconds(500)));

    engine.Stop();
    g_probe = nullptr;

    const auto stats = engine.GetStats();
    EXPECT_EQ(stats.unmatched_order_events, 1U);
    EXPECT_EQ(stats.broadcast_order_events, 0U);
}

TEST(StrategyEngineTest, IsolatesStrategyExceptionsInOrderDispatch) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    {
        std::lock_guard<std::mutex> lock(g_behavior_mutex);
        g_throw_on_order_strategy = "alpha";
    }

    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 1000 * 1000 * 1000;
    StrategyEngine engine(cfg, nullptr);

    StrategyContext base_context;
    ASSERT_TRUE(engine.Start({"alpha", "beta"}, factory_name, base_context, &error)) << error;

    OrderEvent event;
    event.client_order_id = "ord-ex";
    engine.EnqueueOrderEvent(event);

    ASSERT_TRUE(WaitUntil(
        [&]() {
            std::lock_guard<std::mutex> lock(probe.mutex);
            return ContainsEvent(probe.observed_order_events, "beta:ord-ex");
        },
        std::chrono::milliseconds(500)));

    engine.Stop();
    g_probe = nullptr;
    ResetThrowingBehavior();

    const auto stats = engine.GetStats();
    EXPECT_GT(stats.strategy_callback_exceptions, 0U);
}

TEST(StrategyEngineTest, TriggersTimerCallbacks) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 10 * 1000 * 1000;
    StrategyEngine engine(cfg, nullptr);

    StrategyContext base_context;
    ASSERT_TRUE(engine.Start({"alpha"}, factory_name, base_context, &error)) << error;

    ASSERT_TRUE(WaitUntil(
        [&]() {
            std::lock_guard<std::mutex> lock(probe.mutex);
            return !probe.observed_timer_strategies.empty();
        },
        std::chrono::milliseconds(500)));

    engine.Stop();
    g_probe = nullptr;

    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_FALSE(probe.observed_timer_strategies.empty());
}

TEST(StrategyEngineTest, KeepsRiskMissingUntilAccountDayThenForcesAfterOrder) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();
    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error));

    std::mutex sink_mutex;
    std::vector<std::vector<StrategyRiskSnapshot>> batches;
    std::vector<EpochNanos> observed_at;
    StrategyEngineConfig cfg;
    cfg.timer_interval_ns = 10'000'000;
    cfg.risk_snapshot_interval_ns = 60'000'000'000;
    cfg.risk_snapshot_sink = [&](const std::vector<StrategyRiskSnapshot>& rows, EpochNanos as_of_ns,
                                 const std::string&) {
        std::lock_guard<std::mutex> lock(sink_mutex);
        batches.push_back(rows);
        observed_at.push_back(as_of_ns);
    };
    StrategyEngine engine(cfg);
    StrategyContext context;
    context.account_id = "sim-account";
    ASSERT_TRUE(engine.Start({"outer"}, factory_name, context, &error)) << error;
    {
        std::lock_guard<std::mutex> lock(sink_mutex);
        EXPECT_TRUE(batches.empty());
    }

    TradingAccountSnapshot foreign_account;
    foreign_account.account_id = "another-account";
    foreign_account.trading_day = "20260907";
    ASSERT_TRUE(engine.EnqueueAccountSnapshot(foreign_account));
    ASSERT_TRUE(engine.WaitUntilDrained(500));
    {
        std::lock_guard<std::mutex> lock(sink_mutex);
        EXPECT_TRUE(batches.empty());
    }

    TradingAccountSnapshot account;
    account.account_id = "sim-account";
    account.trading_day = "20260907";
    ASSERT_TRUE(engine.EnqueueAccountSnapshot(account));
    ASSERT_TRUE(WaitUntil(
        [&] {
            std::lock_guard<std::mutex> lock(sink_mutex);
            return batches.size() >= 1;
        },
        std::chrono::milliseconds(500)));

    OrderEvent event;
    event.account_id = "sim-account";
    event.strategy_id = "outer";
    event.client_order_id = "force-risk";
    ASSERT_TRUE(engine.EnqueueOrderEvent(event));
    ASSERT_TRUE(WaitUntil(
        [&] {
            std::lock_guard<std::mutex> lock(sink_mutex);
            return batches.size() >= 2;
        },
        std::chrono::milliseconds(500)));
    engine.Stop();
    g_probe = nullptr;

    std::lock_guard<std::mutex> lock(sink_mutex);
    ASSERT_GE(batches.size(), 2U);
    ASSERT_EQ(batches[0].size(), 1U);
    EXPECT_EQ(batches[0][0].account_id, "sim-account");
    EXPECT_EQ(batches[0][0].strategy_id, "outer");
    EXPECT_EQ(batches[0][0].owner_strategy_id, "outer_owner");
    EXPECT_EQ(batches[0][0].as_of_ns, 77);
    EXPECT_EQ(batches[1][0].as_of_ns, 77);
    EXPECT_GT(observed_at[1], observed_at[0]);
    EXPECT_EQ(engine.GetStats().risk_snapshot_failures, 0U);
}

TEST(StrategyEngineTest, RestartedPrivateSnapshotStaysMissingUntilValidAccountDay) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();
    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error));

    const auto directory = std::filesystem::temp_directory_path() /
                           ("quant-hft-risk-restart-" + factory_name);
    std::filesystem::remove_all(directory);
    DashboardSnapshotWriter writer;
    RuntimeIdentity identity{"simnow", "9999", "sim-account", "risk-restart-test"};
    ASSERT_TRUE(writer.Start((directory / "private.json").string(), identity, &error)) << error;

    StrategyEngineConfig cfg;
    cfg.timer_interval_ns = 5'000'000;
    cfg.risk_snapshot_interval_ns = 5'000'000;
    cfg.risk_snapshot_sink = [&](const std::vector<StrategyRiskSnapshot>& rows,
                                 EpochNanos observed_at_ns,
                                 const std::string& trading_day) {
        writer.CaptureStrategyRisk(rows, observed_at_ns, trading_day);
    };
    StrategyEngine engine(cfg);
    StrategyContext context;
    context.account_id = identity.account_id;
    ASSERT_TRUE(engine.Start({"outer"}, factory_name, context, &error)) << error;
    ASSERT_TRUE(WaitUntil([&] { return engine.GetStats().timer_callbacks >= 2; },
                          std::chrono::milliseconds(500)));
    EXPECT_NE(writer.RenderSnapshot(1).find("\"strategy_risk\":{\"quality\":\"missing\""),
              std::string::npos);

    TradingAccountSnapshot account;
    account.account_id = identity.account_id;
    account.trading_day = "20260907";
    ASSERT_TRUE(engine.EnqueueAccountSnapshot(account));
    ASSERT_TRUE(WaitUntil(
        [&] {
            return writer.RenderSnapshot(2).find(
                       "\"strategy_risk\":{\"quality\":\"ok\"") != std::string::npos;
        },
        std::chrono::milliseconds(500)));

    engine.Stop();
    writer.Stop();
    std::filesystem::remove_all(directory);
    g_probe = nullptr;
}

TEST(StrategyEngineTest, RiskObserverFailureDoesNotOverloadOrStopOrderDispatch) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();
    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error));

    StrategyEngineConfig cfg;
    cfg.timer_interval_ns = 1'000'000'000;
    cfg.risk_snapshot_sink = [](const std::vector<StrategyRiskSnapshot>&, EpochNanos,
                                const std::string&) { throw std::runtime_error("observer down"); };
    StrategyEngine engine(cfg);
    StrategyContext context;
    context.account_id = "sim-account";
    ASSERT_TRUE(engine.Start({"outer"}, factory_name, context, &error)) << error;
    TradingAccountSnapshot account;
    account.account_id = "sim-account";
    account.trading_day = "20260907";
    ASSERT_TRUE(engine.EnqueueAccountSnapshot(account));
    ASSERT_TRUE(WaitUntil([&] { return engine.GetStats().risk_snapshot_failures >= 1; },
                          std::chrono::milliseconds(500)));
    OrderEvent event;
    event.strategy_id = "outer";
    event.client_order_id = "observer-failure-order";
    ASSERT_TRUE(engine.EnqueueOrderEvent(event));
    ASSERT_TRUE(WaitUntil(
        [&] {
            std::lock_guard<std::mutex> lock(probe.mutex);
            return ContainsEvent(probe.observed_order_events,
                                 "outer:observer-failure-order");
        },
        std::chrono::milliseconds(500)));
    EXPECT_FALSE(engine.GetHealth().overloaded);
    EXPECT_GE(engine.GetStats().risk_snapshot_failures, 1U);
    engine.Stop();
    g_probe = nullptr;
}

TEST(StrategyEngineTest, DispatchesAccountSnapshotsToAllStrategies) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 1000 * 1000 * 1000;
    StrategyEngine engine(cfg, nullptr);

    StrategyContext base_context;
    ASSERT_TRUE(engine.Start({"alpha", "beta"}, factory_name, base_context, &error)) << error;

    TradingAccountSnapshot snapshot;
    snapshot.balance = 123.0;
    engine.EnqueueAccountSnapshot(snapshot);

    ASSERT_TRUE(WaitUntil(
        [&]() {
            std::lock_guard<std::mutex> lock(probe.mutex);
            return probe.observed_account_snapshots.size() >= 2;
        },
        std::chrono::milliseconds(500)));

    engine.Stop();
    g_probe = nullptr;

    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_TRUE(ContainsEvent(probe.observed_account_snapshots, "alpha:123.000000"));
    EXPECT_TRUE(ContainsEvent(probe.observed_account_snapshots, "beta:123.000000"));
}

TEST(StrategyEngineTest, CollectAllMetricsReturnsCachedMetrics) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 5 * 1000 * 1000;
    cfg.metrics_collect_interval_ns = 5 * 1000 * 1000;
    StrategyEngine engine(cfg, nullptr);

    StrategyContext base_context;
    ASSERT_TRUE(engine.Start({"alpha"}, factory_name, base_context, &error)) << error;

    ASSERT_TRUE(WaitUntil(
        [&]() {
            const auto metrics = engine.CollectAllMetrics();
            return !metrics.empty();
        },
        std::chrono::milliseconds(500)));

    const std::vector<StrategyMetric> metrics = engine.CollectAllMetrics();
    ASSERT_FALSE(metrics.empty());
    EXPECT_EQ(metrics.front().name, "strategy_engine_test_metric");

    engine.Stop();
    g_probe = nullptr;
}

TEST(StrategyEngineTest, LoadsAndSnapshotsStateWithPersistenceHook) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    auto persistence = std::make_shared<TestStatePersistence>();
    persistence->Seed("sim-account:alpha", StrategyState{{"loaded", "1"}});

    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 5 * 1000 * 1000;
    cfg.state_persistence = persistence;
    cfg.load_state_on_start = true;
    cfg.state_snapshot_interval_ns = 5 * 1000 * 1000;
    StrategyEngine engine(cfg, nullptr);

    StrategyContext base_context;
    base_context.account_id = "sim-account";
    ASSERT_TRUE(engine.Start({"alpha"}, factory_name, base_context, &error)) << error;

    StateSnapshot7D state;
    state.instrument_id = "SHFE.ag2406";
    state.ts_ns = 42;
    engine.EnqueueState(state);

    ASSERT_TRUE(WaitUntil(
        [&]() {
            const auto stats = engine.GetStats();
            return stats.state_snapshot_runs > 0 && persistence->load_calls() > 0 &&
                   persistence->save_calls() > 0;
        },
        std::chrono::milliseconds(800)));

    engine.Stop();
    g_probe = nullptr;
}

TEST(StrategyEngineTest, RejectsAdmissionExplicitlyWithoutDroppingAcceptedEvents) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    std::mutex sink_mutex;
    std::vector<SignalIntent> emitted_intents;
    StrategyEngineConfig cfg;
    cfg.queue_capacity = 2;
    cfg.timer_interval_ns = 1000 * 1000 * 1000;
    StrategyEngine engine(cfg, [&](const SignalIntent& intent) {
        std::lock_guard<std::mutex> lock(sink_mutex);
        emitted_intents.push_back(intent);
    });

    StrategyContext base_context;
    ASSERT_TRUE(engine.Start({"alpha"}, factory_name, base_context, &error)) << error;

    g_state_delay_ms.store(25);
    for (EpochNanos ts = 1; ts <= 20; ++ts) {
        StateSnapshot7D state;
        state.instrument_id = "SHFE.ag2406";
        state.ts_ns = ts;
        engine.EnqueueState(state);
    }

    ASSERT_TRUE(WaitUntil(
        [&]() {
            std::lock_guard<std::mutex> lock(probe.mutex);
            return !probe.observed_state_ts.empty();
        },
        std::chrono::milliseconds(500)));

    engine.Stop();
    g_probe = nullptr;
    g_state_delay_ms.store(0);

    const auto stats = engine.GetStats();
    EXPECT_EQ(stats.dropped_oldest_events, 0U);
    EXPECT_GT(stats.rejected_events, 0U);
    EXPECT_TRUE(engine.GetHealth().overloaded);
}

TEST(StrategyEngineTest, ContractSwitchWarmsWithoutEmittingAndStampsNextIntent) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name, []() { return std::make_unique<RecordingStrategy>(); }, &error))
        << error;

    std::mutex sink_mutex;
    std::vector<SignalIntent> emitted_intents;
    StrategyEngineConfig cfg;
    cfg.queue_capacity = 64;
    cfg.timer_interval_ns = 1'000'000'000;
    cfg.contract_identity_resolver =
        [](const std::string& instrument_id) -> std::optional<StrategyContractIdentity> {
        if (instrument_id == "c2609") {
            return StrategyContractIdentity{"c", 9};
        }
        return std::nullopt;
    };
    StrategyEngine engine(cfg, [&](const SignalIntent& intent) {
        std::lock_guard<std::mutex> lock(sink_mutex);
        emitted_intents.push_back(intent);
    });
    StrategyContext context;
    ASSERT_TRUE(engine.Start({"alpha"}, factory_name, context, &error)) << error;

    std::vector<StateSnapshot7D> warmup;
    for (EpochNanos ts : {1, 2, 3}) {
        StateSnapshot7D state;
        state.instrument_id = "c2609";
        state.timeframe_minutes = 5;
        state.ts_ns = ts;
        state.has_bar = true;
        warmup.push_back(state);
    }
    const ContractSwitchContext switch_context{"c", "c2607", "c2609", 7};
    const auto report = engine.ApplyContractSwitch(switch_context, warmup, 1'000);
    ASSERT_TRUE(report.success) << report.error;
    EXPECT_EQ(report.required_warmup_bars, 2);
    EXPECT_EQ(report.replayed_warmup_bars, 2);
    {
        std::lock_guard<std::mutex> lock(sink_mutex);
        EXPECT_TRUE(emitted_intents.empty());
    }
    {
        std::lock_guard<std::mutex> lock(probe.mutex);
        ASSERT_EQ(probe.contract_switches.size(), 1U);
        EXPECT_EQ(probe.contract_switches.front(), "c:c2607->c2609:7");
        ASSERT_EQ(probe.observed_state_ts.size(), 2U);
        EXPECT_EQ(probe.observed_state_ts[0], 2);
        EXPECT_EQ(probe.observed_state_ts[1], 3);
    }

    StateSnapshot7D suppressed;
    suppressed.instrument_id = "c2609";
    suppressed.timeframe_minutes = 5;
    suppressed.ts_ns = 4;
    suppressed.has_bar = true;
    ASSERT_TRUE(engine.ApplyContractWarmupState(suppressed, "c", 7, 1'000));
    {
        std::lock_guard<std::mutex> lock(sink_mutex);
        EXPECT_TRUE(emitted_intents.empty());
    }
    {
        std::lock_guard<std::mutex> lock(probe.mutex);
        ASSERT_EQ(probe.observed_state_ts.size(), 3U);
        EXPECT_EQ(probe.observed_state_ts.back(), 4);
    }

    {
        std::lock_guard<std::mutex> lock(g_behavior_mutex);
        g_throw_on_state_strategy = "alpha";
    }
    StateSnapshot7D failed_warmup = suppressed;
    failed_warmup.ts_ns = 41;
    EXPECT_FALSE(engine.ApplyContractWarmupState(failed_warmup, "c", 7, 1'000));
    ResetThrowingBehavior();

    StateSnapshot7D ready = suppressed;
    ready.ts_ns = 5;
    engine.EnqueueState(ready, "c", 7, true);
    ASSERT_TRUE(WaitUntil(
        [&]() {
            std::lock_guard<std::mutex> lock(sink_mutex);
            return emitted_intents.size() == 1U;
        },
        std::chrono::milliseconds(500)));
    {
        std::lock_guard<std::mutex> lock(sink_mutex);
        EXPECT_EQ(emitted_intents.front().product_id, "c");
        EXPECT_EQ(emitted_intents.front().contract_generation, 7U);
    }

    StateSnapshot7D resolved = ready;
    resolved.ts_ns = 6;
    engine.EnqueueState(resolved);
    ASSERT_TRUE(WaitUntil(
        [&]() {
            std::lock_guard<std::mutex> lock(sink_mutex);
            return emitted_intents.size() == 2U;
        },
        std::chrono::milliseconds(500)));
    {
        std::lock_guard<std::mutex> lock(sink_mutex);
        EXPECT_EQ(emitted_intents.back().product_id, "c");
        EXPECT_EQ(emitted_intents.back().contract_generation, 9U);
    }

    engine.Stop();
    g_probe = nullptr;
}

}  // namespace
namespace {

struct ReliableProbe {
    std::mutex mutex;
    std::condition_variable cv;
    bool entered{false};
    bool released{false};
    std::vector<std::string> seen;
};

class ReliableProbeStrategy final : public ILiveStrategy {
   public:
    explicit ReliableProbeStrategy(std::shared_ptr<ReliableProbe> probe)
        : probe_(std::move(probe)) {}
    void Initialize(const StrategyContext&) override {}
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state) override {
        std::unique_lock<std::mutex> lock(probe_->mutex);
        if (!probe_->entered) {
            probe_->entered = true;
            probe_->cv.notify_all();
            probe_->cv.wait(lock, [&] { return probe_->released; });
        }
        probe_->seen.push_back("state:" + std::to_string(state.ts_ns));
        return {};
    }
    void OnOrderEvent(const OrderEvent& event) override {
        std::lock_guard<std::mutex> lock(probe_->mutex);
        probe_->seen.push_back("order:" + event.client_order_id);
    }
    std::size_t ReconcileNetPositions(
        const std::unordered_map<std::string, std::int32_t>& authoritative_net,
        const std::unordered_map<std::string, double>& authoritative_avg_open,
        std::vector<std::string>*) override {
        std::lock_guard<std::mutex> lock(probe_->mutex);
        const auto net_it = authoritative_net.find("hc2701");
        const auto avg_it = authoritative_avg_open.find("hc2701");
        probe_->seen.push_back(
            "reconcile:" +
            std::to_string(net_it == authoritative_net.end() ? 0 : net_it->second) + ":" +
            std::to_string(avg_it == authoritative_avg_open.end() ? 0.0 : avg_it->second));
        return 0;
    }
    std::vector<SignalIntent> OnTimer(EpochNanos) override {
        std::lock_guard<std::mutex> lock(probe_->mutex);
        probe_->seen.push_back("timer");
        return {};
    }
    void Shutdown() override {}

   private:
    std::shared_ptr<ReliableProbe> probe_;
};

std::string RegisterReliableProbe(const std::shared_ptr<ReliableProbe>& probe) {
    const auto name = UniqueFactoryName();
    std::string error;
    EXPECT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        name, [probe] { return std::make_unique<ReliableProbeStrategy>(probe); }, &error));
    return name;
}

void ReleaseReliableProbe(const std::shared_ptr<ReliableProbe>& probe) {
    std::lock_guard<std::mutex> lock(probe->mutex);
    probe->released = true;
    probe->cv.notify_all();
}

bool WaitForReliableProbe(const std::shared_ptr<ReliableProbe>& probe) {
    std::unique_lock<std::mutex> lock(probe->mutex);
    return probe->cv.wait_for(lock, std::chrono::seconds(1), [&] { return probe->entered; });
}

}  // namespace

TEST(StrategyEngineTest, RetainsFilledOrderAndReportsReliableAdmissionOverflow) {
    auto probe = std::make_shared<ReliableProbe>();
    StrategyEngineConfig config;
    config.queue_capacity = 1;
    config.reliable_queue_capacity = 1;
    config.timer_interval_ns = 1'000'000'000;
    StrategyEngine engine(config);
    std::string error;
    ASSERT_TRUE(engine.Start({"probe"}, RegisterReliableProbe(probe), {}, &error));
    StateSnapshot7D state;
    state.ts_ns = 1;
    ASSERT_TRUE(engine.EnqueueState(state));
    const bool entered = WaitForReliableProbe(probe);
    OrderEvent fill;
    fill.client_order_id = "fill";
    fill.strategy_id = "probe";
    fill.status = OrderStatus::kFilled;
    fill.filled_volume = 1;
    const auto order_result = engine.EnqueueOrderEvent(fill);
    state.ts_ns = 2;
    const auto state_result = engine.EnqueueState(state);
    const auto rejected = engine.EnqueueOrderEvent(fill);
    ReleaseReliableProbe(probe);
    EXPECT_TRUE(entered);
    EXPECT_TRUE(order_result);
    EXPECT_TRUE(state_result);
    EXPECT_EQ(rejected.status, StrategyEnqueueStatus::kQueueFull);
    EXPECT_TRUE(engine.WaitUntilDrained(1000));
    EXPECT_TRUE(engine.GetHealth().overloaded);
    EXPECT_TRUE(engine.AcknowledgeRecovery());
    engine.Stop();
    EXPECT_EQ(probe->seen, (std::vector<std::string>{"state:1", "order:fill", "state:2"}));
    EXPECT_EQ(engine.GetStats().dropped_oldest_events, 0U);
    EXPECT_EQ(engine.GetStats().rejected_reliable_events, 1U);
    EXPECT_EQ(engine.EnqueueOrderEvent(fill).status, StrategyEnqueueStatus::kStopped);
}

TEST(StrategyEngineTest, ReconcilePositionSnapshotIsAcceptedAndProcessedThroughFifo) {
    auto probe = std::make_shared<ReliableProbe>();
    StrategyEngineConfig config;
    config.queue_capacity = 1;
    config.reliable_queue_capacity = 1;
    config.timer_interval_ns = 1'000'000'000;
    StrategyEngine engine(config);
    std::string error;
    ASSERT_TRUE(engine.Start({"probe"}, RegisterReliableProbe(probe), {}, &error));

    StateSnapshot7D state;
    state.ts_ns = 1;
    ASSERT_TRUE(engine.EnqueueState(state));
    ASSERT_TRUE(WaitForReliableProbe(probe));

    OrderEvent fill;
    fill.client_order_id = "fill-before-reconcile";
    fill.strategy_id = "probe";
    fill.status = OrderStatus::kFilled;
    fill.filled_volume = 1;
    ASSERT_TRUE(engine.EnqueueOrderEvent(fill));
    const auto reconcile =
        engine.EnqueueReconcilePositions("", {{"hc2701", -16}}, {{"hc2701", 3368.0}});
    ASSERT_TRUE(reconcile);
    EXPECT_GT(reconcile.sequence, 0U);
    const auto rejected_reconcile =
        engine.EnqueueReconcilePositions("", {{"hc2701", 0}}, {});
    EXPECT_EQ(rejected_reconcile.status, StrategyEnqueueStatus::kQueueFull);

    ReleaseReliableProbe(probe);
    ASSERT_TRUE(engine.WaitUntilDrained(1000));
    engine.Stop();

    EXPECT_EQ(probe->seen,
              (std::vector<std::string>{"state:1", "order:fill-before-reconcile",
                                        "reconcile:-16:3368.000000"}));
    EXPECT_EQ(engine.GetStats().rejected_reliable_events, 1U);
}

TEST(StrategyEngineTest, DeadlineMarkerPreservesFifoAndHealthRemainsReadableWhileWorkerBlocked) {
    auto probe = std::make_shared<ReliableProbe>();
    StrategyEngineConfig config;
    config.queue_capacity = 1;
    config.reliable_queue_capacity = 1;
    config.timer_interval_ns = 5'000'000;
    StrategyEngine engine(config);
    std::string error;
    ASSERT_TRUE(engine.Start({"probe"}, RegisterReliableProbe(probe), {}, &error));
    StateSnapshot7D state;
    state.ts_ns = 1;
    ASSERT_TRUE(engine.EnqueueState(state));
    const bool entered = WaitForReliableProbe(probe);
    const bool deadline_queued =
        WaitUntil([&] { return engine.GetHealth().pending_timer_lateness_ns > 0; },
                  std::chrono::milliseconds(500));
    const auto health = engine.GetHealth();
    OrderEvent order;
    order.client_order_id = "after-deadline";
    order.strategy_id = "probe";
    const auto admitted = engine.EnqueueOrderEvent(order);
    ReleaseReliableProbe(probe);
    EXPECT_TRUE(entered);
    EXPECT_TRUE(deadline_queued);
    EXPECT_TRUE(health.callback_in_progress);
    EXPECT_GT(health.worker_progress_age_ns, 0U);
    EXPECT_TRUE(admitted);
    EXPECT_TRUE(engine.WaitUntilDrained(1000));
    engine.Stop();
    const auto timer = std::find(probe->seen.begin(), probe->seen.end(), "timer");
    const auto event = std::find(probe->seen.begin(), probe->seen.end(), "order:after-deadline");
    ASSERT_NE(timer, probe->seen.end());
    ASSERT_NE(event, probe->seen.end());
    EXPECT_LT(timer, event);
    EXPECT_GT(engine.GetStats().timer_callbacks, 0U);
}

TEST(StrategyEngineTest, StopCompletesQueuedControlPromiseBeforeBlockedWorkerFinishes) {
    auto probe = std::make_shared<ReliableProbe>();
    StrategyEngineConfig config;
    config.timer_interval_ns = 1'000'000'000;
    StrategyEngine engine(config);
    std::string error;
    ASSERT_TRUE(engine.Start({"probe"}, RegisterReliableProbe(probe), {}, &error));
    ASSERT_TRUE(engine.EnqueueState({}));
    const bool entered = WaitForReliableProbe(probe);
    auto barrier = std::async(std::launch::async, [&] {
        return engine.ApplyContractSwitch({"rb", "rb2609", "rb2610", 1}, {}, 2000);
    });
    const bool queued = WaitUntil([&] { return engine.GetHealth().queue_depth != 0; },
                                  std::chrono::milliseconds(500));
    auto stopped = std::async(std::launch::async, [&] { engine.Stop(); });
    const bool woke = barrier.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready;
    ReleaseReliableProbe(probe);
    EXPECT_TRUE(entered);
    EXPECT_TRUE(queued);
    EXPECT_TRUE(woke);
    EXPECT_FALSE(barrier.get().success);
    stopped.get();
}

TEST(StrategyEngineTest, CommittedDeliveryAcknowledgesOnlyAfterSuccessfulSnapshot) {
    ResetThrowingBehavior();
    const auto factory = UniqueFactoryName();
    std::string error;
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory, []() { return std::make_unique<RecordingStrategy>(); }, &error));
    auto persistence = std::make_shared<TestStatePersistence>();
    std::atomic<int> acknowledgements{0};
    std::atomic<int> failed_deliveries{0};
    std::atomic<bool> saved_before_ack{false};
    StrategyEngineConfig config;
    config.state_persistence = persistence;
    config.committed_event_sink = [&](const OrderEvent&) {
        saved_before_ack.store(persistence->save_calls() > 0);
        ++acknowledgements;
        return true;
    };
    config.committed_event_failure_sink = [&](const OrderEvent&) { ++failed_deliveries; };
    StrategyEngine engine(config);
    StrategyContext context;
    context.account_id = "test-account";
    ASSERT_TRUE(engine.Start({"alpha"}, factory, context, &error));
    OrderEvent event;
    event.account_id = context.account_id;
    event.strategy_id = "alpha";
    event.committed_position = Position{};
    event.committed_trade_identity = "committed";
    ASSERT_TRUE(engine.EnqueueOrderEvent(event));
    ASSERT_TRUE(engine.WaitUntilDrained(1000));
    EXPECT_EQ(acknowledgements.load(), 1);
    EXPECT_TRUE(saved_before_ack.load());
    {
        std::lock_guard<std::mutex> lock(g_behavior_mutex);
        g_throw_on_order_strategy = "alpha";
    }
    ASSERT_TRUE(engine.EnqueueOrderEvent(event));
    ASSERT_TRUE(engine.WaitUntilDrained(1000));
    EXPECT_EQ(acknowledgements.load(), 1);
    EXPECT_TRUE(engine.GetHealth().overloaded);
    EXPECT_EQ(failed_deliveries.load(), 1);
    engine.Stop();
    ResetThrowingBehavior();
}

TEST(StrategyEngineTest, MarketGapRecoveryRequiresCompleteWarmupAndPersistsBeforeSuccess) {
    ResetThrowingBehavior();
    Probe probe;
    g_probe = &probe;
    const auto factory = UniqueFactoryName();
    std::string error;
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory, [] { return std::make_unique<RecordingStrategy>(); }, &error));
    auto persistence = std::make_shared<TestStatePersistence>();
    StrategyEngineConfig config;
    config.timer_interval_ns = 10'000'000'000LL;
    config.state_persistence = persistence;
    std::atomic<int> intents{0};
    StrategyEngine engine(config, [&](const SignalIntent&) { ++intents; });
    ASSERT_TRUE(engine.Start({"gap"}, factory, {}, &error));
    MarketGapContext gap{"rb2609", 1};
    auto report = engine.ApplyMarketGapRecovery(gap, {}, 1000);
    EXPECT_FALSE(report.success);
    EXPECT_EQ(report.required_bars.at(5), 2);
    EXPECT_TRUE(probe.contract_switches.empty());
    StateSnapshot7D state;
    state.instrument_id = gap.instrument_id;
    state.timeframe_minutes = 5;
    state.ts_ns = 1;
    state.has_bar = true;
    state.bar_close = 4000;
    auto later = state;
    later.ts_ns = 2;
    report = engine.ApplyMarketGapRecovery(gap, {later, state, state}, 1000);
    EXPECT_TRUE(report.success) << report.error;
    EXPECT_GT(persistence->save_calls(), 0);
    EXPECT_EQ(probe.observed_state_ts, (std::vector<EpochNanos>{1, 2}));
    EXPECT_EQ(intents.load(), 0);
    persistence->FailSave(true);
    report = engine.ApplyMarketGapRecovery(gap, {state, later}, 1000);
    EXPECT_FALSE(report.success);
    EXPECT_EQ(report.error, "injected durable snapshot failure");
    persistence->FailSave(false);
    report = engine.ApplyMarketGapRecovery(gap, {state, later}, 1000);
    EXPECT_TRUE(report.success) << report.error;
    engine.Stop();
    g_probe = nullptr;
}

TEST(StrategyEngineTest, MarketGapEvidenceRejectsPartialBarsAndStaleGenerationAcknowledgement) {
    MarketGapRecovery recovery;
    recovery.MarkGap("rb", 1);
    const auto first = recovery.Snapshot().front().context;
    TimeframeStateEmission emission;
    emission.timeframe_minutes = 5;
    emission.bar.instrument_id = emission.state.instrument_id = "rb";
    emission.bar.period_end_ts_ns = 600'000'000'000LL;
    emission.state.ts_ns = emission.bar.period_end_ts_ns;
    emission.state.timeframe_minutes = 5;
    emission.state.has_bar = true;
    emission.state.bar_close = 4000;
    ASSERT_TRUE(recovery.Observe(emission));
    EXPECT_EQ(recovery.Snapshot().front().states.size(), 1U);
    ASSERT_TRUE(recovery.Observe(emission));
    EXPECT_EQ(recovery.Snapshot().front().states.size(), 1U);
    emission.bar.is_complete = false;
    ASSERT_TRUE(recovery.Observe(emission));
    EXPECT_TRUE(recovery.Snapshot().front().states.empty());
    recovery.MarkGap("rb", 2);
    EXPECT_FALSE(recovery.Complete(first));
    StrategyState saved;
    recovery.SaveState(&saved);
    MarketGapRecovery restarted;
    std::string error;
    ASSERT_TRUE(restarted.LoadState(saved, &error)) << error;
    EXPECT_TRUE(restarted.Suppresses("rb"));
    EXPECT_TRUE(restarted.Snapshot().front().states.empty());
    EXPECT_TRUE(restarted.Complete(restarted.Snapshot().front().context));
    EXPECT_FALSE(restarted.Suppresses("rb"));
}

}  // namespace quant_hft
