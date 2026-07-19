#include "quant_hft/strategy/strategy_engine.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "quant_hft/strategy/live_strategy.h"
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

TEST(StrategyEngineTest, DropsOldestEventsWhenQueueIsFull) {
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
            std::lock_guard<std::mutex> lock(sink_mutex);
            return !emitted_intents.empty();
        },
        std::chrono::milliseconds(500)));

    engine.Stop();
    g_probe = nullptr;
    g_state_delay_ms.store(0);

    const auto stats = engine.GetStats();
    EXPECT_GT(stats.dropped_oldest_events, 0U);
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
}  // namespace quant_hft
