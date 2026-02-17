#include "quant_hft/strategy/live_strategy.h"
#include "quant_hft/strategy/strategy_engine.h"
#include "quant_hft/strategy/strategy_registry.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace quant_hft {
namespace {

struct Probe {
    std::mutex mutex;
    std::vector<std::string> initialized_strategy_ids;
    std::vector<EpochNanos> observed_state_ts;
    std::vector<std::string> observed_order_events;
    std::vector<std::string> observed_timer_strategies;
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
        intent.trace_id = strategy_id_ + "-" + std::to_string(state.ts_ns);
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

    void Shutdown() override {}

private:
    std::string strategy_id_;
};

TEST(StrategyEngineTest, DispatchesStateAndOrderEventsToAllStrategies) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name,
        []() { return std::make_unique<RecordingStrategy>(); },
        &error))
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

TEST(StrategyEngineTest, RoutesOrderEventByStrategyId) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name,
        []() { return std::make_unique<RecordingStrategy>(); },
        &error))
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
        factory_name,
        []() { return std::make_unique<RecordingStrategy>(); },
        &error))
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
        factory_name,
        []() { return std::make_unique<RecordingStrategy>(); },
        &error))
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
        factory_name,
        []() { return std::make_unique<RecordingStrategy>(); },
        &error))
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

TEST(StrategyEngineTest, DropsOldestEventsWhenQueueIsFull) {
    Probe probe;
    g_probe = &probe;
    ResetThrowingBehavior();

    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name,
        []() { return std::make_unique<RecordingStrategy>(); },
        &error))
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

}  // namespace
}  // namespace quant_hft
