#include "quant_hft/strategy/live_strategy.h"
#include "quant_hft/strategy/strategy_engine.h"
#include "quant_hft/strategy/strategy_registry.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
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
    std::vector<std::string> observed_order_ids;
};

Probe* g_probe = nullptr;
std::atomic<int> g_state_delay_ms{0};

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
        if (g_probe != nullptr) {
            std::lock_guard<std::mutex> lock(g_probe->mutex);
            g_probe->observed_order_ids.push_back(event.client_order_id);
        }
    }

    std::vector<SignalIntent> OnTimer(EpochNanos now_ns) override {
        (void)now_ns;
        return {};
    }

    void Shutdown() override {}

private:
    std::string strategy_id_;
};

}  // namespace

TEST(StrategyEngineTest, DispatchesStateAndOrderEventsToAllStrategies) {
    Probe probe;
    g_probe = &probe;

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
            return emitted_intents.size() >= 2 && probe.observed_order_ids.size() >= 2;
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
    EXPECT_EQ(probe.observed_order_ids.size(), 2U);
}

TEST(StrategyEngineTest, DropsOldestEventsWhenQueueIsFull) {
    Probe probe;
    g_probe = &probe;

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

}  // namespace quant_hft
