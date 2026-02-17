#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/strategy/live_strategy.h"

namespace quant_hft {

struct StrategyEngineConfig {
    std::size_t queue_capacity{8192};
    EpochNanos timer_interval_ns{100'000'000};  // 100ms
};

class StrategyEngine {
public:
    struct Stats {
        std::uint64_t enqueued_events{0};
        std::uint64_t processed_events{0};
        std::uint64_t dropped_oldest_events{0};
        std::uint64_t broadcast_order_events{0};
        std::uint64_t unmatched_order_events{0};
        std::uint64_t strategy_callback_exceptions{0};
    };

    using IntentSink = std::function<void(const SignalIntent&)>;

    explicit StrategyEngine(StrategyEngineConfig config = {}, IntentSink intent_sink = nullptr);
    ~StrategyEngine();

    bool Start(const std::vector<std::string>& strategy_ids,
               const std::string& strategy_factory,
               const StrategyContext& base_context,
               std::string* error);
    void Stop();

    void EnqueueState(const StateSnapshot7D& state);
    void EnqueueOrderEvent(const OrderEvent& event);

    Stats GetStats() const;

private:
    enum class EventType {
        kState,
        kOrderEvent,
    };

    struct EngineEvent {
        EventType type{EventType::kState};
        StateSnapshot7D state;
        OrderEvent order_event;
    };

    struct StrategyEntry {
        std::string strategy_id;
        std::unique_ptr<ILiveStrategy> strategy;
    };

    void EnqueueEvent(EngineEvent event);
    void WorkerLoop();
    void DispatchState(const StateSnapshot7D& state);
    void DispatchOrderEvent(const OrderEvent& event);
    void DispatchTimer(EpochNanos now_ns);
    void EmitIntents(const std::string& strategy_id, std::vector<SignalIntent> intents);

    StrategyEngineConfig config_;
    IntentSink intent_sink_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<EngineEvent> queue_;
    std::vector<StrategyEntry> strategies_;
    Stats stats_;
    bool running_{false};
    bool stop_requested_{false};

    std::thread worker_thread_;
};

}  // namespace quant_hft
