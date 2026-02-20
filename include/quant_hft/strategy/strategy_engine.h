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
#include "quant_hft/strategy/state_persistence.h"

namespace quant_hft {

struct StrategyEngineConfig {
    std::size_t queue_capacity{8192};
    EpochNanos timer_interval_ns{100'000'000};  // 100ms
    std::shared_ptr<IStrategyStatePersistence> state_persistence;
    bool load_state_on_start{false};
    EpochNanos state_snapshot_interval_ns{0};
    EpochNanos metrics_collect_interval_ns{1'000'000'000};
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
        std::uint64_t state_snapshot_runs{0};
        std::uint64_t state_snapshot_failures{0};
        std::uint64_t metrics_collection_runs{0};
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
    void EnqueueAccountSnapshot(const TradingAccountSnapshot& snapshot);
    std::vector<StrategyMetric> CollectAllMetrics() const;

    Stats GetStats() const;

private:
    enum class EventType {
        kState,
        kOrderEvent,
        kAccountSnapshot,
    };

    struct EngineEvent {
        EventType type{EventType::kState};
        StateSnapshot7D state;
        OrderEvent order_event;
        TradingAccountSnapshot account_snapshot;
    };

    struct StrategyEntry {
        std::string strategy_id;
        std::unique_ptr<ILiveStrategy> strategy;
    };

    void EnqueueEvent(EngineEvent event);
    void WorkerLoop();
    void DispatchState(const StateSnapshot7D& state);
    void DispatchOrderEvent(const OrderEvent& event);
    void DispatchAccountSnapshot(const TradingAccountSnapshot& snapshot);
    void DispatchTimer(EpochNanos now_ns);
    void MaybeSnapshotStates(EpochNanos now_ns);
    void MaybeCollectMetrics(EpochNanos now_ns);
    void EmitIntents(const std::string& strategy_id, std::vector<SignalIntent> intents);

    StrategyEngineConfig config_;
    IntentSink intent_sink_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<EngineEvent> queue_;
    std::vector<StrategyEntry> strategies_;
    std::vector<StrategyMetric> cached_metrics_;
    std::string account_id_;
    Stats stats_;
    bool running_{false};
    bool stop_requested_{false};
    EpochNanos last_state_snapshot_ns_{0};
    EpochNanos last_metrics_collect_ns_{0};

    std::thread worker_thread_;
};

}  // namespace quant_hft
