#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/strategy/composite_strategy.h"
#include "quant_hft/strategy/live_strategy.h"
#include "quant_hft/strategy/state_persistence.h"

namespace quant_hft {

struct StrategyContractIdentity {
    std::string product_id;
    std::uint64_t generation{0};
};

struct StrategyEngineConfig {
    std::size_t queue_capacity{8192};
    EpochNanos timer_interval_ns{100'000'000};  // 100ms
    std::shared_ptr<IStrategyStatePersistence> state_persistence;
    std::function<void(const StateSnapshot7D&, const std::string&, const CompositeAtomicTraceRow&)>
        indicator_trace_sink;
    std::function<double(const std::string&)> contract_multiplier_resolver;
    std::function<std::optional<StrategyContractIdentity>(const std::string&)>
        contract_identity_resolver;
    bool load_state_on_start{false};
    EpochNanos state_snapshot_interval_ns{0};
    EpochNanos metrics_collect_interval_ns{1'000'000'000};
};

class StrategyEngine {
   public:
    struct ContractSwitchReport {
        bool success{false};
        std::uint64_t generation{0};
        std::int32_t required_warmup_bars{0};
        std::int32_t replayed_warmup_bars{0};
        std::string error;
    };
    struct StrategyLaunchSpec {
        std::string strategy_id;
        std::string strategy_factory;
        StrategyContext context;
    };

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

    bool Start(const std::vector<std::string>& strategy_ids, const std::string& strategy_factory,
               const StrategyContext& base_context, std::string* error);
    bool Start(const std::vector<StrategyLaunchSpec>& launch_specs, std::string* error);
    void Stop();

    void EnqueueState(const StateSnapshot7D& state, const std::string& product_id = {},
                      std::uint64_t contract_generation = 0, bool emit_intents = true);
    void EnqueueMarketTick(const MarketSnapshot& snapshot, const std::string& product_id = {},
                           std::uint64_t contract_generation = 0, bool emit_intents = true);
    void EnqueueOrderEvent(const OrderEvent& event);
    void EnqueueAccountSnapshot(const TradingAccountSnapshot& snapshot);
    // Enqueue an authoritative (broker-truth) signed net position snapshot so the
    // matching strategies reconcile their believed net positions. Routed through
    // the same FIFO queue as order events, which guarantees it is processed after
    // any already-enqueued replay order events (no double counting).
    // `authoritative_avg_open`, when populated, carries broker-derived average
    // open prices keyed by instrument so reconcile-sourced positions can recover
    // an entry price for risk logic (e.g. trailing stops).
    void EnqueueReconcilePositions(
        const std::string& account_id,
        const std::unordered_map<std::string, std::int32_t>& authoritative_net,
        const std::unordered_map<std::string, double>& authoritative_avg_open = {});
    std::vector<StrategyMetric> CollectAllMetrics() const;
    // Waits until every event enqueued before the call has completed its strategy callback.
    // Used as the recovery barrier before trading permission can leave Blocked.
    bool WaitUntilDrained(std::int64_t timeout_ms);
    ContractSwitchReport ApplyContractSwitch(const ContractSwitchContext& context,
                                             const std::vector<StateSnapshot7D>& warmup_states,
                                             std::int64_t timeout_ms);
    bool ApplyContractWarmupState(const StateSnapshot7D& state, const std::string& product_id,
                                  std::uint64_t contract_generation, std::int64_t timeout_ms);

    Stats GetStats() const;

   private:
    enum class EventType {
        kState,
        kMarketTick,
        kOrderEvent,
        kAccountSnapshot,
        kReconcilePositions,
        kContractSwitch,
        kContractWarmupState,
    };

    struct EngineEvent {
        EventType type{EventType::kState};
        StateSnapshot7D state;
        MarketSnapshot market_tick;
        OrderEvent order_event;
        TradingAccountSnapshot account_snapshot;
        std::string reconcile_account_id;
        std::unordered_map<std::string, std::int32_t> reconcile_net;
        std::unordered_map<std::string, double> reconcile_avg_open;
        std::string product_id;
        std::uint64_t contract_generation{0};
        bool emit_intents{true};
        ContractSwitchContext contract_switch;
        std::vector<StateSnapshot7D> warmup_states;
        std::shared_ptr<std::promise<ContractSwitchReport>> contract_switch_promise;
        std::shared_ptr<std::promise<bool>> contract_warmup_promise;
    };

    struct StrategyEntry {
        std::string strategy_id;
        std::string account_id;
        std::unique_ptr<ILiveStrategy> strategy;
    };

    void EnqueueEvent(EngineEvent event);
    void WorkerLoop();
    bool DispatchState(const StateSnapshot7D& state, const std::string& product_id,
                       std::uint64_t contract_generation, bool emit_intents);
    void DispatchMarketTick(const MarketSnapshot& snapshot, const std::string& product_id,
                            std::uint64_t contract_generation, bool emit_intents);
    void DispatchOrderEvent(const OrderEvent& event);
    void DispatchAccountSnapshot(const TradingAccountSnapshot& snapshot);
    void DispatchReconcilePositions(
        const std::string& account_id,
        const std::unordered_map<std::string, std::int32_t>& authoritative_net,
        const std::unordered_map<std::string, double>& authoritative_avg_open);
    void DispatchTimer(EpochNanos now_ns);
    ContractSwitchReport DispatchContractSwitch(const ContractSwitchContext& context,
                                                const std::vector<StateSnapshot7D>& warmup_states);
    void MaybeSnapshotStates(EpochNanos now_ns);
    void SnapshotStates(EpochNanos now_ns);
    void MaybeCollectMetrics(EpochNanos now_ns);
    void EmitIntents(const std::string& strategy_id, std::vector<SignalIntent> intents,
                     const std::string& product_id = {}, std::uint64_t contract_generation = 0);

    StrategyEngineConfig config_;
    IntentSink intent_sink_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<EngineEvent> queue_;
    std::vector<StrategyEntry> strategies_;
    std::vector<StrategyMetric> cached_metrics_;
    Stats stats_;
    bool running_{false};
    bool stop_requested_{false};
    bool dispatching_{false};
    EpochNanos last_state_snapshot_ns_{0};
    EpochNanos last_metrics_collect_ns_{0};

    std::thread worker_thread_;
};

}  // namespace quant_hft
