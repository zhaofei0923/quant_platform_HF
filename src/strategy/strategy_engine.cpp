#include "quant_hft/strategy/strategy_engine.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <exception>
#include <utility>

#include "quant_hft/core/structured_log.h"
#include "quant_hft/strategy/strategy_registry.h"

namespace quant_hft {

namespace {

constexpr EpochNanos kDefaultTimerIntervalNs = 100'000'000;

void EmitStrategyExceptionLog(const std::string& event, const std::string& phase,
                              const std::string& strategy_id, const std::string& error) {
    EmitStructuredLog(nullptr, "strategy_engine", "error", event,
                      {{"phase", phase}, {"strategy_id", strategy_id}, {"error", error}});
}

}  // namespace

StrategyEngine::StrategyEngine(StrategyEngineConfig config, IntentSink intent_sink)
    : config_(std::move(config)), intent_sink_(std::move(intent_sink)) {
    if (config_.queue_capacity == 0) {
        config_.queue_capacity = 1;
    }
    if (config_.timer_interval_ns <= 0) {
        config_.timer_interval_ns = kDefaultTimerIntervalNs;
    }
    if (config_.state_snapshot_interval_ns < 0) {
        config_.state_snapshot_interval_ns = 0;
    }
    if (config_.metrics_collect_interval_ns < 0) {
        config_.metrics_collect_interval_ns = 0;
    }
}

StrategyEngine::~StrategyEngine() { Stop(); }

bool StrategyEngine::Start(const std::vector<std::string>& strategy_ids,
                           const std::string& strategy_factory, const StrategyContext& base_context,
                           std::string* error) {
    std::vector<StrategyLaunchSpec> launch_specs;
    launch_specs.reserve(strategy_ids.size());
    for (const std::string& strategy_id : strategy_ids) {
        StrategyLaunchSpec spec;
        spec.strategy_id = strategy_id;
        spec.strategy_factory = strategy_factory;
        spec.context = base_context;
        launch_specs.push_back(std::move(spec));
    }
    return Start(launch_specs, error);
}

bool StrategyEngine::Start(const std::vector<StrategyLaunchSpec>& launch_specs,
                           std::string* error) {
    Stop();

    if (launch_specs.empty()) {
        if (error != nullptr) {
            *error = "strategy_ids must not be empty";
        }
        return false;
    }

    std::vector<StrategyEntry> initialized;
    initialized.reserve(launch_specs.size());
    try {
        for (const StrategyLaunchSpec& spec : launch_specs) {
            if (spec.strategy_id.empty()) {
                if (error != nullptr) {
                    *error = "strategy_id must not be empty";
                }
                for (auto& entry : initialized) {
                    entry.strategy->Shutdown();
                }
                return false;
            }
            if (spec.strategy_factory.empty()) {
                if (error != nullptr) {
                    *error = "strategy_factory must not be empty";
                }
                for (auto& entry : initialized) {
                    entry.strategy->Shutdown();
                }
                return false;
            }
            auto strategy = StrategyRegistry::Instance().Create(spec.strategy_factory);
            if (strategy == nullptr) {
                if (error != nullptr) {
                    *error = "strategy_factory not found: " + spec.strategy_factory;
                }
                for (auto& entry : initialized) {
                    entry.strategy->Shutdown();
                }
                return false;
            }
            StrategyContext strategy_context = spec.context;
            strategy_context.strategy_id = spec.strategy_id;
            strategy->Initialize(strategy_context);
            initialized.push_back(
                StrategyEntry{spec.strategy_id, strategy_context.account_id, std::move(strategy)});
        }
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = ex.what();
        }
        for (auto& entry : initialized) {
            entry.strategy->Shutdown();
        }
        return false;
    } catch (...) {
        if (error != nullptr) {
            *error = "unknown strategy initialization failure";
        }
        for (auto& entry : initialized) {
            entry.strategy->Shutdown();
        }
        return false;
    }

    if (config_.state_persistence != nullptr && config_.load_state_on_start) {
        for (auto& entry : initialized) {
            if (entry.account_id.empty()) {
                continue;
            }
            StrategyState loaded_state;
            std::string load_error;
            if (!config_.state_persistence->LoadStrategyState(entry.account_id, entry.strategy_id,
                                                              &loaded_state, &load_error)) {
                continue;
            }
            std::string apply_error;
            if (!entry.strategy->LoadState(loaded_state, &apply_error)) {
                if (error != nullptr) {
                    *error = "failed to load strategy state for `" + entry.strategy_id +
                             "`: " + apply_error;
                }
                for (auto& shutdown_entry : initialized) {
                    shutdown_entry.strategy->Shutdown();
                }
                return false;
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.clear();
        strategies_ = std::move(initialized);
        cached_metrics_.clear();
        stats_ = {};
        last_state_snapshot_ns_ = 0;
        last_metrics_collect_ns_ = 0;
        running_ = true;
        stop_requested_ = false;
        dispatching_ = false;
    }

    worker_thread_ = std::thread(&StrategyEngine::WorkerLoop, this);
    return true;
}

void StrategyEngine::Stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ && !worker_thread_.joinable() && strategies_.empty()) {
            return;
        }
        stop_requested_ = true;
    }
    cv_.notify_all();

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }

    SnapshotStates(NowEpochNanos());

    std::vector<StrategyEntry> strategies_to_shutdown;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        strategies_to_shutdown = std::move(strategies_);
        queue_.clear();
        cached_metrics_.clear();
        running_ = false;
        stop_requested_ = false;
        dispatching_ = false;
        last_state_snapshot_ns_ = 0;
        last_metrics_collect_ns_ = 0;
    }

    for (auto& entry : strategies_to_shutdown) {
        try {
            entry.strategy->Shutdown();
        } catch (...) {
        }
    }
}

void StrategyEngine::EnqueueState(const StateSnapshot7D& state, const std::string& product_id,
                                  std::uint64_t contract_generation, bool emit_intents) {
    EngineEvent event;
    event.type = EventType::kState;
    event.state = state;
    event.product_id = product_id;
    event.contract_generation = contract_generation;
    event.emit_intents = emit_intents;
    EnqueueEvent(std::move(event));
}

void StrategyEngine::EnqueueMarketTick(const MarketSnapshot& snapshot,
                                       const std::string& product_id,
                                       std::uint64_t contract_generation, bool emit_intents) {
    EngineEvent event;
    event.type = EventType::kMarketTick;
    event.market_tick = snapshot;
    event.product_id = product_id;
    event.contract_generation = contract_generation;
    event.emit_intents = emit_intents;
    EnqueueEvent(std::move(event));
}

void StrategyEngine::EnqueueOrderEvent(const OrderEvent& event) {
    EngineEvent engine_event;
    engine_event.type = EventType::kOrderEvent;
    engine_event.order_event = event;
    EnqueueEvent(std::move(engine_event));
}

void StrategyEngine::EnqueueAccountSnapshot(const TradingAccountSnapshot& snapshot) {
    EngineEvent event;
    event.type = EventType::kAccountSnapshot;
    event.account_snapshot = snapshot;
    EnqueueEvent(std::move(event));
}

void StrategyEngine::EnqueueReconcilePositions(
    const std::string& account_id,
    const std::unordered_map<std::string, std::int32_t>& authoritative_net,
    const std::unordered_map<std::string, double>& authoritative_avg_open) {
    EngineEvent event;
    event.type = EventType::kReconcilePositions;
    event.reconcile_account_id = account_id;
    event.reconcile_net = authoritative_net;
    event.reconcile_avg_open = authoritative_avg_open;
    EnqueueEvent(std::move(event));
}

std::vector<StrategyMetric> StrategyEngine::CollectAllMetrics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cached_metrics_;
}

bool StrategyEngine::WaitUntilDrained(std::int64_t timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::milliseconds(std::max<std::int64_t>(0, timeout_ms)),
                        [&]() { return queue_.empty() && !dispatching_; });
}

StrategyEngine::ContractSwitchReport StrategyEngine::ApplyContractSwitch(
    const ContractSwitchContext& context, const std::vector<StateSnapshot7D>& warmup_states,
    std::int64_t timeout_ms) {
    ContractSwitchReport failed;
    failed.generation = context.generation;
    if (context.product_id.empty() || context.current_instrument_id.empty() ||
        context.generation == 0) {
        failed.error = "invalid contract switch context";
        return failed;
    }
    auto promise = std::make_shared<std::promise<ContractSwitchReport>>();
    auto future = promise->get_future();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ || stop_requested_) {
            failed.error = "strategy engine is not running";
            return failed;
        }
        EngineEvent event;
        event.type = EventType::kContractSwitch;
        event.contract_switch = context;
        event.warmup_states = warmup_states;
        event.contract_switch_promise = promise;
        queue_.push_back(std::move(event));
        ++stats_.enqueued_events;
    }
    cv_.notify_one();
    const auto wait = std::chrono::milliseconds(std::max<std::int64_t>(0, timeout_ms));
    if (future.wait_for(wait) != std::future_status::ready) {
        failed.error = "strategy contract switch barrier timeout";
        return failed;
    }
    return future.get();
}

bool StrategyEngine::ApplyContractWarmupState(const StateSnapshot7D& state,
                                              const std::string& product_id,
                                              std::uint64_t contract_generation,
                                              std::int64_t timeout_ms) {
    if (state.instrument_id.empty() || product_id.empty() || contract_generation == 0) {
        return false;
    }
    auto promise = std::make_shared<std::promise<bool>>();
    auto future = promise->get_future();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ || stop_requested_) {
            return false;
        }
        EngineEvent event;
        event.type = EventType::kContractWarmupState;
        event.state = state;
        event.product_id = product_id;
        event.contract_generation = contract_generation;
        event.emit_intents = false;
        event.contract_warmup_promise = promise;
        // Contract control events are never dropped by the ordinary bounded-queue policy.
        queue_.push_back(std::move(event));
        ++stats_.enqueued_events;
    }
    cv_.notify_one();
    const auto wait = std::chrono::milliseconds(std::max<std::int64_t>(0, timeout_ms));
    return future.wait_for(wait) == std::future_status::ready && future.get();
}

StrategyEngine::Stats StrategyEngine::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
}

void StrategyEngine::EnqueueEvent(EngineEvent event) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= config_.queue_capacity) {
            queue_.pop_front();
            ++stats_.dropped_oldest_events;
        }
        queue_.push_back(std::move(event));
        ++stats_.enqueued_events;
    }
    cv_.notify_one();
}

void StrategyEngine::WorkerLoop() {
    for (;;) {
        EngineEvent event;
        bool has_event = false;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (queue_.empty()) {
                const auto wait_interval =
                    std::chrono::nanoseconds(std::max<EpochNanos>(1, config_.timer_interval_ns));
                cv_.wait_for(lock, wait_interval,
                             [&]() { return stop_requested_ || !queue_.empty(); });
            }

            if (stop_requested_ && queue_.empty()) {
                break;
            }

            if (!queue_.empty()) {
                event = std::move(queue_.front());
                queue_.pop_front();
                ++stats_.processed_events;
                dispatching_ = true;
                has_event = true;
            }
        }

        if (has_event) {
            if (event.type == EventType::kState) {
                event.state.is_warmup_replay = !event.emit_intents;
                DispatchState(event.state, event.product_id, event.contract_generation,
                              event.emit_intents);
            } else if (event.type == EventType::kMarketTick) {
                DispatchMarketTick(event.market_tick, event.product_id, event.contract_generation,
                                   event.emit_intents);
            } else if (event.type == EventType::kOrderEvent) {
                DispatchOrderEvent(event.order_event);
            } else if (event.type == EventType::kReconcilePositions) {
                DispatchReconcilePositions(event.reconcile_account_id, event.reconcile_net,
                                           event.reconcile_avg_open);
            } else if (event.type == EventType::kContractSwitch) {
                ContractSwitchReport report =
                    DispatchContractSwitch(event.contract_switch, event.warmup_states);
                if (event.contract_switch_promise != nullptr) {
                    try {
                        event.contract_switch_promise->set_value(std::move(report));
                    } catch (...) {
                    }
                }
            } else if (event.type == EventType::kContractWarmupState) {
                event.state.is_warmup_replay = true;
                const bool success =
                    DispatchState(event.state, event.product_id, event.contract_generation, false);
                if (event.contract_warmup_promise != nullptr) {
                    try {
                        event.contract_warmup_promise->set_value(success);
                    } catch (...) {
                    }
                }
            } else {
                DispatchAccountSnapshot(event.account_snapshot);
            }
            {
                std::lock_guard<std::mutex> lock(mutex_);
                dispatching_ = false;
            }
            cv_.notify_all();
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            dispatching_ = true;
        }
        DispatchTimer(NowEpochNanos());
        {
            std::lock_guard<std::mutex> lock(mutex_);
            dispatching_ = false;
        }
        cv_.notify_all();
    }
}

bool StrategyEngine::DispatchState(const StateSnapshot7D& state, const std::string& product_id,
                                   std::uint64_t contract_generation, bool emit_intents) {
    bool success = true;
    std::vector<SignalIntent> intents;
    for (auto& entry : strategies_) {
        try {
            auto* composite = dynamic_cast<CompositeStrategy*>(entry.strategy.get());
            if (composite != nullptr && config_.contract_multiplier_resolver) {
                const double multiplier = config_.contract_multiplier_resolver(state.instrument_id);
                if (std::isfinite(multiplier) && multiplier > 0.0) {
                    composite->SetContractMultiplier(state.instrument_id, multiplier);
                }
            }
            intents = entry.strategy->OnState(state);
            if (config_.indicator_trace_sink) {
                if (composite != nullptr) {
                    const std::vector<CompositeAtomicTraceRow> rows =
                        composite->CollectAtomicIndicatorTrace();
                    for (const CompositeAtomicTraceRow& row : rows) {
                        try {
                            config_.indicator_trace_sink(state, entry.strategy_id, row);
                        } catch (const std::exception& ex) {
                            EmitStrategyExceptionLog("strategy_callback_exception",
                                                     "indicator_trace_sink", entry.strategy_id,
                                                     ex.what());
                            std::lock_guard<std::mutex> lock(mutex_);
                            ++stats_.strategy_callback_exceptions;
                        } catch (...) {
                            EmitStrategyExceptionLog("strategy_callback_exception",
                                                     "indicator_trace_sink", entry.strategy_id,
                                                     "unknown exception");
                            std::lock_guard<std::mutex> lock(mutex_);
                            ++stats_.strategy_callback_exceptions;
                        }
                    }
                }
            }
            if (emit_intents) {
                EmitIntents(entry.strategy_id, std::move(intents), product_id, contract_generation);
            }
        } catch (const std::exception& ex) {
            success = false;
            EmitStrategyExceptionLog("strategy_callback_exception", "state", entry.strategy_id,
                                     ex.what());
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        } catch (...) {
            success = false;
            EmitStrategyExceptionLog("strategy_callback_exception", "state", entry.strategy_id,
                                     "unknown exception");
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        }
    }
    return success;
}

void StrategyEngine::DispatchMarketTick(const MarketSnapshot& snapshot,
                                        const std::string& product_id,
                                        std::uint64_t contract_generation, bool emit_intents) {
    std::vector<SignalIntent> intents;
    for (auto& entry : strategies_) {
        try {
            auto* composite = dynamic_cast<CompositeStrategy*>(entry.strategy.get());
            if (composite != nullptr && config_.contract_multiplier_resolver) {
                const double multiplier =
                    config_.contract_multiplier_resolver(snapshot.instrument_id);
                if (std::isfinite(multiplier) && multiplier > 0.0) {
                    composite->SetContractMultiplier(snapshot.instrument_id, multiplier);
                }
            }
            intents = entry.strategy->OnMarketTick(snapshot);
            if (emit_intents) {
                EmitIntents(entry.strategy_id, std::move(intents), product_id, contract_generation);
            }
        } catch (const std::exception& ex) {
            EmitStrategyExceptionLog("strategy_callback_exception", "market_tick",
                                     entry.strategy_id, ex.what());
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        } catch (...) {
            EmitStrategyExceptionLog("strategy_callback_exception", "market_tick",
                                     entry.strategy_id, "unknown exception");
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        }
    }
}

void StrategyEngine::DispatchOrderEvent(const OrderEvent& event) {
    if (event.strategy_id.empty()) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.broadcast_order_events;
        }
        for (auto& entry : strategies_) {
            try {
                entry.strategy->OnOrderEvent(event);
            } catch (const std::exception& ex) {
                EmitStrategyExceptionLog("strategy_callback_exception", "order_event_broadcast",
                                         entry.strategy_id, ex.what());
                std::lock_guard<std::mutex> lock(mutex_);
                ++stats_.strategy_callback_exceptions;
            } catch (...) {
                EmitStrategyExceptionLog("strategy_callback_exception", "order_event_broadcast",
                                         entry.strategy_id, "unknown exception");
                std::lock_guard<std::mutex> lock(mutex_);
                ++stats_.strategy_callback_exceptions;
            }
        }
        return;
    }

    auto it = std::find_if(strategies_.begin(), strategies_.end(), [&](const StrategyEntry& entry) {
        return entry.strategy_id == event.strategy_id;
    });
    if (it == strategies_.end()) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.unmatched_order_events;
        return;
    }

    try {
        it->strategy->OnOrderEvent(event);
    } catch (const std::exception& ex) {
        EmitStrategyExceptionLog("strategy_callback_exception", "order_event", it->strategy_id,
                                 ex.what());
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.strategy_callback_exceptions;
    } catch (...) {
        EmitStrategyExceptionLog("strategy_callback_exception", "order_event", it->strategy_id,
                                 "unknown exception");
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.strategy_callback_exceptions;
    }
}

void StrategyEngine::DispatchAccountSnapshot(const TradingAccountSnapshot& snapshot) {
    for (auto& entry : strategies_) {
        try {
            entry.strategy->OnAccountSnapshot(snapshot);
        } catch (const std::exception& ex) {
            EmitStrategyExceptionLog("strategy_callback_exception", "account_snapshot",
                                     entry.strategy_id, ex.what());
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        } catch (...) {
            EmitStrategyExceptionLog("strategy_callback_exception", "account_snapshot",
                                     entry.strategy_id, "unknown exception");
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        }
    }
}

void StrategyEngine::DispatchReconcilePositions(
    const std::string& account_id,
    const std::unordered_map<std::string, std::int32_t>& authoritative_net,
    const std::unordered_map<std::string, double>& authoritative_avg_open) {
    for (auto& entry : strategies_) {
        if (!account_id.empty() && entry.account_id != account_id) {
            continue;
        }
        try {
            std::vector<std::string> adjustments;
            const std::size_t adjusted = entry.strategy->ReconcileNetPositions(
                authoritative_net, authoritative_avg_open, &adjustments);
            if (adjusted > 0) {
                std::string joined;
                for (std::size_t i = 0; i < adjustments.size(); ++i) {
                    if (i != 0) {
                        joined += ",";
                    }
                    joined += adjustments[i];
                }
                EmitStructuredLog(nullptr, "strategy_engine", "warn",
                                  "startup_position_reconcile_applied",
                                  {{"strategy_id", entry.strategy_id},
                                   {"account_id", entry.account_id},
                                   {"adjusted_count", std::to_string(adjusted)},
                                   {"adjustments", joined}});
            } else {
                EmitStructuredLog(
                    nullptr, "strategy_engine", "info", "startup_position_reconcile_noop",
                    {{"strategy_id", entry.strategy_id}, {"account_id", entry.account_id}});
            }
        } catch (const std::exception& ex) {
            EmitStrategyExceptionLog("strategy_callback_exception", "reconcile_positions",
                                     entry.strategy_id, ex.what());
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        } catch (...) {
            EmitStrategyExceptionLog("strategy_callback_exception", "reconcile_positions",
                                     entry.strategy_id, "unknown exception");
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        }
    }
}

StrategyEngine::ContractSwitchReport StrategyEngine::DispatchContractSwitch(
    const ContractSwitchContext& context, const std::vector<StateSnapshot7D>& warmup_states) {
    ContractSwitchReport report;
    report.generation = context.generation;
    try {
        for (auto& entry : strategies_) {
            std::string reset_error;
            if (!entry.strategy->ResetForContractSwitch(context, &reset_error)) {
                report.error = "strategy `" + entry.strategy_id + "` reset failed: " + reset_error;
                return report;
            }
            report.required_warmup_bars = std::max(
                report.required_warmup_bars, entry.strategy->RequiredContractWarmupBars(context));
        }

        std::vector<StateSnapshot7D> ordered = warmup_states;
        std::sort(ordered.begin(), ordered.end(), [](const auto& lhs, const auto& rhs) {
            if (lhs.ts_ns != rhs.ts_ns) {
                return lhs.ts_ns < rhs.ts_ns;
            }
            if (lhs.instrument_id != rhs.instrument_id) {
                return lhs.instrument_id < rhs.instrument_id;
            }
            return lhs.timeframe_minutes < rhs.timeframe_minutes;
        });
        ordered.erase(std::unique(ordered.begin(), ordered.end(),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.ts_ns == rhs.ts_ns &&
                                             lhs.instrument_id == rhs.instrument_id &&
                                             lhs.timeframe_minutes == rhs.timeframe_minutes;
                                  }),
                      ordered.end());
        ordered.erase(std::remove_if(ordered.begin(), ordered.end(),
                                     [&](const auto& state) {
                                         return state.instrument_id !=
                                                    context.current_instrument_id ||
                                                state.timeframe_minutes != 5 || !state.has_bar;
                                     }),
                      ordered.end());
        if (report.required_warmup_bars > 0 &&
            ordered.size() > static_cast<std::size_t>(report.required_warmup_bars)) {
            ordered.erase(ordered.begin(), ordered.end() - report.required_warmup_bars);
        }
        for (const auto& state : ordered) {
            StateSnapshot7D replay_state = state;
            replay_state.is_warmup_replay = true;
            for (auto& entry : strategies_) {
                // Warmup deliberately suppresses every returned intent.  The state mutation is
                // identical to live evaluation, but execution cannot observe a replay signal.
                (void)entry.strategy->OnState(replay_state);
            }
            ++report.replayed_warmup_bars;
        }
        report.success = true;
        return report;
    } catch (const std::exception& ex) {
        report.error = ex.what();
    } catch (...) {
        report.error = "unknown strategy contract switch failure";
    }
    return report;
}

void StrategyEngine::DispatchTimer(EpochNanos now_ns) {
    std::vector<SignalIntent> intents;
    for (auto& entry : strategies_) {
        try {
            intents = entry.strategy->OnTimer(now_ns);
            EmitIntents(entry.strategy_id, std::move(intents));
        } catch (const std::exception& ex) {
            EmitStrategyExceptionLog("strategy_callback_exception", "timer", entry.strategy_id,
                                     ex.what());
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        } catch (...) {
            EmitStrategyExceptionLog("strategy_callback_exception", "timer", entry.strategy_id,
                                     "unknown exception");
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        }
    }
    MaybeCollectMetrics(now_ns);
    MaybeSnapshotStates(now_ns);
}

void StrategyEngine::MaybeSnapshotStates(EpochNanos now_ns) {
    if (config_.state_persistence == nullptr || config_.state_snapshot_interval_ns <= 0) {
        return;
    }
    if (last_state_snapshot_ns_ > 0 &&
        (now_ns - last_state_snapshot_ns_) < config_.state_snapshot_interval_ns) {
        return;
    }
    last_state_snapshot_ns_ = now_ns;
    SnapshotStates(now_ns);
}

void StrategyEngine::SnapshotStates(EpochNanos now_ns) {
    (void)now_ns;
    if (config_.state_persistence == nullptr) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.state_snapshot_runs;
    }

    std::uint64_t failures = 0;
    for (auto& entry : strategies_) {
        StrategyState state;
        std::string state_error;
        if (!entry.strategy->SaveState(&state, &state_error)) {
            ++failures;
            continue;
        }
        if (entry.account_id.empty() ||
            !config_.state_persistence->SaveStrategyState(entry.account_id, entry.strategy_id,
                                                          state, &state_error)) {
            ++failures;
            continue;
        }
    }
    if (failures > 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.state_snapshot_failures += failures;
    }
}

void StrategyEngine::MaybeCollectMetrics(EpochNanos now_ns) {
    if (config_.metrics_collect_interval_ns <= 0) {
        return;
    }
    if (last_metrics_collect_ns_ > 0 &&
        (now_ns - last_metrics_collect_ns_) < config_.metrics_collect_interval_ns) {
        return;
    }
    last_metrics_collect_ns_ = now_ns;

    std::vector<StrategyMetric> collected;
    for (auto& entry : strategies_) {
        try {
            std::vector<StrategyMetric> strategy_metrics = entry.strategy->CollectMetrics();
            for (auto& metric : strategy_metrics) {
                if (metric.labels.find("strategy_id") == metric.labels.end()) {
                    metric.labels["strategy_id"] = entry.strategy_id;
                }
                collected.push_back(std::move(metric));
            }
        } catch (const std::exception& ex) {
            EmitStrategyExceptionLog("strategy_callback_exception", "collect_metrics",
                                     entry.strategy_id, ex.what());
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        } catch (...) {
            EmitStrategyExceptionLog("strategy_callback_exception", "collect_metrics",
                                     entry.strategy_id, "unknown exception");
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cached_metrics_ = std::move(collected);
        ++stats_.metrics_collection_runs;
    }
}

void StrategyEngine::EmitIntents(const std::string& strategy_id, std::vector<SignalIntent> intents,
                                 const std::string& product_id, std::uint64_t contract_generation) {
    if (!intent_sink_) {
        return;
    }
    for (auto& intent : intents) {
        if (intent.strategy_id.empty()) {
            intent.strategy_id = strategy_id;
        }
        if (intent.generated_ts_ns <= 0) {
            intent.generated_ts_ns = NowEpochNanos();
        }
        if (intent.product_id.empty()) {
            intent.product_id = product_id;
        }
        if (intent.contract_generation == 0) {
            intent.contract_generation = contract_generation;
        }
        if ((intent.product_id.empty() || intent.contract_generation == 0) &&
            config_.contract_identity_resolver && !intent.instrument_id.empty()) {
            const auto identity = config_.contract_identity_resolver(intent.instrument_id);
            if (identity.has_value()) {
                if (intent.product_id.empty()) {
                    intent.product_id = identity->product_id;
                }
                if (intent.contract_generation == 0) {
                    intent.contract_generation = identity->generation;
                }
            }
        }
        try {
            intent_sink_(intent);
        } catch (const std::exception& ex) {
            EmitStructuredLog(nullptr, "strategy_engine", "error", "strategy_intent_sink_exception",
                              {{"strategy_id", intent.strategy_id},
                               {"trace_id", intent.trace_id},
                               {"instrument_id", intent.instrument_id},
                               {"error", ex.what()}});
            throw;
        } catch (...) {
            EmitStructuredLog(nullptr, "strategy_engine", "error", "strategy_intent_sink_exception",
                              {{"strategy_id", intent.strategy_id},
                               {"trace_id", intent.trace_id},
                               {"instrument_id", intent.instrument_id},
                               {"error", "unknown exception"}});
            throw;
        }
    }
}

}  // namespace quant_hft
