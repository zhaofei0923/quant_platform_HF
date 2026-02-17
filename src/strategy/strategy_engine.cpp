#include "quant_hft/strategy/strategy_engine.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <utility>

#include "quant_hft/strategy/strategy_registry.h"

namespace quant_hft {

namespace {

constexpr EpochNanos kDefaultTimerIntervalNs = 100'000'000;

}  // namespace

StrategyEngine::StrategyEngine(StrategyEngineConfig config, IntentSink intent_sink)
    : config_(std::move(config)), intent_sink_(std::move(intent_sink)) {
    if (config_.queue_capacity == 0) {
        config_.queue_capacity = 1;
    }
    if (config_.timer_interval_ns <= 0) {
        config_.timer_interval_ns = kDefaultTimerIntervalNs;
    }
}

StrategyEngine::~StrategyEngine() {
    Stop();
}

bool StrategyEngine::Start(const std::vector<std::string>& strategy_ids,
                           const std::string& strategy_factory,
                           const StrategyContext& base_context,
                           std::string* error) {
    Stop();

    if (strategy_ids.empty()) {
        if (error != nullptr) {
            *error = "strategy_ids must not be empty";
        }
        return false;
    }
    if (strategy_factory.empty()) {
        if (error != nullptr) {
            *error = "strategy_factory must not be empty";
        }
        return false;
    }

    std::vector<StrategyEntry> initialized;
    initialized.reserve(strategy_ids.size());
    try {
        for (const auto& strategy_id : strategy_ids) {
            auto strategy = StrategyRegistry::Instance().Create(strategy_factory);
            if (strategy == nullptr) {
                if (error != nullptr) {
                    *error = "strategy_factory not found: " + strategy_factory;
                }
                for (auto& entry : initialized) {
                    entry.strategy->Shutdown();
                }
                return false;
            }
            StrategyContext strategy_context = base_context;
            strategy_context.strategy_id = strategy_id;
            strategy->Initialize(strategy_context);
            initialized.push_back(StrategyEntry{strategy_id, std::move(strategy)});
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

    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.clear();
        strategies_ = std::move(initialized);
        stats_ = {};
        running_ = true;
        stop_requested_ = false;
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

    std::vector<StrategyEntry> strategies_to_shutdown;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        strategies_to_shutdown = std::move(strategies_);
        queue_.clear();
        running_ = false;
        stop_requested_ = false;
    }

    for (auto& entry : strategies_to_shutdown) {
        try {
            entry.strategy->Shutdown();
        } catch (...) {
        }
    }
}

void StrategyEngine::EnqueueState(const StateSnapshot7D& state) {
    EngineEvent event;
    event.type = EventType::kState;
    event.state = state;
    EnqueueEvent(std::move(event));
}

void StrategyEngine::EnqueueOrderEvent(const OrderEvent& event) {
    EngineEvent engine_event;
    engine_event.type = EventType::kOrderEvent;
    engine_event.order_event = event;
    EnqueueEvent(std::move(engine_event));
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
                cv_.wait_for(lock, wait_interval, [&]() {
                    return stop_requested_ || !queue_.empty();
                });
            }

            if (stop_requested_ && queue_.empty()) {
                break;
            }

            if (!queue_.empty()) {
                event = std::move(queue_.front());
                queue_.pop_front();
                ++stats_.processed_events;
                has_event = true;
            }
        }

        if (has_event) {
            if (event.type == EventType::kState) {
                DispatchState(event.state);
            } else {
                DispatchOrderEvent(event.order_event);
            }
            continue;
        }

        DispatchTimer(NowEpochNanos());
    }
}

void StrategyEngine::DispatchState(const StateSnapshot7D& state) {
    std::vector<SignalIntent> intents;
    for (auto& entry : strategies_) {
        try {
            intents = entry.strategy->OnState(state);
            EmitIntents(entry.strategy_id, std::move(intents));
        } catch (...) {
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
            } catch (...) {
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
    } catch (...) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.strategy_callback_exceptions;
    }
}

void StrategyEngine::DispatchTimer(EpochNanos now_ns) {
    std::vector<SignalIntent> intents;
    for (auto& entry : strategies_) {
        try {
            intents = entry.strategy->OnTimer(now_ns);
            EmitIntents(entry.strategy_id, std::move(intents));
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.strategy_callback_exceptions;
        }
    }
}

void StrategyEngine::EmitIntents(const std::string& strategy_id, std::vector<SignalIntent> intents) {
    if (!intent_sink_) {
        return;
    }
    for (auto& intent : intents) {
        if (intent.strategy_id.empty()) {
            intent.strategy_id = strategy_id;
        }
        intent_sink_(intent);
    }
}

}  // namespace quant_hft
