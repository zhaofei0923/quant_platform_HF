#include "quant_hft/core/python_callback_dispatcher.h"

#include <algorithm>
#include <chrono>
#include <string>

#include "quant_hft/core/structured_log.h"
#include "quant_hft/monitoring/metric_registry.h"

namespace quant_hft {

namespace {

std::int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::shared_ptr<MonitoringCounter> PythonDispatcherDroppedCounter() {
    static auto counter = MetricRegistry::Instance().BuildCounter(
        "quant_hft_python_callback_dispatcher_dropped_total",
        "Total dropped tasks in PythonCallbackDispatcher");
    return counter;
}

std::shared_ptr<MonitoringCounter> PythonDispatcherCriticalTimeoutCounter() {
    static auto counter = MetricRegistry::Instance().BuildCounter(
        "quant_hft_python_callback_dispatcher_critical_timeout_total",
        "Total critical task enqueue timeouts in PythonCallbackDispatcher");
    return counter;
}

std::shared_ptr<MonitoringCounter> PythonDispatcherCriticalDelayExceededCounter() {
    static auto counter = MetricRegistry::Instance().BuildCounter(
        "quant_hft_python_callback_dispatcher_critical_queue_delay_exceeded_total",
        "Total critical callbacks with queue delay above threshold");
    return counter;
}

}  // namespace

PythonCallbackDispatcher::PythonCallbackDispatcher(std::size_t max_queue_size,
                                                   std::int64_t critical_wait_ms,
                                                   std::int64_t critical_delay_alert_ms)
    : max_queue_size_(std::max<std::size_t>(1, max_queue_size)),
      critical_wait_ms_(std::max<std::int64_t>(1, critical_wait_ms)),
      critical_delay_alert_ms_(std::max<std::int64_t>(1, critical_delay_alert_ms)) {}

PythonCallbackDispatcher::~PythonCallbackDispatcher() {
    Stop();
}

void PythonCallbackDispatcher::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) {
        return;
    }
    stop_ = false;
    worker_ = std::thread(&PythonCallbackDispatcher::WorkerLoop, this);
    running_ = true;
}

void PythonCallbackDispatcher::Stop() {
    std::thread worker;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) {
            return;
        }
        stop_ = true;
        running_ = false;
        worker.swap(worker_);
    }
    cv_.notify_all();
    space_cv_.notify_all();
    if (worker.joinable()) {
        worker.join();
    }
}

bool PythonCallbackDispatcher::Post(Task task, bool is_critical) {
    if (!task) {
        return false;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    if (!running_ || stop_) {
        return false;
    }

    auto has_space = [this]() { return queue_.size() < max_queue_size_ || stop_; };
    if (queue_.size() >= max_queue_size_) {
        if (!is_critical) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            PythonDispatcherDroppedCounter()->Increment();
            EmitStructuredLog(nullptr,
                              "python_callback_dispatcher",
                              "warn",
                              "queue_full",
                              {{"is_critical", "false"},
                               {"queue_depth", std::to_string(queue_.size())},
                               {"queue_capacity", std::to_string(max_queue_size_)},
                               {"dropped_total",
                                std::to_string(dropped_.load(std::memory_order_relaxed))},
                               {"action", "drop"}});
            return false;
        }
        const bool wait_ok = space_cv_.wait_for(
            lock,
            std::chrono::milliseconds(critical_wait_ms_),
            has_space);
        if (!wait_ok || queue_.size() >= max_queue_size_) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            critical_timeout_.fetch_add(1, std::memory_order_relaxed);
            PythonDispatcherDroppedCounter()->Increment();
            PythonDispatcherCriticalTimeoutCounter()->Increment();
            EmitStructuredLog(nullptr,
                              "python_callback_dispatcher",
                              "error",
                              "queue_full",
                              {{"is_critical", "true"},
                               {"queue_depth", std::to_string(queue_.size())},
                               {"queue_capacity", std::to_string(max_queue_size_)},
                               {"dropped_total",
                                std::to_string(dropped_.load(std::memory_order_relaxed))},
                               {"critical_timeout_total",
                                std::to_string(critical_timeout_.load(std::memory_order_relaxed))},
                               {"action", "block_10ms_failed"}});
            return false;
        }
    }

    if (stop_) {
        return false;
    }

    queue_.push_back(QueuedTask{std::move(task), NowNs(), is_critical});
    const auto pending = pending_.fetch_add(1, std::memory_order_relaxed) + 1;
    auto previous_max = max_pending_.load(std::memory_order_relaxed);
    while (pending > previous_max &&
           !max_pending_.compare_exchange_weak(previous_max, pending, std::memory_order_relaxed)) {
    }
    lock.unlock();
    cv_.notify_one();
    return true;
}

PythonCallbackDispatcher::Stats PythonCallbackDispatcher::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    Stats stats;
    stats.pending = pending_.load(std::memory_order_relaxed);
    stats.dropped = dropped_.load(std::memory_order_relaxed);
    stats.critical_timeout = critical_timeout_.load(std::memory_order_relaxed);
    stats.critical_delay_exceeded = critical_delay_exceeded_.load(std::memory_order_relaxed);
    stats.max_pending = max_pending_.load(std::memory_order_relaxed);
    stats.max_queue_size = max_queue_size_;
    stats.last_critical_queue_delay_ms =
        last_critical_queue_delay_ms_.load(std::memory_order_relaxed);
    return stats;
}

void PythonCallbackDispatcher::WorkerLoop() {
    while (true) {
        QueuedTask queued_task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() { return stop_ || !queue_.empty(); });
            if (stop_ && queue_.empty()) {
                return;
            }
            queued_task = std::move(queue_.front());
            queue_.pop_front();
            pending_.fetch_sub(1, std::memory_order_relaxed);
            space_cv_.notify_one();
        }

        if (queued_task.is_critical) {
            const auto delay_ms =
                std::max<std::int64_t>(0, (NowNs() - queued_task.enqueue_ts_ns) / 1'000'000);
            last_critical_queue_delay_ms_.store(delay_ms, std::memory_order_relaxed);
            if (delay_ms > critical_delay_alert_ms_) {
                critical_delay_exceeded_.fetch_add(1, std::memory_order_relaxed);
                PythonDispatcherCriticalDelayExceededCounter()->Increment();
                EmitStructuredLog(
                    nullptr,
                    "python_callback_dispatcher",
                    "warn",
                    "critical_queue_delay",
                    {{"queue_delay_ms", std::to_string(delay_ms)},
                     {"alert_threshold_ms", std::to_string(critical_delay_alert_ms_)},
                     {"queue_depth", std::to_string(pending_.load(std::memory_order_relaxed))},
                     {"exceeded_total",
                      std::to_string(critical_delay_exceeded_.load(std::memory_order_relaxed))}});
            }
        }

        Task task = std::move(queued_task.task);
        if (!task) {
            continue;
        }
        task();
    }
}

}  // namespace quant_hft
