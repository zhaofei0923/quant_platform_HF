#include "quant_hft/core/event_dispatcher.h"

#include <algorithm>
#include <chrono>
#include <string>
#include <unordered_map>
#include <utility>

#include "quant_hft/core/structured_log.h"
#include "quant_hft/monitoring/metric_registry.h"

namespace quant_hft {

namespace {

std::size_t PriorityIndex(EventPriority priority) {
    return static_cast<std::size_t>(priority);
}

std::string PriorityName(std::size_t index) {
    switch (index) {
        case 0:
            return "high";
        case 1:
            return "normal";
        case 2:
            return "low";
        default:
            return "unknown";
    }
}

std::shared_ptr<MonitoringCounter> DispatcherDroppedCounter(const std::string& priority) {
    static std::mutex mutex;
    static std::unordered_map<std::string, std::shared_ptr<MonitoringCounter>> counters;
    std::lock_guard<std::mutex> lock(mutex);
    const auto it = counters.find(priority);
    if (it != counters.end()) {
        return it->second;
    }
    auto counter = MetricRegistry::Instance().BuildCounter(
        "quant_hft_event_dispatcher_dropped_total",
        "Total dropped tasks in EventDispatcher",
        {{"priority", priority}});
    counters.emplace(priority, counter);
    return counter;
}

}  // namespace

EventDispatcher::EventDispatcher(std::size_t worker_threads,
                                 std::size_t max_queue_size_normal,
                                 std::size_t max_queue_size_high)
    : worker_threads_(std::max<std::size_t>(1, worker_threads)),
      max_queue_size_normal_(std::max<std::size_t>(1, max_queue_size_normal)),
      max_queue_size_high_(std::max<std::size_t>(1, max_queue_size_high)) {}

EventDispatcher::~EventDispatcher() {
    Stop();
}

void EventDispatcher::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_) {
        return;
    }
    stop_ = false;
    workers_.reserve(worker_threads_);
    for (std::size_t i = 0; i < worker_threads_; ++i) {
        workers_.emplace_back(&EventDispatcher::WorkerLoop, this);
    }
    started_ = true;
}

void EventDispatcher::Stop() {
    std::vector<std::thread> workers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_) {
            return;
        }
        stop_ = true;
        workers.swap(workers_);
        started_ = false;
    }
    cv_.notify_all();
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

bool EventDispatcher::Post(Task task, EventPriority priority) {
    if (!task) {
        return false;
    }
    const auto index = PriorityIndex(priority);
    if (index >= queues_.size()) {
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_) {
            return false;
        }
        const auto capacity = QueueCapacityByIndex(index);
        if (queues_[index].size() >= capacity) {
            dropped_count_.fetch_add(1, std::memory_order_relaxed);
            DispatcherDroppedCounter(PriorityName(index))->Increment();
            EmitStructuredLog(nullptr,
                              "event_dispatcher",
                              "error",
                              "queue_full",
                              {{"priority", PriorityName(index)},
                               {"queue_depth", std::to_string(queues_[index].size())},
                               {"queue_capacity", std::to_string(capacity)},
                               {"dropped_total",
                                std::to_string(dropped_count_.load(std::memory_order_relaxed))}});
            return false;
        }
        queues_[index].push_back(std::move(task));
        const auto pending = total_pending_.fetch_add(1, std::memory_order_relaxed) + 1;
        auto previous_max = max_pending_.load(std::memory_order_relaxed);
        while (pending > previous_max &&
               !max_pending_.compare_exchange_weak(
                   previous_max, pending, std::memory_order_relaxed)) {
        }
    }
    cv_.notify_one();
    return true;
}

EventDispatcher::Stats EventDispatcher::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    Stats stats;
    stats.pending_high = queues_[PriorityIndex(EventPriority::kHigh)].size();
    stats.pending_normal = queues_[PriorityIndex(EventPriority::kNormal)].size();
    stats.pending_low = queues_[PriorityIndex(EventPriority::kLow)].size();
    stats.total_pending = total_pending_.load(std::memory_order_relaxed);
    stats.processed_total = processed_total_.load();
    stats.dropped_total = dropped_count_.load(std::memory_order_relaxed);
    stats.max_pending = max_pending_.load(std::memory_order_relaxed);
    stats.max_queue_size_normal = max_queue_size_normal_;
    stats.max_queue_size_high = max_queue_size_high_;
    stats.worker_threads = worker_threads_;
    return stats;
}

EventDispatcher::Stats EventDispatcher::Snapshot() const {
    return GetStats();
}

bool EventDispatcher::WaitUntilDrained(std::int64_t timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    return drained_cv_.wait_for(
        lock,
        std::chrono::milliseconds(std::max<std::int64_t>(0, timeout_ms)),
        [this]() { return PendingCountLocked() == 0; });
}

void EventDispatcher::WorkerLoop() {
    while (true) {
        Task task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() {
                return stop_ || PendingCountLocked() > 0;
            });
            if (stop_ && PendingCountLocked() == 0) {
                return;
            }
            for (auto priority = 0U; priority < queues_.size(); ++priority) {
                if (!queues_[priority].empty()) {
                    task = std::move(queues_[priority].front());
                    queues_[priority].pop_front();
                    total_pending_.fetch_sub(1, std::memory_order_relaxed);
                    break;
                }
            }
        }
        if (!task) {
            continue;
        }
        task();
        processed_total_.fetch_add(1);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (PendingCountLocked() == 0) {
                drained_cv_.notify_all();
            }
        }
    }
}

std::size_t EventDispatcher::PendingCountLocked() const {
    return queues_[0].size() + queues_[1].size() + queues_[2].size();
}

std::size_t EventDispatcher::QueueCapacityByIndex(std::size_t index) const {
    if (index == PriorityIndex(EventPriority::kHigh)) {
        return max_queue_size_high_;
    }
    return max_queue_size_normal_;
}

}  // namespace quant_hft
