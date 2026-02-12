#include "quant_hft/core/event_dispatcher.h"

#include <algorithm>
#include <chrono>
#include <utility>

namespace quant_hft {

namespace {

std::size_t PriorityIndex(EventPriority priority) {
    return static_cast<std::size_t>(priority);
}

}  // namespace

EventDispatcher::EventDispatcher(std::size_t worker_threads)
    : worker_threads_(std::max<std::size_t>(1, worker_threads)) {}

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
        queues_[index].push_back(std::move(task));
    }
    cv_.notify_one();
    return true;
}

EventDispatcher::Stats EventDispatcher::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    Stats stats;
    stats.pending_high = queues_[PriorityIndex(EventPriority::kHigh)].size();
    stats.pending_normal = queues_[PriorityIndex(EventPriority::kNormal)].size();
    stats.pending_low = queues_[PriorityIndex(EventPriority::kLow)].size();
    stats.processed_total = processed_total_.load();
    stats.worker_threads = worker_threads_;
    return stats;
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

}  // namespace quant_hft
