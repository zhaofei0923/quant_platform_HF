#include "quant_hft/core/query_scheduler.h"

#include <algorithm>
#include <utility>

namespace quant_hft {

QueryScheduler::QueryScheduler(std::size_t max_qps)
    : max_qps_(std::max<std::size_t>(1, max_qps)),
      tokens_(static_cast<double>(max_qps_)),
      last_refill_(std::chrono::steady_clock::now()) {}

bool QueryScheduler::TrySchedule(QueryTask task) {
    if (!task.execute) {
        return false;
    }
    const auto idx = static_cast<std::size_t>(task.priority);
    if (idx >= queues_.size()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (task.generation != generation_) {
        return false;
    }
    if (!request_ids_.insert(task.request_id).second) {
        return false;
    }
    task.created_at = std::chrono::steady_clock::now();
    queues_[idx].push(std::move(task));
    return true;
}

std::size_t QueryScheduler::DrainOnce() {
    std::lock_guard<std::recursive_mutex> execution_lock(execution_mutex_);
    std::function<void()> timeout_callback;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (in_flight_ && std::chrono::steady_clock::now() >= deadline_) {
            in_flight_ = false;
            timeout_callback = std::move(on_timeout_);
        }
    }
    if (timeout_callback) {
        try {
            timeout_callback();
        } catch (...) { /* Failure callback is a boundary. */
        }
    }
    std::array<std::function<void()>, 1024> executions;
    std::size_t planned = 0;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        RefillTokens();

        if (in_flight_) {
            return 0;
        }

        auto remaining = static_cast<std::size_t>(tokens_);
        if (remaining == 0) {
            return 0;
        }

        for (std::size_t p = 0; p < queues_.size() && remaining > 0; ++p) {
            while (!queues_[p].empty() && remaining > 0 && planned < 1) {
                auto& task = queues_[p].front();
                active_request_id_ = task.request_id;
                active_query_name_ = task.query_name;
                response_success_ = true;
                deadline_ = std::chrono::steady_clock::now() + task.timeout;
                on_timeout_ = std::move(task.on_timeout);
                executions[planned++] = std::move(task.execute);
                queues_[p].pop();
                --remaining;
                tokens_ -= 1.0;
                in_flight_ = true;
            }
        }
    }

    for (std::size_t i = 0; i < planned; ++i) {
        try {
            executions[i]();
        } catch (...) {
            std::function<void()> failure;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                in_flight_ = false;
                failure = std::move(on_timeout_);
            }
            if (failure) {
                try {
                    failure();
                } catch (...) { /* Never escape the polling thread. */
                }
            }
        }
    }
    return planned;
}

bool QueryScheduler::MarkComplete(int request_id, std::uint64_t generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!in_flight_ || request_id != active_request_id_ || generation != generation_) {
        return false;
    }
    in_flight_ = false;
    on_timeout_ = nullptr;
    return true;
}

bool QueryScheduler::IsActive(int request_id, std::uint64_t generation) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return in_flight_ && request_id == active_request_id_ && generation == generation_;
}

std::string QueryScheduler::ActiveQueryName(int request_id, std::uint64_t generation) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return in_flight_ && request_id == active_request_id_ && generation == generation_
               ? active_query_name_
               : std::string{};
}

bool QueryScheduler::RecordResponse(int request_id, std::uint64_t generation, bool success) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!in_flight_ || request_id != active_request_id_ || generation != generation_) {
        return false;
    }
    response_success_ = response_success_ && success;
    return response_success_;
}

void QueryScheduler::Reset(std::uint64_t generation) {
    std::lock_guard<std::recursive_mutex> execution_lock(execution_mutex_);
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& queue : queues_) {
        queue = Queue{};
    }
    generation_ = generation;
    request_ids_.clear();
    in_flight_ = false;
    on_timeout_ = nullptr;
}

std::size_t QueryScheduler::PendingCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t total = 0;
    for (const auto& queue : queues_) {
        total += queue.size();
    }
    return total;
}

void QueryScheduler::SetRateLimit(std::size_t max_qps) {
    std::lock_guard<std::mutex> lock(mutex_);
    max_qps_ = std::max<std::size_t>(1, max_qps);
    tokens_ = std::min(tokens_, static_cast<double>(max_qps_));
}

void QueryScheduler::RefillTokens() {
    const auto now = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_refill_);
    if (elapsed.count() <= 0) {
        return;
    }

    const double refill =
        static_cast<double>(elapsed.count()) * static_cast<double>(max_qps_) / 1000.0;
    tokens_ = std::min(static_cast<double>(max_qps_), tokens_ + refill);
    last_refill_ = now;
}

}  // namespace quant_hft
