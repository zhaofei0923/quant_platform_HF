#include "quant_hft/core/query_scheduler.h"

#include <algorithm>
#include <utility>

namespace quant_hft {

QueryScheduler::QueryScheduler(std::size_t max_qps)
    : max_qps_(max_qps),
      tokens_(static_cast<double>(max_qps)),
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
    task.created_at = std::chrono::steady_clock::now();
    queues_[idx].push(std::move(task));
    return true;
}

std::size_t QueryScheduler::DrainOnce() {
    std::array<std::function<void()>, 1024> executions;
    std::size_t planned = 0;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        RefillTokens();

        auto remaining = static_cast<std::size_t>(tokens_);
        if (remaining == 0) {
            return 0;
        }

        for (std::size_t p = 0; p < queues_.size() && remaining > 0; ++p) {
            while (!queues_[p].empty() && remaining > 0 && planned < executions.size()) {
                executions[planned++] = std::move(queues_[p].front().execute);
                queues_[p].pop();
                --remaining;
                tokens_ -= 1.0;
            }
        }
    }

    for (std::size_t i = 0; i < planned; ++i) {
        executions[i]();
    }
    return planned;
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

    const double refill = static_cast<double>(elapsed.count()) *
                          static_cast<double>(max_qps_) / 1000.0;
    tokens_ = std::min(static_cast<double>(max_qps_), tokens_ + refill);
    last_refill_ = now;
}

}  // namespace quant_hft
