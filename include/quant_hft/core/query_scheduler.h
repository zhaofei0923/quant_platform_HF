#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>

namespace quant_hft {

class QueryScheduler {
public:
    enum class Priority {
        kHigh = 0,
        kNormal = 1,
        kLow = 2,
    };

    struct QueryTask {
        int request_id{0};
        Priority priority{Priority::kNormal};
        std::function<void()> execute;
        std::chrono::steady_clock::time_point created_at;
    };

    explicit QueryScheduler(std::size_t max_qps = 10);

    bool TrySchedule(QueryTask task);
    std::size_t DrainOnce();
    std::size_t PendingCount() const;
    void SetRateLimit(std::size_t max_qps);

private:
    using Queue = std::queue<QueryTask>;

    void RefillTokens();

    mutable std::mutex mutex_;
    std::array<Queue, 3> queues_;
    std::size_t max_qps_{10};
    double tokens_{10.0};
    std::chrono::steady_clock::time_point last_refill_;
};

}  // namespace quant_hft
