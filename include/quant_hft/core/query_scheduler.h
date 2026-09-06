#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <unordered_set>

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
        std::uint64_t generation{0};
        std::chrono::milliseconds timeout{30'000};
        std::function<void()> on_timeout;
        std::string query_name;
    };

    explicit QueryScheduler(std::size_t max_qps = 10);

    bool TrySchedule(QueryTask task);
    std::size_t DrainOnce();
    bool MarkComplete(int request_id, std::uint64_t generation);
    bool IsActive(int request_id, std::uint64_t generation) const;
    std::string ActiveQueryName(int request_id, std::uint64_t generation) const;
    bool RecordResponse(int request_id, std::uint64_t generation, bool success);
    void Reset(std::uint64_t generation);
    std::size_t PendingCount() const;
    void SetRateLimit(std::size_t max_qps);

   private:
    using Queue = std::queue<QueryTask>;

    void RefillTokens();

    mutable std::mutex mutex_;
    // Serializes dispatch and reset without holding mutex_ across SDK/user code.
    std::recursive_mutex execution_mutex_;
    std::array<Queue, 3> queues_;
    std::size_t max_qps_{10};
    double tokens_{10.0};
    std::chrono::steady_clock::time_point last_refill_;
    bool in_flight_{false};
    int active_request_id_{0};
    std::string active_query_name_;
    std::uint64_t generation_{0};
    bool response_success_{true};
    std::chrono::steady_clock::time_point deadline_;
    std::function<void()> on_timeout_;
    std::unordered_set<int> request_ids_;
};

}  // namespace quant_hft
