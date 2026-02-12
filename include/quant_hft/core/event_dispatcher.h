#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "quant_hft/core/event_types.h"

namespace quant_hft {

class EventDispatcher {
public:
    using Task = std::function<void()>;

    struct Stats {
        std::size_t pending_high{0};
        std::size_t pending_normal{0};
        std::size_t pending_low{0};
        std::size_t processed_total{0};
        std::size_t worker_threads{0};
    };

    explicit EventDispatcher(std::size_t worker_threads = 1);
    ~EventDispatcher();

    EventDispatcher(const EventDispatcher&) = delete;
    EventDispatcher& operator=(const EventDispatcher&) = delete;

    void Start();
    void Stop();
    bool Post(Task task, EventPriority priority = EventPriority::kNormal);
    Stats Snapshot() const;
    bool WaitUntilDrained(std::int64_t timeout_ms);

private:
    void WorkerLoop();
    std::size_t PendingCountLocked() const;

    const std::size_t worker_threads_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable drained_cv_;
    std::array<std::deque<Task>, 3> queues_;
    std::vector<std::thread> workers_;
    std::atomic<std::size_t> processed_total_{0};
    bool started_{false};
    bool stop_{false};
};

}  // namespace quant_hft
