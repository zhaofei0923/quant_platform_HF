#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

namespace quant_hft {

class PythonCallbackDispatcher {
public:
    using Task = std::function<void()>;

    struct Stats {
        std::size_t pending{0};
        std::size_t dropped{0};
        std::size_t critical_timeout{0};
        std::size_t critical_delay_exceeded{0};
        std::size_t max_pending{0};
        std::size_t max_queue_size{0};
        std::int64_t last_critical_queue_delay_ms{0};
    };

    explicit PythonCallbackDispatcher(std::size_t max_queue_size = 5000,
                                      std::int64_t critical_wait_ms = 10,
                                      std::int64_t critical_delay_alert_ms = 100);
    ~PythonCallbackDispatcher();

    PythonCallbackDispatcher(const PythonCallbackDispatcher&) = delete;
    PythonCallbackDispatcher& operator=(const PythonCallbackDispatcher&) = delete;

    void Start();
    void Stop();
    bool Post(Task task, bool is_critical = false);
    Stats GetStats() const;

private:
    void WorkerLoop();

    struct QueuedTask {
        Task task;
        std::int64_t enqueue_ts_ns{0};
        bool is_critical{false};
    };

    const std::size_t max_queue_size_;
    const std::int64_t critical_wait_ms_;
    const std::int64_t critical_delay_alert_ms_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable space_cv_;
    std::deque<QueuedTask> queue_;
    std::thread worker_;
    bool running_{false};
    bool stop_{false};
    std::atomic<std::size_t> pending_{0};
    std::atomic<std::size_t> dropped_{0};
    std::atomic<std::size_t> critical_timeout_{0};
    std::atomic<std::size_t> critical_delay_exceeded_{0};
    std::atomic<std::size_t> max_pending_{0};
    std::atomic<std::int64_t> last_critical_queue_delay_ms_{0};
};

}  // namespace quant_hft
