#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace quant_hft {

// Executes due commands without blocking the strategy thread. Commands for one
// account never overlap; deadlines (then submission IDs) order pending commands.
// Risk checks/reservations must run INSIDE the command, immediately before submit.
class AccountExecutionScheduler {
   public:
    using Clock = std::chrono::steady_clock;
    using TaskId = std::uint64_t;
    using Task = std::function<void()>;
    enum class SubmitStatus { kAccepted, kStopped, kFull, kInvalid };
    enum class StopMode { kCancelPending, kDrain };
    struct SubmitResult {
        SubmitStatus status{SubmitStatus::kStopped};
        TaskId id{0};
        explicit operator bool() const noexcept { return status == SubmitStatus::kAccepted; }
    };
    struct Stats {
        std::size_t pending{0};
        std::size_t active{0};
        std::uint64_t completed{0};
        std::uint64_t canceled{0};
        std::uint64_t rejected{0};
        std::uint64_t failed{0};
        std::string last_error;
    };

    explicit AccountExecutionScheduler(std::size_t capacity = 8192, std::size_t worker_count = 2);
    ~AccountExecutionScheduler();
    AccountExecutionScheduler(const AccountExecutionScheduler&) = delete;
    AccountExecutionScheduler& operator=(const AccountExecutionScheduler&) = delete;

    bool Start();
    SubmitResult SubmitAt(const std::string& account_id, Clock::time_point deadline, Task task);
    SubmitResult Submit(const std::string& account_id, Task task);
    bool Cancel(TaskId id);  // Only pending commands can be canceled.
    std::size_t CancelAccount(const std::string& account_id);
    bool Drain(std::int64_t timeout_ms);
    bool DrainUntil(Clock::time_point deadline);
    // Call from an owning/control thread. kDrain retains deadlines and may wait;
    // kCancelPending never starts a pending command after Stop begins.
    void Stop(StopMode mode = StopMode::kCancelPending);
    Stats GetStats() const;

   private:
    struct Command {
        TaskId id;
        std::string account_id;
        Task task;
    };
    void WorkerLoop();

    const std::size_t capacity_;
    const std::size_t worker_count_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::multimap<std::pair<Clock::time_point, TaskId>, Command> pending_;
    std::unordered_set<std::string> active_accounts_;
    std::vector<std::thread> workers_;
    TaskId next_id_{1};
    Stats stats_;
    bool running_{false};
    bool stopping_{false};
};

}  // namespace quant_hft
