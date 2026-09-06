#include "quant_hft/runtime/account_execution_scheduler.h"

#include <algorithm>
#include <exception>
#include <utility>

namespace quant_hft {

AccountExecutionScheduler::AccountExecutionScheduler(std::size_t capacity, std::size_t worker_count)
    : capacity_(std::max<std::size_t>(1, capacity)),
      worker_count_(std::max<std::size_t>(1, worker_count)) {}

AccountExecutionScheduler::~AccountExecutionScheduler() { Stop(); }

bool AccountExecutionScheduler::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_ || !workers_.empty()) return false;
    running_ = true;
    stopping_ = false;
    for (std::size_t i = 0; i < worker_count_; ++i) {
        workers_.emplace_back(&AccountExecutionScheduler::WorkerLoop, this);
    }
    return true;
}

AccountExecutionScheduler::SubmitResult AccountExecutionScheduler::SubmitAt(
    const std::string& account_id, Clock::time_point deadline, Task task) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (account_id.empty() || !task) return {SubmitStatus::kInvalid, 0};
    if (!running_ || stopping_) return {SubmitStatus::kStopped, 0};
    if (pending_.size() >= capacity_) {
        ++stats_.rejected;
        return {SubmitStatus::kFull, 0};
    }
    const TaskId id = next_id_++;
    pending_.emplace(std::make_pair(deadline, id), Command{id, account_id, std::move(task)});
    cv_.notify_all();
    return {SubmitStatus::kAccepted, id};
}

AccountExecutionScheduler::SubmitResult AccountExecutionScheduler::Submit(
    const std::string& account_id, Task task) {
    return SubmitAt(account_id, Clock::now(), std::move(task));
}

bool AccountExecutionScheduler::Cancel(TaskId id) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = pending_.begin(); it != pending_.end(); ++it) {
        if (it->second.id != id) continue;
        pending_.erase(it);
        ++stats_.canceled;
        cv_.notify_all();
        return true;
    }
    return false;
}

std::size_t AccountExecutionScheduler::CancelAccount(const std::string& account_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t canceled = 0;
    for (auto it = pending_.begin(); it != pending_.end();) {
        if (it->second.account_id == account_id) {
            it = pending_.erase(it);
            ++canceled;
        } else {
            ++it;
        }
    }
    stats_.canceled += canceled;
    cv_.notify_all();
    return canceled;
}

bool AccountExecutionScheduler::Drain(std::int64_t timeout_ms) {
    return DrainUntil(Clock::now() +
                      std::chrono::milliseconds(std::max<std::int64_t>(0, timeout_ms)));
}

bool AccountExecutionScheduler::DrainUntil(Clock::time_point deadline) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_until(lock, deadline,
                          [&] { return pending_.empty() && active_accounts_.empty(); });
}

void AccountExecutionScheduler::Stop(StopMode mode) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ && workers_.empty()) return;
        stopping_ = true;
        if (mode == StopMode::kCancelPending) {
            stats_.canceled += pending_.size();
            pending_.clear();
        }
    }
    cv_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
    workers_.clear();
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = false;
    cv_.notify_all();
}

AccountExecutionScheduler::Stats AccountExecutionScheduler::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto result = stats_;
    result.pending = pending_.size();
    result.active = active_accounts_.size();
    return result;
}

void AccountExecutionScheduler::WorkerLoop() {
    for (;;) {
        Command command;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            for (;;) {
                if (stopping_ && pending_.empty()) return;
                auto candidate = pending_.end();
                for (auto it = pending_.begin(); it != pending_.end(); ++it) {
                    if (active_accounts_.count(it->second.account_id) == 0) {
                        candidate = it;
                        break;
                    }
                }
                if (candidate == pending_.end()) {
                    cv_.wait(lock);
                } else if (candidate->first.first > Clock::now()) {
                    const auto deadline = candidate->first.first;
                    cv_.wait_until(lock, deadline);
                } else {
                    command = std::move(candidate->second);
                    pending_.erase(candidate);
                    active_accounts_.insert(command.account_id);
                    break;
                }
            }
        }
        std::string error;
        try {
            command.task();
        } catch (const std::exception& ex) {
            error = ex.what();
        } catch (...) {
            error = "unknown execution task exception";
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            active_accounts_.erase(command.account_id);
            ++stats_.completed;
            if (!error.empty()) {
                ++stats_.failed;
                stats_.last_error = std::move(error);
            }
        }
        cv_.notify_all();
    }
}

}  // namespace quant_hft
