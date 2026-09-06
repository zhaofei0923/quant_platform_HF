#include "quant_hft/core/durable_order_event_inbox.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <utility>

#include "quant_hft/core/wal_replay_loader.h"

namespace quant_hft {

DurableOrderEventInbox::DurableOrderEventInbox(std::size_t capacity, std::size_t barrier_capacity)
    : capacity_(std::max<std::size_t>(1, capacity)),
      barrier_capacity_(std::max<std::size_t>(1, barrier_capacity)) {}

DurableOrderEventInbox::~DurableOrderEventInbox() { Stop(); }

bool DurableOrderEventInbox::Configure(IRegulatorySink* sink, Consumer consumer,
                                       std::function<void()> on_failure) {
    std::lock_guard<std::mutex> ingress(ingress_mutex_);
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_ || !events_.empty() || !barriers_.empty() || sink == nullptr || !consumer) {
        return false;
    }
    sink_ = sink;
    consumer_ = std::move(consumer);
    on_failure_ = std::move(on_failure);
    return true;
}

bool DurableOrderEventInbox::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) return true;
    if (sink_ == nullptr || !consumer_) return false;
    stop_ = false;
    running_ = true;
    worker_ = std::thread(&DurableOrderEventInbox::WorkerLoop, this);
    return true;
}

void DurableOrderEventInbox::Stop() {
    {
        std::lock_guard<std::mutex> ingress(ingress_mutex_);
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    if (worker_.joinable()) worker_.join();
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = false;
    consumer_ = nullptr;
    on_failure_ = nullptr;
    sink_ = nullptr;
}

WalReceipt DurableOrderEventInbox::Accept(const OrderEvent& event) {
    std::lock_guard<std::mutex> ingress(ingress_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ || stop_) {
            WalReceipt receipt;
            receipt.error = "durable inbox stopped";
            return receipt;
        }
    }
    WalReceipt receipt;
    try {
        receipt = sink_->CommitOrderEvent(event);
    } catch (const std::exception& error) {
        receipt.error = error.what();
    } catch (...) {
        receipt.error = "WAL commit threw an unknown exception";
    }
    bool failed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!receipt.durable) {
            persistence_failed_ = true;
            blocked_ = true;
            error_ = receipt.error;
            failed = true;
        } else {
            last_received_ = receipt;
            if (replay_required_ || blocked_ || events_.size() >= capacity_) {
                replay_required_ = true;
                error_ = "durable inbox requires ordered WAL replay";
                failed = true;
            } else {
                events_.push_back(Event{event, receipt});
            }
        }
    }
    cv_.notify_all();
    if (failed) NotifyFailure();
    return receipt;
}

bool DurableOrderEventInbox::PostBarrier(std::function<void()> callback) {
    if (!callback) return false;
    std::lock_guard<std::mutex> ingress(ingress_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ || stop_ || barriers_.size() >= barrier_capacity_) return false;
        barriers_.push_back(Barrier{last_received_, std::move(callback)});
    }
    cv_.notify_all();
    return true;
}

bool DurableOrderEventInbox::BarrierReadyLocked() const {
    if (barriers_.empty()) return false;
    const auto& fence = barriers_.front().after;
    return !fence || (last_delivered_ && last_delivered_->stream_id == fence->stream_id &&
                      last_delivered_->sequence >= fence->sequence);
}

bool DurableOrderEventInbox::Drain(std::int64_t timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::milliseconds(std::max<std::int64_t>(0, timeout_ms)),
                        [&] {
                            return !active_ && events_.empty() && barriers_.empty() &&
                                   !replay_required_ && !blocked_;
                        });
}

bool DurableOrderEventInbox::RunReadyRecoveryBarriers() {
    for (;;) {
        std::function<void()> callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!BarrierReadyLocked()) return true;
            callback = barriers_.front().callback;
        }
        try {
            callback();
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            error_ = "durable inbox barrier callback failed";
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        barriers_.pop_front();
    }
}

bool DurableOrderEventInbox::Recover(const std::string& wal_path, std::int64_t timeout_ms) {
    // Freeze ingress, then wait for the single consumer. A snapshot cannot race new delivery.
    std::lock_guard<std::mutex> ingress(ingress_mutex_);
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!running_ || stop_ || persistence_failed_) return false;
        paused_ = true;
        if (!cv_.wait_for(lock, std::chrono::milliseconds(std::max<std::int64_t>(0, timeout_ms)),
                          [&] { return !active_; })) {
            paused_ = false;
            error_ = "durable inbox consumer did not quiesce";
            cv_.notify_all();
            return false;
        }
    }
    const auto result =
        WalReplayLoader().VisitValidated(wal_path, [&](const WalReplayRecord& record) {
            if (!record.event || record.kind != "order" || !record.receipt.durable) return true;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (last_received_ && last_received_->stream_id != record.receipt.stream_id)
                    return false;
                if (last_delivered_ && last_delivered_->stream_id == record.receipt.stream_id &&
                    record.receipt.sequence <= last_delivered_->sequence)
                    return true;
            }
            if (!RunReadyRecoveryBarriers()) return false;
            bool applied = false;
            try {
                applied = consumer_(*record.event, record.receipt);
            } catch (...) {
            }
            if (!applied) return false;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                last_delivered_ = record.receipt;
                ++delivered_;
                while (!events_.empty() &&
                       events_.front().receipt.stream_id == record.receipt.stream_id &&
                       events_.front().receipt.sequence <= record.receipt.sequence) {
                    events_.pop_front();
                }
            }
            return RunReadyRecoveryBarriers();
        });
    bool recovered = result.completed && RunReadyRecoveryBarriers();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        recovered = recovered && events_.empty() && barriers_.empty() &&
                    (!last_received_ ||
                     (last_delivered_ && last_received_->stream_id == last_delivered_->stream_id &&
                      last_delivered_->sequence >= last_received_->sequence));
        paused_ = false;
        blocked_ = !recovered;
        replay_required_ = !recovered;
        error_ = recovered ? "" : (result.error.empty() ? "WAL recovery incomplete" : result.error);
    }
    cv_.notify_all();
    if (!recovered) NotifyFailure();
    return recovered;
}

bool DurableOrderEventInbox::Healthy() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return running_ && !stop_ && !paused_ && !blocked_ && !replay_required_ && !persistence_failed_;
}

DurableOrderEventInbox::Stats DurableOrderEventInbox::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return Stats{events_.size(),   barriers_.size(),    delivered_, active_,
                 replay_required_, persistence_failed_, error_};
}

void DurableOrderEventInbox::NotifyFailure() const noexcept {
    try {
        if (on_failure_) on_failure_();
    } catch (...) {
        std::fputs("durable inbox failure callback threw\n", stderr);
    }
}

void DurableOrderEventInbox::WorkerLoop() {
    for (;;) {
        std::optional<Event> event;
        std::function<void()> barrier;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&] {
                return stop_ ||
                       (!paused_ && !blocked_ && (BarrierReadyLocked() || !events_.empty()));
            });
            if (stop_) break;
            if (BarrierReadyLocked())
                barrier = barriers_.front().callback;
            else
                event = events_.front();
            active_ = true;
        }
        bool applied = false;
        try {
            if (event)
                applied = consumer_(event->event, event->receipt);
            else {
                barrier();
                applied = true;
            }
        } catch (...) {
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            active_ = false;
            if (applied) {
                if (event) {
                    last_delivered_ = event->receipt;
                    ++delivered_;
                    events_.pop_front();
                } else
                    barriers_.pop_front();
            } else {
                blocked_ = true;
                replay_required_ = true;
                error_ = "durable inbox consumer failed; later delivery blocked";
            }
        }
        cv_.notify_all();
        if (!applied) NotifyFailure();
    }
    cv_.notify_all();
}

}  // namespace quant_hft
