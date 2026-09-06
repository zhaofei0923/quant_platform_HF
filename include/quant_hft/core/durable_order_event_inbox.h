#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "quant_hft/interfaces/regulatory_sink.h"

namespace quant_hft {

// A bounded acceleration queue over the WAL. A full queue never deletes an accepted event:
// it latches replay_required and subsequent records remain in the WAL until explicit recovery.
class DurableOrderEventInbox {
   public:
    using Consumer = std::function<bool(const OrderEvent&, const WalReceipt&)>;
    struct Stats {
        std::size_t pending_events{0};
        std::size_t pending_barriers{0};
        std::uint64_t delivered{0};
        bool active{false};
        bool replay_required{false};
        bool persistence_failed{false};
        std::string error;
    };

    explicit DurableOrderEventInbox(std::size_t capacity = 5000,
                                    std::size_t barrier_capacity = 128);
    ~DurableOrderEventInbox();
    bool Configure(IRegulatorySink* sink, Consumer consumer, std::function<void()> on_failure = {});
    bool Start();
    void Stop();
    WalReceipt Accept(const OrderEvent& event);
    bool PostBarrier(std::function<void()> callback);
    bool Drain(std::int64_t timeout_ms);
    bool Recover(const std::string& wal_path, std::int64_t timeout_ms = 5000);
    bool Healthy() const;
    Stats GetStats() const;

   private:
    struct Event {
        OrderEvent event;
        WalReceipt receipt;
    };
    struct Barrier {
        std::optional<WalReceipt> after;
        std::function<void()> callback;
    };
    bool BarrierReadyLocked() const;
    bool RunReadyRecoveryBarriers();
    void WorkerLoop();
    void NotifyFailure() const noexcept;

    const std::size_t capacity_;
    const std::size_t barrier_capacity_;
    mutable std::mutex ingress_mutex_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::thread worker_;
    IRegulatorySink* sink_{nullptr};
    Consumer consumer_;
    std::function<void()> on_failure_;
    std::deque<Event> events_;
    std::deque<Barrier> barriers_;
    std::optional<WalReceipt> last_received_;
    std::optional<WalReceipt> last_delivered_;
    std::uint64_t delivered_{0};
    bool running_{false};
    bool stop_{false};
    bool paused_{false};
    bool active_{false};
    bool blocked_{false};
    bool replay_required_{false};
    bool persistence_failed_{false};
    std::string error_;
};

}  // namespace quant_hft
