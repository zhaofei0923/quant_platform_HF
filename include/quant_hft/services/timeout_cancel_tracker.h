#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct TimeoutCancelTrackerConfig {
    int max_attempts{3};
    EpochNanos retry_base_ns{1'000'000'000};
    EpochNanos retry_max_ns{5'000'000'000};
};

enum class TimeoutCancelAction {
    kNone,
    kRequestCancel,
    kSuppressAndReconcile,
};

struct TimeoutCancelDecision {
    TimeoutCancelAction action{TimeoutCancelAction::kNone};
    std::int32_t attempts{0};
    std::string reason;
    bool retry_scheduled{false};
    EpochNanos next_retry_ts_ns{0};
};

class TimeoutCancelTracker {
   public:
    TimeoutCancelDecision OnOrderTimedOut(const std::string& client_order_id, EpochNanos now_ns,
                                          const TimeoutCancelTrackerConfig& config);
    TimeoutCancelDecision OnCancelRequestCompleted(const std::string& client_order_id, bool success,
                                                   EpochNanos now_ns,
                                                   const TimeoutCancelTrackerConfig& config,
                                                   const std::string& reason = "");
    TimeoutCancelDecision OnOrderEvent(const OrderEvent& event, EpochNanos now_ns,
                                       const TimeoutCancelTrackerConfig& config);

    void ClearTerminalOrder(const std::string& client_order_id);
    bool IsSuppressed(const std::string& client_order_id) const;
    std::int32_t Attempts(const std::string& client_order_id) const;
    std::size_t TrackedCount() const;

    static bool IsCancelActionFeedback(const OrderEvent& event);
    static bool IsCancelTerminalUnknown(const OrderEvent& event);

   private:
    struct State {
        std::int32_t attempts{0};
        bool awaiting_feedback{false};
        bool suppressed{false};
        bool reconcile_requested{false};
        EpochNanos next_retry_ts_ns{0};
        EpochNanos last_request_ts_ns{0};
        EpochNanos last_update_ts_ns{0};
        std::string reason;
    };

    TimeoutCancelDecision SuppressAndMaybeReconcileLocked(State* state, const std::string& reason,
                                                          EpochNanos now_ns);
    static bool IsTerminalOrderEvent(const OrderEvent& event);
    static int EffectiveMaxAttempts(const TimeoutCancelTrackerConfig& config);
    static EpochNanos RetryDelayForAttempt(std::int32_t attempts,
                                           const TimeoutCancelTrackerConfig& config);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, State> states_;
};

}  // namespace quant_hft
