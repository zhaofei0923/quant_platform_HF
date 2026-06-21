#include "quant_hft/services/timeout_cancel_tracker.h"

#include <algorithm>

namespace quant_hft {
namespace {

bool Contains(const std::string& text, const std::string& needle) {
    return !needle.empty() && text.find(needle) != std::string::npos;
}

bool ContainsAnyTerminalUnknownMarker(const std::string& text) {
    return Contains(text, "ErrorID=26") || Contains(text, "ErrorID = 26") ||
           Contains(text, "already filled") || Contains(text, "already canceled") ||
           Contains(text, "cannot cancel");
}

}  // namespace

TimeoutCancelDecision TimeoutCancelTracker::OnOrderTimedOut(
    const std::string& client_order_id, EpochNanos now_ns,
    const TimeoutCancelTrackerConfig& config) {
    if (client_order_id.empty()) {
        return {};
    }

    std::lock_guard<std::mutex> lock(mutex_);
    State& state = states_[client_order_id];
    if (state.suppressed || state.awaiting_feedback) {
        return TimeoutCancelDecision{TimeoutCancelAction::kNone, state.attempts, state.reason,
                                     false, state.next_retry_ts_ns};
    }
    if (state.next_retry_ts_ns > 0 && now_ns < state.next_retry_ts_ns) {
        return TimeoutCancelDecision{TimeoutCancelAction::kNone, state.attempts,
                                     "retry_cooling_down", false, state.next_retry_ts_ns};
    }
    if (state.attempts >= EffectiveMaxAttempts(config)) {
        return SuppressAndMaybeReconcileLocked(&state, "cancel_attempts_exhausted", now_ns);
    }

    ++state.attempts;
    state.awaiting_feedback = true;
    state.last_request_ts_ns = now_ns;
    state.last_update_ts_ns = now_ns;
    state.reason = "timeout";
    return TimeoutCancelDecision{TimeoutCancelAction::kRequestCancel, state.attempts, state.reason,
                                 false, 0};
}

TimeoutCancelDecision TimeoutCancelTracker::OnCancelRequestCompleted(
    const std::string& client_order_id, bool success, EpochNanos now_ns,
    const TimeoutCancelTrackerConfig& config, const std::string& reason) {
    if (client_order_id.empty()) {
        return {};
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = states_.find(client_order_id);
    if (it == states_.end()) {
        return {};
    }

    State& state = it->second;
    if (success) {
        states_.erase(it);
        return {};
    }
    if (state.suppressed) {
        return TimeoutCancelDecision{TimeoutCancelAction::kNone, state.attempts, state.reason,
                                     false, state.next_retry_ts_ns};
    }

    state.awaiting_feedback = false;
    state.last_update_ts_ns = now_ns;
    if (!reason.empty()) {
        state.reason = reason;
    } else if (state.reason.empty()) {
        state.reason = "cancel_request_failed";
    }

    if (state.attempts >= EffectiveMaxAttempts(config)) {
        return SuppressAndMaybeReconcileLocked(&state, "cancel_attempts_exhausted", now_ns);
    }

    state.next_retry_ts_ns = now_ns + RetryDelayForAttempt(state.attempts, config);
    return TimeoutCancelDecision{TimeoutCancelAction::kNone, state.attempts, state.reason, true,
                                 state.next_retry_ts_ns};
}

TimeoutCancelDecision TimeoutCancelTracker::OnOrderEvent(const OrderEvent& event, EpochNanos now_ns,
                                                         const TimeoutCancelTrackerConfig& config) {
    if (event.client_order_id.empty()) {
        return {};
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (IsTerminalOrderEvent(event)) {
        states_.erase(event.client_order_id);
        return {};
    }
    if (!IsCancelActionFeedback(event)) {
        return {};
    }

    State& state = states_[event.client_order_id];
    state.last_update_ts_ns = now_ns;
    if (!event.reason.empty()) {
        state.reason = event.reason;
    } else if (!event.status_msg.empty()) {
        state.reason = event.status_msg;
    }

    if (IsCancelTerminalUnknown(event)) {
        return SuppressAndMaybeReconcileLocked(&state, "cancel_terminal_unknown", now_ns);
    }

    if (event.status == OrderStatus::kAccepted) {
        state.awaiting_feedback = true;
        return {};
    }
    if (event.status == OrderStatus::kRejected) {
        state.awaiting_feedback = false;
        if (state.attempts >= EffectiveMaxAttempts(config)) {
            return SuppressAndMaybeReconcileLocked(&state, "cancel_attempts_exhausted", now_ns);
        }
        state.next_retry_ts_ns = now_ns + RetryDelayForAttempt(state.attempts, config);
    }
    return {};
}

void TimeoutCancelTracker::ClearTerminalOrder(const std::string& client_order_id) {
    if (client_order_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    states_.erase(client_order_id);
}

bool TimeoutCancelTracker::IsSuppressed(const std::string& client_order_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = states_.find(client_order_id);
    return it != states_.end() && it->second.suppressed;
}

std::int32_t TimeoutCancelTracker::Attempts(const std::string& client_order_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = states_.find(client_order_id);
    return it == states_.end() ? 0 : it->second.attempts;
}

std::size_t TimeoutCancelTracker::TrackedCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return states_.size();
}

bool TimeoutCancelTracker::IsCancelActionFeedback(const OrderEvent& event) {
    return event.event_source == "OnRspOrderAction" || event.event_source == "OnErrRtnOrderAction";
}

bool TimeoutCancelTracker::IsCancelTerminalUnknown(const OrderEvent& event) {
    if (!IsCancelActionFeedback(event) || event.status != OrderStatus::kRejected) {
        return false;
    }
    return ContainsAnyTerminalUnknownMarker(event.reason) ||
           ContainsAnyTerminalUnknownMarker(event.status_msg);
}

TimeoutCancelDecision TimeoutCancelTracker::SuppressAndMaybeReconcileLocked(
    State* state, const std::string& reason, EpochNanos now_ns) {
    if (state == nullptr) {
        return {};
    }
    state->awaiting_feedback = false;
    state->suppressed = true;
    state->last_update_ts_ns = now_ns;
    state->reason = reason;
    if (state->reconcile_requested) {
        return TimeoutCancelDecision{TimeoutCancelAction::kNone, state->attempts, reason, false,
                                     state->next_retry_ts_ns};
    }
    state->reconcile_requested = true;
    return TimeoutCancelDecision{TimeoutCancelAction::kSuppressAndReconcile, state->attempts,
                                 reason, false, 0};
}

bool TimeoutCancelTracker::IsTerminalOrderEvent(const OrderEvent& event) {
    if (IsCancelActionFeedback(event)) {
        return false;
    }
    return event.status == OrderStatus::kFilled || event.status == OrderStatus::kCanceled ||
           event.status == OrderStatus::kRejected;
}

int TimeoutCancelTracker::EffectiveMaxAttempts(const TimeoutCancelTrackerConfig& config) {
    return std::max(1, config.max_attempts);
}

EpochNanos TimeoutCancelTracker::RetryDelayForAttempt(std::int32_t attempts,
                                                      const TimeoutCancelTrackerConfig& config) {
    const EpochNanos base = std::max<EpochNanos>(1, config.retry_base_ns);
    const EpochNanos max_delay = std::max(base, config.retry_max_ns);
    EpochNanos delay = base;
    for (std::int32_t i = 1; i < attempts; ++i) {
        if (delay >= max_delay / 2) {
            return max_delay;
        }
        delay *= 2;
    }
    return std::min(delay, max_delay);
}

}  // namespace quant_hft
