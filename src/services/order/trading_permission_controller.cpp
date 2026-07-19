#include "quant_hft/services/trading_permission_controller.h"

#include <algorithm>
#include <sstream>

namespace quant_hft {
namespace {

constexpr const char* kRecoveryIncomplete = "recovery_incomplete";

}  // namespace

TradingPermissionController::TradingPermissionController(EpochNanos open_reenable_stability_ns)
    : open_reenable_stability_ns_(std::max<EpochNanos>(0, open_reenable_stability_ns)) {
    reasons_.emplace(kRecoveryIncomplete, TradingPermissionMode::kBlocked);
}

void TradingPermissionController::BeginRecovery(std::uint64_t generation, EpochNanos now_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    recovery_generation_ = generation;
    recovery_complete_ = false;
    stability_started_ts_ns_ = 0;
    reasons_[kRecoveryIncomplete] = TradingPermissionMode::kBlocked;
    (void)now_ns;
}

bool TradingPermissionController::MarkRecoveryComplete(std::uint64_t generation, EpochNanos now_ns,
                                                       std::string* error) {
    const auto effective_now = ResolveNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);
    if (generation != recovery_generation_) {
        if (error != nullptr) {
            *error = "recovery generation mismatch";
        }
        return false;
    }
    recovery_complete_ = true;
    reasons_.erase(kRecoveryIncomplete);
    MaybeStartStabilityLocked(effective_now);
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

bool TradingPermissionController::SetBlocked(const std::string& reason, EpochNanos now_ns,
                                             std::string* error) {
    return SetReason(reason, TradingPermissionMode::kBlocked, ResolveNow(now_ns), error);
}

bool TradingPermissionController::SetCloseOnly(const std::string& reason, EpochNanos now_ns,
                                               std::string* error) {
    return SetReason(reason, TradingPermissionMode::kCloseOnly, ResolveNow(now_ns), error);
}

bool TradingPermissionController::ClearReason(const std::string& reason, EpochNanos now_ns) {
    if (reason.empty() || reason == kRecoveryIncomplete) {
        return false;
    }
    const auto effective_now = ResolveNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);
    const auto erased = reasons_.erase(reason);
    if (erased != 0) {
        MaybeStartStabilityLocked(effective_now);
    }
    return erased != 0;
}

TradingPermissionSnapshot TradingPermissionController::GetSnapshot(EpochNanos now_ns) const {
    const auto effective_now = ResolveNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);
    return BuildSnapshotLocked(effective_now);
}

bool TradingPermissionController::CanSubmit(OffsetFlag offset, EpochNanos now_ns,
                                            std::string* rejection_reason) const {
    const auto snapshot = GetSnapshot(now_ns);
    const bool allowed =
        snapshot.mode == TradingPermissionMode::kReady ||
        (snapshot.mode == TradingPermissionMode::kCloseOnly && IsCloseOffset(offset));
    if (rejection_reason != nullptr) {
        rejection_reason->clear();
        if (!allowed) {
            std::ostringstream message;
            message << "trading permission is " << ModeName(snapshot.mode);
            if (!snapshot.reasons.empty()) {
                message << ": ";
                for (std::size_t i = 0; i < snapshot.reasons.size(); ++i) {
                    if (i != 0) {
                        message << ',';
                    }
                    message << snapshot.reasons[i];
                }
            }
            *rejection_reason = message.str();
        }
    }
    return allowed;
}

bool TradingPermissionController::CanSubmit(SignalType signal_type, EpochNanos now_ns,
                                            std::string* rejection_reason) const {
    return CanSubmit(signal_type == SignalType::kOpen ? OffsetFlag::kOpen : OffsetFlag::kClose,
                     now_ns, rejection_reason);
}

bool TradingPermissionController::CanCancel(EpochNanos now_ns,
                                            std::string* rejection_reason) const {
    const auto snapshot = GetSnapshot(now_ns);
    const bool allowed = snapshot.mode != TradingPermissionMode::kBlocked;
    if (rejection_reason != nullptr) {
        rejection_reason->clear();
        if (!allowed) {
            *rejection_reason = "trading permission is Blocked";
        }
    }
    return allowed;
}

const char* TradingPermissionController::ModeName(TradingPermissionMode mode) {
    switch (mode) {
        case TradingPermissionMode::kReady:
            return "Ready";
        case TradingPermissionMode::kCloseOnly:
            return "CloseOnly";
        case TradingPermissionMode::kBlocked:
        default:
            return "Blocked";
    }
}

EpochNanos TradingPermissionController::ResolveNow(EpochNanos now_ns) {
    return now_ns > 0 ? now_ns : NowEpochNanos();
}

bool TradingPermissionController::IsCloseOffset(OffsetFlag offset) {
    return offset == OffsetFlag::kClose || offset == OffsetFlag::kCloseToday ||
           offset == OffsetFlag::kCloseYesterday;
}

bool TradingPermissionController::SetReason(const std::string& reason,
                                            TradingPermissionMode severity, EpochNanos now_ns,
                                            std::string* error) {
    if (reason.empty() || reason == kRecoveryIncomplete ||
        severity == TradingPermissionMode::kReady) {
        if (error != nullptr) {
            *error = "a non-reserved reason and fail-closed severity are required";
        }
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = reasons_.find(reason);
    if (it == reasons_.end() || it->second != severity) {
        reasons_[reason] = severity;
        stability_started_ts_ns_ = 0;
    }
    (void)now_ns;
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

TradingPermissionSnapshot TradingPermissionController::BuildSnapshotLocked(
    EpochNanos now_ns) const {
    TradingPermissionSnapshot snapshot;
    snapshot.recovery_generation = recovery_generation_;
    snapshot.recovery_complete = recovery_complete_;
    snapshot.stability_started_ts_ns = stability_started_ts_ns_;

    bool has_blocked = !recovery_complete_;
    bool has_close_only = false;
    for (const auto& [reason, severity] : reasons_) {
        snapshot.reasons.push_back(reason);
        has_blocked = has_blocked || severity == TradingPermissionMode::kBlocked;
        has_close_only = has_close_only || severity == TradingPermissionMode::kCloseOnly;
    }

    if (has_blocked) {
        snapshot.mode = TradingPermissionMode::kBlocked;
        return snapshot;
    }
    if (has_close_only || stability_started_ts_ns_ <= 0) {
        snapshot.mode = TradingPermissionMode::kCloseOnly;
        return snapshot;
    }

    snapshot.next_ready_ts_ns = stability_started_ts_ns_ + open_reenable_stability_ns_;
    if (now_ns < snapshot.next_ready_ts_ns) {
        snapshot.mode = TradingPermissionMode::kCloseOnly;
        snapshot.reasons.emplace_back("recovery_stability_window");
        return snapshot;
    }
    snapshot.mode = TradingPermissionMode::kReady;
    return snapshot;
}

void TradingPermissionController::MaybeStartStabilityLocked(EpochNanos now_ns) {
    if (!recovery_complete_ || !reasons_.empty()) {
        stability_started_ts_ns_ = 0;
        return;
    }
    if (stability_started_ts_ns_ == 0) {
        stability_started_ts_ns_ = now_ns;
    }
}

}  // namespace quant_hft
