#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct TradingPermissionSnapshot {
    TradingPermissionMode mode{TradingPermissionMode::kBlocked};
    std::uint64_t recovery_generation{0};
    bool recovery_complete{false};
    EpochNanos stability_started_ts_ns{0};
    EpochNanos next_ready_ts_ns{0};
    std::vector<std::string> reasons;
};

// Owns the fail-closed trading admission state independently of transport
// connectivity.  Recovery completion starts a continuous stability window;
// adding any constraint resets that window.
class TradingPermissionController {
   public:
    explicit TradingPermissionController(EpochNanos open_reenable_stability_ns = 30'000'000'000LL);

    void BeginRecovery(std::uint64_t generation, EpochNanos now_ns = 0);
    bool MarkRecoveryComplete(std::uint64_t generation, EpochNanos now_ns = 0,
                              std::string* error = nullptr);

    bool SetBlocked(const std::string& reason, EpochNanos now_ns = 0, std::string* error = nullptr);
    bool SetCloseOnly(const std::string& reason, EpochNanos now_ns = 0,
                      std::string* error = nullptr);
    bool ClearReason(const std::string& reason, EpochNanos now_ns = 0);

    TradingPermissionSnapshot GetSnapshot(EpochNanos now_ns = 0) const;
    bool CanSubmit(OffsetFlag offset, EpochNanos now_ns = 0,
                   std::string* rejection_reason = nullptr) const;
    bool CanSubmit(SignalType signal_type, EpochNanos now_ns = 0,
                   std::string* rejection_reason = nullptr) const;
    bool CanCancel(EpochNanos now_ns = 0, std::string* rejection_reason = nullptr) const;

    static const char* ModeName(TradingPermissionMode mode);

   private:
    static EpochNanos ResolveNow(EpochNanos now_ns);
    static bool IsCloseOffset(OffsetFlag offset);
    bool SetReason(const std::string& reason, TradingPermissionMode severity, EpochNanos now_ns,
                   std::string* error);
    TradingPermissionSnapshot BuildSnapshotLocked(EpochNanos now_ns) const;
    void MaybeStartStabilityLocked(EpochNanos now_ns);

    const EpochNanos open_reenable_stability_ns_;
    mutable std::mutex mutex_;
    std::uint64_t recovery_generation_{0};
    bool recovery_complete_{false};
    EpochNanos stability_started_ts_ns_{0};
    std::map<std::string, TradingPermissionMode> reasons_;
};

}  // namespace quant_hft
