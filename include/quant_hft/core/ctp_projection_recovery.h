#pragma once

#include <string>
#include <unordered_set>

namespace quant_hft {

// Host-side boundary between durable history and the current broker book. Call only
// after a successful domain commit, under the same lock as position reconciliation.
class CtpProjectionRecovery {
   public:
    bool ShouldProjectEvent(bool historical_replay, const std::string& trade_identity = {}) {
        if (historical_replay || !authoritative_snapshot_received_) {
            if (!trade_identity.empty()) snapshot_covered_trades_.insert(trade_identity);
            return false;
        }
        return trade_identity.empty() || snapshot_covered_trades_.count(trade_identity) == 0;
    }

    // The caller must first validate a successful, complete full-account query,
    // reconcile it with all committed trades, and atomically replace the broker book.
    void OnAuthoritativeSnapshotReconciled() { authoritative_snapshot_received_ = true; }

   private:
    bool authoritative_snapshot_received_{false};
    // These fills are already contained in the first authoritative snapshot. Keep
    // their identities so later query redelivery cannot apply their quantities twice.
    std::unordered_set<std::string> snapshot_covered_trades_;
};

}  // namespace quant_hft
