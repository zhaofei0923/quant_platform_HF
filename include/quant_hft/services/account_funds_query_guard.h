#pragma once

#include <cstdint>
#include <map>
#include <utility>

#include "quant_hft/contracts/query_result.h"
#include "quant_hft/contracts/wal_receipt.h"

namespace quant_hft {

// Used under the host's account-ledger mutex. A broker query may replace local
// provisional holds only when no submission/receive fact crossed its request window.
class AccountFundsQueryGuard {
   public:
    void Begin(int request_id, std::uint64_t generation, const WalReceipt& wal,
               std::uint64_t account_epoch) {
        // Timeouts cannot accumulate unbounded state. Discarding a frontier only
        // prevents reconciliation; ordinary conservative snapshot updates still work.
        if (pending_.size() >= 64) pending_.clear();
        pending_[{generation, request_id}] = Frontier{wal, account_epoch};
    }

    bool Consume(const QueryResultMetadata& result, const WalReceipt& wal,
                 std::uint64_t account_epoch, bool quiescent) {
        const auto it = pending_.find({result.generation, result.request_id});
        if (it == pending_.end()) return false;
        const auto expected = it->second;
        pending_.erase(it);
        return result.complete && result.success && result.full_account &&
               result.query_name == "trading_account" && quiescent &&
               expected.account_epoch == account_epoch && expected.wal.stream_id == wal.stream_id &&
               expected.wal.sequence == wal.sequence && expected.wal.checksum == wal.checksum &&
               expected.wal.durable == wal.durable;
    }

   private:
    struct Frontier {
        WalReceipt wal;
        std::uint64_t account_epoch{0};
    };
    std::map<std::pair<std::uint64_t, int>, Frontier> pending_;
};

}  // namespace quant_hft
