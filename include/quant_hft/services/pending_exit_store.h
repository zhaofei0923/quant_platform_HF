#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct PendingExitKey {
    std::string account_id;
    std::string strategy_id;
    std::string instrument_id;
    PositionDirection position_side{PositionDirection::kLong};

    bool operator==(const PendingExitKey& rhs) const;
    bool operator<(const PendingExitKey& rhs) const;
};

enum class PendingExitUpsertResult {
    kInserted,
    kPriorityRaised,
    kAlreadyPending,
    kFailed,
};

// A small append-only, fsync-backed intent store.  It records only close-like
// intent; price and volume are deliberately absent so recovery must query current
// broker position and market data before rebuilding an order.
class PendingExitStore {
   public:
    explicit PendingExitStore(std::string wal_path);

    bool Recover(std::string* error = nullptr);
    bool IsRecovered() const;

    PendingExitUpsertResult Upsert(const PendingExit& pending_exit, std::string* error = nullptr);
    // A pending exit can be tombstoned only after the caller supplies the latest
    // broker-authoritative zero position for the exact side.
    bool RemoveAfterBrokerFlat(const PendingExitKey& key, std::int32_t broker_position_volume,
                               EpochNanos completed_ts_ns, std::string* error = nullptr);

    std::optional<PendingExit> Get(const PendingExitKey& key) const;
    std::vector<PendingExit> List() const;
    std::size_t Size() const;

    static PendingExitKey MakeKey(const PendingExit& pending_exit);
    static int Priority(SignalType signal_type);

   private:
    bool AppendDurable(const std::string& record, std::string* error) const;

    const std::string wal_path_;
    mutable std::mutex mutex_;
    bool recovered_{false};
    std::map<PendingExitKey, PendingExit> pending_;
};

}  // namespace quant_hft
