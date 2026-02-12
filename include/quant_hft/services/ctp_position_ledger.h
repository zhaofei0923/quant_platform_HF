#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct CtpPositionView {
    std::string account_id;
    std::string instrument_id;
    PositionDirection direction{PositionDirection::kLong};
    std::string position_date;
    std::int32_t position{0};
    std::int32_t frozen{0};
    std::int32_t closable{0};
    EpochNanos last_update_ts_ns{0};
};

struct CtpOrderIntentForLedger {
    std::string client_order_id;
    std::string account_id;
    std::string instrument_id;
    PositionDirection direction{PositionDirection::kLong};
    OffsetFlag offset{OffsetFlag::kOpen};
    std::int32_t requested_volume{0};
    // Optional explicit bucket for close orders. Empty means inferred from offset.
    std::string position_date;
};

class CtpPositionLedger {
public:
    bool ApplyInvestorPositionSnapshot(const InvestorPositionSnapshot& snapshot, std::string* error);
    bool RegisterOrderIntent(const CtpOrderIntentForLedger& intent, std::string* error);
    bool ApplyOrderEvent(const OrderEvent& event, std::string* error);

    CtpPositionView GetPosition(const std::string& account_id,
                                const std::string& instrument_id,
                                PositionDirection direction,
                                const std::string& position_date) const;
    std::int32_t GetClosableVolume(const std::string& account_id,
                                   const std::string& instrument_id,
                                   PositionDirection direction,
                                   const std::string& position_date) const;

private:
    struct PositionKey {
        std::string account_id;
        std::string instrument_id;
        PositionDirection direction{PositionDirection::kLong};
        std::string position_date;

        bool operator==(const PositionKey& rhs) const {
            return account_id == rhs.account_id && instrument_id == rhs.instrument_id &&
                   direction == rhs.direction && position_date == rhs.position_date;
        }
    };

    struct PositionKeyHasher {
        std::size_t operator()(const PositionKey& key) const;
    };

    struct PositionBucket {
        std::int32_t position{0};
        std::int32_t frozen{0};
        EpochNanos last_update_ts_ns{0};
    };

    struct PendingOrderState {
        CtpOrderIntentForLedger intent;
        std::string position_date;
        std::int32_t frozen_volume{0};
        std::int32_t last_filled_volume{0};
    };

    static bool IsCloseOffset(OffsetFlag offset);
    static bool IsTerminalStatus(OrderStatus status);
    static std::string NormalizePositionDate(const std::string& raw);
    static std::string ResolvePositionDateForIntent(const CtpOrderIntentForLedger& intent);
    static PositionDirection ParsePositionDirection(const std::string& raw);
    static PositionKey MakeKey(const std::string& account_id,
                               const std::string& instrument_id,
                               PositionDirection direction,
                               const std::string& position_date);
    static std::int32_t ClampNonNegative(std::int32_t value);

    mutable std::mutex mutex_;
    std::unordered_map<PositionKey, PositionBucket, PositionKeyHasher> positions_;
    std::unordered_map<std::string, PendingOrderState> pending_orders_;
};

}  // namespace quant_hft
