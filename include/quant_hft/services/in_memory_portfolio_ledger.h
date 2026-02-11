#pragma once

#include <mutex>
#include <string>
#include <unordered_set>
#include <unordered_map>

#include "quant_hft/interfaces/portfolio_ledger.h"

namespace quant_hft {

class InMemoryPortfolioLedger : public IPortfolioLedger {
public:
    void OnOrderEvent(const OrderEvent& event) override;
    PositionSnapshot GetPositionSnapshot(const std::string& account_id,
                                         const std::string& instrument_id,
                                         PositionDirection direction) const override;

private:
    struct PositionKey {
        std::string account_id;
        std::string instrument_id;
        PositionDirection direction;

        bool operator==(const PositionKey& rhs) const {
            return account_id == rhs.account_id && instrument_id == rhs.instrument_id &&
                   direction == rhs.direction;
        }
    };

    struct PositionKeyHasher {
        std::size_t operator()(const PositionKey& key) const;
    };

    static std::string BuildEventKey(const OrderEvent& event);

    mutable std::mutex mutex_;
    std::unordered_map<PositionKey, PositionSnapshot, PositionKeyHasher> positions_;
    std::unordered_map<std::string, std::int32_t> order_last_filled_;
    std::unordered_set<std::string> applied_event_keys_;
};

}  // namespace quant_hft
