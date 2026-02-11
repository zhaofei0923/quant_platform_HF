#pragma once

#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class IRealtimeCache {
public:
    virtual ~IRealtimeCache() = default;

    virtual void UpsertMarketSnapshot(const MarketSnapshot& snapshot) = 0;
    virtual void UpsertOrderEvent(const OrderEvent& event) = 0;
    virtual void UpsertPositionSnapshot(const PositionSnapshot& position) = 0;
    virtual void UpsertStateSnapshot7D(const StateSnapshot7D& snapshot) = 0;

    virtual bool GetMarketSnapshot(const std::string& instrument_id,
                                   MarketSnapshot* out) const = 0;
    virtual bool GetOrderEvent(const std::string& client_order_id,
                               OrderEvent* out) const = 0;
    virtual bool GetPositionSnapshot(const std::string& account_id,
                                     const std::string& instrument_id,
                                     PositionDirection direction,
                                     PositionSnapshot* out) const = 0;
    virtual bool GetStateSnapshot7D(const std::string& instrument_id,
                                    StateSnapshot7D* out) const = 0;
};

}  // namespace quant_hft
