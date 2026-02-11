#pragma once

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class IPortfolioLedger {
public:
    virtual ~IPortfolioLedger() = default;
    virtual void OnOrderEvent(const OrderEvent& event) = 0;
    virtual PositionSnapshot GetPositionSnapshot(const std::string& account_id,
                                                 const std::string& instrument_id,
                                                 PositionDirection direction) const = 0;
};

}  // namespace quant_hft
