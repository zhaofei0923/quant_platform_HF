#pragma once

#include <functional>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class IMarketStateEngine {
public:
    using StateCallback = std::function<void(const StateSnapshot7D&)>;

    virtual ~IMarketStateEngine() = default;
    virtual void OnMarketSnapshot(const MarketSnapshot& snapshot) = 0;
    virtual StateSnapshot7D GetCurrentState(const std::string& instrument_id) const = 0;
    virtual void RegisterStateCallback(StateCallback callback) = 0;
};

}  // namespace quant_hft
