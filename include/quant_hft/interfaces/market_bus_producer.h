#pragma once

#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class IMarketBusProducer {
public:
    virtual ~IMarketBusProducer() = default;

    virtual bool PublishMarketSnapshot(const MarketSnapshot& snapshot, std::string* error) = 0;
    virtual bool Flush(std::string* error) = 0;
};

}  // namespace quant_hft
