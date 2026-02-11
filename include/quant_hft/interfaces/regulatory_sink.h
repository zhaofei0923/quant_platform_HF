#pragma once

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class IRegulatorySink {
public:
    virtual ~IRegulatorySink() = default;
    virtual bool AppendOrderEvent(const OrderEvent& event) = 0;
    virtual bool AppendTradeEvent(const OrderEvent& event) = 0;
    virtual bool Flush() = 0;
};

}  // namespace quant_hft
