#pragma once

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class IRiskEngine {
public:
    virtual ~IRiskEngine() = default;
    virtual RiskDecision PreCheck(const OrderIntent& intent) const = 0;
};

}  // namespace quant_hft
