#pragma once

#include <cstdint>
#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct RiskContext {
    std::string account_id;
    std::string instrument_id;
    double account_position_notional{0.0};
    std::int32_t active_order_count{0};
    // Optional override of session clock in hhmm format.
    // When <= 0, risk engines should derive time from intent.ts_ns.
    int session_hhmm{-1};
};

class IRiskEngine {
public:
    virtual ~IRiskEngine() = default;
    virtual RiskDecision PreCheck(const OrderIntent& intent,
                                  const RiskContext& context) const = 0;
};

}  // namespace quant_hft
