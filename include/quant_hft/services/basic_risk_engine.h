#pragma once

#include "quant_hft/interfaces/risk_engine.h"

namespace quant_hft {

struct BasicRiskLimits {
    std::int32_t max_order_volume{200};
    double max_order_notional{1'000'000.0};
};

class BasicRiskEngine : public IRiskEngine {
public:
    explicit BasicRiskEngine(BasicRiskLimits limits);

    RiskDecision PreCheck(const OrderIntent& intent) const override;

private:
    BasicRiskLimits limits_;
};

}  // namespace quant_hft
