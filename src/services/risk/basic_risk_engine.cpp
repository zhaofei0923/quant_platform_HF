#include "quant_hft/services/basic_risk_engine.h"

#include <cmath>

namespace quant_hft {

BasicRiskEngine::BasicRiskEngine(BasicRiskLimits limits) : limits_(limits) {}

RiskDecision BasicRiskEngine::PreCheck(const OrderIntent& intent) const {
    if (intent.volume <= 0) {
        return RiskDecision{RiskAction::kReject, "basic.non_positive_volume", "volume must be positive"};
    }
    if (intent.volume > limits_.max_order_volume) {
        return RiskDecision{RiskAction::kReject, "basic.max_order_volume",
                            "volume exceeds max order volume"};
    }
    const auto notional = std::fabs(intent.price) * static_cast<double>(intent.volume);
    if (notional > limits_.max_order_notional) {
        return RiskDecision{RiskAction::kReject, "basic.max_order_notional",
                            "notional exceeds max per-order notional"};
    }
    return RiskDecision{RiskAction::kAllow, "basic.allow", "pass"};
}

}  // namespace quant_hft
