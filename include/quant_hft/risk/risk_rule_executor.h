#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>

#include "quant_hft/contracts/types.h"
#include "quant_hft/risk/risk_manager.h"

namespace quant_hft {

struct RiskRuleTypeHash {
    std::size_t operator()(RiskRuleType value) const {
        return static_cast<std::size_t>(value);
    }
};

class RiskRuleExecutor {
public:
    using CheckFunc =
        std::function<RiskCheckResult(const RiskRule&, const OrderIntent&, const OrderContext&)>;

    void RegisterRule(RiskRuleType type, CheckFunc func);

    RiskCheckResult Execute(const RiskRule& rule,
                            const OrderIntent& intent,
                            const OrderContext& context) const;

private:
    std::unordered_map<RiskRuleType, CheckFunc, RiskRuleTypeHash> registry_;
};

}  // namespace quant_hft
