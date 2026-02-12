#include "quant_hft/risk/risk_rule_executor.h"

#include <utility>

namespace quant_hft {

void RiskRuleExecutor::RegisterRule(RiskRuleType type, CheckFunc func) {
    registry_[type] = std::move(func);
}

RiskCheckResult RiskRuleExecutor::Execute(const RiskRule& rule,
                                          const OrderIntent& intent,
                                          const OrderContext& context) const {
    const auto it = registry_.find(rule.type);
    if (it == registry_.end()) {
        RiskCheckResult result;
        result.allowed = true;
        return result;
    }
    return it->second(rule, intent, context);
}

}  // namespace quant_hft
