#pragma once

#include <functional>
#include <memory>
#include <string>

#include "quant_hft/risk/risk_rule_executor.h"

namespace quant_hft {

class OrderManager;

void RegisterDefaultRiskRules(
    RiskRuleExecutor* executor,
    const std::shared_ptr<OrderManager>& order_manager,
    bool enable_self_trade_prevention,
    const std::function<bool(const std::string&, double, int)>& consume_rate_token);

}  // namespace quant_hft
