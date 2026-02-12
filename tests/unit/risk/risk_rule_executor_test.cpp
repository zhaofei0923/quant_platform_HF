#include <gtest/gtest.h>

#include "quant_hft/risk/risk_rule_executor.h"

namespace quant_hft {
namespace {

TEST(RiskRuleExecutorTest, ExecutesRegisteredRule) {
    RiskRuleExecutor executor;
    executor.RegisterRule(
        RiskRuleType::MAX_ORDER_VOLUME,
        [](const RiskRule& rule, const OrderIntent& intent, const OrderContext&) {
            if (intent.volume > static_cast<int>(rule.threshold)) {
                RiskCheckResult result;
                result.allowed = false;
                result.violated_rule = RiskRuleType::MAX_ORDER_VOLUME;
                result.reason = "exceeded";
                return result;
            }
            RiskCheckResult result;
            result.allowed = true;
            return result;
        });

    RiskRule rule;
    rule.type = RiskRuleType::MAX_ORDER_VOLUME;
    rule.threshold = 2.0;

    OrderIntent intent;
    intent.volume = 3;

    const auto result = executor.Execute(rule, intent, OrderContext{});
    EXPECT_FALSE(result.allowed);
    EXPECT_EQ(result.reason, "exceeded");
}

TEST(RiskRuleExecutorTest, UnregisteredRulePassesByDefault) {
    RiskRuleExecutor executor;
    RiskRule rule;
    rule.type = RiskRuleType::MAX_LEVERAGE;

    const auto result = executor.Execute(rule, OrderIntent{}, OrderContext{});
    EXPECT_TRUE(result.allowed);
}

}  // namespace
}  // namespace quant_hft
