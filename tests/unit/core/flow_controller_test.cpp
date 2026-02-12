#include <gtest/gtest.h>

#include "quant_hft/core/flow_controller.h"

namespace quant_hft {

TEST(FlowControllerTest, AppliesRateLimitRule) {
    FlowController controller;
    FlowRule rule;
    rule.account_id = "acc1";
    rule.type = OperationType::kOrderInsert;
    rule.rate_per_second = 1.0;
    rule.capacity = 1;
    controller.AddRule(rule);

    Operation operation;
    operation.account_id = "acc1";
    operation.type = OperationType::kOrderInsert;
    operation.instrument_id = "SHFE.ag2406";
    EXPECT_TRUE(controller.Check(operation).allowed);
    EXPECT_FALSE(controller.Check(operation).allowed);
}

TEST(FlowControllerTest, AcquireCanWaitForRefill) {
    FlowController controller;
    FlowRule rule;
    rule.account_id = "acc1";
    rule.type = OperationType::kQuery;
    rule.rate_per_second = 10.0;
    rule.capacity = 1;
    controller.AddRule(rule);

    Operation operation;
    operation.account_id = "acc1";
    operation.type = OperationType::kQuery;
    EXPECT_TRUE(controller.Check(operation).allowed);
    EXPECT_FALSE(controller.Check(operation).allowed);
    EXPECT_TRUE(controller.Acquire(operation, /*timeout_ms=*/500).allowed);
}

TEST(FlowControllerTest, InstrumentRuleOverridesAccountRule) {
    FlowController controller;
    FlowRule account_rule;
    account_rule.account_id = "acc1";
    account_rule.type = OperationType::kOrderCancel;
    account_rule.rate_per_second = 1.0;
    account_rule.capacity = 1;
    controller.AddRule(account_rule);

    FlowRule scoped_rule;
    scoped_rule.account_id = "acc1";
    scoped_rule.type = OperationType::kOrderCancel;
    scoped_rule.instrument_id = "SHFE.rb2405";
    scoped_rule.rate_per_second = 100.0;
    scoped_rule.capacity = 2;
    controller.AddRule(scoped_rule);

    Operation scoped;
    scoped.account_id = "acc1";
    scoped.type = OperationType::kOrderCancel;
    scoped.instrument_id = "SHFE.rb2405";
    EXPECT_TRUE(controller.Check(scoped).allowed);
    EXPECT_TRUE(controller.Check(scoped).allowed);
    EXPECT_FALSE(controller.Check(scoped).allowed);
}

}  // namespace quant_hft
