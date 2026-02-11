#include <gtest/gtest.h>

#include "quant_hft/services/basic_risk_engine.h"

namespace quant_hft {

TEST(BasicRiskEngineTest, RejectsInvalidVolume) {
    BasicRiskLimits limits;
    limits.max_order_volume = 10;
    limits.max_order_notional = 100000.0;
    BasicRiskEngine engine(limits);
    OrderIntent intent;
    intent.volume = 0;
    intent.price = 100.0;

    const auto decision = engine.PreCheck(intent);
    EXPECT_EQ(decision.action, RiskAction::kReject);
}

TEST(BasicRiskEngineTest, RejectsOverNotional) {
    BasicRiskLimits limits;
    limits.max_order_volume = 100;
    limits.max_order_notional = 1000.0;
    BasicRiskEngine engine(limits);
    OrderIntent intent;
    intent.volume = 100;
    intent.price = 100.0;

    const auto decision = engine.PreCheck(intent);
    EXPECT_EQ(decision.action, RiskAction::kReject);
    EXPECT_EQ(decision.rule_id, "basic.max_order_notional");
}

TEST(BasicRiskEngineTest, AllowsNormalOrder) {
    BasicRiskEngine engine(BasicRiskLimits{});
    OrderIntent intent;
    intent.volume = 2;
    intent.price = 4500.0;

    const auto decision = engine.PreCheck(intent);
    EXPECT_EQ(decision.action, RiskAction::kAllow);
}

}  // namespace quant_hft
