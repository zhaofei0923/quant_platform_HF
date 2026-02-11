#include <gtest/gtest.h>

#include "quant_hft/services/risk_policy_engine.h"

namespace quant_hft {
namespace {

OrderIntent MakeIntent(const std::string& account,
                       const std::string& instrument,
                       std::int32_t volume,
                       double price,
                       EpochNanos ts_ns = 0) {
    OrderIntent intent;
    intent.account_id = account;
    intent.client_order_id = "ord-1";
    intent.instrument_id = instrument;
    intent.side = Side::kBuy;
    intent.offset = OffsetFlag::kOpen;
    intent.type = OrderType::kLimit;
    intent.volume = volume;
    intent.price = price;
    intent.ts_ns = ts_ns;
    intent.trace_id = "trace-1";
    return intent;
}

TEST(RiskPolicyEngineTest, UsesMostSpecificPolicyAndWritesStructuredAuditFields) {
    RiskPolicyDefaults defaults;
    defaults.max_order_volume = 50;
    defaults.max_order_notional = 500000.0;
    defaults.policy_id = "policy.global";
    defaults.policy_scope = "global";

    RiskPolicyRule account_instrument_rule;
    account_instrument_rule.policy_id = "policy.account.instrument";
    account_instrument_rule.policy_scope = "instrument";
    account_instrument_rule.account_id = "acc-A";
    account_instrument_rule.instrument_id = "SHFE.ag2406";
    account_instrument_rule.max_order_volume = 2;
    account_instrument_rule.max_order_notional = 10000.0;
    account_instrument_rule.decision_tags = "risk,volume";

    RiskPolicyRule account_rule;
    account_rule.policy_id = "policy.account";
    account_rule.policy_scope = "account";
    account_rule.account_id = "acc-A";
    account_rule.max_order_volume = 8;
    account_rule.max_order_notional = 20000.0;

    RiskPolicyEngine engine(defaults, {account_rule, account_instrument_rule});

    RiskContext ctx;
    ctx.account_id = "acc-A";
    ctx.instrument_id = "SHFE.ag2406";

    const auto decision = engine.PreCheck(MakeIntent("acc-A", "SHFE.ag2406", 3, 3000.0), ctx);
    EXPECT_EQ(decision.action, RiskAction::kReject);
    EXPECT_EQ(decision.policy_id, "policy.account.instrument");
    EXPECT_EQ(decision.policy_scope, "instrument");
    EXPECT_EQ(decision.rule_id, "policy.account.instrument.max_order_volume");
    EXPECT_DOUBLE_EQ(decision.observed_value, 3.0);
    EXPECT_DOUBLE_EQ(decision.threshold_value, 2.0);
    EXPECT_EQ(decision.decision_tags, "risk,volume");
}

TEST(RiskPolicyEngineTest, RejectsWhenContextLimitsAreExceeded) {
    RiskPolicyDefaults defaults;
    defaults.max_order_volume = 50;
    defaults.max_order_notional = 500000.0;
    defaults.max_active_orders = 2;
    defaults.max_position_notional = 20000.0;
    defaults.policy_id = "policy.global";
    defaults.policy_scope = "global";

    RiskPolicyEngine engine(defaults, {});

    RiskContext ctx;
    ctx.account_id = "acc-A";
    ctx.instrument_id = "SHFE.ag2406";
    ctx.active_order_count = 3;
    ctx.account_position_notional = 25000.0;

    const auto decision = engine.PreCheck(MakeIntent("acc-A", "SHFE.ag2406", 1, 1000.0), ctx);
    EXPECT_EQ(decision.action, RiskAction::kReject);
    EXPECT_EQ(decision.policy_id, "policy.global");
    EXPECT_EQ(decision.rule_id, "policy.global.max_active_orders");
    EXPECT_DOUBLE_EQ(decision.observed_value, 3.0);
    EXPECT_DOUBLE_EQ(decision.threshold_value, 2.0);
}

TEST(RiskPolicyEngineTest, AppliesSessionWindowPolicy) {
    RiskPolicyDefaults defaults;
    defaults.max_order_volume = 50;
    defaults.max_order_notional = 500000.0;
    defaults.policy_id = "policy.global";
    defaults.policy_scope = "global";

    RiskPolicyRule session_rule;
    session_rule.policy_id = "policy.session.open";
    session_rule.policy_scope = "session";
    session_rule.window_start_hhmm = 900;
    session_rule.window_end_hhmm = 1130;
    session_rule.max_order_notional = 1000.0;
    session_rule.max_order_volume = 10;

    RiskPolicyEngine engine(defaults, {session_rule});

    RiskContext ctx;
    ctx.account_id = "acc-A";
    ctx.instrument_id = "SHFE.ag2406";
    ctx.session_hhmm = 915;

    const auto decision = engine.PreCheck(MakeIntent("acc-A", "SHFE.ag2406", 2, 800.0), ctx);
    EXPECT_EQ(decision.action, RiskAction::kReject);
    EXPECT_EQ(decision.policy_id, "policy.session.open");
    EXPECT_EQ(decision.policy_scope, "session");
    EXPECT_EQ(decision.rule_id, "policy.session.open.max_order_notional");
    EXPECT_DOUBLE_EQ(decision.observed_value, 1600.0);
    EXPECT_DOUBLE_EQ(decision.threshold_value, 1000.0);
}

TEST(RiskPolicyEngineTest, ReloadsPoliciesAndEvaluatesExposure) {
    RiskPolicyDefaults defaults;
    defaults.max_order_volume = 50;
    defaults.max_order_notional = 500000.0;
    defaults.policy_id = "policy.global";
    defaults.policy_scope = "global";

    RiskPolicyEngine engine(defaults, {});

    RiskContext context;
    context.account_id = "acc-A";
    context.instrument_id = "SHFE.ag2406";
    context.account_position_notional = 1000.0;
    context.account_cross_gross_notional = 500.0;
    context.account_cross_net_notional = -250.0;

    const auto before = engine.PreCheck(MakeIntent("acc-A", "SHFE.ag2406", 5, 1000.0), context);
    EXPECT_EQ(before.action, RiskAction::kAllow);
    EXPECT_DOUBLE_EQ(engine.EvaluateExposure(context), 1750.0);

    RiskPolicyDefinition policy;
    policy.policy_id = "policy.account.instrument";
    policy.policy_scope = "instrument";
    policy.account_id = "acc-A";
    policy.instrument_id = "SHFE.ag2406";
    policy.max_order_volume = 2;
    policy.max_order_notional = 10000.0;
    policy.decision_tags = "reloaded";

    std::string error;
    ASSERT_TRUE(engine.ReloadPolicies({policy}, &error)) << error;

    const auto after = engine.PreCheck(MakeIntent("acc-A", "SHFE.ag2406", 5, 1000.0), context);
    EXPECT_EQ(after.action, RiskAction::kReject);
    EXPECT_EQ(after.policy_id, "policy.account.instrument");
    EXPECT_EQ(after.decision_tags, "reloaded");
}

}  // namespace
}  // namespace quant_hft
