#include <gtest/gtest.h>

#include "quant_hft/services/basic_risk_engine.h"

namespace quant_hft {

namespace {

OrderIntent MakeIntent(const std::string& account_id,
                       const std::string& instrument_id,
                       std::int32_t volume,
                       double price,
                       EpochNanos ts_ns = 0) {
    OrderIntent intent;
    intent.account_id = account_id;
    intent.client_order_id = "ord-1";
    intent.instrument_id = instrument_id;
    intent.side = Side::kBuy;
    intent.offset = OffsetFlag::kOpen;
    intent.type = OrderType::kLimit;
    intent.volume = volume;
    intent.price = price;
    intent.ts_ns = ts_ns;
    intent.trace_id = "trace-1";
    return intent;
}

}  // namespace

TEST(BasicRiskEngineTest, RejectsInvalidVolume) {
    BasicRiskLimits limits;
    limits.max_order_volume = 10;
    limits.max_order_notional = 100000.0;
    BasicRiskEngine engine(limits);
    auto intent = MakeIntent("a1", "SHFE.ag2406", 0, 100.0);

    const auto decision = engine.PreCheck(intent);
    EXPECT_EQ(decision.action, RiskAction::kReject);
    EXPECT_EQ(decision.rule_group, "default");
    EXPECT_EQ(decision.rule_version, "v1");
    EXPECT_GT(decision.decision_ts_ns, 0);
}

TEST(BasicRiskEngineTest, RejectsOverNotional) {
    BasicRiskLimits limits;
    limits.max_order_volume = 100;
    limits.max_order_notional = 1000.0;
    BasicRiskEngine engine(limits);
    auto intent = MakeIntent("a1", "SHFE.ag2406", 100, 100.0);

    const auto decision = engine.PreCheck(intent);
    EXPECT_EQ(decision.action, RiskAction::kReject);
    EXPECT_EQ(decision.rule_id, "risk.default.max_order_notional");
}

TEST(BasicRiskEngineTest, AllowsNormalOrder) {
    BasicRiskEngine engine(BasicRiskLimits{});
    auto intent = MakeIntent("a1", "SHFE.ag2406", 2, 4500.0);

    const auto decision = engine.PreCheck(intent);
    EXPECT_EQ(decision.action, RiskAction::kAllow);
    EXPECT_EQ(decision.rule_id, "risk.default.allow");
}

TEST(BasicRiskEngineTest, MatchesMostSpecificRuleAndWritesMetadata) {
    BasicRiskLimits defaults;
    defaults.max_order_volume = 10;
    defaults.max_order_notional = 100000.0;
    defaults.rule_group = "default-group";
    defaults.rule_version = "v0";

    BasicRiskRule account_instrument;
    account_instrument.rule_id = "ag-account-rule";
    account_instrument.rule_group = "ag-opening";
    account_instrument.rule_version = "2026.03";
    account_instrument.account_id = "acc-A";
    account_instrument.instrument_id = "SHFE.ag2406";
    account_instrument.max_order_volume = 2;
    account_instrument.max_order_notional = 12000.0;

    BasicRiskRule instrument_only;
    instrument_only.rule_id = "ag-instrument-rule";
    instrument_only.rule_group = "ag-default";
    instrument_only.rule_version = "2026.01";
    instrument_only.instrument_id = "SHFE.ag2406";
    instrument_only.max_order_volume = 8;
    instrument_only.max_order_notional = 50000.0;

    BasicRiskEngine engine(defaults, {instrument_only, account_instrument});

    const auto decision = engine.PreCheck(MakeIntent("acc-A", "SHFE.ag2406", 3, 3000.0));
    EXPECT_EQ(decision.action, RiskAction::kReject);
    EXPECT_EQ(decision.rule_id, "ag-account-rule.max_order_volume");
    EXPECT_EQ(decision.rule_group, "ag-opening");
    EXPECT_EQ(decision.rule_version, "2026.03");
    EXPECT_GT(decision.decision_ts_ns, 0);
}

TEST(BasicRiskEngineTest, FallsBackToDefaultWhenNoRuleMatches) {
    BasicRiskLimits defaults;
    defaults.max_order_volume = 4;
    defaults.max_order_notional = 20000.0;
    defaults.rule_group = "default-group";
    defaults.rule_version = "2026.01";

    BasicRiskRule unrelated;
    unrelated.rule_id = "rb-account";
    unrelated.rule_group = "rb-group";
    unrelated.rule_version = "2026.02";
    unrelated.account_id = "acc-B";
    unrelated.instrument_id = "SHFE.rb2405";
    unrelated.max_order_volume = 2;
    unrelated.max_order_notional = 8000.0;

    BasicRiskEngine engine(defaults, {unrelated});
    const auto decision = engine.PreCheck(MakeIntent("acc-A", "SHFE.ag2406", 5, 1000.0));
    EXPECT_EQ(decision.action, RiskAction::kReject);
    EXPECT_EQ(decision.rule_group, "default-group");
    EXPECT_EQ(decision.rule_version, "2026.01");
    EXPECT_EQ(decision.rule_id, "risk.default.max_order_volume");
}

}  // namespace quant_hft
