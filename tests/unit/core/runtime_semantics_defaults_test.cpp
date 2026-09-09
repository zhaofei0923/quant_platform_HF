#include <gtest/gtest.h>
#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/runtime_semantics_loader.h"
namespace quant_hft {
TEST(ReplayMarketParityTest, SharedRuntimeDefaultsMatchLiveLoaderAndRejectDecisionDrift) {
    RuntimeSemanticsConfig shared;
    CtpFileConfig live;
    std::string error;
    EXPECT_TRUE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error)) << error;
    live.execution.cancel_after_ms = 2000;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("cancel_after_ms"), std::string::npos);
    shared.cancel_after_ms = 2000;
    EXPECT_TRUE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error)) << error;
    live.market_bar.allowed_lateness_ms = 700;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("market_bar_allowed_lateness_ms"), std::string::npos);
    shared.market_bar_allowed_lateness_ms = 700;
    live.dominant_contract_min_hold_ms += 1;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("dominant_contract_min_hold_ms"), std::string::npos);
    shared.dominant_contract_min_hold_ms = live.dominant_contract_min_hold_ms;
    live.risk.max_margin_to_equity_ratio = 0.30;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("risk_max_margin_to_equity_ratio"), std::string::npos);
    shared.risk_max_margin_to_equity_ratio = 0.30;
    shared.risk_rule_groups = "group_a, group_b";
    live.risk.rules.resize(2);
    live.risk.rules[0].rule_group = "group_a";
    live.risk.rules[1].rule_group = "group_b";
    EXPECT_TRUE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error)) << error;
    live.risk.rules[1].rule_group = "other";
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("risk_rule_groups"), std::string::npos);
}

}
