#include <gtest/gtest.h>

#include "quant_hft/core/ctp_md_adapter.h"

namespace quant_hft {

namespace {

MarketDataConnectConfig BuildSimConfig() {
    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "pwd";
    cfg.is_production_mode = false;
    return cfg;
}

}  // namespace

TEST(CTPMdAdapterTest, ConnectSubscribeAndUnsubscribe) {
    CTPMdAdapter adapter(10, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));
    EXPECT_TRUE(adapter.IsReady());
    EXPECT_EQ(adapter.SessionState(), MdSessionState::kReady);
    EXPECT_TRUE(adapter.Subscribe({"SHFE.ag2406"}));
    EXPECT_TRUE(adapter.Unsubscribe({"SHFE.ag2406"}));
}

TEST(CTPMdAdapterTest, FailedConnectExposesDiagnostic) {
    CTPMdAdapter adapter(10, 1);
    auto config = BuildSimConfig();
    config.password.clear();
    EXPECT_FALSE(adapter.Connect(config));
    EXPECT_FALSE(adapter.GetLastConnectDiagnostic().empty());
}

}  // namespace quant_hft
