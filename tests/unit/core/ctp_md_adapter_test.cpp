#include "quant_hft/core/ctp_md_adapter.h"

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <vector>

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

TEST(CTPMdAdapterTest, SaturationReportsGapAndPreservesAcceptedTickOrder) {
    auto gateway = std::make_shared<CtpGatewayAdapter>();
    CTPMdAdapter adapter(gateway, 4, 1);
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    std::vector<int> observed;
    std::atomic<int> gaps{0};
    adapter.RegisterTickCallback([&](const MarketSnapshot& tick) {
        if (tick.volume == 1) {
            entered.set_value();
            released.wait();
        }
        observed.push_back(static_cast<int>(tick.volume));
    });
    adapter.RegisterGapCallback([&](const MarketSnapshot& tick, const std::string& reason) {
        EXPECT_EQ(tick.volume, 3);
        EXPECT_EQ(reason, "market_queue_full");
        ++gaps;
    });
    MarketSnapshot tick;
    tick.instrument_id = "test";
    tick.volume = 1;
    ASSERT_TRUE(adapter.SubmitSnapshot(tick));
    if (entered.get_future().wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        release.set_value();
        FAIL() << "market worker failed to start";
    }
    tick.volume = 2;
    EXPECT_TRUE(adapter.SubmitSnapshot(tick));
    tick.volume = 3;
    EXPECT_FALSE(adapter.SubmitSnapshot(tick));
    release.set_value();
    adapter.StopEventDelivery();
    EXPECT_EQ(gaps.load(), 1);
    EXPECT_EQ(observed, (std::vector<int>{1, 2}));
    EXPECT_FALSE(adapter.SubmitSnapshot(tick));
}

}  // namespace quant_hft
