#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "quant_hft/core/ctp_gateway_adapter.h"

namespace quant_hft {

TEST(CtpGatewayAdapterTest, ConnectSubscribeAndOrderFlow) {
    CtpGatewayAdapter adapter(10);

    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "p1";
    cfg.is_production_mode = false;

    ASSERT_TRUE(adapter.Connect(cfg));
    ASSERT_TRUE(adapter.IsHealthy());
    ASSERT_TRUE(adapter.Subscribe({"SHFE.ag2406"}));

    std::atomic<int> order_events{0};
    adapter.RegisterOrderEventCallback([&order_events](const OrderEvent&) {
        order_events.fetch_add(1);
    });

    OrderIntent intent;
    intent.account_id = "a1";
    intent.client_order_id = "ord1";
    intent.instrument_id = "SHFE.ag2406";
    intent.volume = 1;
    intent.price = 1.0;
    intent.trace_id = "t1";

    ASSERT_TRUE(adapter.PlaceOrder(intent));
    EXPECT_EQ(order_events.load(), 1);

    ASSERT_TRUE(adapter.CancelOrder("ord1", "t2"));
    EXPECT_EQ(order_events.load(), 2);
}

TEST(CtpGatewayAdapterTest, QueryAndOffsetApplySrc) {
    CtpGatewayAdapter adapter(10);

    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "p1";
    cfg.is_production_mode = false;

    ASSERT_TRUE(adapter.Connect(cfg));
    ASSERT_TRUE(adapter.EnqueueUserSessionQuery(1));

    const auto session = adapter.GetLastUserSession();
    EXPECT_EQ(session.investor_id, "191202");

    adapter.UpdateOffsetApplySrc('2');
    EXPECT_EQ(adapter.GetOffsetApplySrc(), '2');
}

TEST(CtpGatewayAdapterTest, CallbackCanReenterCancelOrderWithoutLockContention) {
    using namespace std::chrono_literals;

    CtpGatewayAdapter adapter(10);

    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "p1";
    cfg.is_production_mode = false;

    ASSERT_TRUE(adapter.Connect(cfg));

    std::atomic<bool> first_accept_seen{false};
    std::atomic<bool> cancel_result{false};
    bool cancel_finished_in_callback = false;
    std::mutex wait_mutex;
    std::condition_variable wait_cv;
    bool cancel_done = false;
    std::thread cancel_thread;

    adapter.RegisterOrderEventCallback(
        [&](const OrderEvent& event) {
            if (event.status != OrderStatus::kAccepted || first_accept_seen.exchange(true)) {
                return;
            }

            cancel_thread = std::thread(
                [&]() {
                    const bool ok = adapter.CancelOrder(event.client_order_id, "trace-cancel");
                    cancel_result.store(ok);
                    {
                        std::lock_guard<std::mutex> lock(wait_mutex);
                        cancel_done = true;
                    }
                    wait_cv.notify_one();
                });

            std::unique_lock<std::mutex> lock(wait_mutex);
            cancel_finished_in_callback =
                wait_cv.wait_for(lock, 50ms, [&]() { return cancel_done; });
        });

    OrderIntent intent;
    intent.account_id = "a1";
    intent.client_order_id = "ord-reenter-1";
    intent.instrument_id = "SHFE.ag2406";
    intent.volume = 1;
    intent.price = 1.0;
    intent.trace_id = "trace-order";

    ASSERT_TRUE(adapter.PlaceOrder(intent));
    if (cancel_thread.joinable()) {
        cancel_thread.join();
    }

    EXPECT_TRUE(first_accept_seen.load());
    EXPECT_TRUE(cancel_finished_in_callback);
    EXPECT_TRUE(cancel_result.load());
}

TEST(CtpGatewayAdapterTest, ConnectFailureExposesDiagnostic) {
    CtpGatewayAdapter adapter(10);

    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "";
    cfg.is_production_mode = false;

    EXPECT_FALSE(adapter.Connect(cfg));
    const auto diagnostic = adapter.GetLastConnectDiagnostic();
    EXPECT_NE(diagnostic.find("validation failed"), std::string::npos);
}

TEST(CtpGatewayAdapterTest, SuccessfulConnectClearsDiagnostic) {
    CtpGatewayAdapter adapter(10);

    MarketDataConnectConfig invalid_cfg;
    invalid_cfg.market_front_address = "tcp://sim-md";
    invalid_cfg.trader_front_address = "tcp://sim-td";
    invalid_cfg.broker_id = "9999";
    invalid_cfg.user_id = "191202";
    invalid_cfg.investor_id = "191202";
    invalid_cfg.password = "";
    invalid_cfg.is_production_mode = false;
    EXPECT_FALSE(adapter.Connect(invalid_cfg));
    EXPECT_FALSE(adapter.GetLastConnectDiagnostic().empty());

    MarketDataConnectConfig valid_cfg = invalid_cfg;
    valid_cfg.password = "p1";
    ASSERT_TRUE(adapter.Connect(valid_cfg));
    EXPECT_TRUE(adapter.GetLastConnectDiagnostic().empty());
}

}  // namespace quant_hft
