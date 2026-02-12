#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

#include "quant_hft/core/ctp_trader_adapter.h"

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

OrderIntent BuildOrderIntent(const std::string& strategy_id, const std::string& client_order_id) {
    OrderIntent intent;
    intent.account_id = "acc1";
    intent.client_order_id = client_order_id;
    intent.strategy_id = strategy_id;
    intent.instrument_id = "SHFE.ag2406";
    intent.volume = 1;
    intent.price = 4000.0;
    intent.trace_id = "trace-1";
    return intent;
}

}  // namespace

TEST(CTPTraderAdapterTest, RejectsOrdersBeforeSettlementConfirm) {
    CTPTraderAdapter adapter(10, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));
    EXPECT_EQ(adapter.SessionState(), TraderSessionState::kLoggedIn);

    auto intent = BuildOrderIntent("stratA", "ord-1");
    EXPECT_FALSE(adapter.PlaceOrder(intent));

    ASSERT_TRUE(adapter.ConfirmSettlement());
    EXPECT_EQ(adapter.SessionState(), TraderSessionState::kReady);
    EXPECT_TRUE(adapter.PlaceOrder(intent));
}

TEST(CTPTraderAdapterTest, RequiresStrategyIdForOrderPlacement) {
    CTPTraderAdapter adapter(10, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));
    ASSERT_TRUE(adapter.ConfirmSettlement());

    auto invalid_intent = BuildOrderIntent("", "ord-2");
    EXPECT_FALSE(adapter.PlaceOrder(invalid_intent));

    auto valid_intent = BuildOrderIntent("stratA", "ord-3");
    EXPECT_TRUE(adapter.PlaceOrder(valid_intent));
}

TEST(CTPTraderAdapterTest, DispatchesOrderCallbacksOnWorkerThread) {
    CTPTraderAdapter adapter(10, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));
    ASSERT_TRUE(adapter.ConfirmSettlement());

    std::thread::id callback_thread;
    std::mutex mutex;
    std::condition_variable cv;
    bool callback_seen = false;
    adapter.RegisterOrderEventCallback([&](const OrderEvent&) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            callback_thread = std::this_thread::get_id();
            callback_seen = true;
        }
        cv.notify_one();
    });

    const auto main_thread = std::this_thread::get_id();
    ASSERT_TRUE(adapter.PlaceOrder(BuildOrderIntent("stratA", "ord-4")));

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(500), [&]() { return callback_seen; }));
    EXPECT_NE(callback_thread, main_thread);
}

TEST(CTPTraderAdapterTest, BuildOrderRefUsesStrategyTimestampSequenceFormat) {
    CTPTraderAdapter adapter(10, 1);
    const auto order_ref = adapter.BuildOrderRef("demo");
    EXPECT_NE(order_ref.find("demo_"), std::string::npos);
    EXPECT_NE(order_ref.find('_'), std::string::npos);
}

TEST(CTPTraderAdapterTest, AllowsOrderAndTradeQueriesAfterLogin) {
    CTPTraderAdapter adapter(10, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));
    EXPECT_EQ(adapter.SessionState(), TraderSessionState::kLoggedIn);
    EXPECT_TRUE(adapter.EnqueueOrderQuery(101));
    EXPECT_TRUE(adapter.EnqueueTradeQuery(102));
}

}  // namespace quant_hft
