#include <chrono>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
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
    cfg.settlement_confirm_required = true;
    cfg.is_production_mode = false;
    return cfg;
}

OrderIntent BuildOrderIntent(const std::string& strategy_id) {
    OrderIntent intent;
    intent.account_id = "acc1";
    intent.client_order_id.clear();
    intent.strategy_id = strategy_id;
    intent.instrument_id = "SHFE.ag2406";
    intent.volume = 1;
    intent.price = 4000.0;
    intent.trace_id = "trace-1";
    return intent;
}

bool WaitUntil(const std::function<bool()>& predicate, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
}

class FakeGateway final : public CtpGatewayAdapter {
public:
    FakeGateway()
        : CtpGatewayAdapter(100) {}

    bool Connect(const MarketDataConnectConfig& config) override {
        std::lock_guard<std::mutex> lock(mutex_);
        config_ = config;
        connected_ = true;
        healthy_ = true;
        return true;
    }

    void Disconnect() override {
        ConnectionStateCallback cb;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            connected_ = false;
            healthy_ = false;
            cb = connection_state_callback_;
        }
        if (cb) {
            cb(false);
        }
    }

    bool IsHealthy() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return healthy_;
    }

    bool PlaceOrder(const OrderIntent& intent) override {
        std::lock_guard<std::mutex> lock(mutex_);
        ++place_order_calls_;
        last_order_intent_ = intent;
        return place_order_success_;
    }

    bool RequestUserLogin(int request_id,
                          const std::string&,
                          const std::string&,
                          const std::string&) override {
        LoginResponseCallback cb;
        bool do_callback = false;
        int error_code = 0;
        std::string error_msg;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++request_user_login_calls_;
            last_login_request_id_ = request_id;
            cb = login_response_callback_;
            do_callback = auto_login_response_;
            error_code = login_error_code_;
            error_msg = login_error_msg_;
        }
        if (do_callback && cb) {
            cb(request_id, error_code, error_msg);
        }
        return request_user_login_submit_success_;
    }

    bool RequestSettlementInfoConfirm(int request_id) override {
        SettlementConfirmCallback cb;
        bool do_callback = false;
        int error_code = 0;
        std::string error_msg;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++request_settlement_confirm_calls_;
            last_settlement_request_id_ = request_id;
            cb = settlement_confirm_callback_;
            do_callback = auto_settlement_response_;
            error_code = settlement_error_code_;
            error_msg = settlement_error_msg_;
        }
        if (do_callback && cb) {
            cb(request_id, error_code, error_msg);
        }
        return request_settlement_submit_success_;
    }

    bool EnqueueOrderQuery(int request_id) override {
        QueryCompleteCallback cb;
        bool do_callback = false;
        bool success = true;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++enqueue_order_query_calls_;
            cb = query_complete_callback_;
            do_callback = auto_query_complete_;
            success = query_success_;
        }
        if (do_callback && cb) {
            cb(request_id, "order", success);
        }
        return enqueue_query_submit_success_;
    }

    bool EnqueueTradeQuery(int request_id) override {
        QueryCompleteCallback cb;
        bool do_callback = false;
        bool success = true;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++enqueue_trade_query_calls_;
            cb = query_complete_callback_;
            do_callback = auto_query_complete_;
            success = query_success_;
        }
        if (do_callback && cb) {
            cb(request_id, "trade", success);
        }
        return enqueue_query_submit_success_;
    }

    void RegisterOrderEventCallback(OrderEventCallback callback) override {
        std::lock_guard<std::mutex> lock(mutex_);
        order_event_callback_ = std::move(callback);
    }

    void RegisterConnectionStateCallback(ConnectionStateCallback callback) override {
        std::lock_guard<std::mutex> lock(mutex_);
        connection_state_callback_ = std::move(callback);
    }

    void RegisterLoginResponseCallback(LoginResponseCallback callback) override {
        std::lock_guard<std::mutex> lock(mutex_);
        login_response_callback_ = std::move(callback);
    }

    void RegisterQueryCompleteCallback(QueryCompleteCallback callback) override {
        std::lock_guard<std::mutex> lock(mutex_);
        query_complete_callback_ = std::move(callback);
    }

    void RegisterSettlementConfirmCallback(SettlementConfirmCallback callback) override {
        std::lock_guard<std::mutex> lock(mutex_);
        settlement_confirm_callback_ = std::move(callback);
    }

    void SetHealthy(bool healthy) {
        std::lock_guard<std::mutex> lock(mutex_);
        healthy_ = healthy;
    }

    void EmitConnectionState(bool healthy) {
        ConnectionStateCallback cb;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            healthy_ = healthy;
            cb = connection_state_callback_;
        }
        if (cb) {
            cb(healthy);
        }
    }

    int request_user_login_calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return request_user_login_calls_;
    }

    int request_settlement_confirm_calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return request_settlement_confirm_calls_;
    }

    int enqueue_order_query_calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return enqueue_order_query_calls_;
    }

    int enqueue_trade_query_calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return enqueue_trade_query_calls_;
    }

    OrderIntent last_order_intent() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_order_intent_;
    }

    void set_auto_login_response(bool value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto_login_response_ = value;
    }

    void EmitOrderEvent(const OrderEvent& event) {
        OrderEventCallback cb;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cb = order_event_callback_;
        }
        if (cb) {
            cb(event);
        }
    }

private:
    mutable std::mutex mutex_;
    MarketDataConnectConfig config_{};
    bool connected_{false};
    bool healthy_{false};
    bool place_order_success_{true};
    bool request_user_login_submit_success_{true};
    bool request_settlement_submit_success_{true};
    bool enqueue_query_submit_success_{true};
    bool auto_login_response_{true};
    bool auto_settlement_response_{true};
    bool auto_query_complete_{true};
    bool query_success_{true};
    int login_error_code_{0};
    int settlement_error_code_{0};
    std::string login_error_msg_;
    std::string settlement_error_msg_;
    int last_login_request_id_{0};
    int last_settlement_request_id_{0};
    int place_order_calls_{0};
    int request_user_login_calls_{0};
    int request_settlement_confirm_calls_{0};
    int enqueue_order_query_calls_{0};
    int enqueue_trade_query_calls_{0};
    OrderIntent last_order_intent_{};
    ConnectionStateCallback connection_state_callback_;
    LoginResponseCallback login_response_callback_;
    QueryCompleteCallback query_complete_callback_;
    SettlementConfirmCallback settlement_confirm_callback_;
    OrderEventCallback order_event_callback_;
};

}  // namespace

TEST(CTPTraderAdapterTest, DisconnectTriggersReconnectScheduling) {
    auto fake_gateway = std::make_shared<FakeGateway>();
    CTPTraderAdapter adapter(fake_gateway, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));
    ASSERT_TRUE(adapter.ConfirmSettlement());
    ASSERT_TRUE(adapter.IsReady());

    fake_gateway->EmitConnectionState(false);
    fake_gateway->SetHealthy(true);

    EXPECT_TRUE(WaitUntil([&]() { return fake_gateway->request_user_login_calls() >= 1; }, 2500));
}

TEST(CTPTraderAdapterTest, ReconnectPerformsLoginAndConfirmSettlement) {
    auto fake_gateway = std::make_shared<FakeGateway>();
    CTPTraderAdapter adapter(fake_gateway, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));
    ASSERT_TRUE(adapter.ConfirmSettlement());
    ASSERT_TRUE(adapter.IsReady());

    fake_gateway->EmitConnectionState(false);
    fake_gateway->SetHealthy(true);

    EXPECT_TRUE(WaitUntil([&]() { return adapter.IsReady(); }, 3500));
    EXPECT_GE(fake_gateway->request_user_login_calls(), 1);
    EXPECT_GE(fake_gateway->request_settlement_confirm_calls(), 1);
    EXPECT_GE(fake_gateway->enqueue_order_query_calls(), 1);
    EXPECT_GE(fake_gateway->enqueue_trade_query_calls(), 1);
}

TEST(CTPTraderAdapterTest, RecoverOrdersAndTradesQueriesCtp) {
    auto fake_gateway = std::make_shared<FakeGateway>();
    CTPTraderAdapter adapter(fake_gateway, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));

    EXPECT_TRUE(adapter.RecoverOrdersAndTrades(500));
    EXPECT_EQ(fake_gateway->enqueue_order_query_calls(), 1);
    EXPECT_EQ(fake_gateway->enqueue_trade_query_calls(), 1);
}

TEST(CTPTraderAdapterTest, LoginAsyncReturnsFutureAndResolvesOnSuccess) {
    auto fake_gateway = std::make_shared<FakeGateway>();
    CTPTraderAdapter adapter(fake_gateway, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));

    auto future = adapter.LoginAsync("9999", "191202", "pwd", 500);
    ASSERT_EQ(future.wait_for(std::chrono::milliseconds(500)), std::future_status::ready);
    const auto result = future.get();
    EXPECT_EQ(result.first, 0);
    EXPECT_EQ(fake_gateway->request_user_login_calls(), 1);
}

TEST(CTPTraderAdapterTest, LoginAsyncTimesOut) {
    auto fake_gateway = std::make_shared<FakeGateway>();
    fake_gateway->set_auto_login_response(false);
    CTPTraderAdapter adapter(fake_gateway, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));

    auto future = adapter.LoginAsync("9999", "191202", "pwd", 80);
    ASSERT_EQ(future.wait_for(std::chrono::milliseconds(500)), std::future_status::ready);
    const auto result = future.get();
    EXPECT_EQ(result.first, -1);
    EXPECT_NE(result.second.find("timeout"), std::string::npos);
}

TEST(CTPTraderAdapterTest, PlaceOrderWithRefReturnsNonEmptyString) {
    auto fake_gateway = std::make_shared<FakeGateway>();
    CTPTraderAdapter adapter(fake_gateway, 1);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));
    ASSERT_TRUE(adapter.ConfirmSettlement());

    auto intent = BuildOrderIntent("stratA");
    const auto client_order_id = adapter.PlaceOrderWithRef(intent);
    ASSERT_FALSE(client_order_id.empty());
    EXPECT_EQ(client_order_id.rfind("stratA_", 0), 0U);
    EXPECT_EQ(fake_gateway->last_order_intent().client_order_id, client_order_id);
}

TEST(CTPTraderAdapterTest, PythonCriticalDispatchTimeoutTriggersCircuitBreakerCallback) {
    auto fake_gateway = std::make_shared<FakeGateway>();
    CTPTraderAdapter adapter(fake_gateway, 1, 1, 5);
    ASSERT_TRUE(adapter.Connect(BuildSimConfig()));

    std::atomic<bool> breaker_triggered{false};
    adapter.SetCircuitBreaker([&breaker_triggered](bool opened) {
        if (opened) {
            breaker_triggered.store(true);
        }
    });

    adapter.RegisterOrderEventCallback([](const OrderEvent&) {
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
    });

    OrderEvent event;
    event.account_id = "acc1";
    event.client_order_id = "ord-timeout";
    event.order_ref = "ord-timeout";
    event.instrument_id = "SHFE.ag2406";
    event.status = OrderStatus::kAccepted;
    event.event_source = "OnRtnOrder";
    event.ts_ns = 1;

    fake_gateway->EmitOrderEvent(event);
    fake_gateway->EmitOrderEvent(event);
    fake_gateway->EmitOrderEvent(event);

    EXPECT_TRUE(WaitUntil([&breaker_triggered]() { return breaker_triggered.load(); }, 2000));
}

}  // namespace quant_hft
