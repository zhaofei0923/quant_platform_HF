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
    std::atomic<int> accepted_events{0};
    std::atomic<int> canceled_events{0};
    adapter.RegisterOrderEventCallback(
        [&](const OrderEvent& event) {
            order_events.fetch_add(1);
            if (event.status == OrderStatus::kAccepted) {
                accepted_events.fetch_add(1);
                EXPECT_EQ(event.side, Side::kSell);
                EXPECT_EQ(event.offset, OffsetFlag::kCloseToday);
                EXPECT_EQ(event.instrument_id, "SHFE.ag2406");
            }
            if (event.status == OrderStatus::kCanceled) {
                canceled_events.fetch_add(1);
                EXPECT_EQ(event.side, Side::kSell);
                EXPECT_EQ(event.offset, OffsetFlag::kCloseToday);
                EXPECT_EQ(event.instrument_id, "SHFE.ag2406");
            }
        });

    OrderIntent intent;
    intent.account_id = "a1";
    intent.client_order_id = "ord1";
    intent.instrument_id = "SHFE.ag2406";
    intent.side = Side::kSell;
    intent.offset = OffsetFlag::kCloseToday;
    intent.volume = 1;
    intent.price = 1.0;
    intent.trace_id = "t1";

    ASSERT_TRUE(adapter.PlaceOrder(intent));
    EXPECT_EQ(order_events.load(), 1);
    EXPECT_EQ(accepted_events.load(), 1);

    ASSERT_TRUE(adapter.CancelOrder("ord1", "t2"));
    EXPECT_EQ(order_events.load(), 2);
    EXPECT_EQ(canceled_events.load(), 1);
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

TEST(CtpGatewayAdapterTest, QuerySnapshotsInSimulatedMode) {
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
    ASSERT_TRUE(adapter.Subscribe({"SHFE.ag2406"}));

    std::atomic<int> account_callbacks{0};
    std::atomic<int> position_callbacks{0};
    std::atomic<int> instrument_callbacks{0};
    std::atomic<int> broker_param_callbacks{0};
    adapter.RegisterTradingAccountSnapshotCallback(
        [&](const TradingAccountSnapshot& snapshot) {
            EXPECT_EQ(snapshot.investor_id, "191202");
            account_callbacks.fetch_add(1);
        });
    adapter.RegisterInvestorPositionSnapshotCallback(
        [&](const std::vector<InvestorPositionSnapshot>& snapshots) {
            EXPECT_TRUE(snapshots.empty());
            position_callbacks.fetch_add(1);
        });
    adapter.RegisterInstrumentMetaSnapshotCallback(
        [&](const std::vector<InstrumentMetaSnapshot>& snapshots) {
            EXPECT_FALSE(snapshots.empty());
            instrument_callbacks.fetch_add(1);
        });
    adapter.RegisterBrokerTradingParamsSnapshotCallback(
        [&](const BrokerTradingParamsSnapshot& snapshot) {
            EXPECT_FALSE(snapshot.margin_price_type.empty());
            broker_param_callbacks.fetch_add(1);
        });

    EXPECT_TRUE(adapter.EnqueueTradingAccountQuery(11));
    EXPECT_TRUE(adapter.EnqueueInvestorPositionQuery(12));
    EXPECT_TRUE(adapter.EnqueueInstrumentQuery(13));
    EXPECT_TRUE(adapter.EnqueueBrokerTradingParamsQuery(14));
    EXPECT_TRUE(adapter.EnqueueInstrumentMarginRateQuery(15, "SHFE.ag2406"));
    EXPECT_TRUE(adapter.EnqueueInstrumentCommissionRateQuery(16, "SHFE.ag2406"));

    const auto account = adapter.GetLastTradingAccountSnapshot();
    EXPECT_EQ(account.investor_id, "191202");
    const auto positions = adapter.GetLastInvestorPositionSnapshots();
    EXPECT_TRUE(positions.empty());
    const auto metas = adapter.GetLastInstrumentMetaSnapshots();
    EXPECT_FALSE(metas.empty());
    EXPECT_EQ(metas.front().instrument_id, "SHFE.ag2406");
    const auto broker_params = adapter.GetLastBrokerTradingParamsSnapshot();
    EXPECT_FALSE(broker_params.margin_price_type.empty());

    EXPECT_EQ(account_callbacks.load(), 1);
    EXPECT_EQ(position_callbacks.load(), 1);
    EXPECT_EQ(instrument_callbacks.load(), 1);
    EXPECT_EQ(broker_param_callbacks.load(), 1);
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

TEST(CtpGatewayAdapterTest, NormalizeMarketSnapshotCleansInvalidValuesAndFallbacks) {
    MarketSnapshot snapshot;
    snapshot.instrument_id = "DCE.i2409";
    snapshot.exchange_id = "";
    snapshot.trading_day = "";
    snapshot.action_day = "20260211";
    snapshot.update_time = "21:15:08";
    snapshot.update_millisec = -12;
    snapshot.settlement_price = 1.7976931348623157e+308;
    snapshot.average_price_raw = 1.7976931348623157e+308;

    CtpGatewayAdapter::NormalizeMarketSnapshot(&snapshot);

    EXPECT_EQ(snapshot.exchange_id, "DCE");
    EXPECT_EQ(snapshot.trading_day, "20260211");
    EXPECT_EQ(snapshot.action_day, "20260211");
    EXPECT_EQ(snapshot.update_millisec, 0);
    EXPECT_DOUBLE_EQ(snapshot.settlement_price, 0.0);
    EXPECT_FALSE(snapshot.is_valid_settlement);
    EXPECT_DOUBLE_EQ(snapshot.average_price_norm, 0.0);
}

TEST(CtpGatewayAdapterTest, NormalizeMarketSnapshotKeepsValidValues) {
    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.ag2406";
    snapshot.exchange_id = "SHFE";
    snapshot.trading_day = "20260211";
    snapshot.action_day = "";
    snapshot.update_time = "09:31:05";
    snapshot.update_millisec = 500;
    snapshot.settlement_price = 4890.5;
    snapshot.average_price_raw = 4888.0;

    CtpGatewayAdapter::NormalizeMarketSnapshot(&snapshot);

    EXPECT_EQ(snapshot.exchange_id, "SHFE");
    EXPECT_EQ(snapshot.trading_day, "20260211");
    EXPECT_EQ(snapshot.action_day, "20260211");
    EXPECT_EQ(snapshot.update_millisec, 500);
    EXPECT_DOUBLE_EQ(snapshot.settlement_price, 4890.5);
    EXPECT_TRUE(snapshot.is_valid_settlement);
    EXPECT_DOUBLE_EQ(snapshot.average_price_norm, 4888.0);
}

}  // namespace quant_hft
