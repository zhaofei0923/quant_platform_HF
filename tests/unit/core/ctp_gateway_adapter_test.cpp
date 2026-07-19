#include "quant_hft/core/ctp_gateway_adapter.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

#include "quant_hft/core/ctp_text.h"

namespace quant_hft {

TEST(CtpTextTest, DecodesGbkErrorMessageAndKnownPlaceholder) {
    const std::string gb18030_settlement_unconfirmed =
        "\xBD\xE1\xCB\xE3\xBD\xE1\xB9\xFB\xCE\xB4\xC8\xB7\xC8\xCF";

    EXPECT_EQ(ctp::DecodeCtpText(gb18030_settlement_unconfirmed), "结算结果未确认");
    EXPECT_EQ(ctp::DecodeCtpErrorMessage(42, gb18030_settlement_unconfirmed.c_str()),
              "结算结果未确认");
    EXPECT_EQ(ctp::DecodeCtpErrorMessage(42, "CTP:??????????????"), "结算结果未确认");
    EXPECT_EQ(ctp::DecodeCtpErrorMessage(9999, "plain ascii error"), "plain ascii error");
}

TEST(CtpGatewayAdapterTest, ConnectSubscribeAndOrderFlow) {
    CtpGatewayAdapter adapter(10);

    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "test_user";
    cfg.investor_id = "test_investor";
    cfg.password = "test_password";
    cfg.is_production_mode = false;

    ASSERT_TRUE(adapter.Connect(cfg));
    ASSERT_TRUE(adapter.IsHealthy());
    ASSERT_TRUE(adapter.Subscribe({"SHFE.ag2406"}));

    std::atomic<int> order_events{0};
    std::atomic<int> accepted_events{0};
    std::atomic<int> canceled_events{0};
    adapter.RegisterOrderEventCallback([&](const OrderEvent& event) {
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

TEST(CtpGatewayAdapterTest, PlaceOrderEmitsSubmitMappingBeforeOrderCallback) {
    CtpGatewayAdapter adapter(10);

    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "test_user";
    cfg.investor_id = "test_investor";
    cfg.password = "test_password";
    cfg.is_production_mode = false;

    ASSERT_TRUE(adapter.Connect(cfg));

    std::atomic<int> sequence{0};
    CtpOrderSubmitMapping observed_mapping;
    adapter.RegisterOrderSubmitPrepareCallback(
        [&](const CtpOrderSubmitMapping& mapping, std::string*) {
            EXPECT_EQ(sequence.fetch_add(1), 0);
            EXPECT_EQ(mapping.phase, OrderSubmitMappingPhase::kPrepared);
            // The durability hook must be safe to re-enter read-only gateway APIs.
            EXPECT_GT(adapter.GetSessionGeneration(), 0U);
            return true;
        });
    adapter.RegisterOrderSubmitMappingCallback([&](const CtpOrderSubmitMapping& mapping) {
        EXPECT_EQ(sequence.fetch_add(1), 1);
        observed_mapping = mapping;
    });
    adapter.RegisterOrderEventCallback([&](const OrderEvent& event) {
        EXPECT_EQ(sequence.fetch_add(1), 2);
        EXPECT_EQ(event.client_order_id, "ord-map-1");
    });

    OrderIntent intent;
    intent.account_id = "a1";
    intent.client_order_id = "ord-map-1";
    intent.strategy_id = "strat-map";
    intent.instrument_id = "SHFE.ag2406";
    intent.side = Side::kBuy;
    intent.offset = OffsetFlag::kOpen;
    intent.volume = 1;
    intent.price = 4010.0;
    intent.trace_id = "trace-map";

    ASSERT_TRUE(adapter.PlaceOrder(intent));
    EXPECT_EQ(sequence.load(), 3);
    EXPECT_EQ(observed_mapping.client_order_id, "ord-map-1");
    EXPECT_EQ(observed_mapping.order_ref, "ord-map-1");
    EXPECT_EQ(observed_mapping.trace_id, "trace-map");
    EXPECT_EQ(observed_mapping.strategy_id, "strat-map");
    EXPECT_EQ(observed_mapping.instrument_id, "SHFE.ag2406");
    EXPECT_EQ(observed_mapping.phase, OrderSubmitMappingPhase::kSubmitted);
}

TEST(CtpGatewayAdapterTest, DurablePrepareFailureProducesNoSubmissionOrOrderCallback) {
    CtpGatewayAdapter adapter(10);
    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "test_user";
    cfg.investor_id = "test_investor";
    cfg.password = "test_password";
    ASSERT_TRUE(adapter.Connect(cfg));

    std::atomic<int> prepare_calls{0};
    std::atomic<int> submitted_mapping_calls{0};
    std::atomic<int> order_callbacks{0};
    adapter.RegisterOrderSubmitPrepareCallback(
        [&](const CtpOrderSubmitMapping& mapping, std::string* error) {
            ++prepare_calls;
            EXPECT_EQ(mapping.phase, OrderSubmitMappingPhase::kPrepared);
            if (error != nullptr) {
                *error = "simulated WAL flush failure";
            }
            return false;
        });
    adapter.RegisterOrderSubmitMappingCallback(
        [&](const CtpOrderSubmitMapping&) { ++submitted_mapping_calls; });
    adapter.RegisterOrderEventCallback([&](const OrderEvent&) { ++order_callbacks; });

    OrderIntent intent;
    intent.account_id = "a1";
    intent.client_order_id = "ord-wal-fail";
    intent.strategy_id = "strat";
    intent.instrument_id = "SHFE.ag2406";
    intent.volume = 1;
    intent.price = 4010.0;
    intent.trading_day = "20260719";

    EXPECT_FALSE(adapter.PlaceOrder(intent));
    EXPECT_EQ(prepare_calls.load(), 1);
    EXPECT_EQ(submitted_mapping_calls.load(), 0);
    EXPECT_EQ(order_callbacks.load(), 0);
    EXPECT_FALSE(adapter.CancelOrder(intent.client_order_id, "cancel-after-prepare-failure"));
}

TEST(CtpGatewayAdapterTest, ConnectionStateListenersAreIndependentAndRemovable) {
    CtpGatewayAdapter adapter(10);
    std::atomic<int> first{0};
    std::atomic<int> second{0};
    const auto first_token = adapter.AddConnectionStateListener([&](bool) { first.fetch_add(1); });
    const auto second_token =
        adapter.AddConnectionStateListener([&](bool) { second.fetch_add(1); });

    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "test_user";
    cfg.investor_id = "test_investor";
    cfg.password = "test_password";
    ASSERT_TRUE(adapter.Connect(cfg));
    EXPECT_GE(first.load(), 1);
    EXPECT_GE(second.load(), 1);

    adapter.RemoveConnectionStateListener(first_token);
    const int first_before_disconnect = first.load();
    const int second_before_disconnect = second.load();
    adapter.Disconnect();
    EXPECT_EQ(first.load(), first_before_disconnect);
    EXPECT_EQ(second.load(), second_before_disconnect + 1);
    adapter.RemoveConnectionStateListener(second_token);
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
    std::atomic<int> margin_rate_callbacks{0};
    std::atomic<int> commission_rate_callbacks{0};
    std::atomic<int> order_comm_rate_callbacks{0};
    adapter.RegisterTradingAccountSnapshotCallback([&](const TradingAccountSnapshot& snapshot) {
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
    adapter.RegisterInstrumentMarginRateSnapshotCallback(
        [&](const std::vector<InstrumentMarginRateSnapshot>& snapshots) {
            ASSERT_EQ(snapshots.size(), 1U);
            EXPECT_EQ(snapshots.front().instrument_id, "SHFE.ag2406");
            EXPECT_DOUBLE_EQ(snapshots.front().long_margin_ratio_by_money, 0.1);
            margin_rate_callbacks.fetch_add(1);
        });
    adapter.RegisterInstrumentCommissionRateSnapshotCallback(
        [&](const std::vector<InstrumentCommissionRateSnapshot>& snapshots) {
            ASSERT_EQ(snapshots.size(), 1U);
            EXPECT_EQ(snapshots.front().instrument_id, "SHFE.ag2406");
            EXPECT_DOUBLE_EQ(snapshots.front().open_ratio_by_money, 0.0001);
            commission_rate_callbacks.fetch_add(1);
        });
    adapter.RegisterInstrumentOrderCommRateSnapshotCallback(
        [&](const std::vector<InstrumentOrderCommRateSnapshot>& snapshots) {
            ASSERT_EQ(snapshots.size(), 1U);
            EXPECT_EQ(snapshots.front().instrument_id, "SHFE.ag2406");
            EXPECT_DOUBLE_EQ(snapshots.front().order_comm_by_volume, 0.0);
            order_comm_rate_callbacks.fetch_add(1);
        });

    EXPECT_TRUE(adapter.EnqueueTradingAccountQuery(11));
    EXPECT_TRUE(adapter.EnqueueInvestorPositionQuery(12));
    EXPECT_TRUE(adapter.EnqueueInstrumentQuery(13));
    EXPECT_TRUE(adapter.EnqueueInstrumentQuery(18, "DCE.c2607"));
    EXPECT_TRUE(adapter.EnqueueBrokerTradingParamsQuery(14));
    EXPECT_TRUE(adapter.EnqueueInstrumentMarginRateQuery(15, "SHFE.ag2406"));
    EXPECT_TRUE(adapter.EnqueueInstrumentCommissionRateQuery(16, "SHFE.ag2406"));
    EXPECT_TRUE(adapter.EnqueueInstrumentOrderCommRateQuery(17, "SHFE.ag2406"));

    const auto account = adapter.GetLastTradingAccountSnapshot();
    EXPECT_EQ(account.investor_id, "191202");
    const auto positions = adapter.GetLastInvestorPositionSnapshots();
    EXPECT_TRUE(positions.empty());
    const auto metas = adapter.GetLastInstrumentMetaSnapshots();
    EXPECT_FALSE(metas.empty());
    EXPECT_NE(std::find_if(metas.begin(), metas.end(),
                           [](const auto& meta) { return meta.instrument_id == "SHFE.ag2406"; }),
              metas.end());
    EXPECT_NE(std::find_if(metas.begin(), metas.end(),
                           [](const auto& meta) { return meta.instrument_id == "DCE.c2607"; }),
              metas.end());
    const auto broker_params = adapter.GetLastBrokerTradingParamsSnapshot();
    EXPECT_FALSE(broker_params.margin_price_type.empty());
    EXPECT_EQ(adapter.GetLastInstrumentMarginRateSnapshots().size(), 1U);
    EXPECT_EQ(adapter.GetLastInstrumentCommissionRateSnapshots().size(), 1U);
    EXPECT_EQ(adapter.GetLastInstrumentOrderCommRateSnapshots().size(), 1U);

    EXPECT_EQ(account_callbacks.load(), 1);
    EXPECT_EQ(position_callbacks.load(), 1);
    EXPECT_EQ(instrument_callbacks.load(), 2);
    EXPECT_EQ(broker_param_callbacks.load(), 1);
    EXPECT_EQ(margin_rate_callbacks.load(), 1);
    EXPECT_EQ(commission_rate_callbacks.load(), 1);
    EXPECT_EQ(order_comm_rate_callbacks.load(), 1);
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

    adapter.RegisterOrderEventCallback([&](const OrderEvent& event) {
        if (event.status != OrderStatus::kAccepted || first_accept_seen.exchange(true)) {
            return;
        }

        cancel_thread = std::thread([&]() {
            const bool ok = adapter.CancelOrder(event.client_order_id, "trace-cancel");
            cancel_result.store(ok);
            {
                std::lock_guard<std::mutex> lock(wait_mutex);
                cancel_done = true;
            }
            wait_cv.notify_one();
        });

        std::unique_lock<std::mutex> lock(wait_mutex);
        cancel_finished_in_callback = wait_cv.wait_for(lock, 50ms, [&]() { return cancel_done; });
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
    EXPECT_TRUE(snapshot.action_day.empty());
    EXPECT_EQ(snapshot.exchange_ts_ns, 0);
    EXPECT_EQ(snapshot.update_millisec, 500);
    EXPECT_DOUBLE_EQ(snapshot.settlement_price, 4890.5);
    EXPECT_TRUE(snapshot.is_valid_settlement);
    EXPECT_DOUBLE_EQ(snapshot.average_price_norm, 0.0);
    EXPECT_FALSE(snapshot.average_price_norm_valid);
}

TEST(CtpGatewayAdapterTest, ParsesExchangeTimestampAsAsiaShanghaiWithMilliseconds) {
    const auto actual =
        CtpGatewayAdapter::ParseMarketExchangeTimestamp("20260701", "09:31:05", 500);
    // 2026-07-01 09:31:05.500 Asia/Shanghai == 2026-07-01 01:31:05.500 UTC.
    EXPECT_EQ(actual, 1782869465500000000LL);
    EXPECT_EQ(CtpGatewayAdapter::ParseMarketExchangeTimestamp("", "09:31:05", 500), 0);
    EXPECT_EQ(CtpGatewayAdapter::ParseMarketExchangeTimestamp("20260701", "25:31:05", 500), 0);
}

}  // namespace quant_hft
