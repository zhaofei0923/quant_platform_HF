#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/services/order_manager.h"

namespace quant_hft {
namespace {

class FakeTradingDomainStore final : public ITradingDomainStore {
public:
    bool UpsertOrder(const Order& order, std::string* error) override {
        (void)error;
        orders.push_back(order);
        return true;
    }

    bool AppendTrade(const Trade& trade, std::string* error) override {
        (void)error;
        trades.push_back(trade);
        return true;
    }

    bool UpsertPosition(const Position& position, std::string* error) override {
        (void)position;
        (void)error;
        return true;
    }

    bool UpsertAccount(const Account& account, std::string* error) override {
        (void)account;
        (void)error;
        return true;
    }

    bool AppendRiskEvent(const RiskEventRecord& risk_event, std::string* error) override {
        (void)risk_event;
        (void)error;
        return true;
    }

    bool MarkProcessedOrderEvent(const ProcessedOrderEventRecord& event, std::string* error) override {
        (void)error;
        processed.insert(event.event_key);
        return true;
    }

    bool ExistsProcessedOrderEvent(const std::string& event_key,
                                   bool* exists,
                                   std::string* error) const override {
        (void)error;
        if (exists != nullptr) {
            *exists = processed.find(event_key) != processed.end();
        }
        return true;
    }

    bool InsertPositionDetailFromTrade(const Trade& trade, std::string* error) override {
        (void)trade;
        (void)error;
        return true;
    }

    bool ClosePositionDetailFifo(const Trade& trade, std::string* error) override {
        (void)trade;
        (void)error;
        return true;
    }

    bool LoadPositionSummary(const std::string& account_id,
                             const std::string& strategy_id,
                             std::vector<Position>* out,
                             std::string* error) const override {
        (void)account_id;
        (void)strategy_id;
        (void)error;
        if (out != nullptr) {
            out->clear();
        }
        return true;
    }

    bool UpdateOrderCancelRetry(const std::string& client_order_id,
                                std::int32_t cancel_retry_count,
                                EpochNanos last_cancel_ts_ns,
                                std::string* error) override {
        (void)client_order_id;
        (void)cancel_retry_count;
        (void)last_cancel_ts_ns;
        (void)error;
        return true;
    }

    std::vector<Order> orders;
    std::vector<Trade> trades;
    mutable std::unordered_set<std::string> processed;
};

OrderIntent BuildIntent(const std::string& order_id) {
    OrderIntent intent;
    intent.account_id = "acc1";
    intent.strategy_id = "s1";
    intent.instrument_id = "SHFE.ag2406";
    intent.client_order_id = order_id;
    intent.volume = 2;
    intent.price = 5000.0;
    intent.ts_ns = 1;
    return intent;
}

OrderEvent BuildAcceptedEvent(const std::string& order_id) {
    OrderEvent event;
    event.account_id = "acc1";
    event.client_order_id = order_id;
    event.order_ref = order_id;
    event.instrument_id = "SHFE.ag2406";
    event.exchange_id = "SHFE";
    event.front_id = 1;
    event.session_id = 2;
    event.status = OrderStatus::kAccepted;
    event.total_volume = 2;
    event.filled_volume = 0;
    event.event_source = "OnRtnOrder";
    event.exchange_ts_ns = 100;
    event.ts_ns = 100;
    return event;
}

TEST(OrderManagerTest, ValidStateTransitionAndPersistence) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    OrderManager manager(store);
    (void)manager.CreateOrder(BuildIntent("ord-1"));

    auto accepted = BuildAcceptedEvent("ord-1");
    Order order;
    std::string error;
    ASSERT_TRUE(manager.OnOrderEvent(accepted, &order, &error)) << error;
    EXPECT_EQ(order.status, OrderStatus::kAccepted);
    EXPECT_FALSE(store->orders.empty());
}

TEST(OrderManagerTest, DuplicateEventIgnoredByIdempotency) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    OrderManager manager(store);
    (void)manager.CreateOrder(BuildIntent("ord-dup"));

    auto accepted = BuildAcceptedEvent("ord-dup");
    Order order;
    std::string error;
    ASSERT_TRUE(manager.OnOrderEvent(accepted, &order, &error)) << error;
    const auto first_count = store->processed.size();
    ASSERT_TRUE(manager.OnOrderEvent(accepted, &order, &error)) << error;
    EXPECT_EQ(store->processed.size(), first_count);
}

TEST(OrderManagerTest, InvalidTransitionRejected) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    OrderManager manager(store);
    (void)manager.CreateOrder(BuildIntent("ord-invalid"));

    auto canceled = BuildAcceptedEvent("ord-invalid");
    canceled.status = OrderStatus::kCanceled;
    canceled.ts_ns = 101;
    canceled.exchange_ts_ns = 101;
    Order order;
    std::string error;
    ASSERT_TRUE(manager.OnOrderEvent(canceled, &order, &error)) << error;

    auto late_fill = canceled;
    late_fill.status = OrderStatus::kFilled;
    late_fill.filled_volume = 2;
    late_fill.ts_ns = 102;
    late_fill.exchange_ts_ns = 102;
    EXPECT_FALSE(manager.OnOrderEvent(late_fill, &order, &error));
}

TEST(OrderManagerTest, TradeEventIdempotentByTradeId) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    OrderManager manager(store);
    (void)manager.CreateOrder(BuildIntent("ord-trade"));

    auto trade_event = BuildAcceptedEvent("ord-trade");
    trade_event.event_source = "OnRtnTrade";
    trade_event.trade_id = "trade-1";
    trade_event.status = OrderStatus::kFilled;
    trade_event.total_volume = 1;
    trade_event.filled_volume = 1;
    trade_event.avg_fill_price = 5001.0;
    trade_event.ts_ns = 200;
    trade_event.exchange_ts_ns = 200;

    Trade trade;
    std::string error;
    ASSERT_TRUE(manager.OnTradeEvent(trade_event, &trade, &error)) << error;
    ASSERT_TRUE(manager.OnTradeEvent(trade_event, &trade, &error)) << error;
    EXPECT_EQ(store->trades.size(), 1U);
}

TEST(OrderManagerTest, GetActiveOrdersByStrategyFiltersCorrectly) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    OrderManager manager(store);

    auto first = BuildIntent("ord-s1");
    first.strategy_id = "s1";
    first.instrument_id = "SHFE.ag2406";
    (void)manager.CreateOrder(first);

    auto second = BuildIntent("ord-s2");
    second.strategy_id = "s2";
    second.instrument_id = "SHFE.rb2405";
    (void)manager.CreateOrder(second);

    const auto s1_orders = manager.GetActiveOrdersByStrategy("s1");
    ASSERT_EQ(s1_orders.size(), 1U);
    EXPECT_EQ(s1_orders.front().strategy_id, "s1");

    const auto filtered = manager.GetActiveOrdersByStrategy("s1", "SHFE.ag2406");
    ASSERT_EQ(filtered.size(), 1U);
    EXPECT_EQ(filtered.front().symbol, "SHFE.ag2406");
}

}  // namespace
}  // namespace quant_hft

