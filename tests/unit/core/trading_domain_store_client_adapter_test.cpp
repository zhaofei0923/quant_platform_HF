#include "quant_hft/core/trading_domain_store_client_adapter.h"

#include <memory>
#include <string>

#include <gtest/gtest.h>

namespace quant_hft {
namespace {

TEST(TradingDomainStoreClientAdapterTest, WritesDomainRowsToConfiguredSchema) {
    auto sql_client = std::make_shared<InMemoryTimescaleSqlClient>();
    StorageRetryPolicy retry_policy;
    TradingDomainStoreClientAdapter adapter(sql_client, retry_policy, "trading_core");

    Order order;
    order.order_id = "ord-1";
    order.account_id = "acc-1";
    order.strategy_id = "s1";
    order.symbol = "SHFE.ag2406";
    order.exchange = "SHFE";
    order.quantity = 2;
    order.filled_quantity = 1;
    order.price = 5000.0;
    order.message = "accepted";

    std::string error;
    EXPECT_TRUE(adapter.UpsertOrder(order, &error)) << error;

    Trade trade;
    trade.trade_id = "tr-1";
    trade.order_id = "ord-1";
    trade.account_id = "acc-1";
    trade.strategy_id = "s1";
    trade.symbol = "SHFE.ag2406";
    trade.exchange = "SHFE";
    trade.quantity = 1;
    trade.price = 5000.0;
    EXPECT_TRUE(adapter.AppendTrade(trade, &error)) << error;

    Position position;
    position.account_id = "acc-1";
    position.strategy_id = "s1";
    position.symbol = "SHFE.ag2406";
    position.exchange = "SHFE";
    position.long_qty = 1;
    EXPECT_TRUE(adapter.UpsertPosition(position, &error)) << error;

    Account account;
    account.account_id = "acc-1";
    account.balance = 100000.0;
    account.available = 90000.0;
    EXPECT_TRUE(adapter.UpsertAccount(account, &error)) << error;

    RiskEventRecord risk_event;
    risk_event.account_id = "acc-1";
    risk_event.strategy_id = "s1";
    risk_event.event_type = 1;
    risk_event.event_level = 2;
    risk_event.event_desc = "risk check";
    EXPECT_TRUE(adapter.AppendRiskEvent(risk_event, &error)) << error;

    EXPECT_EQ(sql_client->QueryAllRows("trading_core.orders", &error).size(), 1U);
    EXPECT_EQ(sql_client->QueryAllRows("trading_core.trades", &error).size(), 1U);
    EXPECT_EQ(sql_client->QueryAllRows("trading_core.position_summary", &error).size(), 1U);
    EXPECT_EQ(sql_client->QueryAllRows("trading_core.account_funds", &error).size(), 1U);
    EXPECT_EQ(sql_client->QueryAllRows("trading_core.risk_events", &error).size(), 1U);
}

TEST(TradingDomainStoreClientAdapterTest, RejectsMissingRequiredFields) {
    auto sql_client = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter adapter(sql_client, StorageRetryPolicy{}, "trading_core");

    Order invalid_order;
    std::string error;
    EXPECT_FALSE(adapter.UpsertOrder(invalid_order, &error));
    EXPECT_FALSE(error.empty());
}

TEST(TradingDomainStoreClientAdapterTest, PersistsTimestampWithUtcOffsetSuffix) {
    auto sql_client = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter adapter(sql_client, StorageRetryPolicy{}, "trading_core");

    Order order;
    order.order_id = "ord-ts-1";
    order.account_id = "acc-1";
    order.strategy_id = "s1";
    order.symbol = "SHFE.ag2406";
    order.exchange = "SHFE";
    order.quantity = 1;
    order.price = 5000.0;
    order.created_at_ns = 1'738'750'123'456'789'000LL;
    order.updated_at_ns = order.created_at_ns;

    std::string error;
    ASSERT_TRUE(adapter.UpsertOrder(order, &error)) << error;
    const auto rows = sql_client->QueryAllRows("trading_core.orders", &error);
    ASSERT_EQ(rows.size(), 1U) << error;

    const auto insert_time_it = rows[0].find("insert_time");
    ASSERT_NE(insert_time_it, rows[0].end());
    const std::string& insert_time = insert_time_it->second;
    ASSERT_GE(insert_time.size(), 6U);
    EXPECT_EQ(insert_time.substr(insert_time.size() - 6), "+00:00");
}

TEST(TradingDomainStoreClientAdapterTest, SkipsDuplicateOrderAndTradeByBusinessKeys) {
    auto sql_client = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter adapter(sql_client, StorageRetryPolicy{}, "trading_core");

    Order order;
    order.order_id = "ord-dup-1";
    order.account_id = "acc-1";
    order.strategy_id = "s1";
    order.symbol = "SHFE.ag2406";
    order.exchange = "SHFE";
    order.quantity = 1;
    order.price = 5000.0;

    std::string error;
    ASSERT_TRUE(adapter.UpsertOrder(order, &error)) << error;
    ASSERT_TRUE(adapter.UpsertOrder(order, &error)) << error;

    Trade trade;
    trade.trade_id = "tr-dup-1";
    trade.order_id = order.order_id;
    trade.account_id = order.account_id;
    trade.strategy_id = order.strategy_id;
    trade.symbol = order.symbol;
    trade.exchange = order.exchange;
    trade.quantity = 1;
    trade.price = 5000.0;
    ASSERT_TRUE(adapter.AppendTrade(trade, &error)) << error;
    ASSERT_TRUE(adapter.AppendTrade(trade, &error)) << error;

    EXPECT_EQ(sql_client->QueryAllRows("trading_core.orders", &error).size(), 1U);
    EXPECT_EQ(sql_client->QueryAllRows("trading_core.trades", &error).size(), 1U);
}

}  // namespace
}  // namespace quant_hft
