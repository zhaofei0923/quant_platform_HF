#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "quant_hft/core/libpq_timescale_sql_client.h"
#include "quant_hft/core/storage_client_pool.h"
#include "quant_hft/core/trading_domain_store_client_adapter.h"

namespace quant_hft {
namespace {

// The caller provisions a disposable database with migrations 004 and 007 plus
// default order/trade/position_detail partitions. No production connection default.
class AtomicTradePostgresTest : public ::testing::Test {
   protected:
    void SetUp() override {
        const char* dsn = std::getenv("QUANT_HFT_TEST_POSTGRES_DSN");
        if (dsn == nullptr || std::string(dsn).empty())
            GTEST_SKIP() << "isolated PostgreSQL DSN not configured";
        TimescaleConnectionConfig config;
        config.dsn = dsn;
        sql = std::make_shared<LibpqTimescaleSqlClient>(config);
        store = std::make_unique<TradingDomainStoreClientAdapter>(sql, StorageRetryPolicy{});
        account = "test-" + std::to_string(NowEpochNanos());
    }
    TradeApplyRequest Fill(const std::string& id, std::uint64_t sequence) const {
        TradeApplyRequest request;
        auto& trade = request.trade;
        trade.account_id = account;
        trade.broker_id = "test-broker";
        trade.strategy_id = "test-strategy";
        trade.trade_id = trade.raw_trade_id = id;
        trade.order_id = "nonnumeric-local-ref";
        trade.symbol = "rb";
        trade.exchange = "SHFE";
        trade.trading_day = "20260907";
        trade.quantity = 2;
        trade.price = 4000;
        trade.trade_ts_ns = NowEpochNanos();
        request.receipt.stream_id = account;
        request.receipt.sequence = sequence;
        request.receipt.checksum = static_cast<std::uint32_t>(sequence + 1);
        request.receipt.durable = true;
        return request;
    }
    std::shared_ptr<LibpqTimescaleSqlClient> sql;
    std::unique_ptr<TradingDomainStoreClientAdapter> store;
    std::string account;
};

TEST_F(AtomicTradePostgresTest, SameConnectionRollbackIncludesEveryCallbackWrite) {
    std::string error;
    EXPECT_FALSE(sql->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            EXPECT_TRUE(tx.LockTransactionKey(account, e));
            EXPECT_TRUE(tx.InsertRow("trading_core.account_brokers",
                                     {{"account_id", account}, {"broker_id", "test"}}, e))
                << *e;
            return false;
        },
        &error));
    EXPECT_TRUE(
        sql->QueryRows("trading_core.account_brokers", "account_id", account, &error).empty());
    EXPECT_THROW(sql->RunInTransaction(
                     [&](ITimescaleSqlClient& tx, std::string* e) -> bool {
                         tx.InsertRow("trading_core.account_brokers",
                                      {{"account_id", account}, {"broker_id", "test"}}, e);
                         throw std::runtime_error("injected abort");
                     },
                     &error),
                 std::runtime_error);
    EXPECT_TRUE(
        sql->QueryRows("trading_core.account_brokers", "account_id", account, &error).empty());
}

TEST_F(AtomicTradePostgresTest, LifecycleUpsertUpdatesExistingPartitionedOrder) {
    Order order;
    order.order_id = "local-ref";
    order.account_id = account;
    order.strategy_id = "test-strategy";
    order.symbol = "rb";
    order.exchange = "SHFE";
    order.quantity = 3;
    order.price = 4000;
    order.created_at_ns = NowEpochNanos();
    std::string error;
    ASSERT_TRUE(store->UpsertOrder(order, &error)) << error;
    const auto before = sql->QueryRows("trading_core.orders", "account_id", account, &error);
    ASSERT_EQ(before.size(), 1U);
    order.created_at_ns += 1000000;
    order.status = OrderStatus::kCanceled;
    order.filled_quantity = 1;
    ASSERT_TRUE(store->UpsertOrder(order, &error)) << error;
    const auto after = sql->QueryRows("trading_core.orders", "account_id", account, &error);
    ASSERT_EQ(after.size(), 1U);
    EXPECT_EQ(after.front().at("order_id"), before.front().at("order_id"));
    EXPECT_EQ(after.front().at("insert_time"), before.front().at("insert_time"));
    EXPECT_EQ(after.front().at("volume_traded"), "1");
    EXPECT_EQ(after.front().at("volume_canceled"), "2");
    EXPECT_EQ(after.front().at("order_status"),
              std::to_string(static_cast<int>(OrderStatus::kCanceled)));
}

TEST_F(AtomicTradePostgresTest, ApplyDuplicateCloseAndReceiptConflictAreAtomic) {
    std::string error;
    ASSERT_TRUE(store->BindRuntimeIdentity("test", "test-broker", account, "integration", &error))
        << error;
    auto request = Fill("open", 0);
    TradeApplyResult result;
    ASSERT_TRUE(store->ApplyTrade(request, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
    ASSERT_TRUE(store->ApplyTrade(request, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kDuplicate);
    auto close = Fill("close", 1);
    close.trade.side = Side::kSell;
    close.trade.offset = OffsetFlag::kCloseToday;
    close.trade.quantity = 1;
    ASSERT_TRUE(store->ApplyTrade(close, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
    EXPECT_EQ(result.position.long_qty, 1);
    auto conflicting_receipt = Fill("another-open", 1);
    EXPECT_FALSE(store->ApplyTrade(conflicting_receipt, &result, &error));
    EXPECT_EQ(sql->QueryRows("trading_core.trades", "account_id", account, nullptr).size(), 2U);
    std::vector<Position> positions;
    ASSERT_TRUE(store->LoadPositionSummary(account, "test-strategy", &positions, &error)) << error;
    ASSERT_EQ(positions.size(), 1U);
    EXPECT_EQ(positions.front().long_qty, 1);
    DomainWatermark watermark;
    ASSERT_TRUE(store->LoadWatermark(account, &watermark, &error));
    EXPECT_EQ(watermark.next_sequence, 2U);
}

TEST_F(AtomicTradePostgresTest, SeparateConnectionsSerializeAccountAndPreserveEveryFill) {
    std::vector<std::thread> threads;
    for (int i = 0; i != 8; ++i)
        threads.emplace_back([&, i] {
            auto request = Fill("concurrent-" + std::to_string(i), i);
            TradeApplyResult result;
            std::string error;
            EXPECT_TRUE(store->ApplyTrade(request, &result, &error)) << error;
            EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
        });
    for (auto& thread : threads) thread.join();
    std::vector<Position> positions;
    std::string error;
    ASSERT_TRUE(store->LoadPositionSummary(account, "test-strategy", &positions, &error)) << error;
    ASSERT_EQ(positions.size(), 1U);
    EXPECT_EQ(positions.front().long_qty, 16);
    EXPECT_EQ(positions.front().version, 8U);
    DomainWatermark watermark;
    ASSERT_TRUE(store->LoadWatermark(account, &watermark, &error));
    EXPECT_EQ(watermark.next_sequence, 8U);
}

TEST_F(AtomicTradePostgresTest, TradingDayRolloverAndIndependentOutboxConsumersPersist) {
    std::string error;
    TradeApplyResult result;
    ASSERT_TRUE(store->ApplyTrade(Fill("open", 0), &result, &error)) << error;
    ASSERT_TRUE(store->AcknowledgeOutbox(result.outbox_id, &error)) << error;
    ASSERT_TRUE(store->AdvanceTradingDay(account, "test-broker", "20260908", &error)) << error;
    ASSERT_TRUE(store->AdvanceTradingDay(account, "test-broker", "20260908", &error)) << error;
    std::vector<TradeOutboxRecord> pending;
    ASSERT_TRUE(store->LoadPendingOutboxForConsumer("strategy", account, &pending, &error))
        << error;
    ASSERT_EQ(pending.size(), 2U);
    EXPECT_EQ(pending.back().event_kind, "position_rollover");
    EXPECT_EQ(pending.back().position.long_today_qty, 0);
    EXPECT_EQ(pending.back().position.long_yd_qty, 2);
    EXPECT_EQ(pending.back().commit_sequence, 2U);
    ASSERT_TRUE(store->LoadPendingOutbox(account, &pending, &error)) << error;
    EXPECT_EQ(pending.size(), 1U);
    ASSERT_TRUE(store->LoadTradeHistory(account, "", &pending, &error)) << error;
    EXPECT_EQ(pending.size(), 1U);
}
}  // namespace
}  // namespace quant_hft
