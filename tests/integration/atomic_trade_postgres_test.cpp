#include <gtest/gtest.h>

#include <atomic>
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

// The caller provisions a disposable database with migrations 004, 007, 008 and 009 plus
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

TEST_F(AtomicTradePostgresTest, VerifiedFeesAndTheirEvidenceCommitWithExactMixedCloseAllocation) {
    auto open = Fill("verified-yesterday", 0);
    open.trade.exchange = "DCE";
    open.trade.symbol = "m2701";
    open.trade.price = 100;
    open.require_verified_accounting = true;
    auto& policy = open.accounting_policy;
    policy.valuation_inputs_verified = true;
    policy.contract_multiplier = 10;
    policy.valuation_source = "isolated-pg-fixture-v1";
    policy.fee_model = TradeFeeModel::kMoneyPlusVolumeV1;
    policy.close_fee = {0.001, 2};
    policy.close_today_fee = {0.002, 5};
    policy.fee_date_basis = "close_allocation_v1";
    policy.fee_allocation_source = "isolated-pg-fixture";
    policy.fee_allocation_version = "v1";
    policy.generic_close_priority = GenericClosePriority::kYesterdayFirst;
    policy.close_rule_source = "isolated-pg-fixture";
    policy.close_rule_version = "v1";
    std::string error;
    TradeApplyResult result;
    ASSERT_TRUE(store->ApplyTrade(open, &result, &error)) << error;
    auto today = open;
    today.trade.trade_id = today.trade.raw_trade_id = "verified-today";
    today.trade.trading_day = "20260908";
    today.receipt.sequence = 1;
    today.receipt.checksum = 2;
    ASSERT_TRUE(store->ApplyTrade(today, &result, &error)) << error;
    auto close = today;
    close.trade.trade_id = close.trade.raw_trade_id = "verified-close";
    close.trade.side = Side::kSell;
    close.trade.offset = OffsetFlag::kClose;
    close.trade.price = 120;
    close.trade.quantity = 3;
    close.receipt.sequence = 2;
    close.receipt.checksum = 3;
    ASSERT_TRUE(store->ApplyTrade(close, &result, &error)) << error;
    EXPECT_EQ(result.close_allocation.today, 1);
    EXPECT_EQ(result.close_allocation.yesterday, 2);
    close.accounting_policy = {};
    ASSERT_TRUE(store->ApplyTrade(close, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kDuplicate);
    std::vector<TradeOutboxRecord> history;
    ASSERT_TRUE(store->LoadTradeHistory(account, "", &history, &error)) << error;
    ASSERT_EQ(history.size(), 3U);
    EXPECT_NEAR(history.back().trade.commission, 13.8, 1e-9);
    EXPECT_DOUBLE_EQ(history.back().trade.profit, 600);
    EXPECT_TRUE(history.back().trade.valuation_complete);
    EXPECT_EQ(history.back().fee_model, "money_plus_volume_v1");
    EXPECT_EQ(history.back().fee_date_basis, "close_allocation_v1");
    EXPECT_EQ(history.back().fee_allocation_version, "v1");
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

TEST_F(AtomicTradePostgresTest, IndependentEconomicOwnerAndPhysicalCloseDatesPersistSeparately) {
    std::string error;
    ASSERT_TRUE(store->ConfigureIndependentStrategyBooks(account, {{"A", 50000}, {"B", 50000}},
                                                        &error)) << error;
    auto a = Fill("a-yesterday", 0);
    a.trade.strategy_id = "A";
    a.trade.exchange = "DCE";
    a.trade.symbol = "m2701";
    a.trade.quantity = 1;
    a.trade.price = 100;
    auto& policy = a.accounting_policy;
    policy.valuation_inputs_verified = true;
    policy.contract_multiplier = 10;
    policy.valuation_source = "isolated-pg-independent-v1";
    policy.fee_model = TradeFeeModel::kMoneyPlusVolumeV1;
    policy.open_fee = {0, 1};
    policy.close_fee = {0, 2};
    policy.close_today_fee = {0, 7};
    policy.fee_date_basis = "close_allocation_v1";
    policy.fee_allocation_source = "isolated-pg-independent";
    policy.fee_allocation_version = "v1";
    policy.generic_close_priority = GenericClosePriority::kYesterdayFirst;
    policy.close_rule_source = "isolated-pg-independent";
    policy.close_rule_version = "v1";
    TradeApplyResult result;
    ASSERT_TRUE(store->ApplyTrade(a, &result, &error)) << error;
    auto b = a;
    b.trade.strategy_id = "B";
    b.trade.trade_id = b.trade.raw_trade_id = "b-today";
    b.trade.trading_day = "20260908";
    b.trade.price = 120;
    b.receipt.sequence = 1;
    b.receipt.checksum = 2;
    ASSERT_TRUE(store->ApplyTrade(b, &result, &error)) << error;
    auto close = b;
    close.trade.trade_id = close.trade.raw_trade_id = "b-close";
    close.trade.side = Side::kSell;
    close.trade.offset = OffsetFlag::kClose;
    close.trade.price = 130;
    close.receipt.sequence = 2;
    close.receipt.checksum = 3;
    ASSERT_TRUE(store->ApplyTrade(close, &result, &error)) << error;
    EXPECT_EQ(result.close_allocation.today, 1);
    EXPECT_EQ(result.broker_close_allocation.yesterday, 1);
    ASSERT_TRUE(store->ApplyTrade(close, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kDuplicate);
    StrategyCapitalSnapshot capital;
    std::vector<Position> own;
    ASSERT_TRUE(store->LoadStrategyBook(account, "B", &capital, &own, &error)) << error;
    EXPECT_DOUBLE_EQ(capital.realized_pnl, 100);
    EXPECT_DOUBLE_EQ(capital.commission, 3);
    ASSERT_TRUE(store->LoadPositionSummary(account, "A", &own, &error)) << error;
    ASSERT_EQ(own.size(), 1U);
    EXPECT_EQ(own.front().long_qty, 1);
    EXPECT_DOUBLE_EQ(own.front().avg_long_price, 100);
    ASSERT_TRUE(store->LoadBrokerPositionSummary(account, &own, &error)) << error;
    ASSERT_EQ(own.size(), 1U);
    EXPECT_EQ(own.front().long_today_qty, 1);
    EXPECT_EQ(own.front().long_yd_qty, 0);
}

TEST_F(AtomicTradePostgresTest, AccountWideOpenReservationsSerializeAcrossIndependentOwners) {
    std::string error;
    ASSERT_TRUE(store->ConfigureIndependentStrategyBooks(account, {{"A", 1000}, {"B", 1000}},
                                                        &error)) << error;
    std::atomic<int> accepted{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&, i] {
            StrategyOpenReservationRequest request;
            request.intent.account_id = account;
            request.intent.strategy_id = i % 2 == 0 ? "A" : "B";
            request.intent.client_order_id = "open-" + std::to_string(i);
            request.intent.instrument_id = "rb";
            request.intent.offset = OffsetFlag::kOpen;
            request.intent.price = 100;
            request.intent.volume = 1;
            request.new_margin_and_fee = 600;
            request.max_margin_to_equity_ratio = 1;
            request.account_equity = 2000;
            request.max_account_margin_to_equity_ratio = 0.4;
            std::string reserve_error;
            if (store->ReserveStrategyOpen(request, &reserve_error)) ++accepted;
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(accepted, 1);
    EXPECT_EQ(sql->QueryRows("trading_core.strategy_open_reservations", "account_id", account,
                            &error).size(), 1U) << error;
}
}  // namespace
}  // namespace quant_hft
