#include "quant_hft/core/trading_domain_store_client_adapter.h"

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

namespace quant_hft {
namespace {

TradeApplyRequest Fill(const std::string& id, std::uint64_t sequence = 0) {
    TradeApplyRequest request;
    request.trade.trade_id = id;
    request.trade.raw_trade_id = id;
    request.trade.order_id = "local-ref";
    request.trade.account_id = "acc";
    request.trade.broker_id = "broker";
    request.trade.strategy_id = "strategy";
    request.trade.symbol = "rb";
    request.trade.exchange = "SHFE";
    request.trade.trading_day = "20260907";
    request.trade.price = 4000;
    request.trade.quantity = 2;
    request.trade.trade_ts_ns = 1;
    request.receipt.stream_id = "stream";
    request.receipt.sequence = sequence;
    request.receipt.checksum = static_cast<std::uint32_t>(sequence + 1);
    request.receipt.durable = true;
    return request;
}

class FaultingSql final : public ITimescaleSqlClient {
   public:
    FaultingSql(ITimescaleSqlClient& inner, int fail_on) : inner_(inner), fail_on_(fail_on) {}
    bool RunInTransaction(const Transaction& fn, std::string* error) override {
        return inner_.RunInTransaction(
            [&](ITimescaleSqlClient& tx, std::string* e) {
                FaultingSql fault(tx, fail_on_);
                return fn(fault, e);
            },
            error);
    }
    bool LockTransactionKey(const std::string& key, std::string* e) override {
        return inner_.LockTransactionKey(key, e);
    }
    bool InsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   std::string* e) override {
        return !Fail(e) && inner_.InsertRow(table, row, e);
    }
    bool UpsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   const std::vector<std::string>& keys, const std::vector<std::string>& updates,
                   std::string* e) override {
        return !Fail(e) && inner_.UpsertRow(table, row, keys, updates, e);
    }
    std::vector<std::unordered_map<std::string, std::string>> QueryRows(
        const std::string& table, const std::string& key, const std::string& value,
        std::string* e) const override {
        return inner_.QueryRows(table, key, value, e);
    }
    std::vector<std::unordered_map<std::string, std::string>> QueryAllRows(
        const std::string& table, std::string* e) const override {
        return inner_.QueryAllRows(table, e);
    }
    bool Ping(std::string* e) const override { return inner_.Ping(e); }

   private:
    bool Fail(std::string* e) {
        if (++writes_ != fail_on_) return false;
        if (e != nullptr) *e = "injected write failure";
        return true;
    }
    ITimescaleSqlClient& inner_;
    int fail_on_;
    int writes_{0};
};

TEST(TradingDomainStoreClientAdapterTest, EveryApplyWriteFailureRollsBackAndCanBeReplayed) {
    for (int failure = 1; failure <= 9; ++failure) {
        SCOPED_TRACE(failure);
        auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
        auto fault = std::make_shared<FaultingSql>(*sql, failure);
        TradingDomainStoreClientAdapter failing(fault, {}, "trading_core");
        auto request = Fill("atomic");
        TradeApplyResult result;
        std::string error;
        EXPECT_FALSE(failing.ApplyTrade(request, &result, &error));
        EXPECT_EQ(result.status, TradeApplyStatus::kFailed);
        for (const auto* name :
             {"trades", "position_summary", "position_detail", "trade_applications", "trade_outbox",
              "domain_receipts", "domain_watermarks", "account_brokers"})
            EXPECT_TRUE(sql->QueryAllRows(std::string("trading_core.") + name, nullptr).empty());
        TradingDomainStoreClientAdapter recovered(sql, {}, "trading_core");
        ASSERT_TRUE(recovered.ApplyTrade(request, &result, &error)) << error;
        EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
        EXPECT_EQ(result.position.long_qty, 2);
    }
}

TEST(TradingDomainStoreClientAdapterTest, ConcurrentSameAccountFillsDoNotLoseUpdates) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    std::vector<std::thread> threads;
    for (int i = 0; i != 12; ++i)
        threads.emplace_back([&, i] {
            TradeApplyResult result;
            std::string error;
            ASSERT_TRUE(store.ApplyTrade(Fill(std::to_string(i), i), &result, &error)) << error;
            EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
        });
    for (auto& thread : threads) thread.join();
    std::string error;
    std::vector<Position> positions;
    ASSERT_TRUE(store.LoadPositionSummary("acc", "strategy", &positions, &error));
    ASSERT_EQ(positions.size(), 1U);
    EXPECT_EQ(positions.front().long_qty, 24);
    EXPECT_EQ(positions.front().version, 12U);
    DomainWatermark watermark;
    ASSERT_TRUE(store.LoadWatermark("stream", &watermark, &error));
    EXPECT_EQ(watermark.next_sequence, 12U);
}

TEST(TradingDomainStoreClientAdapterTest, OutboxConsumersAcknowledgeIndependently) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    TradeApplyResult result;
    std::string error;
    ASSERT_TRUE(store.ApplyTrade(Fill("first"), &result, &error));
    ASSERT_TRUE(store.AcknowledgeOutbox(result.outbox_id, &error));
    std::vector<TradeOutboxRecord> pending;
    ASSERT_TRUE(store.LoadPendingOutbox("acc", &pending, &error));
    EXPECT_TRUE(pending.empty());
    ASSERT_TRUE(store.LoadPendingOutboxForConsumer("strategy", "acc", &pending, &error));
    ASSERT_EQ(pending.size(), 1U);
    EXPECT_EQ(pending.front().trade.quantity, 2);
    EXPECT_EQ(pending.front().position.long_qty, 2);
    EXPECT_EQ(pending.front().position.version, 1U);
    ASSERT_TRUE(store.AcknowledgeOutboxForConsumer("strategy", result.outbox_id, &error));
    ASSERT_TRUE(store.LoadPendingOutboxForConsumer("risk", "acc", &pending, &error));
    EXPECT_EQ(pending.size(), 1U);
}

TEST(TradingDomainStoreClientAdapterTest,
     RuntimeEnvironmentBindingCannotBeReusedAcrossBrokersOrEnvironments) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    std::string error;
    ASSERT_TRUE(store.BindRuntimeIdentity("simnow", "broker", "acc", "instance", &error));
    ASSERT_TRUE(store.BindRuntimeIdentity("simnow", "broker", "acc", "restart", &error));
    EXPECT_FALSE(store.BindRuntimeIdentity("production", "broker", "acc", "instance", &error));
    EXPECT_FALSE(store.BindRuntimeIdentity("simnow", "other", "acc", "instance", &error));
}

TEST(TradingDomainStoreClientAdapterTest, AtomicApplyDeduplicatesAndQuarantinesPayloadConflict) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    auto request = Fill("fill");
    std::string error;
    TradeApplyResult result;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
    EXPECT_EQ(result.position.long_qty, 2);
    request.receipt.sequence = 1;
    request.receipt.checksum = 2;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kDuplicate);
    request.trade.price = 4001;
    request.receipt.sequence = 2;
    request.receipt.checksum = 3;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    EXPECT_EQ(result.status, TradeApplyStatus::kConflict);
    EXPECT_EQ(sql->QueryAllRows("trading_core.trades", nullptr).size(), 1U);
    EXPECT_EQ(sql->QueryAllRows("trading_core.position_detail", nullptr).size(), 1U);
    EXPECT_EQ(sql->QueryAllRows("trading_core.trade_outbox", nullptr).size(), 1U);
    EXPECT_EQ(sql->QueryAllRows("trading_core.trade_conflicts", nullptr).size(), 1U);
    DomainWatermark watermark;
    ASSERT_TRUE(store.LoadWatermark("stream", &watermark, &error));
    EXPECT_EQ(watermark.next_sequence, 2U);
}

TEST(TradingDomainStoreClientAdapterTest, ReceiptGapsAdvanceOnlyWhenEveryRecordIsApplied) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    auto request = Fill("fill", 12);
    request.receipt.first_sequence = 10;
    TradeApplyResult result;
    DomainWatermark watermark;
    std::string error;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error)) << error;
    ASSERT_TRUE(store.LoadWatermark("stream", &watermark, &error));
    EXPECT_EQ(watermark.next_sequence, 10U);
    request.receipt.sequence = 10;
    ASSERT_TRUE(store.AcknowledgeReceipt(request.receipt, &error)) << error;
    ASSERT_TRUE(store.LoadWatermark("stream", &watermark, &error));
    EXPECT_EQ(watermark.next_sequence, 11U);
    request.receipt.sequence = 11;
    ASSERT_TRUE(store.AcknowledgeReceipt(request.receipt, &error)) << error;
    ASSERT_TRUE(store.LoadWatermark("stream", &watermark, &error));
    EXPECT_EQ(watermark.next_sequence, 13U);
}

TEST(TradingDomainStoreClientAdapterTest, ReceiptCannotBeReboundToAnotherTradeOrChecksum) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    auto request = Fill("first");
    TradeApplyResult result;
    std::string error;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    EXPECT_EQ(result.status, TradeApplyStatus::kDuplicate);
    request.trade.raw_trade_id = "second";
    EXPECT_FALSE(store.ApplyTrade(request, &result, &error));
    EXPECT_EQ(sql->QueryAllRows("trading_core.trades", nullptr).size(), 1U);
    request.trade.raw_trade_id = "first";
    request.receipt.checksum += 1;
    EXPECT_FALSE(store.ApplyTrade(request, &result, &error));
    DomainWatermark watermark;
    ASSERT_TRUE(store.LoadWatermark("stream", &watermark, &error));
    EXPECT_EQ(watermark.next_sequence, 1U);
}

TEST(TradingDomainStoreClientAdapterTest, DirectionAndTradingDayArePartOfIdentity) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    auto request = Fill("same-raw");
    std::string error;
    TradeApplyResult result;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    request.trade.side = Side::kSell;
    request.receipt.sequence = 1;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
    EXPECT_EQ(result.position.long_qty, 2);
    EXPECT_EQ(result.position.short_qty, 2);
    request.trade.trading_day = "20260908";
    request.receipt.sequence = 2;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
    EXPECT_EQ(result.position.short_yd_qty, 2);
    EXPECT_EQ(result.position.short_today_qty, 2);
    EXPECT_EQ(sql->QueryAllRows("trading_core.trades", nullptr).size(), 3U);
}

TEST(TradingDomainStoreClientAdapterTest, ExactCloseAllocationKeepsSidesAndHedgeBucketsSeparate) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    auto request = Fill("yesterday");
    TradeApplyResult result;
    std::string error;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    request = Fill("today", 1);
    request.trade.trading_day = "20260908";
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    request = Fill("short", 2);
    request.trade.trading_day = "20260908";
    request.trade.side = Side::kSell;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    request = Fill("close-yd", 3);
    request.trade.trading_day = "20260908";
    request.trade.side = Side::kSell;
    request.trade.offset = OffsetFlag::kCloseYesterday;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error)) << error;
    EXPECT_EQ(result.position.long_today_qty, 2);
    EXPECT_EQ(result.position.long_yd_qty, 0);
    EXPECT_EQ(result.position.short_qty, 2);
    EXPECT_EQ(result.close_allocation.yesterday, 2);
    request = Fill("too-much-yd", 4);
    request.trade.trading_day = "20260908";
    request.trade.side = Side::kSell;
    request.trade.offset = OffsetFlag::kCloseYesterday;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    EXPECT_EQ(result.status, TradeApplyStatus::kConflict);
    EXPECT_EQ(sql->QueryAllRows("trading_core.trades", nullptr).size(), 4U);
}

TEST(TradingDomainStoreClientAdapterTest, CloseCannotConsumeAnotherHedgeBook) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    auto open = Fill("hedge-open");
    open.trade.hedge_flag = HedgeFlag::kHedge;
    TradeApplyResult result;
    std::string error;
    ASSERT_TRUE(store.ApplyTrade(open, &result, &error));
    auto close = Fill("close", 1);
    close.trade.side = Side::kSell;
    close.trade.offset = OffsetFlag::kCloseToday;
    ASSERT_TRUE(store.ApplyTrade(close, &result, &error));
    EXPECT_EQ(result.status, TradeApplyStatus::kConflict);
    close.trade.hedge_flag = HedgeFlag::kHedge;
    ASSERT_TRUE(store.ApplyTrade(close, &result, &error));
    EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
    EXPECT_EQ(result.position.long_qty, 0);
}

TEST(TradingDomainStoreClientAdapterTest, RuntimeAccountRejectsRawBackfillAndUnattributedTrade) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    std::string error;
    ASSERT_TRUE(store.BindRuntimeIdentity("test", "broker", "acc", "instance", &error));
    auto request = Fill("fill");
    EXPECT_FALSE(store.AppendTrade(request.trade, &error));
    request.trade.strategy_id.clear();
    TradeApplyResult result;
    EXPECT_FALSE(store.ApplyTrade(request, &result, &error));
    EXPECT_TRUE(sql->QueryAllRows("trading_core.trades", nullptr).empty());
}

TEST(TradingDomainStoreClientAdapterTest, BrokerBaselineCoversHistoryWithoutAddingAgain) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    auto request = Fill("covered");
    PositionBaseline baseline;
    baseline.baseline_id = "verified-query-set";
    baseline.account_id = "acc";
    baseline.broker_id = "broker";
    baseline.trading_day = "20260907";
    baseline.complete = true;
    Position position;
    position.account_id = "acc";
    position.strategy_id = "strategy";
    position.symbol = "rb";
    position.exchange = "SHFE";
    position.long_qty = position.long_today_qty = 2;
    baseline.positions = {position};
    baseline.covered_trades = {request.trade};
    std::string error;
    ASSERT_TRUE(store.InstallPositionBaseline(baseline, &error)) << error;
    TradeApplyResult result;
    request.historical = true;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kCoveredByBaseline);
    std::vector<Position> positions;
    ASSERT_TRUE(store.LoadPositionSummary("acc", "strategy", &positions, &error));
    ASSERT_EQ(positions.size(), 1U);
    EXPECT_EQ(positions.front().long_qty, 2);
    EXPECT_TRUE(sql->QueryAllRows("trading_core.trades", nullptr).empty());
    request.trade.raw_trade_id = "unknown-history";
    request.receipt.sequence = 1;
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    EXPECT_EQ(result.status, TradeApplyStatus::kConflict);
    EXPECT_FALSE(store.InstallPositionBaseline(baseline, &error));
}

TEST(TradingDomainStoreClientAdapterTest, LiveGenericCloseRequiresRuleAndUsesItsExactAllocation) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    TradeApplyResult result;
    std::string error;
    auto yesterday = Fill("yd");
    yesterday.trade.exchange = "DCE";
    ASSERT_TRUE(store.ApplyTrade(yesterday, &result, &error));
    auto today = Fill("td", 1);
    today.trade.exchange = "DCE";
    today.trade.trading_day = "20260908";
    ASSERT_TRUE(store.ApplyTrade(today, &result, &error));
    auto close = Fill("close", 2);
    close.trade.exchange = "DCE";
    close.trade.trading_day = "20260908";
    close.trade.side = Side::kSell;
    close.trade.offset = OffsetFlag::kClose;
    ASSERT_TRUE(store.ApplyTrade(close, &result, &error));
    EXPECT_EQ(result.status, TradeApplyStatus::kConflict);
    close.accounting_policy.generic_close_priority = GenericClosePriority::kYesterdayFirst;
    close.accounting_policy.close_rule_source = "verified-test-policy";
    close.accounting_policy.close_rule_version = "test-v1";
    ASSERT_TRUE(store.ApplyTrade(close, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
    EXPECT_EQ(result.close_allocation.yesterday, 2);
    EXPECT_EQ(result.position.long_today_qty, 2);
    std::vector<TradeOutboxRecord> history;
    ASSERT_TRUE(store.LoadTradeHistory("acc", "", &history, &error));
    ASSERT_EQ(history.size(), 3U);
    EXPECT_EQ(history.back().close_rule_source, "verified-test-policy");
    EXPECT_EQ(history.back().commit_sequence, 3U);
    EXPECT_EQ(history.back().close_allocation.yesterday, 2);
    ASSERT_TRUE(store.ApplyTrade(close, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kDuplicate);
    EXPECT_EQ(result.close_allocation.yesterday, 2);
    EXPECT_EQ(result.close_allocation.today, 0);
    EXPECT_EQ(result.commit_sequence, 3U);
}

TEST(TradingDomainStoreClientAdapterTest,
     VerifiedOpeningPriceValuationPersistsComputedProfitWithoutBlockingUnvaluedFacts) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    TradeApplyResult result;
    std::string error;
    auto open = Fill("open");
    ASSERT_TRUE(store.ApplyTrade(open, &result, &error));
    auto close = Fill("close", 1);
    close.trade.side = Side::kSell;
    close.trade.offset = OffsetFlag::kCloseToday;
    close.trade.price = 3990;
    close.accounting_policy.valuation_inputs_verified = true;
    close.accounting_policy.contract_multiplier = 10;
    close.accounting_policy.commission = 3;
    close.accounting_policy.valuation_source = "verified-test-contract-and-fee-schedule";
    ASSERT_TRUE(store.ApplyTrade(close, &result, &error));
    std::vector<TradeOutboxRecord> history;
    ASSERT_TRUE(store.LoadTradeHistory("acc", "20260907", &history, &error));
    ASSERT_EQ(history.size(), 2U);
    EXPECT_FALSE(history.front().trade.valuation_complete);
    EXPECT_TRUE(history.back().trade.valuation_complete);
    EXPECT_DOUBLE_EQ(history.back().trade.profit, -200);
    EXPECT_DOUBLE_EQ(history.back().trade.commission, 3);
}

TEST(TradingDomainStoreClientAdapterTest,
     VerifiedMixedCloseFeesUseCommittedAllocationAndReplayRawPayload) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    TradeAccountingPolicy policy;
    policy.valuation_inputs_verified = true;
    policy.contract_multiplier = 10;
    policy.valuation_source = "sample:manual-v1";
    policy.generic_close_priority = GenericClosePriority::kYesterdayFirst;
    policy.close_rule_source = "sample";
    policy.close_rule_version = "v1";
    policy.fee_model = TradeFeeModel::kMoneyPlusVolumeV1;
    policy.open_fee = {0, 1};
    policy.close_fee = {0.001, 2};
    policy.close_today_fee = {0.002, 5};
    policy.fee_date_basis = "close_allocation_v1";
    policy.fee_allocation_source = "broker-sample";
    policy.fee_allocation_version = "v1";
    auto open = Fill("fee-yd");
    open.trade.price = 100;
    open.trade.exchange = "DCE";
    open.accounting_policy = policy;
    open.require_verified_accounting = true;
    TradeApplyResult result;
    std::string error;
    ASSERT_TRUE(store.ApplyTrade(open, &result, &error)) << error;
    auto today = open;
    today.trade.raw_trade_id = today.trade.trade_id = "fee-td";
    today.trade.trading_day = "20260908";
    today.receipt.sequence = 1;
    ASSERT_TRUE(store.ApplyTrade(today, &result, &error)) << error;
    auto close = today;
    close.trade.raw_trade_id = close.trade.trade_id = "fee-close";
    close.trade.side = Side::kSell;
    close.trade.offset = OffsetFlag::kClose;
    close.trade.quantity = 3;
    close.trade.price = 120;
    close.receipt.sequence = 2;
    ASSERT_TRUE(store.ApplyTrade(close, &result, &error)) << error;
    ASSERT_EQ(result.status, TradeApplyStatus::kApplied);
    EXPECT_EQ(result.close_allocation.yesterday, 2);
    EXPECT_EQ(result.close_allocation.today, 1);
    std::vector<TradeOutboxRecord> history;
    ASSERT_TRUE(store.LoadTradeHistory("acc", "", &history, &error));
    ASSERT_EQ(history.size(), 3U);
    EXPECT_NEAR(history.back().trade.commission, 13.8, 1e-12);
    EXPECT_DOUBLE_EQ(history.back().trade.profit, 600);
    EXPECT_EQ(history.back().fee_date_basis, "close_allocation_v1");
    EXPECT_EQ(history.back().fee_allocation_version, "v1");
    close.accounting_policy = {};  // Lost live evidence cannot prevent an already committed retry.
    ASSERT_TRUE(store.ApplyTrade(close, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kDuplicate);
    EXPECT_EQ(result.close_allocation.yesterday, 2);
}

TEST(TradingDomainStoreClientAdapterTest,
     RequiredValuationLeavesUnknownNewFillAndWatermarkPending) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    auto request = Fill("pending-valuation");
    request.require_verified_accounting = true;
    TradeApplyResult result;
    std::string error;
    EXPECT_FALSE(store.ApplyTrade(request, &result, &error));
    EXPECT_TRUE(sql->QueryAllRows("trading_core.trade_applications", &error).empty());
    EXPECT_TRUE(sql->QueryAllRows("trading_core.domain_receipts", &error).empty());
    request.accounting_policy.valuation_inputs_verified = true;
    request.accounting_policy.contract_multiplier = 10;
    request.accounting_policy.valuation_source = "verified-test";
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error)) << error;
    EXPECT_EQ(result.status, TradeApplyStatus::kApplied);
    request.accounting_policy = {};
    ASSERT_TRUE(store.ApplyTrade(request, &result, &error));
    EXPECT_EQ(result.status, TradeApplyStatus::kDuplicate);
}

TEST(TradingDomainStoreClientAdapterTest, InMemoryTransactionRollsBackOnFalseAndException) {
    InMemoryTimescaleSqlClient sql;
    std::string error;
    EXPECT_FALSE(sql.RunInTransaction(
        [](ITimescaleSqlClient& tx, std::string* e) {
            EXPECT_TRUE(tx.InsertRow("facts", {{"id", "1"}}, e));
            return false;
        },
        &error));
    EXPECT_TRUE(sql.QueryAllRows("facts", &error).empty());
    EXPECT_THROW(sql.RunInTransaction(
                     [](ITimescaleSqlClient& tx, std::string* e) -> bool {
                         tx.InsertRow("facts", {{"id", "2"}}, e);
                         throw std::runtime_error("injected crash before commit");
                     },
                     &error),
                 std::runtime_error);
    EXPECT_TRUE(sql.QueryAllRows("facts", &error).empty());
}

TEST(TradingDomainStoreClientAdapterTest,
     TradingDayAdvanceIsAtomicIdempotentAndEmitsPositionOutbox) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    TradeApplyResult result;
    std::string error;
    ASSERT_TRUE(store.ApplyTrade(Fill("open"), &result, &error));
    ASSERT_TRUE(store.AdvanceTradingDay("acc", "broker", "20260908", &error)) << error;
    ASSERT_TRUE(store.AdvanceTradingDay("acc", "broker", "20260908", &error));
    EXPECT_FALSE(store.AdvanceTradingDay("acc", "broker", "20260907", &error));
    std::vector<Position> positions;
    ASSERT_TRUE(store.LoadPositionSummary("acc", "strategy", &positions, &error));
    ASSERT_EQ(positions.size(), 1U);
    EXPECT_EQ(positions.front().long_today_qty, 0);
    EXPECT_EQ(positions.front().long_yd_qty, 2);
    EXPECT_EQ(positions.front().version, 2U);
    std::vector<TradeOutboxRecord> pending;
    ASSERT_TRUE(store.LoadPendingOutboxForConsumer("strategy", "acc", &pending, &error));
    ASSERT_EQ(pending.size(), 2U);
    EXPECT_EQ(pending.back().event_kind, "position_rollover");
    EXPECT_EQ(pending.back().position.long_yd_qty, 2);
    EXPECT_EQ(pending.back().commit_sequence, 2U);
    ASSERT_TRUE(store.LoadTradeHistory("acc", "", &pending, &error));
    EXPECT_EQ(pending.size(), 1U);
}

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

TEST(TradingDomainStoreClientAdapterTest, OrderLifecycleUpdatesOneAccountScopedRow) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {});
    Order order;
    order.order_id = "ref";
    order.account_id = "account";
    order.strategy_id = "strategy";
    order.symbol = "rb";
    order.quantity = 3;
    order.created_at_ns = 1;
    std::string error;
    ASSERT_TRUE(store.UpsertOrder(order, &error)) << error;
    auto rows = sql->QueryAllRows("trading_core.orders", &error);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows.front().at("volume_canceled"), "0");
    const auto created = rows.front().at("insert_time");
    order.created_at_ns = 1000000000LL;
    order.status = OrderStatus::kCanceled;
    order.filled_quantity = 1;
    ASSERT_TRUE(store.UpsertOrder(order, &error)) << error;
    rows = sql->QueryAllRows("trading_core.orders", &error);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows.front().at("insert_time"), created);
    EXPECT_EQ(rows.front().at("volume_traded"), "1");
    EXPECT_EQ(rows.front().at("volume_canceled"), "2");
    EXPECT_EQ(rows.front().at("order_status"),
              std::to_string(static_cast<int>(OrderStatus::kCanceled)));
    order.account_id = "other";
    ASSERT_TRUE(store.UpsertOrder(order, &error)) << error;
    EXPECT_EQ(sql->QueryAllRows("trading_core.orders", &error).size(), 2U);
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
