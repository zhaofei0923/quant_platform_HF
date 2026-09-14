#include "quant_hft/core/ctp_projection_recovery.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "quant_hft/core/trading_domain_store_client_adapter.h"
#include "quant_hft/services/ctp_position_ledger.h"
#include "quant_hft/services/position_reconciliation.h"

namespace quant_hft {
namespace {

TradeApplyRequest Fill(const std::string& id, std::uint64_t sequence) {
    TradeApplyRequest request;
    request.trade.trade_id = request.trade.raw_trade_id = id;
    request.trade.order_id = id;
    request.trade.account_id = "test-account";
    request.trade.broker_id = "test-broker";
    request.trade.strategy_id = "strategy";
    request.trade.symbol = "hc2701";
    request.trade.exchange = "SHFE";
    request.trade.trading_day = "20260908";
    request.trade.price = 3370;
    request.trade.quantity = 2;
    request.trade.trade_ts_ns = static_cast<EpochNanos>(sequence + 1);
    request.receipt.stream_id = "replay";
    request.receipt.sequence = sequence;
    request.receipt.checksum = static_cast<std::uint32_t>(sequence + 1);
    request.receipt.durable = true;
    return request;
}

InvestorPositionSnapshot BrokerPosition(const std::string& date, int quantity) {
    InvestorPositionSnapshot row;
    row.account_id = "test-account";
    row.instrument_id = "hc2701";
    row.exchange_id = "SHFE";
    row.hedge_flag = "1";
    row.posi_direction = "2";
    row.position_date = date;
    row.position = quantity;
    row.today_position = date == "1" ? quantity : 0;
    row.yd_position = date == "2" ? quantity : 0;
    return row;
}

TEST(CtpProjectionRecoveryTest, CrossDayHistoryRebuildsDomainWithoutInventingBrokerBuckets) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    CtpPositionLedger broker;
    broker.UseCommittedTradeAccounting(true);
    CtpProjectionRecovery recovery;
    std::string error;
    const auto open = Fill("historical-open", 0);
    auto close = Fill("historical-close", 1);
    close.trade.trading_day = "20260909";
    close.trade.side = Side::kSell;
    close.trade.offset = OffsetFlag::kCloseYesterday;
    close.trade.quantity = 1;
    TradeApplyResult opened, closed;
    ASSERT_TRUE(store.ApplyTrade(open, &opened, &error)) << error;
    ASSERT_EQ(opened.status, TradeApplyStatus::kApplied);
    EXPECT_FALSE(recovery.ShouldProjectEvent(true, opened.identity_key));
    ASSERT_TRUE(store.ApplyTrade(close, &closed, &error)) << error;
    ASSERT_EQ(closed.status, TradeApplyStatus::kApplied);
    ASSERT_EQ(closed.broker_close_allocation.yesterday, 1);
    EXPECT_FALSE(recovery.ShouldProjectEvent(true, closed.identity_key));
    EXPECT_EQ(
        broker.GetPosition("test-account", "hc2701", PositionDirection::kLong, "today", "SHFE")
            .position,
        0);

    // The old startup path puts the prior day's open into today, so yesterday's
    // valid domain close fails before the core has even connected to the broker.
    CtpPositionLedger old_projection;
    old_projection.UseCommittedTradeAccounting(true);
    ASSERT_TRUE(old_projection.ApplyCommittedTrade(opened.identity_key, open.trade, {}, &error));
    EXPECT_FALSE(old_projection.ApplyCommittedTrade(closed.identity_key, close.trade,
                                                    closed.broker_close_allocation, &error));
    EXPECT_EQ(error, "broker projection disagrees with committed close allocation");

    std::vector<Position> committed;
    ASSERT_TRUE(store.LoadBrokerPositionSummary("test-account", &committed, &error)) << error;
    QueryResult<InvestorPositionSnapshot> query;
    query.metadata.success = query.metadata.complete = query.metadata.full_account = true;
    query.rows = {BrokerPosition("2", 1)};
    ASSERT_TRUE(ReconcileBrokerPositions("test-account", query, committed).matched);
    ASSERT_TRUE(broker.ReplaceInvestorPositionSnapshotBatch("test-account", query.rows, &error));
    recovery.OnAuthoritativeSnapshotReconciled();

    // Domain redelivery still validates/deduplicates, while the queried broker
    // quantity already includes the history and must not be changed a second time.
    TradeApplyResult duplicate;
    ASSERT_TRUE(store.ApplyTrade(close, &duplicate, &error)) << error;
    ASSERT_EQ(duplicate.status, TradeApplyStatus::kDuplicate);
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, duplicate.identity_key));
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, opened.identity_key));
    EXPECT_EQ(
        broker.GetPosition("test-account", "hc2701", PositionDirection::kLong, "yesterday", "SHFE")
            .position,
        1);
    EXPECT_EQ(sql->QueryAllRows("trading_core.trades", nullptr).size(), 2U);
    DomainWatermark watermark;
    ASSERT_TRUE(store.LoadWatermark("replay", &watermark, &error));
    EXPECT_EQ(watermark.next_sequence, 2U);
}

TEST(CtpProjectionRecoveryTest, PreBaselineQueryTradesAreCoveredButSubsequentLiveTradesProject) {
    CtpProjectionRecovery recovery;
    CtpPositionLedger broker;
    broker.UseCommittedTradeAccounting(true);
    std::string error;
    auto queried = Fill("queried", 0).trade;
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, "queried-identity"));
    EXPECT_FALSE(recovery.ShouldProjectEvent(false));
    ASSERT_TRUE(broker.ReplaceInvestorPositionSnapshotBatch("test-account",
                                                            {BrokerPosition("1", 2)}, &error));
    recovery.OnAuthoritativeSnapshotReconciled();
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, "queried-identity"));
    EXPECT_TRUE(recovery.ShouldProjectEvent(false));
    auto live = queried;
    live.quantity = 1;
    ASSERT_TRUE(recovery.ShouldProjectEvent(false, "new-live-identity"));
    ASSERT_TRUE(broker.ApplyCommittedTrade("new-live-identity", live, {}, &error));
    ASSERT_TRUE(broker.ApplyCommittedTrade("new-live-identity", live, {}, &error));
    EXPECT_EQ(
        broker.GetPosition("test-account", "hc2701", PositionDirection::kLong, "today", "SHFE")
            .position,
        3);
}

TEST(CtpProjectionRecoveryTest, InvalidOrPartialBaselineCannotActivateBrokerProjection) {
    CtpProjectionRecovery recovery;
    CtpPositionLedger broker;
    broker.UseCommittedTradeAccounting(true);
    QueryResult<InvestorPositionSnapshot> query;
    query.metadata.success = query.metadata.complete = true;
    query.rows = {BrokerPosition("2", 1)};
    EXPECT_FALSE(ReconcileBrokerPositions("test-account", query, {}).matched);
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, "while-partial"));
    query.metadata.full_account = true;
    query.metadata.success = false;
    EXPECT_FALSE(ReconcileBrokerPositions("test-account", query, {}).matched);
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, "while-failed"));
    query.metadata.success = true;
    query.metadata.complete = false;
    EXPECT_FALSE(ReconcileBrokerPositions("test-account", query, {}).matched);
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, "while-incomplete"));
    query.metadata.complete = true;
    EXPECT_FALSE(ReconcileBrokerPositions("test-account", query, {}).matched);
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, "while-mismatched"));
    auto wrong_account = BrokerPosition("2", 1);
    wrong_account.account_id = "different-account";
    std::string error;
    EXPECT_FALSE(
        broker.ReplaceInvestorPositionSnapshotBatch("test-account", {wrong_account}, &error));
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, "while-invalid"));
}

TEST(CtpProjectionRecoveryTest, TradeBeforeStaleQueryRequiresMatchingSnapshotBeforeActivation) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    TradingDomainStoreClientAdapter store(sql, {}, "trading_core");
    CtpProjectionRecovery recovery;
    CtpPositionLedger broker;
    broker.UseCommittedTradeAccounting(true);
    auto request = Fill("recovery-query-fill", 0);
    TradeApplyResult committed;
    std::string error;
    ASSERT_TRUE(store.ApplyTrade(request, &committed, &error)) << error;
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, committed.identity_key));
    std::vector<Position> positions;
    ASSERT_TRUE(store.LoadBrokerPositionSummary("test-account", &positions, &error));
    QueryResult<InvestorPositionSnapshot> query;
    query.metadata.success = query.metadata.complete = query.metadata.full_account = true;
    // This full query started before the just-committed fill and still reports flat.
    ASSERT_FALSE(ReconcileBrokerPositions("test-account", query, positions).matched);
    EXPECT_FALSE(recovery.ShouldProjectEvent(false));
    EXPECT_EQ(
        broker.GetPosition("test-account", "hc2701", PositionDirection::kLong, "today", "SHFE")
            .position,
        0);
    query.rows = {BrokerPosition("1", 2)};
    ASSERT_TRUE(ReconcileBrokerPositions("test-account", query, positions).matched);
    ASSERT_TRUE(broker.ReplaceInvestorPositionSnapshotBatch("test-account", query.rows, &error));
    recovery.OnAuthoritativeSnapshotReconciled();
    EXPECT_TRUE(recovery.ShouldProjectEvent(false));
    EXPECT_FALSE(recovery.ShouldProjectEvent(false, committed.identity_key));
    EXPECT_EQ(
        broker.GetPosition("test-account", "hc2701", PositionDirection::kLong, "today", "SHFE")
            .position,
        2);
}

TEST(CtpProjectionRecoveryTest, LiveCloseAllocationMismatchIsNotSuppressedOrMarkedApplied) {
    CtpProjectionRecovery recovery;
    CtpPositionLedger broker;
    broker.UseCommittedTradeAccounting(true);
    std::string error;
    ASSERT_TRUE(broker.ReplaceInvestorPositionSnapshotBatch("test-account",
                                                            {BrokerPosition("2", 1)}, &error));
    recovery.OnAuthoritativeSnapshotReconciled();
    auto close = Fill("live-close", 0).trade;
    close.quantity = 1;
    close.side = Side::kSell;
    close.offset = OffsetFlag::kCloseYesterday;
    CloseAllocation wrong;
    wrong.today = 1;
    ASSERT_TRUE(recovery.ShouldProjectEvent(false, "live-close-identity"));
    EXPECT_FALSE(broker.ApplyCommittedTrade("live-close-identity", close, wrong, &error));
    EXPECT_EQ(error, "broker projection disagrees with committed close allocation");
    EXPECT_EQ(
        broker.GetPosition("test-account", "hc2701", PositionDirection::kLong, "yesterday", "SHFE")
            .position,
        1);
    CloseAllocation correct;
    correct.yesterday = 1;
    ASSERT_TRUE(recovery.ShouldProjectEvent(false, "live-close-identity"));
    ASSERT_TRUE(broker.ApplyCommittedTrade("live-close-identity", close, correct, &error));
    EXPECT_EQ(
        broker.GetPosition("test-account", "hc2701", PositionDirection::kLong, "yesterday", "SHFE")
            .position,
        0);
}

}  // namespace
}  // namespace quant_hft
