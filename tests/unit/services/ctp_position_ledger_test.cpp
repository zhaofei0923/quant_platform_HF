#include "quant_hft/services/ctp_position_ledger.h"

#include <gtest/gtest.h>

#include <string>

namespace quant_hft {
namespace {

InvestorPositionSnapshot MakePositionSnapshot(
    std::int32_t position, const std::string& instrument_id = "SHFE.ag2406",
    const std::string& exchange_id = "SHFE", PositionDirection direction = PositionDirection::kLong,
    const std::string& position_date = "today", std::int32_t frozen = 0) {
    InvestorPositionSnapshot snapshot;
    snapshot.account_id = "acc-1";
    snapshot.investor_id = "acc-1";
    snapshot.instrument_id = instrument_id;
    snapshot.exchange_id = exchange_id;
    snapshot.posi_direction = direction == PositionDirection::kLong ? "2" : "3";
    snapshot.position_date = position_date;
    snapshot.position = position;
    snapshot.today_position = position_date == "today" ? position : 0;
    snapshot.yd_position = position_date == "yesterday" ? position : 0;
    snapshot.long_frozen = direction == PositionDirection::kLong ? frozen : 0;
    snapshot.short_frozen = direction == PositionDirection::kShort ? frozen : 0;
    snapshot.ts_ns = 1;
    snapshot.source = "ctp";
    return snapshot;
}

InvestorPositionSnapshot MakeLongTodaySnapshot(std::int32_t position) {
    return MakePositionSnapshot(position);
}

OrderEvent MakeOrderEvent(const std::string& client_order_id, OrderStatus status,
                          std::int32_t total_volume, std::int32_t filled_volume, EpochNanos ts_ns) {
    OrderEvent event;
    event.client_order_id = client_order_id;
    event.account_id = "acc-1";
    event.instrument_id = "SHFE.ag2406";
    event.exchange_id = "SHFE";
    event.status = status;
    event.total_volume = total_volume;
    event.filled_volume = filled_volume;
    event.ts_ns = ts_ns;
    return event;
}

}  // namespace

TEST(CtpPositionLedgerTest, GenericCloseUsesYesterdayBucketWhenDateOmitted) {
    CtpPositionLedger ledger;
    std::string error;

    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(43, "c2607", "DCE", PositionDirection::kLong, "yesterday"), &error))
        << error;

    CtpOrderIntentForLedger close_intent;
    close_intent.client_order_id = "ord-close-yesterday";
    close_intent.account_id = "acc-1";
    close_intent.instrument_id = "c2607";
    close_intent.exchange_id = "DCE";
    close_intent.direction = PositionDirection::kLong;
    close_intent.offset = OffsetFlag::kClose;
    close_intent.requested_volume = 43;
    ASSERT_TRUE(ledger.RegisterOrderIntent(close_intent, &error)) << error;

    const auto yesterday =
        ledger.GetPosition("acc-1", "c2607", PositionDirection::kLong, "yesterday", "DCE");
    EXPECT_EQ(yesterday.position, 43);
    EXPECT_EQ(yesterday.frozen, 43);
    EXPECT_EQ(yesterday.closable, 0);

    ASSERT_TRUE(ledger.ApplyOrderEvent(
        MakeOrderEvent("ord-close-yesterday", OrderStatus::kFilled, 43, 43, 2), &error))
        << error;
    const auto after_fill =
        ledger.GetPosition("acc-1", "c2607", PositionDirection::kLong, "yesterday", "DCE");
    EXPECT_EQ(after_fill.position, 0);
    EXPECT_EQ(after_fill.frozen, 0);
    EXPECT_EQ(after_fill.closable, 0);
}

TEST(CtpPositionLedgerTest, GenericCloseAllocatesTodayBeforeYesterday) {
    CtpPositionLedger ledger;
    std::string error;

    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(10, "c2607", "DCE", PositionDirection::kLong, "today"), &error))
        << error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(40, "c2607", "DCE", PositionDirection::kLong, "yesterday"), &error))
        << error;

    CtpOrderIntentForLedger close_intent;
    close_intent.client_order_id = "ord-close-mixed";
    close_intent.account_id = "acc-1";
    close_intent.instrument_id = "c2607";
    close_intent.exchange_id = "DCE";
    close_intent.direction = PositionDirection::kLong;
    close_intent.offset = OffsetFlag::kClose;
    close_intent.requested_volume = 43;
    ASSERT_TRUE(ledger.RegisterOrderIntent(close_intent, &error)) << error;

    auto today = ledger.GetPosition("acc-1", "c2607", PositionDirection::kLong, "today", "DCE");
    auto yesterday =
        ledger.GetPosition("acc-1", "c2607", PositionDirection::kLong, "yesterday", "DCE");
    EXPECT_EQ(today.frozen, 10);
    EXPECT_EQ(yesterday.frozen, 33);

    ASSERT_TRUE(ledger.ApplyOrderEvent(
        MakeOrderEvent("ord-close-mixed", OrderStatus::kPartiallyFilled, 43, 12, 2), &error))
        << error;
    today = ledger.GetPosition("acc-1", "c2607", PositionDirection::kLong, "today", "DCE");
    yesterday = ledger.GetPosition("acc-1", "c2607", PositionDirection::kLong, "yesterday", "DCE");
    EXPECT_EQ(today.position, 0);
    EXPECT_EQ(today.frozen, 0);
    EXPECT_EQ(yesterday.position, 38);
    EXPECT_EQ(yesterday.frozen, 31);

    ASSERT_TRUE(ledger.ApplyOrderEvent(
        MakeOrderEvent("ord-close-mixed", OrderStatus::kCanceled, 43, 12, 3), &error))
        << error;
    yesterday = ledger.GetPosition("acc-1", "c2607", PositionDirection::kLong, "yesterday", "DCE");
    EXPECT_EQ(yesterday.position, 38);
    EXPECT_EQ(yesterday.frozen, 0);
    EXPECT_EQ(yesterday.closable, 38);
}

TEST(CtpPositionLedgerTest, GenericCloseRejectsWithoutMutatingBucketsWhenTotalShort) {
    CtpPositionLedger ledger;
    std::string error;

    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(2, "c2607", "DCE", PositionDirection::kLong, "today"), &error))
        << error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(3, "c2607", "DCE", PositionDirection::kLong, "yesterday"), &error))
        << error;

    CtpOrderIntentForLedger close_intent;
    close_intent.client_order_id = "ord-close-too-large";
    close_intent.account_id = "acc-1";
    close_intent.instrument_id = "c2607";
    close_intent.exchange_id = "DCE";
    close_intent.direction = PositionDirection::kLong;
    close_intent.offset = OffsetFlag::kClose;
    close_intent.requested_volume = 6;
    EXPECT_FALSE(ledger.RegisterOrderIntent(close_intent, &error));
    EXPECT_NE(error.find("requested=6"), std::string::npos);
    EXPECT_NE(error.find("total_closable=5"), std::string::npos);

    const auto today =
        ledger.GetPosition("acc-1", "c2607", PositionDirection::kLong, "today", "DCE");
    const auto yesterday =
        ledger.GetPosition("acc-1", "c2607", PositionDirection::kLong, "yesterday", "DCE");
    EXPECT_EQ(today.position, 2);
    EXPECT_EQ(today.frozen, 0);
    EXPECT_EQ(yesterday.position, 3);
    EXPECT_EQ(yesterday.frozen, 0);
}

TEST(CtpPositionLedgerTest, ExplicitCloseTodayDoesNotBorrowYesterday) {
    CtpPositionLedger ledger;
    std::string error;

    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(43, "c2607", "DCE", PositionDirection::kLong, "yesterday"), &error))
        << error;

    CtpOrderIntentForLedger close_intent;
    close_intent.client_order_id = "ord-close-today-only";
    close_intent.account_id = "acc-1";
    close_intent.instrument_id = "c2607";
    close_intent.exchange_id = "DCE";
    close_intent.direction = PositionDirection::kLong;
    close_intent.offset = OffsetFlag::kCloseToday;
    close_intent.requested_volume = 1;
    EXPECT_FALSE(ledger.RegisterOrderIntent(close_intent, &error));
    EXPECT_NE(error.find("today_closable=0"), std::string::npos);

    const auto yesterday =
        ledger.GetPosition("acc-1", "c2607", PositionDirection::kLong, "yesterday", "DCE");
    EXPECT_EQ(yesterday.position, 43);
    EXPECT_EQ(yesterday.frozen, 0);
    EXPECT_EQ(yesterday.closable, 43);
}

TEST(CtpPositionLedgerTest, FreezesAndReleasesRemainingOnPartialFillThenCancel) {
    CtpPositionLedger ledger;
    std::string error;

    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(MakeLongTodaySnapshot(10), &error)) << error;

    CtpOrderIntentForLedger close_intent;
    close_intent.client_order_id = "ord-close-1";
    close_intent.account_id = "acc-1";
    close_intent.instrument_id = "SHFE.ag2406";
    close_intent.direction = PositionDirection::kLong;
    close_intent.offset = OffsetFlag::kCloseToday;
    close_intent.requested_volume = 6;
    ASSERT_TRUE(ledger.RegisterOrderIntent(close_intent, &error)) << error;

    const auto before =
        ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today");
    EXPECT_EQ(before.position, 10);
    EXPECT_EQ(before.frozen, 6);
    EXPECT_EQ(before.closable, 4);

    ASSERT_TRUE(ledger.ApplyOrderEvent(
        MakeOrderEvent("ord-close-1", OrderStatus::kPartiallyFilled, 6, 2, 2), &error))
        << error;
    const auto after_partial =
        ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today");
    EXPECT_EQ(after_partial.position, 8);
    EXPECT_EQ(after_partial.frozen, 4);
    EXPECT_EQ(after_partial.closable, 4);

    ASSERT_TRUE(ledger.ApplyOrderEvent(
        MakeOrderEvent("ord-close-1", OrderStatus::kCanceled, 6, 2, 3), &error))
        << error;
    const auto after_cancel =
        ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today");
    EXPECT_EQ(after_cancel.position, 8);
    EXPECT_EQ(after_cancel.frozen, 0);
    EXPECT_EQ(after_cancel.closable, 8);
}

TEST(CtpPositionLedgerTest, RejectReleasesAllFrozenVolume) {
    CtpPositionLedger ledger;
    std::string error;

    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(MakeLongTodaySnapshot(5), &error)) << error;

    CtpOrderIntentForLedger close_intent;
    close_intent.client_order_id = "ord-close-2";
    close_intent.account_id = "acc-1";
    close_intent.instrument_id = "SHFE.ag2406";
    close_intent.direction = PositionDirection::kLong;
    close_intent.offset = OffsetFlag::kCloseToday;
    close_intent.requested_volume = 3;
    ASSERT_TRUE(ledger.RegisterOrderIntent(close_intent, &error)) << error;

    ASSERT_TRUE(ledger.ApplyOrderEvent(
        MakeOrderEvent("ord-close-2", OrderStatus::kRejected, 3, 0, 2), &error))
        << error;
    const auto snapshot =
        ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today");
    EXPECT_EQ(snapshot.position, 5);
    EXPECT_EQ(snapshot.frozen, 0);
    EXPECT_EQ(snapshot.closable, 5);
}

TEST(CtpPositionLedgerTest, CancelActionRejectedDoesNotReleaseFrozenVolume) {
    CtpPositionLedger ledger;
    std::string error;

    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(MakeLongTodaySnapshot(5), &error)) << error;

    CtpOrderIntentForLedger close_intent;
    close_intent.client_order_id = "ord-cancel-reject";
    close_intent.account_id = "acc-1";
    close_intent.instrument_id = "SHFE.ag2406";
    close_intent.direction = PositionDirection::kLong;
    close_intent.offset = OffsetFlag::kCloseToday;
    close_intent.requested_volume = 3;
    ASSERT_TRUE(ledger.RegisterOrderIntent(close_intent, &error)) << error;

    OrderEvent cancel_rejected =
        MakeOrderEvent("ord-cancel-reject", OrderStatus::kRejected, 3, 0, 2);
    cancel_rejected.event_source = "OnErrRtnOrderAction";
    ASSERT_TRUE(ledger.ApplyOrderEvent(cancel_rejected, &error)) << error;

    const auto snapshot =
        ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today");
    EXPECT_EQ(snapshot.position, 5);
    EXPECT_EQ(snapshot.frozen, 3);
    EXPECT_EQ(snapshot.closable, 2);
}

TEST(CtpPositionLedgerTest, OpenFillAddsPositionWithoutFreeze) {
    CtpPositionLedger ledger;
    std::string error;

    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(MakeLongTodaySnapshot(5), &error)) << error;

    CtpOrderIntentForLedger open_intent;
    open_intent.client_order_id = "ord-open-1";
    open_intent.account_id = "acc-1";
    open_intent.instrument_id = "SHFE.ag2406";
    open_intent.direction = PositionDirection::kLong;
    open_intent.offset = OffsetFlag::kOpen;
    open_intent.requested_volume = 2;
    ASSERT_TRUE(ledger.RegisterOrderIntent(open_intent, &error)) << error;

    ASSERT_TRUE(
        ledger.ApplyOrderEvent(MakeOrderEvent("ord-open-1", OrderStatus::kFilled, 2, 2, 3), &error))
        << error;
    const auto snapshot =
        ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today");
    EXPECT_EQ(snapshot.position, 7);
    EXPECT_EQ(snapshot.frozen, 0);
    EXPECT_EQ(snapshot.closable, 7);
}

TEST(CtpPositionLedgerTest, ExchangeAndHedgeFlagSeparatePositionBuckets) {
    CtpPositionLedger ledger;
    std::string error;

    auto hedge_snapshot = MakeLongTodaySnapshot(4);
    hedge_snapshot.instrument_id = "rb2405";
    hedge_snapshot.exchange_id = "SHFE";
    hedge_snapshot.hedge_flag = "3";
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(hedge_snapshot, &error)) << error;

    auto spec_snapshot = hedge_snapshot;
    spec_snapshot.position = 9;
    spec_snapshot.today_position = 9;
    spec_snapshot.exchange_id = "DCE";
    spec_snapshot.hedge_flag = "1";
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(spec_snapshot, &error)) << error;

    const auto shfe_hedge =
        ledger.GetPosition("acc-1", "rb2405", PositionDirection::kLong, "today", "SHFE", "hedge");
    const auto dce_spec = ledger.GetPosition("acc-1", "rb2405", PositionDirection::kLong, "today",
                                             "DCE", "speculation");

    EXPECT_EQ(shfe_hedge.position, 4);
    EXPECT_EQ(dce_spec.position, 9);
    EXPECT_EQ(ledger.GetClosableVolume("acc-1", "rb2405", PositionDirection::kLong, "today"), 0);
}

TEST(CtpPositionLedgerTest, BatchReplacementRemovesPositionsMissingFromBrokerTruth) {
    CtpPositionLedger ledger;
    std::string error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(4, "rb2405", "SHFE", PositionDirection::kLong, "today"), &error));
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(7, "ag2406", "SHFE", PositionDirection::kShort, "yesterday"), &error));

    const std::vector<InvestorPositionSnapshot> replacement = {
        MakePositionSnapshot(2, "rb2405", "SHFE", PositionDirection::kLong, "today")};
    ASSERT_TRUE(ledger.ReplaceInvestorPositionSnapshotBatch("acc-1", replacement, &error)) << error;

    EXPECT_EQ(
        ledger.GetPosition("acc-1", "rb2405", PositionDirection::kLong, "today", "SHFE").position,
        2);
    EXPECT_EQ(ledger.GetPosition("acc-1", "ag2406", PositionDirection::kShort, "yesterday", "SHFE")
                  .position,
              0);
}

TEST(CtpPositionLedgerTest, EmptyBatchAuthoritativelyClearsAccountOnly) {
    CtpPositionLedger ledger;
    std::string error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(MakeLongTodaySnapshot(5), &error));
    auto other_account = MakePositionSnapshot(8, "rb2405");
    other_account.account_id = "acc-2";
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(other_account, &error));

    ASSERT_TRUE(ledger.ReplaceInvestorPositionSnapshotBatch("acc-1", {}, &error)) << error;

    EXPECT_EQ(ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today", "SHFE")
                  .position,
              0);
    EXPECT_EQ(
        ledger.GetPosition("acc-2", "rb2405", PositionDirection::kLong, "today", "SHFE").position,
        8);
}

TEST(CtpPositionLedgerTest, InvalidBatchDoesNotPartiallyReplaceAccount) {
    CtpPositionLedger ledger;
    std::string error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(MakeLongTodaySnapshot(5), &error));
    auto valid = MakePositionSnapshot(9);
    auto foreign = MakePositionSnapshot(3, "rb2405");
    foreign.account_id = "acc-2";

    EXPECT_FALSE(ledger.ReplaceInvestorPositionSnapshotBatch("acc-1", {valid, foreign}, &error));
    EXPECT_EQ(ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today", "SHFE")
                  .position,
              5);
}

TEST(CtpPositionLedgerTest, DuplicateNormalizedBrokerKeyIsRejectedAtomically) {
    CtpPositionLedger ledger;
    std::string error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(MakeLongTodaySnapshot(5), &error));
    auto first = MakePositionSnapshot(2);
    auto duplicate = first;
    duplicate.position = 3;

    EXPECT_FALSE(ledger.ReplaceInvestorPositionSnapshotBatch("acc-1", {first, duplicate}, &error));
    EXPECT_EQ(ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today", "SHFE")
                  .position,
              5);
}

}  // namespace quant_hft
