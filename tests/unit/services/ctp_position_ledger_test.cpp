#include <string>

#include <gtest/gtest.h>

#include "quant_hft/services/ctp_position_ledger.h"

namespace quant_hft {
namespace {

InvestorPositionSnapshot MakeLongTodaySnapshot(std::int32_t position) {
    InvestorPositionSnapshot snapshot;
    snapshot.account_id = "acc-1";
    snapshot.investor_id = "acc-1";
    snapshot.instrument_id = "SHFE.ag2406";
    snapshot.exchange_id = "SHFE";
    snapshot.posi_direction = "2";
    snapshot.position_date = "today";
    snapshot.position = position;
    snapshot.today_position = position;
    snapshot.yd_position = 0;
    snapshot.long_frozen = 0;
    snapshot.short_frozen = 0;
    snapshot.ts_ns = 1;
    snapshot.source = "ctp";
    return snapshot;
}

OrderEvent MakeOrderEvent(const std::string& client_order_id,
                          OrderStatus status,
                          std::int32_t total_volume,
                          std::int32_t filled_volume,
                          EpochNanos ts_ns) {
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

    const auto before = ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today");
    EXPECT_EQ(before.position, 10);
    EXPECT_EQ(before.frozen, 6);
    EXPECT_EQ(before.closable, 4);

    ASSERT_TRUE(ledger.ApplyOrderEvent(
                    MakeOrderEvent("ord-close-1", OrderStatus::kPartiallyFilled, 6, 2, 2),
                    &error))
        << error;
    const auto after_partial =
        ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today");
    EXPECT_EQ(after_partial.position, 8);
    EXPECT_EQ(after_partial.frozen, 4);
    EXPECT_EQ(after_partial.closable, 4);

    ASSERT_TRUE(
        ledger.ApplyOrderEvent(MakeOrderEvent("ord-close-1", OrderStatus::kCanceled, 6, 2, 3), &error))
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

    ASSERT_TRUE(
        ledger.ApplyOrderEvent(MakeOrderEvent("ord-close-2", OrderStatus::kRejected, 3, 0, 2), &error))
        << error;
    const auto snapshot = ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today");
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

    OrderEvent cancel_rejected = MakeOrderEvent(
        "ord-cancel-reject", OrderStatus::kRejected, 3, 0, 2);
    cancel_rejected.event_source = "OnErrRtnOrderAction";
    ASSERT_TRUE(ledger.ApplyOrderEvent(cancel_rejected, &error)) << error;

    const auto snapshot = ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong,
                                             "today");
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
    const auto snapshot = ledger.GetPosition("acc-1", "SHFE.ag2406", PositionDirection::kLong, "today");
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
    const auto dce_spec = ledger.GetPosition(
        "acc-1", "rb2405", PositionDirection::kLong, "today", "DCE", "speculation");

    EXPECT_EQ(shfe_hedge.position, 4);
    EXPECT_EQ(dce_spec.position, 9);
    EXPECT_EQ(ledger.GetClosableVolume("acc-1", "rb2405", PositionDirection::kLong, "today"), 0);
}

}  // namespace quant_hft
