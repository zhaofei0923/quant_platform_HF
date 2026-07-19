#include "quant_hft/services/in_memory_portfolio_ledger.h"

#include <gtest/gtest.h>

namespace quant_hft {

namespace {

OrderEvent MakeFilledEvent(const std::string& client_order_id, std::int32_t filled_volume,
                           EpochNanos ts_ns) {
    OrderEvent event;
    event.account_id = "a1";
    event.client_order_id = client_order_id;
    event.instrument_id = "SHFE.ag2406";
    event.status = filled_volume > 0 ? OrderStatus::kPartiallyFilled : OrderStatus::kAccepted;
    event.total_volume = 10;
    event.filled_volume = filled_volume;
    event.avg_fill_price = 4500.0;
    event.ts_ns = ts_ns;
    event.trace_id = "trace";
    return event;
}

}  // namespace

TEST(InMemoryPortfolioLedgerTest, UsesFillDeltaInsteadOfAbsoluteFill) {
    InMemoryPortfolioLedger ledger;

    ledger.OnOrderEvent(MakeFilledEvent("ord-1", 1, 1));
    ledger.OnOrderEvent(MakeFilledEvent("ord-1", 2, 2));

    const auto pos = ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kLong);
    EXPECT_EQ(pos.volume, 2);
    EXPECT_DOUBLE_EQ(pos.avg_price, 4500.0);
}

TEST(InMemoryPortfolioLedgerTest, IgnoresReplayDuplicateEvent) {
    InMemoryPortfolioLedger ledger;

    const auto evt = MakeFilledEvent("ord-2", 3, 3);
    ledger.OnOrderEvent(evt);
    ledger.OnOrderEvent(evt);

    const auto pos = ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kLong);
    EXPECT_EQ(pos.volume, 3);
}

TEST(InMemoryPortfolioLedgerTest, SellOpenCreatesShortWithoutReasonConvention) {
    InMemoryPortfolioLedger ledger;

    auto evt = MakeFilledEvent("ord-3", 2, 10);
    evt.side = Side::kSell;
    evt.reason = "unrelated-broker-message";
    ledger.OnOrderEvent(evt);

    const auto short_pos =
        ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kShort);
    const auto long_pos = ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kLong);
    EXPECT_EQ(short_pos.volume, 2);
    EXPECT_EQ(long_pos.volume, 0);
}

TEST(InMemoryPortfolioLedgerTest, SellCloseReducesLongAndPreservesEntryPrice) {
    InMemoryPortfolioLedger ledger;

    auto open = MakeFilledEvent("ord-open-long", 5, 10);
    open.side = Side::kBuy;
    open.offset = OffsetFlag::kOpen;
    ledger.OnOrderEvent(open);

    auto close = MakeFilledEvent("ord-close-long", 2, 11);
    close.side = Side::kSell;
    close.offset = OffsetFlag::kCloseToday;
    close.avg_fill_price = 4600.0;
    ledger.OnOrderEvent(close);

    const auto position = ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kLong);
    EXPECT_EQ(position.volume, 3);
    EXPECT_DOUBLE_EQ(position.avg_price, 4500.0);
}

TEST(InMemoryPortfolioLedgerTest, BuyCloseReducesShortUsingCumulativeFillDelta) {
    InMemoryPortfolioLedger ledger;

    auto open = MakeFilledEvent("ord-open-short", 5, 10);
    open.side = Side::kSell;
    ledger.OnOrderEvent(open);

    auto close = MakeFilledEvent("ord-close-short", 1, 11);
    close.side = Side::kBuy;
    close.offset = OffsetFlag::kClose;
    ledger.OnOrderEvent(close);
    close.filled_volume = 3;
    close.ts_ns = 12;
    ledger.OnOrderEvent(close);

    const auto position =
        ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kShort);
    EXPECT_EQ(position.volume, 2);
    EXPECT_DOUBLE_EQ(position.avg_price, 4500.0);
}

TEST(InMemoryPortfolioLedgerTest, UsesIncrementalNotionalFromCumulativeAverage) {
    InMemoryPortfolioLedger ledger;

    auto first = MakeFilledEvent("ord-changing-average", 1, 10);
    first.avg_fill_price = 100.0;
    ledger.OnOrderEvent(first);
    auto second = first;
    second.filled_volume = 2;
    second.avg_fill_price = 110.0;
    second.ts_ns = 11;
    ledger.OnOrderEvent(second);

    const auto position = ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kLong);
    ASSERT_EQ(position.volume, 2);
    EXPECT_DOUBLE_EQ(position.avg_price, 110.0);
}

TEST(InMemoryPortfolioLedgerTest, SameClientOrderIdAcrossTradingDaysDoesNotCollide) {
    InMemoryPortfolioLedger ledger;

    auto first_day = MakeFilledEvent("reused-client-id", 1, 10);
    first_day.trading_day = "20260701";
    ledger.OnOrderEvent(first_day);
    auto second_day = MakeFilledEvent("reused-client-id", 2, 11);
    second_day.trading_day = "20260702";
    ledger.OnOrderEvent(second_day);

    const auto position = ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kLong);
    EXPECT_EQ(position.volume, 3);
}

}  // namespace quant_hft
