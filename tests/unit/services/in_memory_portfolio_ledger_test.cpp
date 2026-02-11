#include <gtest/gtest.h>

#include "quant_hft/services/in_memory_portfolio_ledger.h"

namespace quant_hft {

namespace {

OrderEvent MakeFilledEvent(const std::string& client_order_id,
                           std::int32_t filled_volume,
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

TEST(InMemoryPortfolioLedgerTest, TracksShortDirectionFromReasonField) {
    InMemoryPortfolioLedger ledger;

    auto evt = MakeFilledEvent("ord-3", 2, 10);
    evt.reason = "short";
    ledger.OnOrderEvent(evt);

    const auto short_pos =
        ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kShort);
    const auto long_pos =
        ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kLong);
    EXPECT_EQ(short_pos.volume, 2);
    EXPECT_EQ(long_pos.volume, 0);
}

}  // namespace quant_hft
