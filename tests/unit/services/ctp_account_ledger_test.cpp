#include <gtest/gtest.h>

#include "quant_hft/services/ctp_account_ledger.h"

namespace quant_hft {

TEST(CtpAccountLedgerTest, ResolvesFourMarginPriceModes) {
    CtpMarginPriceInputs prices;
    prices.pre_settlement_price = 100.0;
    prices.settlement_price = 110.0;
    prices.average_price = 105.0;
    prices.open_price = 95.0;

    EXPECT_DOUBLE_EQ(CtpAccountLedger::ResolveMarginPrice('1', prices), 100.0);
    EXPECT_DOUBLE_EQ(CtpAccountLedger::ResolveMarginPrice('2', prices), 110.0);
    EXPECT_DOUBLE_EQ(CtpAccountLedger::ResolveMarginPrice('3', prices), 105.0);
    EXPECT_DOUBLE_EQ(CtpAccountLedger::ResolveMarginPrice('4', prices), 95.0);
}

TEST(CtpAccountLedgerTest, ComputesMarginUsingSelectedPriceMode) {
    CtpMarginPriceInputs prices;
    prices.pre_settlement_price = 100.0;
    prices.settlement_price = 120.0;
    prices.average_price = 115.0;
    prices.open_price = 90.0;
    const double margin = CtpAccountLedger::ComputePositionMargin('2', prices, 3, 10, 0.12);
    EXPECT_DOUBLE_EQ(margin, 432.0);
}

TEST(CtpAccountLedgerTest, AppliesDailyMarkToMarketAndRollsTradingDay) {
    CtpAccountLedger ledger;

    TradingAccountSnapshot snapshot;
    snapshot.account_id = "acc-1";
    snapshot.investor_id = "acc-1";
    snapshot.balance = 100000.0;
    snapshot.available = 80000.0;
    snapshot.trading_day = "20260210";
    ledger.ApplyTradingAccountSnapshot(snapshot);

    ledger.ApplyDailySettlement(100.0, 104.0, 3, 10);
    EXPECT_DOUBLE_EQ(ledger.balance(), 100120.0);
    EXPECT_DOUBLE_EQ(ledger.available(), 80120.0);
    EXPECT_DOUBLE_EQ(ledger.daily_settlement_pnl(), 120.0);

    ledger.RollTradingDay("20260211");
    EXPECT_EQ(ledger.trading_day(), "20260211");
    EXPECT_DOUBLE_EQ(ledger.daily_settlement_pnl(), 0.0);
}

}  // namespace quant_hft
