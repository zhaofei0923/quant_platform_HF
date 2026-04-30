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

TEST(CtpAccountLedgerTest, ComputesOrderMarginAndCommissionWithByVolumeComponents) {
    CtpOrderFundInputs inputs;
    inputs.client_order_id = "ord-funds";
    inputs.price = 100.0;
    inputs.volume = 2;
    inputs.volume_multiple = 10;
    inputs.margin_ratio_by_money = 0.12;
    inputs.margin_ratio_by_volume = 5.0;
    inputs.commission_ratio_by_money = 0.001;
    inputs.commission_ratio_by_volume = 1.5;

    EXPECT_DOUBLE_EQ(CtpAccountLedger::ComputeOrderMargin(inputs), 250.0);
    EXPECT_DOUBLE_EQ(CtpAccountLedger::ComputeOrderCommission(inputs), 5.0);
}

TEST(CtpAccountLedgerTest, ReservesFundsAndReleasesUnfilledAmountOnCancel) {
    CtpAccountLedger ledger;

    TradingAccountSnapshot snapshot;
    snapshot.account_id = "acc-1";
    snapshot.available = 1000.0;
    snapshot.balance = 1000.0;
    ledger.ApplyTradingAccountSnapshot(snapshot);

    CtpOrderFundInputs inputs;
    inputs.client_order_id = "ord-reserve";
    inputs.price = 100.0;
    inputs.volume = 2;
    inputs.volume_multiple = 10;
    inputs.margin_ratio_by_money = 0.1;
    inputs.commission_ratio_by_volume = 1.0;

    std::string error;
    ASSERT_TRUE(ledger.ReserveOrderFunds(inputs, &error)) << error;
    EXPECT_DOUBLE_EQ(ledger.available(), 798.0);
    EXPECT_DOUBLE_EQ(ledger.frozen_margin(), 200.0);
    EXPECT_DOUBLE_EQ(ledger.frozen_commission(), 2.0);

    OrderEvent partial;
    partial.client_order_id = "ord-reserve";
    partial.status = OrderStatus::kPartiallyFilled;
    partial.filled_volume = 1;
    ASSERT_TRUE(ledger.ApplyOrderEvent(partial, &error)) << error;
    EXPECT_DOUBLE_EQ(ledger.current_margin(), 100.0);
    EXPECT_DOUBLE_EQ(ledger.commission(), 1.0);
    EXPECT_DOUBLE_EQ(ledger.frozen_margin(), 100.0);
    EXPECT_DOUBLE_EQ(ledger.frozen_commission(), 1.0);
    EXPECT_DOUBLE_EQ(ledger.available(), 798.0);
    EXPECT_DOUBLE_EQ(ledger.balance(), 999.0);

    OrderEvent canceled = partial;
    canceled.status = OrderStatus::kCanceled;
    ASSERT_TRUE(ledger.ApplyOrderEvent(canceled, &error)) << error;
    EXPECT_DOUBLE_EQ(ledger.available(), 899.0);
    EXPECT_DOUBLE_EQ(ledger.frozen_margin(), 0.0);
    EXPECT_DOUBLE_EQ(ledger.frozen_commission(), 0.0);
}

TEST(CtpAccountLedgerTest, CancelActionRejectedDoesNotReleaseReservedFunds) {
    CtpAccountLedger ledger;

    TradingAccountSnapshot snapshot;
    snapshot.account_id = "acc-1";
    snapshot.available = 1000.0;
    snapshot.balance = 1000.0;
    ledger.ApplyTradingAccountSnapshot(snapshot);

    CtpOrderFundInputs inputs;
    inputs.client_order_id = "ord-cancel-reject";
    inputs.price = 100.0;
    inputs.volume = 1;
    inputs.volume_multiple = 10;
    inputs.margin_ratio_by_money = 0.1;
    inputs.commission_ratio_by_volume = 1.0;

    std::string error;
    ASSERT_TRUE(ledger.ReserveOrderFunds(inputs, &error)) << error;

    OrderEvent cancel_rejected;
    cancel_rejected.client_order_id = "ord-cancel-reject";
    cancel_rejected.status = OrderStatus::kRejected;
    cancel_rejected.event_source = "OnErrRtnOrderAction";
    ASSERT_TRUE(ledger.ApplyOrderEvent(cancel_rejected, &error)) << error;

    EXPECT_DOUBLE_EQ(ledger.available(), 899.0);
    EXPECT_DOUBLE_EQ(ledger.frozen_margin(), 100.0);
    EXPECT_DOUBLE_EQ(ledger.frozen_commission(), 1.0);
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
