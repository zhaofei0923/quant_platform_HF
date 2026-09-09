#include <gtest/gtest.h>

#include <limits>

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

TEST(CtpAccountLedgerTest, CapsProjectedOpeningMarginAtAccountEquityRatio) {
    TradingAccountSnapshot snapshot;
    snapshot.account_id = "acc-1";
    snapshot.balance = 200000.0;
    snapshot.available = 194000.0;
    snapshot.curr_margin = 5000.0;
    snapshot.frozen_margin = 1000.0;

    CtpOrderFundInputs inputs;
    inputs.client_order_id = "ord-margin-pass";
    inputs.offset = OffsetFlag::kOpen;
    inputs.price = 3375.0;
    inputs.volume = 10;
    inputs.volume_multiple = 10;
    inputs.margin_ratio_by_money = 0.16;

    CtpAccountLedger passing_ledger(0.30);
    passing_ledger.ApplyTradingAccountSnapshot(snapshot);
    std::string error;
    ASSERT_TRUE(passing_ledger.ReserveOrderFunds(inputs, &error)) << error;
    EXPECT_DOUBLE_EQ(passing_ledger.current_margin(), 5000.0);
    EXPECT_DOUBLE_EQ(passing_ledger.frozen_margin(), 55000.0);

    CtpAccountLedger rejecting_ledger(0.30);
    rejecting_ledger.ApplyTradingAccountSnapshot(snapshot);
    inputs.client_order_id = "ord-margin-reject";
    inputs.volume = 11;
    EXPECT_FALSE(rejecting_ledger.ReserveOrderFunds(inputs, &error));
    EXPECT_NE(error.find("projected opening margin exceeds"), std::string::npos);
    EXPECT_DOUBLE_EQ(rejecting_ledger.frozen_margin(), 1000.0);

    CtpAccountLedger close_ledger(0.30);
    snapshot.curr_margin = 70000.0;
    snapshot.frozen_margin = 0.0;
    snapshot.available = 130000.0;
    close_ledger.ApplyTradingAccountSnapshot(snapshot);
    inputs.volume = 5;
    inputs.margin_ratio_by_money = 0.0;
    int close_index = 0;
    for (const auto offset :
         {OffsetFlag::kClose, OffsetFlag::kCloseToday, OffsetFlag::kCloseYesterday}) {
        inputs.client_order_id = "ord-close-over-ratio-" + std::to_string(++close_index);
        inputs.offset = offset;
        EXPECT_TRUE(close_ledger.ReserveOrderFunds(inputs, &error)) << error;
    }
}

TEST(CtpAccountLedgerTest, MarginRatioGateFailsClosedWithoutOpeningMargin) {
    CtpAccountLedger ledger(0.30);
    TradingAccountSnapshot snapshot;
    snapshot.balance = 200000.0;
    snapshot.available = 200000.0;
    ledger.ApplyTradingAccountSnapshot(snapshot);

    CtpOrderFundInputs inputs;
    inputs.client_order_id = "ord-no-margin-rate";
    inputs.offset = OffsetFlag::kOpen;
    inputs.price = 3375.0;
    inputs.volume = 1;
    inputs.volume_multiple = 10;
    std::string error;
    EXPECT_FALSE(ledger.ReserveOrderFunds(inputs, &error));
    EXPECT_NE(error.find("requires positive equity and order margin"), std::string::npos);

    inputs.margin_ratio_by_money = 0.16;
    snapshot.balance = 0.0;
    ledger.ApplyTradingAccountSnapshot(snapshot);
    inputs.client_order_id = "ord-no-equity";
    EXPECT_FALSE(ledger.ReserveOrderFunds(inputs, &error));
    EXPECT_NE(error.find("requires positive equity and order margin"), std::string::npos);

    snapshot.balance = 200000.0;
    snapshot.curr_margin = std::numeric_limits<double>::quiet_NaN();
    ledger.ApplyTradingAccountSnapshot(snapshot);
    inputs.client_order_id = "ord-invalid-current-margin";
    EXPECT_FALSE(ledger.ReserveOrderFunds(inputs, &error));
    EXPECT_NE(error.find("requires positive equity and order margin"), std::string::npos);

    snapshot.curr_margin = 0.0;
    ledger.ApplyTradingAccountSnapshot(snapshot);
    inputs.client_order_id = "ord-invalid-money-rate";
    inputs.margin_ratio_by_money = std::numeric_limits<double>::quiet_NaN();
    inputs.margin_ratio_by_volume = 100.0;
    EXPECT_FALSE(ledger.ReserveOrderFunds(inputs, &error));
    EXPECT_NE(error.find("requires positive equity and order margin"), std::string::npos);

    inputs.client_order_id = "ord-relative-margin-rate";
    inputs.margin_ratio_by_money = 0.16;
    inputs.margin_ratio_by_volume = 0.0;
    inputs.margin_rate_is_relative = true;
    EXPECT_FALSE(ledger.ReserveOrderFunds(inputs, &error));
    EXPECT_NE(error.find("relative broker margin rate"), std::string::npos);
}

TEST(CtpAccountLedgerTest, MarginRatioGateUsesLatestBrokerEquity) {
    CtpAccountLedger ledger(0.30);
    TradingAccountSnapshot snapshot;
    snapshot.balance = 200000.0;
    snapshot.available = 180000.0;
    snapshot.curr_margin = 20000.0;
    ledger.ApplyTradingAccountSnapshot(snapshot);

    CtpOrderFundInputs inputs;
    inputs.client_order_id = "ord-current-equity";
    inputs.offset = OffsetFlag::kOpen;
    inputs.price = 3000.0;
    inputs.volume = 10;
    inputs.volume_multiple = 10;
    inputs.margin_ratio_by_money = 0.10;
    std::string error;
    ASSERT_TRUE(ledger.ReserveOrderFunds(inputs, &error)) << error;

    CtpAccountLedger lower_equity_ledger(0.30);
    snapshot.balance = 100000.0;
    snapshot.available = 80000.0;
    lower_equity_ledger.ApplyTradingAccountSnapshot(snapshot);
    inputs.client_order_id = "ord-lower-equity";
    EXPECT_FALSE(lower_equity_ledger.ReserveOrderFunds(inputs, &error));
    EXPECT_NE(error.find("projected_margin=50000"), std::string::npos);
    EXPECT_NE(error.find("limit=30000"), std::string::npos);
}

TEST(CtpAccountLedgerTest, MarginRatioGateIncludesPreviouslyFrozenOpeningMargin) {
    CtpAccountLedger ledger(0.30);
    TradingAccountSnapshot snapshot;
    snapshot.balance = 100000.0;
    snapshot.available = 100000.0;
    ledger.ApplyTradingAccountSnapshot(snapshot);

    CtpOrderFundInputs inputs;
    inputs.client_order_id = "ord-first-margin";
    inputs.offset = OffsetFlag::kOpen;
    inputs.price = 1000.0;
    inputs.volume = 2;
    inputs.volume_multiple = 10;
    inputs.margin_ratio_by_money = 0.10;
    std::string error;
    ASSERT_TRUE(ledger.ReserveOrderFunds(inputs, &error)) << error;
    EXPECT_DOUBLE_EQ(ledger.frozen_margin(), 2000.0);

    inputs.client_order_id = "ord-second-margin";
    inputs.volume = 29;
    EXPECT_FALSE(ledger.ReserveOrderFunds(inputs, &error));
    EXPECT_NE(error.find("projected_margin=31000"), std::string::npos);
    EXPECT_NE(error.find("limit=30000"), std::string::npos);
    EXPECT_DOUBLE_EQ(ledger.frozen_margin(), 2000.0);
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
