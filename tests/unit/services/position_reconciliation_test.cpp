#include "quant_hft/services/position_reconciliation.h"

#include <gtest/gtest.h>

namespace quant_hft {
namespace {
QueryResult<InvestorPositionSnapshot> FullQuery() {
    QueryResult<InvestorPositionSnapshot> query;
    query.metadata.success = query.metadata.complete = query.metadata.full_account = true;
    return query;
}

TEST(PositionReconciliationTest, EmptyFailedOrPartialQueryCannotConfirmFlatAccount) {
    auto query = FullQuery();
    EXPECT_TRUE(ReconcileBrokerPositions("test", query, {}).matched);
    query.metadata.success = false;
    EXPECT_FALSE(ReconcileBrokerPositions("test", query, {}).matched);
    query.metadata.success = true;
    query.metadata.full_account = false;
    EXPECT_FALSE(ReconcileBrokerPositions("test", query, {}).matched);
}

TEST(PositionReconciliationTest, OffsetLongAndShortCannotHideUnattributedBrokerHoldings) {
    auto query = FullQuery();
    InvestorPositionSnapshot row;
    row.account_id = "test";
    row.instrument_id = "rb2701";
    row.exchange_id = "SHFE";
    row.hedge_flag = "1";
    row.position_date = "1";
    row.position = row.today_position = 2;
    row.posi_direction = "2";
    query.rows.push_back(row);
    row.posi_direction = "3";
    query.rows.push_back(row);
    EXPECT_EQ(ReconcileBrokerPositions("test", query, {}).differences.size(), 2U);
}

TEST(PositionReconciliationTest, NonShfeUsesRemainingYesterdayAndChecksHedgeIdentity) {
    auto query = FullQuery();
    InvestorPositionSnapshot row;
    row.account_id = "test";
    row.instrument_id = "m2701";
    row.exchange_id = "DCE";
    row.hedge_flag = "3";
    row.posi_direction = "2";
    row.position = 5;
    row.today_position = 2;
    row.yd_position = 100;  // Static start-of-day value is not the remaining yesterday bucket.
    query.rows.push_back(row);
    Position position;
    position.account_id = "test";
    position.strategy_id = "owned";
    position.symbol = "m2701";
    position.exchange = "DCE";
    position.hedge_flag = HedgeFlag::kHedge;
    position.long_qty = 5;
    position.long_today_qty = 2;
    position.long_yd_qty = 3;
    EXPECT_TRUE(ReconcileBrokerPositions("test", query, {position}).matched);
    position.hedge_flag = HedgeFlag::kSpeculation;
    EXPECT_FALSE(ReconcileBrokerPositions("test", query, {position}).matched);
}
}  // namespace
}  // namespace quant_hft
