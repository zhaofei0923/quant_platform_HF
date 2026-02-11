#include <gtest/gtest.h>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/timescale_event_store.h"

namespace quant_hft {

namespace {

OrderIntent MakeIntent(const std::string& order_id, EpochNanos ts_ns) {
    OrderIntent intent;
    intent.account_id = "acc-1";
    intent.client_order_id = order_id;
    intent.instrument_id = "SHFE.ag2406";
    intent.side = Side::kBuy;
    intent.offset = OffsetFlag::kOpen;
    intent.volume = 1;
    intent.price = 4500.0;
    intent.ts_ns = ts_ns;
    intent.trace_id = "trace";
    return intent;
}

}  // namespace

TEST(TimescaleEventStoreTest, AppendsAndQueriesMarketSnapshotsByInstrument) {
    TimescaleEventStore store;

    MarketSnapshot a;
    a.instrument_id = "SHFE.ag2406";
    a.last_price = 4500.0;
    a.recv_ts_ns = 10;
    store.AppendMarketSnapshot(a);

    MarketSnapshot b;
    b.instrument_id = "DCE.i2409";
    b.last_price = 810.0;
    b.recv_ts_ns = 11;
    store.AppendMarketSnapshot(b);

    MarketSnapshot c = a;
    c.last_price = 4501.0;
    c.recv_ts_ns = 12;
    store.AppendMarketSnapshot(c);

    const auto ag_rows = store.GetMarketSnapshots("SHFE.ag2406");
    ASSERT_EQ(ag_rows.size(), 2U);
    EXPECT_DOUBLE_EQ(ag_rows.back().last_price, 4501.0);
    EXPECT_EQ(store.GetMarketSnapshots("DCE.i2409").size(), 1U);
}

TEST(TimescaleEventStoreTest, AppendsOrderAndRiskDecisionEvents) {
    TimescaleEventStore store;

    OrderEvent order;
    order.account_id = "acc-1";
    order.client_order_id = "ord-1";
    order.instrument_id = "SHFE.ag2406";
    order.status = OrderStatus::kAccepted;
    order.total_volume = 1;
    order.ts_ns = 20;
    store.AppendOrderEvent(order);

    order.status = OrderStatus::kFilled;
    order.filled_volume = 1;
    order.avg_fill_price = 4500.0;
    order.ts_ns = 21;
    store.AppendOrderEvent(order);

    RiskDecision decision;
    decision.action = RiskAction::kAllow;
    decision.rule_id = "BASIC_LIMIT";
    decision.rule_group = "default";
    decision.rule_version = "v1";
    decision.decision_ts_ns = 30;
    decision.reason = "ok";
    store.AppendRiskDecision(MakeIntent("ord-1", 19), decision);

    const auto orders = store.GetOrderEvents("ord-1");
    ASSERT_EQ(orders.size(), 2U);
    EXPECT_EQ(orders.back().status, OrderStatus::kFilled);

    const auto risks = store.GetRiskDecisionRows();
    ASSERT_EQ(risks.size(), 1U);
    EXPECT_EQ(risks[0].intent.client_order_id, "ord-1");
    EXPECT_EQ(risks[0].decision.rule_id, "BASIC_LIMIT");
    EXPECT_EQ(risks[0].decision.rule_group, "default");
    EXPECT_EQ(risks[0].decision.rule_version, "v1");
    EXPECT_EQ(risks[0].decision.decision_ts_ns, 30);
}

}  // namespace quant_hft
