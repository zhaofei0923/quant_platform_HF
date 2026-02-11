#include <gtest/gtest.h>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/redis_realtime_store.h"

namespace quant_hft {

TEST(RedisRealtimeStoreTest, BuildsCanonicalKeys) {
    EXPECT_EQ(RedisKeyBuilder::OrderInfo("ord-1"), "trade:order:ord-1:info");
    EXPECT_EQ(RedisKeyBuilder::MarketTickLatest("SHFE.ag2406"),
              "market:tick:SHFE.ag2406:latest");
    EXPECT_EQ(
        RedisKeyBuilder::Position("acc-1", "SHFE.ag2406", PositionDirection::kLong),
        "trade:position:acc-1:SHFE.ag2406:LONG");
}

TEST(RedisRealtimeStoreTest, StoresAndRetrievesLatestEntities) {
    RedisRealtimeStore store;

    MarketSnapshot market;
    market.instrument_id = "SHFE.ag2406";
    market.last_price = 4500.5;
    market.recv_ts_ns = 1;
    store.UpsertMarketSnapshot(market);

    OrderEvent order;
    order.account_id = "acc-1";
    order.client_order_id = "ord-1";
    order.instrument_id = "SHFE.ag2406";
    order.status = OrderStatus::kAccepted;
    order.total_volume = 2;
    order.ts_ns = 2;
    store.UpsertOrderEvent(order);
    order.status = OrderStatus::kFilled;
    order.filled_volume = 2;
    order.avg_fill_price = 4500.5;
    order.ts_ns = 3;
    store.UpsertOrderEvent(order);

    PositionSnapshot pos;
    pos.account_id = "acc-1";
    pos.instrument_id = "SHFE.ag2406";
    pos.direction = PositionDirection::kLong;
    pos.volume = 2;
    pos.avg_price = 4500.5;
    pos.ts_ns = 4;
    store.UpsertPositionSnapshot(pos);

    MarketSnapshot got_market;
    ASSERT_TRUE(store.GetMarketSnapshot("SHFE.ag2406", &got_market));
    EXPECT_DOUBLE_EQ(got_market.last_price, 4500.5);

    OrderEvent got_order;
    ASSERT_TRUE(store.GetOrderEvent("ord-1", &got_order));
    EXPECT_EQ(got_order.status, OrderStatus::kFilled);
    EXPECT_EQ(got_order.filled_volume, 2);

    PositionSnapshot got_pos;
    ASSERT_TRUE(store.GetPositionSnapshot("acc-1", "SHFE.ag2406",
                                          PositionDirection::kLong, &got_pos));
    EXPECT_EQ(got_pos.volume, 2);
}

TEST(RedisRealtimeStoreTest, ReturnsFalseWhenEntityMissingOrInvalidOutPointer) {
    RedisRealtimeStore store;
    EXPECT_FALSE(store.GetMarketSnapshot("missing", nullptr));

    OrderEvent order;
    EXPECT_FALSE(store.GetOrderEvent("missing", &order));

    PositionSnapshot pos;
    EXPECT_FALSE(store.GetPositionSnapshot("acc", "inst", PositionDirection::kLong, &pos));
}

}  // namespace quant_hft
