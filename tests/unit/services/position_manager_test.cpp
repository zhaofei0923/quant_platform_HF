#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/core/trading_domain_store_client_adapter.h"
#include "quant_hft/core/timescale_sql_client.h"
#include "quant_hft/services/position_manager.h"

namespace quant_hft {
namespace {

Trade BuildOpenTrade(const std::string& trade_id, Side side, int qty) {
    Trade trade;
    trade.trade_id = trade_id;
    trade.order_id = "ord-" + trade_id;
    trade.account_id = "acc1";
    trade.strategy_id = "s1";
    trade.symbol = "SHFE.ag2406";
    trade.exchange = "SHFE";
    trade.side = side;
    trade.offset = OffsetFlag::kOpen;
    trade.price = 5000.0;
    trade.quantity = qty;
    trade.trade_ts_ns = 100;
    return trade;
}

TEST(PositionManagerTest, OpenTradeUpdatesPgAndRedis) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    auto store = std::make_shared<TradingDomainStoreClientAdapter>(
        sql, StorageRetryPolicy{}, "trading_core");
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    PositionManager manager(store, redis);

    std::string error;
    ASSERT_TRUE(manager.UpdatePosition(BuildOpenTrade("t1", Side::kBuy, 2), &error)) << error;

    const auto rows = sql->QueryRows("trading_core.position_summary", "account_id", "acc1", &error);
    ASSERT_FALSE(rows.empty());
    std::unordered_map<std::string, std::string> hash;
    ASSERT_TRUE(redis->HGetAll("position:acc1:SHFE.ag2406", &hash, &error)) << error;
    EXPECT_EQ(hash["long_volume"], "2");
}

TEST(PositionManagerTest, CloseTradeReducesVolume) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    auto store = std::make_shared<TradingDomainStoreClientAdapter>(
        sql, StorageRetryPolicy{}, "trading_core");
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    PositionManager manager(store, redis);

    std::string error;
    ASSERT_TRUE(manager.UpdatePosition(BuildOpenTrade("t2", Side::kBuy, 3), &error)) << error;

    Trade close = BuildOpenTrade("t3", Side::kSell, 1);
    close.offset = OffsetFlag::kClose;
    close.quantity = 1;
    close.trade_ts_ns = 200;
    ASSERT_TRUE(manager.UpdatePosition(close, &error)) << error;

    std::unordered_map<std::string, std::string> hash;
    ASSERT_TRUE(redis->HGetAll("position:acc1:SHFE.ag2406", &hash, &error)) << error;
    EXPECT_EQ(hash["long_volume"], "2");
}

TEST(PositionManagerTest, ReconcileWritesSnapshotToRedis) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    auto store = std::make_shared<TradingDomainStoreClientAdapter>(
        sql, StorageRetryPolicy{}, "trading_core");
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    PositionManager manager(store, redis);

    Position summary;
    summary.account_id = "acc1";
    summary.strategy_id = "s1";
    summary.symbol = "SHFE.ag2406";
    summary.exchange = "SHFE";
    summary.long_qty = 5;
    summary.short_qty = 1;
    summary.long_today_qty = 2;
    summary.short_today_qty = 0;
    summary.long_yd_qty = 3;
    summary.short_yd_qty = 1;
    std::string error;
    ASSERT_TRUE(store->UpsertPosition(summary, &error)) << error;

    ASSERT_TRUE(manager.ReconcilePositions("acc1", "s1", "2026-02-12", &error)) << error;
    std::unordered_map<std::string, std::string> hash;
    ASSERT_TRUE(redis->HGetAll("position:acc1:SHFE.ag2406", &hash, &error)) << error;
    EXPECT_EQ(hash["long_volume"], "5");
    EXPECT_EQ(hash["short_volume"], "1");
}

}  // namespace
}  // namespace quant_hft
