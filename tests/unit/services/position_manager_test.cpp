#include "quant_hft/services/position_manager.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/core/timescale_sql_client.h"
#include "quant_hft/core/trading_domain_store_client_adapter.h"

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

class LoseFirstProjectionAcknowledgement final : public InMemoryRedisHashClient {
   public:
    bool HSetVersioned(const std::string& key,
                       const std::unordered_map<std::string, std::string>& fields,
                       std::uint64_t version, std::string* error) override {
        if (!InMemoryRedisHashClient::HSetVersioned(key, fields, version, error)) return false;
        if (first_) {
            first_ = false;
            if (error != nullptr) *error = "injected lost Redis reply after successful write";
            return false;
        }
        return true;
    }

   private:
    bool first_{true};
};

TEST(PositionManagerTest, LostRedisReplyLeavesOutboxAndRetryDoesNotDoubleCount) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    auto store = std::make_shared<TradingDomainStoreClientAdapter>(sql, StorageRetryPolicy{});
    auto redis = std::make_shared<LoseFirstProjectionAcknowledgement>();
    PositionManager manager(store, redis);
    std::string error;
    EXPECT_FALSE(manager.UpdatePosition(BuildOpenTrade("retry", Side::kBuy, 2), &error));
    std::vector<TradeOutboxRecord> pending;
    ASSERT_TRUE(store->LoadPendingOutbox("acc1", &pending, &error));
    ASSERT_EQ(pending.size(), 1U);
    ASSERT_TRUE(manager.DrainOutbox("acc1", &error)) << error;
    ASSERT_TRUE(manager.DrainOutbox("acc1", &error)) << error;
    std::unordered_map<std::string, std::string> fields;
    ASSERT_TRUE(redis->HGetAll("position:acc1:SHFE.ag2406", &fields, &error));
    EXPECT_EQ(fields.at("long_volume"), "2");
    EXPECT_EQ(sql->QueryAllRows("trading_core.trades", nullptr).size(), 1U);
    ASSERT_TRUE(store->LoadPendingOutbox("acc1", &pending, &error));
    EXPECT_TRUE(pending.empty());
}

TEST(PositionManagerTest, VersionedProjectionNeverReplacesNewerSnapshot) {
    InMemoryRedisHashClient redis;
    std::string error;
    ASSERT_TRUE(redis.HSetVersioned("position", {{"long_volume", "9"}}, 9, &error));
    ASSERT_TRUE(redis.HSetVersioned("position", {{"long_volume", "2"}}, 2, &error));
    std::unordered_map<std::string, std::string> fields;
    ASSERT_TRUE(redis.HGetAll("position", &fields, &error));
    EXPECT_EQ(fields.at("long_volume"), "9");
}

TEST(PositionManagerTest, OpenTradeUpdatesPgAndRedis) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    auto store = std::make_shared<TradingDomainStoreClientAdapter>(sql, StorageRetryPolicy{},
                                                                   "trading_core");
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
    auto store = std::make_shared<TradingDomainStoreClientAdapter>(sql, StorageRetryPolicy{},
                                                                   "trading_core");
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    PositionManager manager(store, redis);

    std::string error;
    ASSERT_TRUE(manager.UpdatePosition(BuildOpenTrade("t2", Side::kBuy, 3), &error)) << error;

    Trade close = BuildOpenTrade("t3", Side::kSell, 1);
    close.offset = OffsetFlag::kCloseToday;
    close.quantity = 1;
    close.trade_ts_ns = 200;
    ASSERT_TRUE(manager.UpdatePosition(close, &error)) << error;

    std::unordered_map<std::string, std::string> hash;
    ASSERT_TRUE(redis->HGetAll("position:acc1:SHFE.ag2406", &hash, &error)) << error;
    EXPECT_EQ(hash["long_volume"], "2");
}

TEST(PositionManagerTest, ReconcileWritesSnapshotToRedis) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    auto store = std::make_shared<TradingDomainStoreClientAdapter>(sql, StorageRetryPolicy{},
                                                                   "trading_core");
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

namespace quant_hft {
namespace {
TEST(PositionManagerTest, IndependentAccountRedisDatesUseBrokerPhysicalBuckets) {
    auto sql = std::make_shared<InMemoryTimescaleSqlClient>();
    auto store = std::make_shared<TradingDomainStoreClientAdapter>(sql, StorageRetryPolicy{});
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    PositionManager manager(store, redis);
    std::string error;
    ASSERT_TRUE(
        store->ConfigureIndependentStrategyBooks("acc1", {{"A", 50000}, {"B", 50000}}, &error));
    TradeApplyRequest request;
    request.allow_ephemeral = true;
    request.trade = BuildOpenTrade("a", Side::kBuy, 2);
    request.trade.exchange = "DCE";
    request.trade.broker_id = "broker";
    request.trade.symbol = "m2609";
    request.trade.strategy_id = "A";
    request.trade.trading_day = "20260907";
    request.trade.price = 100;
    request.accounting_policy.valuation_inputs_verified = true;
    request.accounting_policy.contract_multiplier = 10;
    request.accounting_policy.valuation_source = "test";
    request.accounting_policy.generic_close_priority = GenericClosePriority::kYesterdayFirst;
    request.accounting_policy.close_rule_source = "test";
    request.accounting_policy.close_rule_version = "1";
    TradeApplyResult result;
    ASSERT_TRUE(store->ApplyTrade(request, &result, &error)) << error;
    ASSERT_TRUE(store->AdvanceTradingDay("acc1", "broker", "20260908", &error)) << error;
    request.trade.trade_id = "b";
    request.trade.strategy_id = "B";
    request.trade.trading_day = "20260908";
    request.trade.price = 110;
    ASSERT_TRUE(store->ApplyTrade(request, &result, &error)) << error;
    request.trade.trade_id = "bclose";
    request.trade.side = Side::kSell;
    request.trade.offset = OffsetFlag::kClose;
    request.trade.quantity = 1;
    request.trade.price = 120;
    ASSERT_TRUE(store->ApplyTrade(request, &result, &error)) << error;
    ASSERT_TRUE(manager.DrainOutbox("acc1", &error)) << error;
    std::unordered_map<std::string, std::string> fields;
    ASSERT_TRUE(redis->HGetAll("position:acc1:m2609", &fields, &error));
    EXPECT_EQ(fields.at("long_yd"), "1");
    EXPECT_EQ(fields.at("long_today"), "2");
    const auto own = manager.GetCurrentPositions("acc1");
    ASSERT_EQ(own.size(), 2U);
    int economic_yd = 0;
    for (const auto& p : own) economic_yd += p.long_yd_qty;
    EXPECT_EQ(economic_yd, 2);
}
}  // namespace
}  // namespace quant_hft
