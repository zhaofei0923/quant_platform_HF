#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/core/redis_realtime_store_client_adapter.h"

namespace quant_hft {

namespace {

class FlakyRedisClient : public IRedisHashClient {
public:
    explicit FlakyRedisClient(int fail_times) : fail_times_(fail_times) {}

    bool HSet(const std::string& key,
              const std::unordered_map<std::string, std::string>& fields,
              std::string* error) override {
        ++hset_calls_;
        if (hset_calls_ <= fail_times_) {
            if (error != nullptr) {
                *error = "transient";
            }
            return false;
        }
        storage_[key] = fields;
        return true;
    }

    bool HGetAll(const std::string& key,
                 std::unordered_map<std::string, std::string>* out,
                 std::string* error) const override {
        const auto it = storage_.find(key);
        if (it == storage_.end()) {
            if (error != nullptr) {
                *error = "missing";
            }
            return false;
        }
        if (out == nullptr) {
            if (error != nullptr) {
                *error = "null out";
            }
            return false;
        }
        *out = it->second;
        return true;
    }

    bool Ping(std::string* error) const override {
        (void)error;
        return true;
    }

    int hset_calls() const { return hset_calls_; }

private:
    int fail_times_{0};
    int hset_calls_{0};
    std::unordered_map<std::string,
                       std::unordered_map<std::string, std::string>>
        storage_;
};

}  // namespace

TEST(RedisRealtimeStoreClientAdapterTest, RoundTripsOrderAndMarketData) {
    auto client = std::make_shared<InMemoryRedisHashClient>();
    RedisRealtimeStoreClientAdapter store(client, StorageRetryPolicy{});

    MarketSnapshot market;
    market.instrument_id = "SHFE.ag2406";
    market.last_price = 4520.5;
    market.recv_ts_ns = 100;
    store.UpsertMarketSnapshot(market);

    OrderEvent order;
    order.account_id = "acc-1";
    order.client_order_id = "ord-1";
    order.instrument_id = "SHFE.ag2406";
    order.status = OrderStatus::kPartiallyFilled;
    order.total_volume = 4;
    order.filled_volume = 2;
    order.avg_fill_price = 4520.0;
    order.ts_ns = 101;
    order.venue = "SIM";
    order.route_id = "route-sim-2";
    order.slippage_bps = 1.0;
    order.impact_cost = 6.5;
    store.UpsertOrderEvent(order);

    MarketSnapshot got_market;
    ASSERT_TRUE(store.GetMarketSnapshot("SHFE.ag2406", &got_market));
    EXPECT_DOUBLE_EQ(got_market.last_price, 4520.5);

    OrderEvent got_order;
    ASSERT_TRUE(store.GetOrderEvent("ord-1", &got_order));
    EXPECT_EQ(got_order.status, OrderStatus::kPartiallyFilled);
    EXPECT_EQ(got_order.filled_volume, 2);
    EXPECT_EQ(got_order.venue, "SIM");
    EXPECT_EQ(got_order.route_id, "route-sim-2");
    EXPECT_DOUBLE_EQ(got_order.slippage_bps, 1.0);
    EXPECT_DOUBLE_EQ(got_order.impact_cost, 6.5);
}

TEST(RedisRealtimeStoreClientAdapterTest, RoundTripsStateSnapshot7D) {
    auto client = std::make_shared<InMemoryRedisHashClient>();
    RedisRealtimeStoreClientAdapter store(client, StorageRetryPolicy{});

    StateSnapshot7D state;
    state.instrument_id = "SHFE.ag2406";
    state.trend = {0.12, 0.9};
    state.volatility = {0.34, 0.8};
    state.liquidity = {0.56, 0.7};
    state.sentiment = {-0.78, 0.6};
    state.seasonality = {0.0, 0.2};
    state.pattern = {0.1, 0.3};
    state.event_drive = {0.0, 0.2};
    state.ts_ns = 123;

    store.UpsertStateSnapshot7D(state);

    StateSnapshot7D got;
    ASSERT_TRUE(store.GetStateSnapshot7D("SHFE.ag2406", &got));
    EXPECT_EQ(got.instrument_id, "SHFE.ag2406");
    EXPECT_DOUBLE_EQ(got.trend.score, 0.12);
    EXPECT_DOUBLE_EQ(got.trend.confidence, 0.9);
    EXPECT_EQ(got.ts_ns, 123);
}

TEST(RedisRealtimeStoreClientAdapterTest, RetriesTransientWriteFailure) {
    auto client = std::make_shared<FlakyRedisClient>(2);
    StorageRetryPolicy policy;
    policy.max_attempts = 3;
    policy.initial_backoff_ms = 0;
    policy.max_backoff_ms = 0;
    RedisRealtimeStoreClientAdapter store(client, policy);

    MarketSnapshot market;
    market.instrument_id = "SHFE.ag2406";
    market.last_price = 4520.5;
    market.recv_ts_ns = 100;
    store.UpsertMarketSnapshot(market);

    EXPECT_EQ(client->hset_calls(), 3);
}

TEST(RedisRealtimeStoreClientAdapterTest, StopsAtMaxAttempts) {
    auto client = std::make_shared<FlakyRedisClient>(10);
    StorageRetryPolicy policy;
    policy.max_attempts = 2;
    policy.initial_backoff_ms = 0;
    policy.max_backoff_ms = 0;
    RedisRealtimeStoreClientAdapter store(client, policy);

    MarketSnapshot market;
    market.instrument_id = "SHFE.ag2406";
    market.last_price = 4520.5;
    market.recv_ts_ns = 100;
    store.UpsertMarketSnapshot(market);

    EXPECT_EQ(client->hset_calls(), 2);
}

}  // namespace quant_hft
