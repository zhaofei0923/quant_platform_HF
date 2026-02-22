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

    bool HIncrBy(const std::string& key,
                 const std::string& field,
                 std::int64_t delta,
                 std::string* error) override {
        auto& hash = storage_[key];
        std::int64_t current = 0;
        const auto it = hash.find(field);
        if (it != hash.end() && !it->second.empty()) {
            current = std::stoll(it->second);
        }
        hash[field] = std::to_string(current + delta);
        (void)error;
        return true;
    }

    bool Expire(const std::string& key, int ttl_seconds, std::string* error) override {
        if (ttl_seconds <= 0) {
            if (error != nullptr) {
                *error = "invalid ttl";
            }
            return false;
        }
        const auto it = storage_.find(key);
        if (it == storage_.end()) {
            if (error != nullptr) {
                *error = "missing";
            }
            return false;
        }
        ++expire_calls_[key];
        return true;
    }

    bool Ping(std::string* error) const override {
        (void)error;
        return true;
    }

    int hset_calls() const { return hset_calls_; }
    int expire_calls_for(const std::string& key) const {
        const auto it = expire_calls_.find(key);
        if (it == expire_calls_.end()) {
            return 0;
        }
        return it->second;
    }

private:
    int fail_times_{0};
    int hset_calls_{0};
    std::unordered_map<std::string, int> expire_calls_;
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
    order.exchange_id = "SHFE";
    order.status_msg = "partially traded";
    order.order_submit_status = "3";
    order.order_ref = "1001";
    order.front_id = 7;
    order.session_id = 8;
    order.trade_id = "trade-1";
    order.event_source = "OnRtnOrder";
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
    EXPECT_EQ(got_order.exchange_id, "SHFE");
    EXPECT_EQ(got_order.status_msg, "partially traded");
    EXPECT_EQ(got_order.order_submit_status, "3");
    EXPECT_EQ(got_order.order_ref, "1001");
    EXPECT_EQ(got_order.front_id, 7);
    EXPECT_EQ(got_order.session_id, 8);
    EXPECT_EQ(got_order.trade_id, "trade-1");
    EXPECT_EQ(got_order.event_source, "OnRtnOrder");
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
    state.bar_open = 4510.0;
    state.bar_high = 4530.0;
    state.bar_low = 4500.0;
    state.bar_close = 4520.0;
    state.bar_volume = 123.0;
    state.timeframe_minutes = 5;
    state.has_bar = true;
    state.market_regime = MarketRegime::kWeakTrend;
    state.ts_ns = 123;

    store.UpsertStateSnapshot7D(state);

    StateSnapshot7D got;
    ASSERT_TRUE(store.GetStateSnapshot7D("SHFE.ag2406", &got));
    EXPECT_EQ(got.instrument_id, "SHFE.ag2406");
    EXPECT_DOUBLE_EQ(got.trend.score, 0.12);
    EXPECT_DOUBLE_EQ(got.trend.confidence, 0.9);
    EXPECT_DOUBLE_EQ(got.bar_open, 4510.0);
    EXPECT_DOUBLE_EQ(got.bar_high, 4530.0);
    EXPECT_DOUBLE_EQ(got.bar_low, 4500.0);
    EXPECT_DOUBLE_EQ(got.bar_close, 4520.0);
    EXPECT_DOUBLE_EQ(got.bar_volume, 123.0);
    EXPECT_EQ(got.timeframe_minutes, 5);
    EXPECT_TRUE(got.has_bar);
    EXPECT_EQ(got.market_regime, MarketRegime::kWeakTrend);
    EXPECT_EQ(got.ts_ns, 123);
}

TEST(RedisRealtimeStoreClientAdapterTest, ReadsLegacyStateSnapshotWithoutBarFields) {
    auto client = std::make_shared<InMemoryRedisHashClient>();
    RedisRealtimeStoreClientAdapter store(client, StorageRetryPolicy{});

    std::string error;
    ASSERT_TRUE(client->HSet(
        RedisKeyBuilder::StateSnapshot7DLatest("SHFE.ag2406"),
        {{"instrument_id", "SHFE.ag2406"},
         {"trend_score", "0.1"},
         {"trend_confidence", "0.9"},
         {"volatility_score", "0.2"},
         {"volatility_confidence", "0.8"},
         {"liquidity_score", "0.3"},
         {"liquidity_confidence", "0.7"},
         {"sentiment_score", "0.4"},
         {"sentiment_confidence", "0.6"},
         {"seasonality_score", "0.0"},
         {"seasonality_confidence", "0.2"},
         {"pattern_score", "0.1"},
         {"pattern_confidence", "0.3"},
         {"event_drive_score", "0.0"},
         {"event_drive_confidence", "0.2"},
         {"ts_ns", "100"}},
        &error))
        << error;

    StateSnapshot7D got;
    ASSERT_TRUE(store.GetStateSnapshot7D("SHFE.ag2406", &got));
    EXPECT_DOUBLE_EQ(got.bar_open, 0.0);
    EXPECT_DOUBLE_EQ(got.bar_high, 0.0);
    EXPECT_DOUBLE_EQ(got.bar_low, 0.0);
    EXPECT_DOUBLE_EQ(got.bar_close, 0.0);
    EXPECT_DOUBLE_EQ(got.bar_volume, 0.0);
    EXPECT_EQ(got.timeframe_minutes, 1);
    EXPECT_FALSE(got.has_bar);
    EXPECT_EQ(got.market_regime, MarketRegime::kUnknown);
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

TEST(RedisRealtimeStoreClientAdapterTest, AppliesTtlByKeyType) {
    auto client = std::make_shared<FlakyRedisClient>(0);
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

    OrderEvent order;
    order.account_id = "acc-1";
    order.client_order_id = "ord-ttl";
    order.instrument_id = "SHFE.ag2406";
    order.status = OrderStatus::kAccepted;
    order.total_volume = 2;
    order.filled_volume = 0;
    order.ts_ns = 101;
    store.UpsertOrderEvent(order);

    PositionSnapshot position;
    position.account_id = "acc-1";
    position.instrument_id = "SHFE.ag2406";
    position.direction = PositionDirection::kLong;
    position.volume = 1;
    position.ts_ns = 102;
    store.UpsertPositionSnapshot(position);

    StateSnapshot7D state;
    state.instrument_id = "SHFE.ag2406";
    state.trend = {0.1, 0.9};
    state.volatility = {0.2, 0.8};
    state.liquidity = {0.3, 0.7};
    state.sentiment = {0.1, 0.2};
    state.seasonality = {0.1, 0.2};
    state.pattern = {0.1, 0.2};
    state.event_drive = {0.1, 0.2};
    state.ts_ns = 103;
    store.UpsertStateSnapshot7D(state);

    EXPECT_EQ(client->expire_calls_for(RedisKeyBuilder::MarketTickLatest("SHFE.ag2406")), 1);
    EXPECT_EQ(client->expire_calls_for(RedisKeyBuilder::OrderInfo("ord-ttl")), 1);
    EXPECT_EQ(client->expire_calls_for(RedisKeyBuilder::StateSnapshot7DLatest("SHFE.ag2406")), 1);
    EXPECT_EQ(client->expire_calls_for(RedisKeyBuilder::Position("acc-1",
                                                                 "SHFE.ag2406",
                                                                 PositionDirection::kLong)),
              0);
}

}  // namespace quant_hft
