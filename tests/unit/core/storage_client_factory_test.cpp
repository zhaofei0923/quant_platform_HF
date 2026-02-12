#include <cstdlib>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "quant_hft/core/storage_client_factory.h"

namespace quant_hft {

TEST(StorageClientFactoryTest, CreatesInMemoryClientsByDefault) {
    StorageConnectionConfig config;
    std::string error;

    auto redis = StorageClientFactory::CreateRedisClient(config, &error);
    ASSERT_NE(redis, nullptr);
    EXPECT_TRUE(redis->Ping(&error));

    auto timescale = StorageClientFactory::CreateTimescaleClient(config, &error);
    ASSERT_NE(timescale, nullptr);
    EXPECT_TRUE(timescale->Ping(&error));
}

TEST(StorageClientFactoryTest, ExternalModeWithoutDriversReturnsUnavailableClient) {
    StorageConnectionConfig config;
    config.redis.mode = StorageBackendMode::kExternal;
    config.timescale.mode = StorageBackendMode::kExternal;
    config.allow_inmemory_fallback = false;

    std::string error;
    auto redis = StorageClientFactory::CreateRedisClient(config, &error);
    ASSERT_NE(redis, nullptr);
    EXPECT_FALSE(redis->Ping(&error));
    EXPECT_NE(error.find("external redis"), std::string::npos);

    error.clear();
    auto timescale = StorageClientFactory::CreateTimescaleClient(config, &error);
    ASSERT_NE(timescale, nullptr);
    EXPECT_FALSE(timescale->Ping(&error));
    EXPECT_NE(error.find("external timescaledb"), std::string::npos);
}

TEST(StorageClientFactoryTest, FallsBackToInMemoryWhenEnabled) {
    StorageConnectionConfig config;
    config.redis.mode = StorageBackendMode::kExternal;
    config.timescale.mode = StorageBackendMode::kExternal;
    config.allow_inmemory_fallback = true;

    std::string error;
    auto redis = StorageClientFactory::CreateRedisClient(config, &error);
    ASSERT_NE(redis, nullptr);
    EXPECT_TRUE(redis->Ping(&error));

    auto timescale = StorageClientFactory::CreateTimescaleClient(config, &error);
    ASSERT_NE(timescale, nullptr);
    EXPECT_TRUE(timescale->Ping(&error));
}

TEST(StorageClientFactoryTest, LoadsConnectionConfigFromEnvironment) {
    setenv("QUANT_HFT_REDIS_MODE", "external", 1);
    setenv("QUANT_HFT_REDIS_HOST", "127.0.0.1", 1);
    setenv("QUANT_HFT_REDIS_PORT", "6380", 1);
    setenv("QUANT_HFT_TIMESCALE_MODE", "external", 1);
    setenv("QUANT_HFT_TIMESCALE_DSN", "postgres://user:pwd@localhost:5432/quant", 1);
    setenv("QUANT_HFT_TRADING_SCHEMA", "trading_core", 1);
    setenv("QUANT_HFT_ANALYTICS_SCHEMA", "analytics_ts", 1);
    setenv("QUANT_HFT_MARKET_BUS_MODE", "kafka", 1);
    setenv("QUANT_HFT_KAFKA_BROKERS", "127.0.0.1:9092", 1);
    setenv("QUANT_HFT_KAFKA_MARKET_TOPIC", "quant_hft.market.snapshots.v1", 1);
    setenv("QUANT_HFT_KAFKA_SPOOL_DIR", "runtime/market_bus_spool", 1);
    setenv("QUANT_HFT_CLICKHOUSE_MODE", "external", 1);
    setenv("QUANT_HFT_CLICKHOUSE_HOST", "127.0.0.1", 1);
    setenv("QUANT_HFT_CLICKHOUSE_PORT", "9000", 1);
    setenv("QUANT_HFT_STORAGE_ALLOW_FALLBACK", "false", 1);

    const auto config = StorageConnectionConfig::FromEnvironment();
    EXPECT_EQ(config.redis.mode, StorageBackendMode::kExternal);
    EXPECT_EQ(config.redis.host, "127.0.0.1");
    EXPECT_EQ(config.redis.port, 6380);
    EXPECT_EQ(config.timescale.mode, StorageBackendMode::kExternal);
    EXPECT_EQ(config.timescale.dsn, "postgres://user:pwd@localhost:5432/quant");
    EXPECT_EQ(config.timescale.trading_schema, "trading_core");
    EXPECT_EQ(config.timescale.analytics_schema, "analytics_ts");
    EXPECT_EQ(config.kafka.mode, MarketBusMode::kKafka);
    EXPECT_EQ(config.kafka.brokers, "127.0.0.1:9092");
    EXPECT_EQ(config.kafka.market_topic, "quant_hft.market.snapshots.v1");
    EXPECT_EQ(config.kafka.spool_dir, "runtime/market_bus_spool");
    EXPECT_EQ(config.clickhouse.mode, StorageBackendMode::kExternal);
    EXPECT_EQ(config.clickhouse.host, "127.0.0.1");
    EXPECT_EQ(config.clickhouse.port, 9000);
    EXPECT_FALSE(config.allow_inmemory_fallback);
}

TEST(StorageClientFactoryTest, CreatesUnavailableKafkaProducerWhenDriverDisabled) {
    StorageConnectionConfig config;
    config.kafka.mode = MarketBusMode::kKafka;
    std::string error;

    auto market_bus = StorageClientFactory::CreateMarketBusProducer(config, &error);
    ASSERT_NE(market_bus, nullptr);

    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.ag2406";
    snapshot.exchange_ts_ns = 1;
    snapshot.recv_ts_ns = 2;

#if defined(QUANT_HFT_ENABLE_KAFKA_EXTERNAL) && QUANT_HFT_ENABLE_KAFKA_EXTERNAL
    (void)error;
    SUCCEED();
#else
    EXPECT_FALSE(market_bus->PublishMarketSnapshot(snapshot, &error));
    EXPECT_NE(error.find("external kafka"), std::string::npos);
#endif
}

TEST(StorageClientFactoryTest, ClickHouseHealthCheckRejectsInvalidPort) {
    StorageConnectionConfig config;
    config.clickhouse.mode = StorageBackendMode::kExternal;
    config.clickhouse.host = "127.0.0.1";
    config.clickhouse.port = 0;

    std::string error;
    EXPECT_FALSE(StorageClientFactory::CheckClickHouseHealth(config, &error));
    EXPECT_NE(error.find("port"), std::string::npos);
}

}  // namespace quant_hft
