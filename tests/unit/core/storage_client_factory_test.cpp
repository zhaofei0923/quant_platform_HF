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
    setenv("QUANT_HFT_STORAGE_ALLOW_FALLBACK", "false", 1);

    const auto config = StorageConnectionConfig::FromEnvironment();
    EXPECT_EQ(config.redis.mode, StorageBackendMode::kExternal);
    EXPECT_EQ(config.redis.host, "127.0.0.1");
    EXPECT_EQ(config.redis.port, 6380);
    EXPECT_EQ(config.timescale.mode, StorageBackendMode::kExternal);
    EXPECT_EQ(config.timescale.dsn, "postgres://user:pwd@localhost:5432/quant");
    EXPECT_EQ(config.timescale.trading_schema, "trading_core");
    EXPECT_EQ(config.timescale.analytics_schema, "analytics_ts");
    EXPECT_FALSE(config.allow_inmemory_fallback);
}

}  // namespace quant_hft
