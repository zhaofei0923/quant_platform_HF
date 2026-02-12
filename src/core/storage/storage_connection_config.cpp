#include "quant_hft/core/storage_connection_config.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>

namespace quant_hft {

namespace {

std::string ToLower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return text;
}

std::string GetEnvOrDefault(const char* key, const std::string& default_value) {
    const char* raw = std::getenv(key);
    if (raw == nullptr) {
        return default_value;
    }
    return std::string(raw);
}

int GetEnvOrDefaultInt(const char* key, int default_value) {
    const char* raw = std::getenv(key);
    if (raw == nullptr) {
        return default_value;
    }
    try {
        return std::stoi(raw);
    } catch (...) {
        return default_value;
    }
}

bool ParseBoolWithDefault(const std::string& raw, bool default_value) {
    const auto value = ToLower(raw);
    if (value == "1" || value == "true" || value == "yes" || value == "on") {
        return true;
    }
    if (value == "0" || value == "false" || value == "no" || value == "off") {
        return false;
    }
    return default_value;
}

StorageBackendMode ParseMode(const std::string& raw, StorageBackendMode default_mode) {
    const auto value = ToLower(raw);
    if (value == "external") {
        return StorageBackendMode::kExternal;
    }
    if (value == "in_memory" || value == "inmemory" || value == "memory") {
        return StorageBackendMode::kInMemory;
    }
    return default_mode;
}

MarketBusMode ParseMarketBusMode(const std::string& raw, MarketBusMode default_mode) {
    const auto value = ToLower(raw);
    if (value == "disabled" || value == "off" || value == "none") {
        return MarketBusMode::kDisabled;
    }
    if (value == "kafka") {
        return MarketBusMode::kKafka;
    }
    return default_mode;
}

}  // namespace

StorageConnectionConfig StorageConnectionConfig::FromEnvironment() {
    StorageConnectionConfig config;
    config.redis.mode = ParseMode(GetEnvOrDefault("QUANT_HFT_REDIS_MODE", "in_memory"),
                                  StorageBackendMode::kInMemory);
    config.redis.host = GetEnvOrDefault("QUANT_HFT_REDIS_HOST", config.redis.host);
    config.redis.port = GetEnvOrDefaultInt("QUANT_HFT_REDIS_PORT", config.redis.port);
    config.redis.username = GetEnvOrDefault("QUANT_HFT_REDIS_USER", config.redis.username);
    config.redis.password = GetEnvOrDefault("QUANT_HFT_REDIS_PASSWORD", config.redis.password);
    config.redis.tls_enabled = ParseBoolWithDefault(
        GetEnvOrDefault("QUANT_HFT_REDIS_TLS", config.redis.tls_enabled ? "true" : "false"),
        config.redis.tls_enabled);
    config.redis.connect_timeout_ms = GetEnvOrDefaultInt("QUANT_HFT_REDIS_CONNECT_TIMEOUT_MS",
                                                         config.redis.connect_timeout_ms);
    config.redis.read_timeout_ms = GetEnvOrDefaultInt("QUANT_HFT_REDIS_READ_TIMEOUT_MS",
                                                      config.redis.read_timeout_ms);

    config.timescale.mode =
        ParseMode(GetEnvOrDefault("QUANT_HFT_TIMESCALE_MODE", "in_memory"),
                  StorageBackendMode::kInMemory);
    config.timescale.dsn = GetEnvOrDefault("QUANT_HFT_TIMESCALE_DSN", config.timescale.dsn);
    config.timescale.host = GetEnvOrDefault("QUANT_HFT_TIMESCALE_HOST", config.timescale.host);
    config.timescale.port = GetEnvOrDefaultInt("QUANT_HFT_TIMESCALE_PORT", config.timescale.port);
    config.timescale.database =
        GetEnvOrDefault("QUANT_HFT_TIMESCALE_DB", config.timescale.database);
    config.timescale.user = GetEnvOrDefault("QUANT_HFT_TIMESCALE_USER", config.timescale.user);
    config.timescale.password =
        GetEnvOrDefault("QUANT_HFT_TIMESCALE_PASSWORD", config.timescale.password);
    config.timescale.ssl_mode =
        GetEnvOrDefault("QUANT_HFT_TIMESCALE_SSLMODE", config.timescale.ssl_mode);
    config.timescale.connect_timeout_ms = GetEnvOrDefaultInt(
        "QUANT_HFT_TIMESCALE_CONNECT_TIMEOUT_MS", config.timescale.connect_timeout_ms);
    config.timescale.trading_schema =
        GetEnvOrDefault("QUANT_HFT_TRADING_SCHEMA", config.timescale.trading_schema);
    config.timescale.analytics_schema =
        GetEnvOrDefault("QUANT_HFT_ANALYTICS_SCHEMA", config.timescale.analytics_schema);

    config.kafka.mode = ParseMarketBusMode(GetEnvOrDefault("QUANT_HFT_MARKET_BUS_MODE", "disabled"),
                                           MarketBusMode::kDisabled);
    config.kafka.brokers = GetEnvOrDefault("QUANT_HFT_KAFKA_BROKERS", config.kafka.brokers);
    config.kafka.market_topic =
        GetEnvOrDefault("QUANT_HFT_KAFKA_MARKET_TOPIC", config.kafka.market_topic);
    config.kafka.spool_dir =
        GetEnvOrDefault("QUANT_HFT_KAFKA_SPOOL_DIR", config.kafka.spool_dir);
    config.kafka.producer_command_template = GetEnvOrDefault(
        "QUANT_HFT_KAFKA_PRODUCER_CMD_TEMPLATE", config.kafka.producer_command_template);
    config.kafka.message_timeout_ms =
        GetEnvOrDefaultInt("QUANT_HFT_KAFKA_MESSAGE_TIMEOUT_MS", config.kafka.message_timeout_ms);

    config.clickhouse.mode = ParseMode(GetEnvOrDefault("QUANT_HFT_CLICKHOUSE_MODE", "in_memory"),
                                       StorageBackendMode::kInMemory);
    config.clickhouse.host =
        GetEnvOrDefault("QUANT_HFT_CLICKHOUSE_HOST", config.clickhouse.host);
    config.clickhouse.port =
        GetEnvOrDefaultInt("QUANT_HFT_CLICKHOUSE_PORT", config.clickhouse.port);
    config.clickhouse.database =
        GetEnvOrDefault("QUANT_HFT_CLICKHOUSE_DB", config.clickhouse.database);
    config.clickhouse.user =
        GetEnvOrDefault("QUANT_HFT_CLICKHOUSE_USER", config.clickhouse.user);
    config.clickhouse.password =
        GetEnvOrDefault("QUANT_HFT_CLICKHOUSE_PASSWORD", config.clickhouse.password);
    config.clickhouse.connect_timeout_ms = GetEnvOrDefaultInt(
        "QUANT_HFT_CLICKHOUSE_CONNECT_TIMEOUT_MS", config.clickhouse.connect_timeout_ms);

    config.allow_inmemory_fallback =
        ParseBoolWithDefault(GetEnvOrDefault("QUANT_HFT_STORAGE_ALLOW_FALLBACK", "true"),
                             true);
    return config;
}

}  // namespace quant_hft
