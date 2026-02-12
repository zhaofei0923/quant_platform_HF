#pragma once

#include <string>

namespace quant_hft {

enum class StorageBackendMode {
    kInMemory,
    kExternal,
};

enum class MarketBusMode {
    kDisabled,
    kKafka,
};

struct RedisConnectionConfig {
    StorageBackendMode mode{StorageBackendMode::kInMemory};
    std::string host{"127.0.0.1"};
    int port{6379};
    std::string username;
    std::string password;
    bool tls_enabled{false};
    int connect_timeout_ms{1000};
    int read_timeout_ms{1000};
};

struct TimescaleConnectionConfig {
    StorageBackendMode mode{StorageBackendMode::kInMemory};
    std::string dsn;
    std::string host{"127.0.0.1"};
    int port{5432};
    std::string database{"quant"};
    std::string user;
    std::string password;
    std::string ssl_mode{"disable"};
    int connect_timeout_ms{2000};
    std::string trading_schema{"trading_core"};
    std::string analytics_schema{"analytics_ts"};
};

struct KafkaConnectionConfig {
    MarketBusMode mode{MarketBusMode::kDisabled};
    std::string brokers{"127.0.0.1:9092"};
    std::string market_topic{"quant_hft.market.snapshots.v1"};
    std::string spool_dir{"runtime/market_bus_spool"};
    std::string producer_command_template{"kcat -P -b {brokers} -t {topic}"};
    int message_timeout_ms{500};
};

struct ClickHouseConnectionConfig {
    StorageBackendMode mode{StorageBackendMode::kInMemory};
    std::string host{"127.0.0.1"};
    int port{9000};
    std::string database{"quant_hft"};
    std::string user{"quant_hft"};
    std::string password{"quant_hft"};
    int connect_timeout_ms{1000};
};

struct StorageConnectionConfig {
    RedisConnectionConfig redis;
    TimescaleConnectionConfig timescale;
    KafkaConnectionConfig kafka;
    ClickHouseConnectionConfig clickhouse;
    bool allow_inmemory_fallback{true};

    static StorageConnectionConfig FromEnvironment();
};

}  // namespace quant_hft
