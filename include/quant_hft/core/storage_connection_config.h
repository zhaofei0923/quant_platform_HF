#pragma once

#include <string>

namespace quant_hft {

enum class StorageBackendMode {
    kInMemory,
    kExternal,
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

struct StorageConnectionConfig {
    RedisConnectionConfig redis;
    TimescaleConnectionConfig timescale;
    bool allow_inmemory_fallback{true};

    static StorageConnectionConfig FromEnvironment();
};

}  // namespace quant_hft
