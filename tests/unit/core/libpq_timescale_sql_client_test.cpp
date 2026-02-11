#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "quant_hft/core/libpq_timescale_sql_client.h"
#include "quant_hft/core/storage_connection_config.h"

namespace quant_hft {

namespace {

TimescaleConnectionConfig BuildConfig() {
    TimescaleConnectionConfig config;
    config.mode = StorageBackendMode::kExternal;
    config.host = "127.0.0.1";
    config.port = 1;
    config.database = "quant";
    config.user = "postgres";
    config.password = "postgres";
    config.connect_timeout_ms = 200;
    return config;
}

}  // namespace

TEST(LibpqTimescaleSqlClientTest, RejectsInvalidTableNameBeforeNetworkAccess) {
    LibpqTimescaleSqlClient client(BuildConfig());
    std::string error;
    EXPECT_FALSE(client.InsertRow("order-events",
                                  std::unordered_map<std::string, std::string>{{"k", "v"}},
                                  &error));
    EXPECT_NE(error.find("invalid table"), std::string::npos);
}

TEST(LibpqTimescaleSqlClientTest, PingReturnsFalseWhenServerUnavailable) {
    LibpqTimescaleSqlClient client(BuildConfig());
    std::string error;
    EXPECT_FALSE(client.Ping(&error));
    EXPECT_FALSE(error.empty());
}

}  // namespace quant_hft
