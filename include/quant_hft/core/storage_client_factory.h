#pragma once

#include <memory>
#include <string>

#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/core/storage_connection_config.h"
#include "quant_hft/core/timescale_sql_client.h"
#include "quant_hft/interfaces/market_bus_producer.h"

namespace quant_hft {

class StorageClientFactory {
public:
    static std::shared_ptr<IRedisHashClient> CreateRedisClient(
        const StorageConnectionConfig& config,
        std::string* error);

    static std::shared_ptr<ITimescaleSqlClient> CreateTimescaleClient(
        const StorageConnectionConfig& config,
        std::string* error);

    static std::shared_ptr<IMarketBusProducer> CreateMarketBusProducer(
        const StorageConnectionConfig& config,
        std::string* error);

    static bool CheckClickHouseHealth(
        const StorageConnectionConfig& config,
        std::string* error);
};

}  // namespace quant_hft
