#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/interfaces/trading_domain_store.h"

namespace quant_hft {

class PositionManager {
public:
    PositionManager(std::shared_ptr<ITradingDomainStore> domain_store,
                    std::shared_ptr<IRedisHashClient> redis_client);

    bool UpdatePosition(const Trade& trade, std::string* error);
    std::vector<Position> GetCurrentPositions(const std::string& account_id) const;
    bool ReconcilePositions(const std::string& account_id,
                            const std::string& strategy_id,
                            const std::string& trading_day,
                            std::string* error);

private:
    static std::string PositionRedisKey(const std::string& account_id,
                                        const std::string& instrument_id);
    bool SyncPositionToRedis(const Position& before, const Position& after, std::string* error);

    std::shared_ptr<ITradingDomainStore> domain_store_;
    std::shared_ptr<IRedisHashClient> redis_client_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Position> latest_positions_;
};

}  // namespace quant_hft

