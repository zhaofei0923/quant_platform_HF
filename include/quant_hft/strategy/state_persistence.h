#pragma once

#include <memory>
#include <string>

#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/strategy/live_strategy.h"

namespace quant_hft {

class IStrategyStatePersistence {
   public:
    virtual ~IStrategyStatePersistence() = default;
    virtual bool SaveStrategyState(const std::string& account_id, const std::string& strategy_id,
                                   const StrategyState& state, std::string* error) = 0;
    virtual bool LoadStrategyState(const std::string& account_id, const std::string& strategy_id,
                                   StrategyState* state, std::string* error) const = 0;
};

class RedisStrategyStatePersistence final : public IStrategyStatePersistence {
   public:
    RedisStrategyStatePersistence(std::shared_ptr<IRedisHashClient> redis_client,
                                  std::string key_prefix, int ttl_seconds);

    bool SaveStrategyState(const std::string& account_id, const std::string& strategy_id,
                           const StrategyState& state, std::string* error) override;
    bool LoadStrategyState(const std::string& account_id, const std::string& strategy_id,
                           StrategyState* state, std::string* error) const override;

   private:
    std::string BuildKey(const std::string& account_id, const std::string& strategy_id) const;

    std::shared_ptr<IRedisHashClient> redis_client_;
    std::string key_prefix_;
    int ttl_seconds_{0};
};

}  // namespace quant_hft
