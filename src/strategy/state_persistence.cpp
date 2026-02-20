#include "quant_hft/strategy/state_persistence.h"

namespace quant_hft {

RedisStrategyStatePersistence::RedisStrategyStatePersistence(
    std::shared_ptr<IRedisHashClient> redis_client, std::string key_prefix, int ttl_seconds)
    : redis_client_(std::move(redis_client)),
      key_prefix_(key_prefix.empty() ? "strategy_state" : std::move(key_prefix)),
      ttl_seconds_(ttl_seconds) {}

bool RedisStrategyStatePersistence::SaveStrategyState(const std::string& account_id,
                                                      const std::string& strategy_id,
                                                      const StrategyState& state,
                                                      std::string* error) {
    if (redis_client_ == nullptr) {
        if (error != nullptr) {
            *error = "redis client is null";
        }
        return false;
    }
    if (account_id.empty() || strategy_id.empty()) {
        if (error != nullptr) {
            *error = "account_id and strategy_id must be non-empty";
        }
        return false;
    }
    const std::string key = BuildKey(account_id, strategy_id);
    if (!redis_client_->HSet(key, state, error)) {
        return false;
    }
    if (ttl_seconds_ > 0 && !redis_client_->Expire(key, ttl_seconds_, error)) {
        return false;
    }
    return true;
}

bool RedisStrategyStatePersistence::LoadStrategyState(const std::string& account_id,
                                                      const std::string& strategy_id,
                                                      StrategyState* state,
                                                      std::string* error) const {
    if (state == nullptr) {
        if (error != nullptr) {
            *error = "state output is null";
        }
        return false;
    }
    if (redis_client_ == nullptr) {
        if (error != nullptr) {
            *error = "redis client is null";
        }
        return false;
    }
    if (account_id.empty() || strategy_id.empty()) {
        if (error != nullptr) {
            *error = "account_id and strategy_id must be non-empty";
        }
        return false;
    }
    return redis_client_->HGetAll(BuildKey(account_id, strategy_id), state, error);
}

std::string RedisStrategyStatePersistence::BuildKey(const std::string& account_id,
                                                    const std::string& strategy_id) const {
    return key_prefix_ + ":" + account_id + ":" + strategy_id;
}

}  // namespace quant_hft
