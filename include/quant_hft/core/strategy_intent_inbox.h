#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/redis_hash_client.h"

namespace quant_hft {

struct StrategyIntentBatch {
    std::int64_t seq{0};
    EpochNanos ts_ns{0};
    std::vector<SignalIntent> intents;
};

class StrategyIntentInbox {
public:
    explicit StrategyIntentInbox(std::shared_ptr<IRedisHashClient> client);

    bool ReadLatest(const std::string& strategy_id,
                    StrategyIntentBatch* out,
                    std::string* error);

private:
    static std::string BuildKey(const std::string& strategy_id);
    static bool ParseInt64(const std::unordered_map<std::string, std::string>& hash,
                           const std::string& key,
                           std::int64_t* out);
    static std::string GetOrEmpty(const std::unordered_map<std::string, std::string>& hash,
                                  const std::string& key);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::int64_t> last_seq_by_strategy_;
    std::shared_ptr<IRedisHashClient> client_;
};

}  // namespace quant_hft
