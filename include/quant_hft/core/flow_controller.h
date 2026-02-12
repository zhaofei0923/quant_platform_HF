#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace quant_hft {

enum class OperationType {
    kOrderInsert = 0,
    kOrderCancel = 1,
    kQuery = 2,
    kSettlementQuery = 3,
};

struct Operation {
    std::string account_id;
    OperationType type{OperationType::kOrderInsert};
    std::string instrument_id;
};

struct FlowRule {
    std::string account_id;
    OperationType type{OperationType::kOrderInsert};
    std::string instrument_id;
    double rate_per_second{1.0};
    int capacity{1};
};

struct FlowResult {
    bool allowed{false};
    std::string reason;
    int wait_ms{0};
};

class TokenBucket {
public:
    TokenBucket();
    TokenBucket(double rate_per_second, int capacity);

    bool TryAcquire();
    bool Acquire(int timeout_ms);
    void SetRate(double rate_per_second);

private:
    void RefillLocked(std::chrono::steady_clock::time_point now);

    mutable std::mutex mutex_;
    double rate_per_second_{1.0};
    int capacity_{1};
    double tokens_{1.0};
    std::chrono::steady_clock::time_point last_refill_;
};

class FlowController {
public:
    FlowController() = default;

    void AddRule(const FlowRule& rule);
    FlowResult Check(const Operation& operation);
    FlowResult Acquire(const Operation& operation, int timeout_ms);

private:
    struct Key {
        std::string account_id;
        OperationType type{OperationType::kOrderInsert};
        std::string instrument_id;

        bool operator==(const Key& other) const {
            return account_id == other.account_id &&
                   type == other.type &&
                   instrument_id == other.instrument_id;
        }
    };

    struct KeyHash {
        std::size_t operator()(const Key& key) const;
    };

    std::shared_ptr<TokenBucket> FindBucketLocked(const Operation& operation);

    std::mutex mutex_;
    std::unordered_map<Key, std::shared_ptr<TokenBucket>, KeyHash> buckets_;
};

}  // namespace quant_hft
