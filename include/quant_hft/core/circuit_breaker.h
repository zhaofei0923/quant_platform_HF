#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace quant_hft {

struct CircuitBreakerConfig {
    int failure_threshold{5};
    int timeout_ms{1000};
    int half_open_timeout_ms{5000};
};

class CircuitBreaker {
public:
    enum class State {
        kClosed = 0,
        kOpen = 1,
        kHalfOpen = 2,
    };

    explicit CircuitBreaker(CircuitBreakerConfig config = {});

    bool AllowRequest();
    void RecordSuccess();
    void RecordFailure();
    void Reset();
    State CurrentState() const;

private:
    CircuitBreakerConfig config_;
    mutable std::mutex mutex_;
    State state_{State::kClosed};
    int failure_count_{0};
    std::chrono::steady_clock::time_point last_failure_time_;
};

enum class BreakerScope {
    kStrategy = 0,
    kAccount = 1,
    kSystem = 2,
};

class CircuitBreakerManager {
public:
    CircuitBreakerManager();

    void Configure(BreakerScope scope, const CircuitBreakerConfig& config, bool enabled);
    bool Allow(BreakerScope scope, const std::string& key);
    void RecordSuccess(BreakerScope scope, const std::string& key);
    void RecordFailure(BreakerScope scope, const std::string& key);
    CircuitBreaker::State CurrentState(BreakerScope scope, const std::string& key) const;

private:
    struct ScopeConfig {
        bool enabled{true};
        CircuitBreakerConfig config{};
    };

    struct Key {
        BreakerScope scope{BreakerScope::kSystem};
        std::string id;

        bool operator==(const Key& other) const {
            return scope == other.scope && id == other.id;
        }
    };

    struct KeyHash {
        std::size_t operator()(const Key& key) const;
    };

    static std::string NormalizeId(BreakerScope scope, const std::string& key);
    ScopeConfig ScopeConfigFor(BreakerScope scope) const;
    std::shared_ptr<CircuitBreaker> GetOrCreate(BreakerScope scope, const std::string& key);
    std::shared_ptr<CircuitBreaker> Get(BreakerScope scope, const std::string& key) const;

    mutable std::mutex mutex_;
    ScopeConfig strategy_scope_;
    ScopeConfig account_scope_;
    ScopeConfig system_scope_;
    std::unordered_map<Key, std::shared_ptr<CircuitBreaker>, KeyHash> breakers_;
};

}  // namespace quant_hft
