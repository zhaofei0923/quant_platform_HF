#include "quant_hft/core/circuit_breaker.h"

#include <algorithm>

namespace quant_hft {

CircuitBreaker::CircuitBreaker(CircuitBreakerConfig config)
    : config_(config),
      last_failure_time_(std::chrono::steady_clock::now()) {
    config_.failure_threshold = std::max(1, config_.failure_threshold);
    config_.timeout_ms = std::max(1, config_.timeout_ms);
    config_.half_open_timeout_ms = std::max(1, config_.half_open_timeout_ms);
}

bool CircuitBreaker::AllowRequest() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == State::kClosed) {
        return true;
    }
    const auto now = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - last_failure_time_);
    if (state_ == State::kOpen &&
        elapsed.count() >= config_.half_open_timeout_ms) {
        state_ = State::kHalfOpen;
        return true;
    }
    return state_ == State::kHalfOpen;
}

void CircuitBreaker::RecordSuccess() {
    std::lock_guard<std::mutex> lock(mutex_);
    failure_count_ = 0;
    state_ = State::kClosed;
}

void CircuitBreaker::RecordFailure() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == State::kHalfOpen) {
        state_ = State::kOpen;
        failure_count_ = config_.failure_threshold;
        last_failure_time_ = std::chrono::steady_clock::now();
        return;
    }
    ++failure_count_;
    if (failure_count_ >= config_.failure_threshold) {
        state_ = State::kOpen;
        last_failure_time_ = std::chrono::steady_clock::now();
    }
}

void CircuitBreaker::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = State::kClosed;
    failure_count_ = 0;
    last_failure_time_ = std::chrono::steady_clock::now();
}

CircuitBreaker::State CircuitBreaker::CurrentState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_;
}

CircuitBreakerManager::CircuitBreakerManager() = default;

void CircuitBreakerManager::Configure(BreakerScope scope,
                                      const CircuitBreakerConfig& config,
                                      bool enabled) {
    std::lock_guard<std::mutex> lock(mutex_);
    ScopeConfig scope_config;
    scope_config.enabled = enabled;
    scope_config.config = config;
    switch (scope) {
        case BreakerScope::kStrategy:
            strategy_scope_ = scope_config;
            break;
        case BreakerScope::kAccount:
            account_scope_ = scope_config;
            break;
        case BreakerScope::kSystem:
            system_scope_ = scope_config;
            break;
    }
}

bool CircuitBreakerManager::Allow(BreakerScope scope, const std::string& key) {
    const auto scope_config = ScopeConfigFor(scope);
    if (!scope_config.enabled) {
        return true;
    }
    return GetOrCreate(scope, key)->AllowRequest();
}

void CircuitBreakerManager::RecordSuccess(BreakerScope scope, const std::string& key) {
    const auto scope_config = ScopeConfigFor(scope);
    if (!scope_config.enabled) {
        return;
    }
    GetOrCreate(scope, key)->RecordSuccess();
}

void CircuitBreakerManager::RecordFailure(BreakerScope scope, const std::string& key) {
    const auto scope_config = ScopeConfigFor(scope);
    if (!scope_config.enabled) {
        return;
    }
    GetOrCreate(scope, key)->RecordFailure();
}

CircuitBreaker::State CircuitBreakerManager::CurrentState(BreakerScope scope,
                                                          const std::string& key) const {
    const auto scope_config = ScopeConfigFor(scope);
    if (!scope_config.enabled) {
        return CircuitBreaker::State::kClosed;
    }
    const auto breaker = Get(scope, key);
    return breaker == nullptr ? CircuitBreaker::State::kClosed : breaker->CurrentState();
}

std::size_t CircuitBreakerManager::KeyHash::operator()(const Key& key) const {
    const auto h1 = std::hash<int>{}(static_cast<int>(key.scope));
    const auto h2 = std::hash<std::string>{}(key.id);
    return h1 ^ (h2 << 1U);
}

std::string CircuitBreakerManager::NormalizeId(BreakerScope scope, const std::string& key) {
    if (scope == BreakerScope::kSystem) {
        return "__system__";
    }
    return key;
}

CircuitBreakerManager::ScopeConfig CircuitBreakerManager::ScopeConfigFor(BreakerScope scope) const {
    std::lock_guard<std::mutex> lock(mutex_);
    switch (scope) {
        case BreakerScope::kStrategy:
            return strategy_scope_;
        case BreakerScope::kAccount:
            return account_scope_;
        case BreakerScope::kSystem:
            return system_scope_;
    }
    return system_scope_;
}

std::shared_ptr<CircuitBreaker> CircuitBreakerManager::GetOrCreate(BreakerScope scope,
                                                                   const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto normalized_id = NormalizeId(scope, key);
    const Key map_key{scope, normalized_id};
    if (const auto it = breakers_.find(map_key); it != breakers_.end()) {
        return it->second;
    }
    const auto scope_config = scope == BreakerScope::kStrategy
                                  ? strategy_scope_
                                  : (scope == BreakerScope::kAccount ? account_scope_
                                                                     : system_scope_);
    auto breaker = std::make_shared<CircuitBreaker>(scope_config.config);
    breakers_.emplace(map_key, breaker);
    return breaker;
}

std::shared_ptr<CircuitBreaker> CircuitBreakerManager::Get(BreakerScope scope,
                                                            const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto normalized_id = NormalizeId(scope, key);
    const Key map_key{scope, normalized_id};
    const auto it = breakers_.find(map_key);
    if (it == breakers_.end()) {
        return {};
    }
    return it->second;
}

}  // namespace quant_hft
