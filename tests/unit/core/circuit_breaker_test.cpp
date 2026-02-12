#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "quant_hft/core/circuit_breaker.h"

namespace quant_hft {

TEST(CircuitBreakerTest, OpensAfterFailureThreshold) {
    CircuitBreakerConfig config;
    config.failure_threshold = 2;
    config.timeout_ms = 100;
    config.half_open_timeout_ms = 100;
    CircuitBreaker breaker(config);

    EXPECT_TRUE(breaker.AllowRequest());
    breaker.RecordFailure();
    EXPECT_EQ(breaker.CurrentState(), CircuitBreaker::State::kClosed);
    breaker.RecordFailure();
    EXPECT_EQ(breaker.CurrentState(), CircuitBreaker::State::kOpen);
    EXPECT_FALSE(breaker.AllowRequest());
}

TEST(CircuitBreakerTest, TransitionsToHalfOpenThenClosedOnSuccess) {
    CircuitBreakerConfig config;
    config.failure_threshold = 1;
    config.timeout_ms = 100;
    config.half_open_timeout_ms = 10;
    CircuitBreaker breaker(config);

    breaker.RecordFailure();
    EXPECT_EQ(breaker.CurrentState(), CircuitBreaker::State::kOpen);
    EXPECT_FALSE(breaker.AllowRequest());

    std::this_thread::sleep_for(std::chrono::milliseconds(12));
    EXPECT_TRUE(breaker.AllowRequest());
    EXPECT_EQ(breaker.CurrentState(), CircuitBreaker::State::kHalfOpen);

    breaker.RecordSuccess();
    EXPECT_EQ(breaker.CurrentState(), CircuitBreaker::State::kClosed);
}

TEST(CircuitBreakerManagerTest, DisabledScopeBypassesBreaker) {
    CircuitBreakerManager manager;
    CircuitBreakerConfig config;
    config.failure_threshold = 1;
    config.timeout_ms = 100;
    config.half_open_timeout_ms = 100;
    manager.Configure(BreakerScope::kStrategy, config, /*enabled=*/false);

    EXPECT_TRUE(manager.Allow(BreakerScope::kStrategy, "strat-A"));
    manager.RecordFailure(BreakerScope::kStrategy, "strat-A");
    EXPECT_EQ(manager.CurrentState(BreakerScope::kStrategy, "strat-A"),
              CircuitBreaker::State::kClosed);
}

TEST(CircuitBreakerManagerTest, TracksIndependentScopes) {
    CircuitBreakerManager manager;
    CircuitBreakerConfig config;
    config.failure_threshold = 1;
    config.timeout_ms = 100;
    config.half_open_timeout_ms = 100;
    manager.Configure(BreakerScope::kAccount, config, /*enabled=*/true);
    manager.Configure(BreakerScope::kSystem, config, /*enabled=*/true);

    manager.RecordFailure(BreakerScope::kAccount, "acc-1");
    EXPECT_EQ(manager.CurrentState(BreakerScope::kAccount, "acc-1"),
              CircuitBreaker::State::kOpen);

    EXPECT_EQ(manager.CurrentState(BreakerScope::kSystem, "ignored"),
              CircuitBreaker::State::kClosed);
}

}  // namespace quant_hft
