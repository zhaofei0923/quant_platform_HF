#include "quant_hft/strategy/state_persistence.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

namespace quant_hft {
namespace {

TEST(StatePersistenceTest, SavesAndLoadsStrategyState) {
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    RedisStrategyStatePersistence persistence(redis, "strategy_state", 60);

    StrategyState state;
    state["k1"] = "v1";
    state["k2"] = "v2";

    std::string error;
    ASSERT_TRUE(persistence.SaveStrategyState("acct", "alpha", state, &error)) << error;

    StrategyState loaded;
    ASSERT_TRUE(persistence.LoadStrategyState("acct", "alpha", &loaded, &error)) << error;
    EXPECT_EQ(loaded.at("k1"), "v1");
    EXPECT_EQ(loaded.at("k2"), "v2");
}

TEST(StatePersistenceTest, ExpiresWhenTtlElapsed) {
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    RedisStrategyStatePersistence persistence(redis, "strategy_state", 1);

    StrategyState state;
    state["k"] = "v";
    std::string error;
    ASSERT_TRUE(persistence.SaveStrategyState("acct", "beta", state, &error)) << error;

    std::this_thread::sleep_for(std::chrono::seconds(2));

    StrategyState loaded;
    EXPECT_FALSE(persistence.LoadStrategyState("acct", "beta", &loaded, &error));
}

}  // namespace
}  // namespace quant_hft
