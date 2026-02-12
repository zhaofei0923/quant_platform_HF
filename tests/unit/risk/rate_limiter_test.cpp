#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "quant_hft/core/flow_controller.h"

namespace quant_hft {
namespace {

TEST(RateLimiterTest, TokenBucketRejectsWhenCapacityExhausted) {
    TokenBucket bucket(1.0, 1);
    EXPECT_TRUE(bucket.TryAcquire());
    EXPECT_FALSE(bucket.TryAcquire());
}

TEST(RateLimiterTest, TokenBucketRefillsAfterInterval) {
    TokenBucket bucket(5.0, 1);
    EXPECT_TRUE(bucket.TryAcquire());
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    EXPECT_TRUE(bucket.TryAcquire());
}

}  // namespace
}  // namespace quant_hft
