#include <gtest/gtest.h>

#include "quant_hft/core/event_object_pool.h"

namespace quant_hft {

TEST(EventObjectPoolTest, ReusesReleasedBuffer) {
    EventObjectPool pool(/*capacity=*/1, /*buffer_size=*/64);
    auto first = pool.Acquire();
    ASSERT_NE(first, nullptr);
    const auto* first_ptr = first.get();
    first.reset();

    auto second = pool.Acquire();
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(second.get(), first_ptr);
}

TEST(EventObjectPoolTest, FallsBackWhenCapacityExceeded) {
    EventObjectPool pool(/*capacity=*/1, /*buffer_size=*/16);
    auto first = pool.Acquire();
    auto fallback = pool.Acquire();
    ASSERT_NE(first, nullptr);
    ASSERT_NE(fallback, nullptr);
    EXPECT_NE(first.get(), fallback.get());

    const auto stats = pool.Snapshot();
    EXPECT_EQ(stats.fallback_allocations, 1U);
}

TEST(EventObjectPoolTest, ExpandsBufferForLargeEvents) {
    EventObjectPool pool(/*capacity=*/1, /*buffer_size=*/8);
    auto buffer = pool.Acquire(/*min_capacity=*/64);
    ASSERT_NE(buffer, nullptr);
    EXPECT_GE(buffer->size(), 64U);
}

}  // namespace quant_hft
