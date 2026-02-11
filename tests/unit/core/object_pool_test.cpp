#include <gtest/gtest.h>

#include "quant_hft/core/object_pool.h"

namespace quant_hft {

TEST(ObjectPoolTest, AcquireCreatesSlotWithConfiguredSize) {
    ObjectPool pool(/*capacity=*/2, /*buffer_size=*/64);

    auto buffer = pool.Acquire();
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(buffer->size(), 64U);

    const auto stats = pool.Snapshot();
    EXPECT_EQ(stats.capacity, 2U);
    EXPECT_EQ(stats.created_slots, 1U);
    EXPECT_EQ(stats.in_use_slots, 1U);
}

TEST(ObjectPoolTest, ReusesReleasedSlot) {
    ObjectPool pool(/*capacity=*/1, /*buffer_size=*/16);

    auto first = pool.Acquire();
    ASSERT_NE(first, nullptr);
    const auto* first_ptr = first.get();
    first.reset();

    auto second = pool.Acquire();
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(second.get(), first_ptr);

    const auto stats = pool.Snapshot();
    EXPECT_EQ(stats.reused_slots, 1U);
    EXPECT_EQ(stats.fallback_allocations, 0U);
}

TEST(ObjectPoolTest, UsesFallbackAllocationWhenPoolExhausted) {
    ObjectPool pool(/*capacity=*/1, /*buffer_size=*/8);

    auto pooled = pool.Acquire();
    auto fallback = pool.Acquire();
    ASSERT_NE(pooled, nullptr);
    ASSERT_NE(fallback, nullptr);
    EXPECT_NE(fallback.get(), pooled.get());

    const auto stats = pool.Snapshot();
    EXPECT_EQ(stats.created_slots, 1U);
    EXPECT_EQ(stats.in_use_slots, 1U);
    EXPECT_EQ(stats.fallback_allocations, 1U);
}

}  // namespace quant_hft
