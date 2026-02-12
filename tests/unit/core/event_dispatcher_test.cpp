#include <chrono>
#include <mutex>
#include <string>

#include <gtest/gtest.h>

#include "quant_hft/core/event_dispatcher.h"

namespace quant_hft {

TEST(EventDispatcherTest, ProcessesHigherPriorityTasksFirst) {
    EventDispatcher dispatcher(1);
    std::mutex mutex;
    std::string sequence;

    ASSERT_TRUE(dispatcher.Post(
        [&]() {
            std::lock_guard<std::mutex> lock(mutex);
            sequence += "L";
        },
        EventPriority::kLow));
    ASSERT_TRUE(dispatcher.Post(
        [&]() {
            std::lock_guard<std::mutex> lock(mutex);
            sequence += "H";
        },
        EventPriority::kHigh));
    ASSERT_TRUE(dispatcher.Post(
        [&]() {
            std::lock_guard<std::mutex> lock(mutex);
            sequence += "N";
        },
        EventPriority::kNormal));

    dispatcher.Start();
    ASSERT_TRUE(dispatcher.WaitUntilDrained(/*timeout_ms=*/1000));
    dispatcher.Stop();

    EXPECT_EQ(sequence, "HNL");
}

TEST(EventDispatcherTest, StopRejectsNewTasks) {
    EventDispatcher dispatcher(1);
    dispatcher.Start();
    dispatcher.Stop();
    EXPECT_FALSE(dispatcher.Post([] {}, EventPriority::kNormal));
}

TEST(EventDispatcherTest, SnapshotTracksProcessedCount) {
    EventDispatcher dispatcher(2);
    dispatcher.Start();
    ASSERT_TRUE(dispatcher.Post([] {}, EventPriority::kHigh));
    ASSERT_TRUE(dispatcher.Post([] {}, EventPriority::kNormal));
    ASSERT_TRUE(dispatcher.WaitUntilDrained(/*timeout_ms=*/1000));
    const auto stats = dispatcher.Snapshot();
    dispatcher.Stop();

    EXPECT_EQ(stats.pending_high, 0U);
    EXPECT_EQ(stats.pending_normal, 0U);
    EXPECT_EQ(stats.pending_low, 0U);
    EXPECT_GE(stats.processed_total, 2U);
}

}  // namespace quant_hft
