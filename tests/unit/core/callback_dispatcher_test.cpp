#include "quant_hft/core/callback_dispatcher.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>

namespace quant_hft {

TEST(CallbackDispatcherTest, NonCriticalTaskDropsWhenQueueFull) {
    CallbackDispatcher dispatcher(1, 10);
    dispatcher.Start();

    std::promise<void> first_started;
    std::promise<void> release_first;
    const auto release_future = release_first.get_future().share();

    ASSERT_TRUE(dispatcher.Post(
        [&first_started, release_future]() {
            first_started.set_value();
            release_future.wait();
        },
        true));

    ASSERT_EQ(first_started.get_future().wait_for(std::chrono::milliseconds(200)),
              std::future_status::ready);
    ASSERT_TRUE(dispatcher.Post([] {}, false));
    EXPECT_FALSE(dispatcher.Post([] {}, false));

    release_first.set_value();
    dispatcher.Stop();

    const auto stats = dispatcher.GetStats();
    EXPECT_GE(stats.dropped, 1U);
}

TEST(CallbackDispatcherTest, CriticalTaskTimesOutWhenQueueFull) {
    CallbackDispatcher dispatcher(1, 10);
    dispatcher.Start();

    std::promise<void> first_started;
    std::promise<void> release_first;
    const auto release_future = release_first.get_future().share();

    ASSERT_TRUE(dispatcher.Post(
        [&first_started, release_future]() {
            first_started.set_value();
            release_future.wait();
        },
        true));

    ASSERT_EQ(first_started.get_future().wait_for(std::chrono::milliseconds(200)),
              std::future_status::ready);
    ASSERT_TRUE(dispatcher.Post([] {}, false));

    const auto started_at = std::chrono::steady_clock::now();
    EXPECT_FALSE(dispatcher.Post([] {}, true));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - started_at)
                             .count();
    EXPECT_GE(elapsed, 8);

    release_first.set_value();
    dispatcher.Stop();

    const auto stats = dispatcher.GetStats();
    EXPECT_GE(stats.critical_timeout, 1U);
}

}  // namespace quant_hft
