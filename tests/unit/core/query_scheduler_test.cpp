#include "quant_hft/core/query_scheduler.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

namespace quant_hft {

TEST(QuerySchedulerTest, RespectsRateLimit) {
    QueryScheduler scheduler(10);
    std::atomic<int> executed{0};

    for (int i = 0; i < 20; ++i) {
        scheduler.TrySchedule(QueryScheduler::QueryTask{
            i,
            QueryScheduler::Priority::kNormal,
            [&executed] { executed.fetch_add(1); },
        });
    }

    const auto first = scheduler.DrainOnce();
    EXPECT_EQ(first, 1U);
    EXPECT_EQ(executed.load(), static_cast<int>(first));

    EXPECT_EQ(scheduler.DrainOnce(), 0U);
    scheduler.MarkComplete();

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    const auto second = scheduler.DrainOnce();
    EXPECT_EQ(second, 1U);
    EXPECT_EQ(executed.load(), static_cast<int>(first + second));
    EXPECT_EQ(scheduler.PendingCount(), 20U - first - second);
}

TEST(QuerySchedulerTest, PriorityOrdering) {
    QueryScheduler scheduler(2);
    std::string order;

    scheduler.TrySchedule(QueryScheduler::QueryTask{
        1,
        QueryScheduler::Priority::kLow,
        [&order] { order += "L"; },
    });
    scheduler.TrySchedule(QueryScheduler::QueryTask{
        2,
        QueryScheduler::Priority::kHigh,
        [&order] { order += "H"; },
    });

    scheduler.DrainOnce();
    EXPECT_EQ(order, "H");
    scheduler.MarkComplete();
    scheduler.DrainOnce();
    EXPECT_EQ(order, "HL");
}

TEST(QuerySchedulerTest, DeferredTaskOwnsStateUntilDrainedAfterCompletion) {
    QueryScheduler scheduler(10);
    std::weak_ptr<int> observed_state;
    int executed = 0;

    scheduler.TrySchedule(QueryScheduler::QueryTask{
        1,
        QueryScheduler::Priority::kHigh,
        [&executed] { ++executed; },
    });

    {
        auto state = std::make_shared<int>(41);
        observed_state = state;
        scheduler.TrySchedule(QueryScheduler::QueryTask{
            2,
            QueryScheduler::Priority::kNormal,
            [state, &executed] {
                EXPECT_EQ(*state, 41);
                *state = 42;
                ++executed;
            },
        });
    }

    EXPECT_FALSE(observed_state.expired());
    EXPECT_EQ(scheduler.DrainOnce(), 1U);
    EXPECT_EQ(executed, 1);
    EXPECT_FALSE(observed_state.expired());

    scheduler.MarkComplete();
    EXPECT_EQ(scheduler.DrainOnce(), 1U);
    EXPECT_EQ(executed, 2);
    EXPECT_TRUE(observed_state.expired());
}

TEST(QuerySchedulerTest, PendingTaskWaitsForTokenInsteadOfBeingStranded) {
    QueryScheduler scheduler(20);
    int executed = 0;
    for (int index = 0; index < 20; ++index) {
        ASSERT_TRUE(scheduler.TrySchedule(QueryScheduler::QueryTask{
            index,
            QueryScheduler::Priority::kNormal,
            [&executed] { ++executed; },
        }));
        ASSERT_EQ(scheduler.DrainOnce(), 1U);
        scheduler.MarkComplete();
    }

    ASSERT_TRUE(scheduler.TrySchedule(QueryScheduler::QueryTask{
        21,
        QueryScheduler::Priority::kNormal,
        [&executed] { ++executed; },
    }));
    const auto started_at = std::chrono::steady_clock::now();
    EXPECT_EQ(scheduler.DrainOnce(), 1U);
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - started_at)
                                .count();
    EXPECT_GE(elapsed_ms, 10);
    EXPECT_EQ(executed, 21);
}

}  // namespace quant_hft
