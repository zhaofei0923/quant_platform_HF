#include "quant_hft/core/query_scheduler.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
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
    scheduler.MarkComplete(0, 0);

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
    scheduler.MarkComplete(2, 0);
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

    scheduler.MarkComplete(1, 0);
    EXPECT_EQ(scheduler.DrainOnce(), 1U);
    EXPECT_EQ(executed, 2);
    EXPECT_TRUE(observed_state.expired());
}

TEST(QuerySchedulerTest, CompletionMustMatchTheActiveRequestAndGeneration) {
    QueryScheduler scheduler;
    scheduler.Reset(2);
    QueryScheduler::QueryTask task;
    task.request_id = 7;
    task.generation = 2;
    task.execute = [] {};
    ASSERT_TRUE(scheduler.TrySchedule(task));
    ASSERT_EQ(scheduler.DrainOnce(), 1U);
    EXPECT_FALSE(scheduler.MarkComplete(7, 1));
    EXPECT_FALSE(scheduler.MarkComplete(8, 2));
    EXPECT_TRUE(scheduler.IsActive(7, 2));
    EXPECT_TRUE(scheduler.MarkComplete(7, 2));
    EXPECT_FALSE(scheduler.TrySchedule(task));  // IDs are not reused within one connection.
}

TEST(QuerySchedulerTest, IntermediateResponseErrorPersistsUntilCompletion) {
    QueryScheduler scheduler;
    QueryScheduler::QueryTask task;
    task.request_id = 7;
    task.execute = [] {};
    ASSERT_TRUE(scheduler.TrySchedule(task));
    ASSERT_EQ(scheduler.DrainOnce(), 1U);
    EXPECT_TRUE(scheduler.RecordResponse(7, 0, true));
    EXPECT_FALSE(scheduler.RecordResponse(7, 0, false));
    EXPECT_FALSE(scheduler.RecordResponse(7, 0, true));
}

TEST(QuerySchedulerTest, TimeoutDoesNotLetLateCompletionReleaseNextRequest) {
    QueryScheduler scheduler;
    bool expired = false;
    QueryScheduler::QueryTask first;
    first.request_id = 1;
    first.timeout = std::chrono::milliseconds(0);
    first.execute = [] {};
    first.on_timeout = [&] { expired = true; };
    auto second = first;
    second.request_id = 2;
    second.timeout = std::chrono::seconds(5);
    ASSERT_TRUE(scheduler.TrySchedule(first));
    ASSERT_TRUE(scheduler.TrySchedule(second));
    ASSERT_EQ(scheduler.DrainOnce(), 1U);
    ASSERT_EQ(scheduler.DrainOnce(), 1U);
    EXPECT_TRUE(expired);
    EXPECT_FALSE(scheduler.MarkComplete(1, 0));
    EXPECT_TRUE(scheduler.IsActive(2, 0));
}

TEST(QuerySchedulerTest, ResetWaitsForExecutingRequestBeforeApiCanBeReleased) {
    QueryScheduler scheduler;
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    QueryScheduler::QueryTask task;
    task.request_id = 1;
    task.execute = [&] {
        entered.set_value();
        released.wait();
    };
    ASSERT_TRUE(scheduler.TrySchedule(task));
    auto drain = std::async(std::launch::async, [&] { return scheduler.DrainOnce(); });
    entered.get_future().wait();
    auto reset = std::async(std::launch::async, [&] { scheduler.Reset(1); });
    EXPECT_EQ(reset.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(drain.get(), 1U);
    reset.get();
    EXPECT_FALSE(scheduler.IsActive(1, 0));
    task.generation = 1;
    task.execute = [] {};
    EXPECT_TRUE(scheduler.TrySchedule(task));
}

TEST(QuerySchedulerTest, ThrowingRequestAndFailureHandlerDoNotEscapePoller) {
    QueryScheduler scheduler;
    int failed = 0;
    QueryScheduler::QueryTask task;
    task.request_id = 1;
    task.execute = [] { throw std::runtime_error("request failed"); };
    task.on_timeout = [&] {
        ++failed;
        throw std::runtime_error("handler failed");
    };
    ASSERT_TRUE(scheduler.TrySchedule(task));
    EXPECT_NO_THROW(EXPECT_EQ(scheduler.DrainOnce(), 1U));
    EXPECT_EQ(failed, 1);
    EXPECT_FALSE(scheduler.IsActive(1, 0));
}

}  // namespace quant_hft
