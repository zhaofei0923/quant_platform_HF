#include "quant_hft/runtime/account_execution_scheduler.h"

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <mutex>
#include <vector>

namespace quant_hft {

TEST(AccountExecutionSchedulerTest, SerializesSameAccountWhileOtherAccountCanProgress) {
    AccountExecutionScheduler scheduler(16, 2);
    ASSERT_TRUE(scheduler.Start());
    std::promise<void> started;
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<int> active{0};
    std::atomic<int> overlap{0};
    std::atomic<bool> other_finished{false};
    ASSERT_TRUE(scheduler.Submit("a", [&] {
        ++active;
        started.set_value();
        released.wait();
        --active;
    }));
    const bool entered =
        started.get_future().wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    EXPECT_TRUE(scheduler.Submit("a", [&] {
        if (active != 0) ++overlap;
    }));
    EXPECT_TRUE(scheduler.Submit("b", [&] { other_finished = true; }));
    const auto deadline = AccountExecutionScheduler::Clock::now() + std::chrono::seconds(1);
    while (!other_finished && AccountExecutionScheduler::Clock::now() < deadline)
        std::this_thread::yield();
    const bool progressed = other_finished;
    EXPECT_FALSE(scheduler.Drain(1));
    release.set_value();
    EXPECT_TRUE(entered);
    EXPECT_TRUE(progressed);
    EXPECT_TRUE(scheduler.Drain(1000));
    EXPECT_EQ(overlap, 0);
    scheduler.Stop();
}

TEST(AccountExecutionSchedulerTest, HonorsDeadlineAndExplicitCapacityAndCancellation) {
    AccountExecutionScheduler scheduler(2, 1);
    ASSERT_TRUE(scheduler.Start());
    std::atomic<int> called{0};
    const auto later = AccountExecutionScheduler::Clock::now() + std::chrono::seconds(30);
    const auto one = scheduler.SubmitAt("a", later, [&] { ++called; });
    ASSERT_TRUE(one);
    ASSERT_TRUE(scheduler.SubmitAt("a", later, [&] { ++called; }));
    EXPECT_EQ(scheduler.SubmitAt("a", later, [] {}).status,
              AccountExecutionScheduler::SubmitStatus::kFull);
    EXPECT_FALSE(scheduler.Drain(5));
    EXPECT_TRUE(scheduler.Cancel(one.id));
    EXPECT_FALSE(scheduler.Cancel(one.id));
    EXPECT_EQ(scheduler.CancelAccount("a"), 1U);
    EXPECT_TRUE(scheduler.Drain(100));
    EXPECT_EQ(called, 0);
    scheduler.Stop();
    EXPECT_EQ(scheduler.Submit("a", [] {}).status,
              AccountExecutionScheduler::SubmitStatus::kStopped);
}

TEST(AccountExecutionSchedulerTest, StopCancelsFutureTasksAndExceptionsAreObservable) {
    AccountExecutionScheduler scheduler;
    ASSERT_TRUE(scheduler.Start());
    ASSERT_TRUE(scheduler.Submit("a", [] { throw std::runtime_error("execution failure"); }));
    ASSERT_TRUE(scheduler.Drain(1000));
    std::atomic<bool> called{false};
    ASSERT_TRUE(scheduler.SubmitAt("a",
                                   AccountExecutionScheduler::Clock::now() + std::chrono::hours(1),
                                   [&] { called = true; }));
    scheduler.Stop();
    EXPECT_FALSE(called);
    EXPECT_EQ(scheduler.GetStats().failed, 1U);
    EXPECT_EQ(scheduler.GetStats().last_error, "execution failure");
    EXPECT_EQ(scheduler.GetStats().canceled, 1U);
    EXPECT_TRUE(scheduler.Drain(0));
}

}  // namespace quant_hft
