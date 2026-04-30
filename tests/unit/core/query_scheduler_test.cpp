#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "quant_hft/core/query_scheduler.h"

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

}  // namespace quant_hft
