#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "quant_hft/backtest/live_data_feed.h"

namespace quant_hft::backtest {

TEST(LiveDataFeedTest, SubscribeDoesNotCrash) {
    LiveDataFeed feed;
    EXPECT_NO_THROW(feed.Subscribe({"rb2405"}, nullptr, nullptr));
}

TEST(LiveDataFeedTest, RunStopBlocksAndUnblocks) {
    LiveDataFeed feed;
    std::thread worker([&feed]() { feed.Run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    feed.Stop();
    worker.join();
    SUCCEED();
}

TEST(LiveDataFeedTest, GetHistoryReturnsEmpty) {
    LiveDataFeed feed;
    auto start = Timestamp::FromSql("2024-01-01");
    auto end = Timestamp::FromSql("2024-01-02");

    auto bars = feed.GetHistoryBars("rb2405", start, end, "1min");
    EXPECT_TRUE(bars.empty());

    auto ticks = feed.GetHistoryTicks("rb2405", start, end);
    EXPECT_TRUE(ticks.empty());
}

TEST(LiveDataFeedTest, IsLiveReturnsTrue) {
    LiveDataFeed feed;
    EXPECT_TRUE(feed.IsLive());
}

}  // namespace quant_hft::backtest
