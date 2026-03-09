#include <filesystem>

#include <gtest/gtest.h>

#include "quant_hft/backtest/backtest_data_feed.h"
#include "tick_partition_fixture.h"

namespace fs = std::filesystem;

namespace quant_hft::backtest {

class BacktestDataFeedTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_dir_ = fs::temp_directory_path() / "backtest_data_feed_test";
        fs::remove_all(temp_dir_);

        partition_dir_ =
            temp_dir_ / "source=rb" / "trading_day=2024-01-01" / "instrument_id=rb2405";
        parquet_file_ = partition_dir_ / "part-0000.parquet";
        std::string fixture_error;
        ASSERT_TRUE(test::WriteTickPartitionFixture(
            parquet_file_,
            {
                Tick{.symbol = "rb2405",
                     .exchange = "SHFE",
                     .ts_ns = 1704067200000000000,
                     .last_price = 3500.0,
                     .last_volume = 1,
                     .ask_price1 = 3501.0,
                     .ask_volume1 = 3,
                     .bid_price1 = 3499.0,
                     .bid_volume1 = 2,
                     .volume = 100,
                     .turnover = 350000.0,
                     .open_interest = 1200000},
                Tick{.symbol = "rb2405",
                     .exchange = "SHFE",
                     .ts_ns = 1704067201000000000,
                     .last_price = 3501.0,
                     .last_volume = 1,
                     .ask_price1 = 3502.0,
                     .ask_volume1 = 3,
                     .bid_price1 = 3500.0,
                     .bid_volume1 = 2,
                     .volume = 101,
                     .turnover = 353601.0,
                     .open_interest = 1200100},
            },
            &fixture_error))
            << fixture_error;
    }

    void TearDown() override {
        fs::remove_all(temp_dir_);
    }

    fs::path temp_dir_;
    fs::path partition_dir_;
    fs::path parquet_file_;
};

TEST_F(BacktestDataFeedTest, RunInvokesTickCallbackAndAdvancesCurrentTime) {
    BacktestDataFeed feed(temp_dir_.string(), Timestamp::FromSql("2024-01-01"),
                          Timestamp::FromSql("2024-01-02"));
    int callback_count = 0;
    feed.Subscribe({"rb2405"},
                   [&](const Tick&) { ++callback_count; },
                   nullptr);

    feed.Run();

    EXPECT_EQ(callback_count, 2);
    EXPECT_EQ(feed.CurrentTime().ToEpochNanos(), 1704067201000000000);
}

TEST_F(BacktestDataFeedTest, GetHistoryTicksDelegatesToParquetFeed) {
    BacktestDataFeed feed(temp_dir_.string(), Timestamp::FromSql("2024-01-01"),
                          Timestamp::FromSql("2024-01-02"));

    auto ticks = feed.GetHistoryTicks("rb2405", Timestamp::FromSql("2024-01-01"),
                                      Timestamp::FromSql("2024-01-02"));
    ASSERT_EQ(ticks.size(), 2U);
    EXPECT_EQ(ticks.front().symbol, "rb2405");
    EXPECT_EQ(ticks.back().ts_ns, 1704067201000000000);
}

TEST_F(BacktestDataFeedTest, StopInterruptsRunLoop) {
    BacktestDataFeed feed(temp_dir_.string(), Timestamp::FromSql("2024-01-01"),
                          Timestamp::FromSql("2024-01-02"));

    int callback_count = 0;
    feed.Subscribe({"rb2405"},
                   [&](const Tick&) {
                       ++callback_count;
                       feed.Stop();
                   },
                   nullptr);

    feed.Run();
    EXPECT_EQ(callback_count, 1);
}

}  // namespace quant_hft::backtest
