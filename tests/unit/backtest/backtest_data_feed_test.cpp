#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

#include "quant_hft/backtest/backtest_data_feed.h"

namespace fs = std::filesystem;

namespace quant_hft::backtest {

class BacktestDataFeedTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_dir_ = fs::temp_directory_path() / "backtest_data_feed_test";
        fs::remove_all(temp_dir_);

        partition_dir_ =
            temp_dir_ / "source=rb" / "trading_day=2024-01-01" / "instrument_id=rb2405";
        fs::create_directories(partition_dir_);

        parquet_file_ = partition_dir_ / "part-0000.parquet";
        std::ofstream parquet(parquet_file_);
        parquet << "PAR1";

        std::ofstream meta(parquet_file_.string() + ".meta");
        meta << "min_ts_ns=1704067200000000000\n";
        meta << "max_ts_ns=1704067201000000000\n";
        meta << "row_count=2\n";

        std::ofstream ticks(parquet_file_.string() + ".ticks.csv");
        ticks << "symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,ask_volume1,volume,turnover,open_interest\n";
        ticks << "rb2405,SHFE,1704067200000000000,3500.0,1,3499.0,2,3501.0,3,100,350000.0,1200000\n";
        ticks << "rb2405,SHFE,1704067201000000000,3501.0,1,3500.0,2,3502.0,3,101,353601.0,1200100\n";
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
