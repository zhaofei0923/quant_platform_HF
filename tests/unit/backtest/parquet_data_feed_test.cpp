#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

#include "quant_hft/backtest/parquet_data_feed.h"

namespace quant_hft {

TEST(ParquetDataFeedTest, RegisterAndQueryByWindowAndInstrument) {
    ParquetDataFeed feed;
    EXPECT_TRUE(feed.RegisterPartition(ParquetPartitionMeta{
        .file_path = "runtime/backtest/parquet/source=rb/trading_day=20260101/instrument_id=rb2405/part-0000.parquet",
        .trading_day = "20260101",
        .instrument_id = "rb2405",
        .min_ts_ns = 100,
        .max_ts_ns = 200,
        .row_count = 10,
    }));
    EXPECT_TRUE(feed.RegisterPartition(ParquetPartitionMeta{
        .file_path = "runtime/backtest/parquet/source=rb/trading_day=20260101/instrument_id=rb2406/part-0000.parquet",
        .trading_day = "20260101",
        .instrument_id = "rb2406",
        .min_ts_ns = 220,
        .max_ts_ns = 300,
        .row_count = 8,
    }));

    const auto filtered = feed.QueryPartitions(120, 260, "rb2405");
    ASSERT_EQ(filtered.size(), 1U);
    EXPECT_EQ(filtered.front().instrument_id, "rb2405");
    EXPECT_EQ(filtered.front().min_ts_ns, 100);
    EXPECT_EQ(feed.PartitionCount(), 2U);
}

TEST(ParquetDataFeedTest, DiscoverFromDirectoryParsesPartitionAndMeta) {
    const std::filesystem::path root =
        std::filesystem::temp_directory_path() / "quant_hft_parquet_feed_test";
    std::filesystem::remove_all(root);

    const std::filesystem::path partition =
        root / "source=rb" / "trading_day=20260102" / "instrument_id=rb2405";
    std::filesystem::create_directories(partition);

    const std::filesystem::path parquet_file = partition / "part-0000.parquet";
    std::ofstream parquet_out(parquet_file);
    parquet_out << "PAR1";
    parquet_out.close();

    std::ofstream meta_out(parquet_file.string() + ".meta");
    meta_out << "min_ts_ns=1000\n";
    meta_out << "max_ts_ns=2000\n";
    meta_out << "row_count=25\n";
    meta_out.close();

    ParquetDataFeed feed;
    const auto found = feed.DiscoverFromDirectory(root.string());
    ASSERT_EQ(found.size(), 1U);
    EXPECT_EQ(found.front().trading_day, "20260102");
    EXPECT_EQ(found.front().instrument_id, "rb2405");
    EXPECT_EQ(found.front().min_ts_ns, 1000);
    EXPECT_EQ(found.front().max_ts_ns, 2000);
    EXPECT_EQ(found.front().row_count, 25U);

    std::filesystem::remove_all(root);
}

}  // namespace quant_hft
