#include "quant_hft/backtest/parquet_data_feed.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>

#include "quant_hft/backtest/replay_runtime.h"
#include "tick_partition_fixture.h"

namespace quant_hft {

TEST(ParquetDataFeedTest, RegisterAndQueryByWindowAndInstrument) {
    ParquetDataFeed feed;
    EXPECT_TRUE(feed.RegisterPartition(ParquetPartitionMeta{
        .file_path = "runtime/backtest/parquet/source=rb/trading_day=20260101/instrument_id=rb2405/"
                     "part-0000.parquet",
        .trading_day = "20260101",
        .instrument_id = "rb2405",
        .min_ts_ns = 100,
        .max_ts_ns = 200,
        .row_count = 10,
    }));
    EXPECT_TRUE(feed.RegisterPartition(ParquetPartitionMeta{
        .file_path = "runtime/backtest/parquet/source=rb/trading_day=20260101/instrument_id=rb2406/"
                     "part-0000.parquet",
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

namespace quant_hft {
TEST(ParquetDataFeedTest, CursorBoundsResidentBatchAndStopsWithoutScanningWholePartition) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("quant_hft_parquet_cursor_batch_" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);
    std::vector<Tick> rows;
    for (int i = 0; i < 20000; ++i) {
        Tick tick;
        tick.symbol = "DCE.c2605";
        tick.ts_ns = 1000 + i;
        tick.last_price = i;
        rows.push_back(tick);
    }
    const auto path = dir / "batch.parquet";
    std::string error;
    ASSERT_TRUE(backtest::test::WriteTickPartitionFixture(path, rows, &error)) << error;
    ParquetPartitionMeta meta;
    meta.file_path = path.string();
    meta.instrument_id = "DCE.c2605";
    ParquetTickCursor cursor;
    ASSERT_TRUE(
        cursor.Open(meta, Timestamp(0), Timestamp(1'000'000), {"ts_ns", "last_price"}, 32, &error))
        << error;
    for (int i = 0; i < 17; ++i) {
        Tick tick;
        bool has = false;
        ASSERT_TRUE(cursor.Next(&tick, &has, &error)) << error;
        ASSERT_TRUE(has);
        EXPECT_EQ(tick.ts_ns, 1000 + i);
    }
    EXPECT_EQ(cursor.metrics().scan_rows, 17);
    EXPECT_LE(cursor.metrics().buffered_rows_high_water, 32);
    EXPECT_LT(cursor.metrics().scan_rows, static_cast<std::int64_t>(rows.size()));
    std::filesystem::remove_all(dir);
}

TEST(ParquetDataFeedTest, CursorRejectsNonMonotonicPartition) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("quant_hft_parquet_cursor_disorder_" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);
    Tick a, b;
    a.symbol = b.symbol = "DCE.c2605";
    a.ts_ns = 200;
    b.ts_ns = 100;
    const auto path = dir / "disorder.parquet";
    std::string error;
    ASSERT_TRUE(backtest::test::WriteTickPartitionFixture(path, {a, b}, &error)) << error;
    ParquetPartitionMeta meta;
    meta.file_path = path.string();
    meta.instrument_id = a.symbol;
    ParquetTickCursor cursor;
    ASSERT_TRUE(cursor.Open(meta, Timestamp(0), Timestamp(1000), {}, 1, &error)) << error;
    Tick tick;
    bool has = false;
    ASSERT_TRUE(cursor.Next(&tick, &has, &error));
    ASSERT_TRUE(has);
    EXPECT_FALSE(cursor.Next(&tick, &has, &error));
    EXPECT_NE(error.find("non-monotonic"), std::string::npos);
    std::filesystem::remove_all(dir);
}

TEST(ParquetDataFeedTest, StreamingMergeUsesStablePartitionOrdinalAndLazyFuturePartitions) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("quant_hft_streaming_merge_" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir / "_manifest");
    const auto base = backtest::detail::ReplayMinuteStartEpochNs("20260515 09:00");
    std::ofstream manifest(dir / "_manifest/partitions.jsonl");
    std::string error;
    for (int partition = 0; partition < 3; ++partition) {
        std::vector<Tick> rows;
        for (int i = 0; i < 10000; ++i) {
            Tick tick;
            tick.symbol = "DCE.c2605";
            tick.exchange = "DCE";
            tick.ts_ns = base + (partition == 2 ? 60'000'000'000LL : 0) + i * 1'000'000LL;
            tick.last_price = partition * 10000 + i;
            rows.push_back(tick);
        }
        const auto filename = std::to_string(partition) + ".parquet";
        const auto path = dir / filename;
        ASSERT_TRUE(backtest::test::WriteTickPartitionFixture(path, rows, &error)) << error;
        std::ofstream(path.string() + ".meta", std::ios::app)
            << "schema_version=v3\nsource_csv_fingerprint=stream-test\n";
        manifest << "{\"file_path\":\"" << filename
                 << "\",\"source\":\"c\",\"trading_day\":\"20260515\",\"instrument_id\":\"DCE."
                    "c2605\",\"min_ts_ns\":"
                 << rows.front().ts_ns << ",\"max_ts_ns\":" << rows.back().ts_ns
                 << ",\"row_count\":10000}\n";
    }
    manifest.close();
    backtest::BacktestCliSpec spec;
    spec.dataset_root = dir.string();
    spec.dataset_manifest = (dir / "_manifest/partitions.jsonl").string();
    spec.start_date = spec.end_date = "20260515";
    spec.symbols = {"DCE.c2605"};
    spec.streaming = true;
    spec.max_ticks = 3;
    std::vector<backtest::ReplayTick> ticks;
    backtest::ReplayReport report;
    ASSERT_TRUE(backtest::LoadParquetTicks(spec, &ticks, &report, &error)) << error;
    ASSERT_EQ(ticks.size(), 3U);
    EXPECT_DOUBLE_EQ(ticks[0].last_price, 0);
    EXPECT_DOUBLE_EQ(ticks[1].last_price, 10000);
    EXPECT_DOUBLE_EQ(ticks[2].last_price, 1);
    EXPECT_LE(report.buffered_input_rows_high_water, 2 * 4096);
    EXPECT_LE(report.scan_rows, 4);
    EXPECT_TRUE(report.early_stop_hit);
    auto materialized_spec = spec;
    materialized_spec.streaming = false;
    std::vector<backtest::ReplayTick> materialized;
    backtest::ReplayReport materialized_report;
    ASSERT_TRUE(
        backtest::LoadParquetTicks(materialized_spec, &materialized, &materialized_report, &error))
        << error;
    ASSERT_EQ(materialized.size(), ticks.size());
    for (std::size_t i = 0; i < ticks.size(); ++i) {
        EXPECT_EQ(materialized[i].ts_ns, ticks[i].ts_ns);
        EXPECT_EQ(materialized[i].last_price, ticks[i].last_price);
    }

    std::filesystem::remove_all(dir);
}
}  // namespace quant_hft

namespace quant_hft {
TEST(ParquetDataFeedTest, TradingDaySelectionKeepsPreviousFridayNight) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("quant_hft_night_cursor_" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir / "_manifest");
    Tick tick;
    tick.symbol = "SHFE.rb2610";
    tick.exchange = "SHFE";
    tick.ts_ns = Timestamp::FromSql("2026-05-15 21:00:00").ToEpochNanos();
    tick.last_price = 3000;
    std::string error;
    const auto partition = dir / "night.parquet";
    ASSERT_TRUE(backtest::test::WriteTickPartitionFixture(partition, {tick}, &error)) << error;
    std::ofstream(dir / "_manifest/partitions.jsonl")
        << "{\"file_path\":\"night.parquet\",\"trading_day\":\"20260518\",\"instrument_id\":\"SHFE."
           "rb2610\",\"min_ts_ns\":"
        << tick.ts_ns << ",\"max_ts_ns\":" << tick.ts_ns << ",\"row_count\":1}\n";
    backtest::BacktestCliSpec spec;
    spec.dataset_root = dir.string();
    spec.strict_parquet = false;
    spec.start_date = spec.end_date = "20260518";
    spec.symbols = {tick.symbol};
    std::vector<backtest::ReplayTick> loaded;
    backtest::ReplayReport report;
    ASSERT_TRUE(backtest::LoadParquetTicks(spec, &loaded, &report, &error)) << error;
    ASSERT_EQ(loaded.size(), 1U);
    EXPECT_EQ(loaded.front().trading_day, "20260518");
    EXPECT_EQ(loaded.front().ts_ns, tick.ts_ns);
    std::filesystem::remove_all(dir);
}
}  // namespace quant_hft
