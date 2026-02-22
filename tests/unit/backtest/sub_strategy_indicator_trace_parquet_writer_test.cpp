#include "quant_hft/backtest/sub_strategy_indicator_trace_parquet_writer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <utility>

#if QUANT_HFT_ENABLE_ARROW_PARQUET
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#endif

namespace quant_hft {
namespace {

bool IsDateTimeSecondPrecision(const std::string& value) {
    if (value.size() != 19 && value.size() != 23) {
        return false;
    }
    const bool base = value[4] == '-' && value[7] == '-' && value[10] == ' ' && value[13] == ':' &&
                      value[16] == ':';
    if (!base) {
        return false;
    }
    if (value.size() == 23) {
        return value[19] == '.';
    }
    return true;
}

std::filesystem::path UniqueTracePath(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           (stem + "_" + std::to_string(stamp) + ".parquet");
}

#if QUANT_HFT_ENABLE_ARROW_PARQUET
template <typename ReaderPtr>
auto OpenParquetReaderCompat(const std::shared_ptr<arrow::io::RandomAccessFile>& input,
                             ReaderPtr* reader, int)
    -> decltype(parquet::arrow::OpenFile(input, arrow::default_memory_pool()), bool()) {
    auto reader_result = parquet::arrow::OpenFile(input, arrow::default_memory_pool());
    if (!reader_result.ok()) {
        return false;
    }
    *reader = std::move(reader_result).ValueOrDie();
    return *reader != nullptr;
}

template <typename ReaderPtr>
auto OpenParquetReaderCompat(const std::shared_ptr<arrow::io::RandomAccessFile>& input,
                             ReaderPtr* reader, long)
    -> decltype(parquet::arrow::OpenFile(input, arrow::default_memory_pool(), reader), bool()) {
    auto reader_status = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), reader);
    return reader_status.ok() && *reader != nullptr;
}
#endif

TEST(SubStrategyIndicatorTraceParquetWriterTest, OpenFailsWhenArrowWriterDisabled) {
#if QUANT_HFT_ENABLE_ARROW_PARQUET
    GTEST_SKIP() << "Arrow parquet writer is enabled in this build";
#else
    SubStrategyIndicatorTraceParquetWriter writer;
    std::string error;
    const std::filesystem::path path = UniqueTracePath("sub_strategy_trace_disabled");
    EXPECT_FALSE(writer.Open(path.string(), &error));
    EXPECT_NE(error.find("QUANT_HFT_ENABLE_ARROW_PARQUET=ON"), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(path));
#endif
}

TEST(SubStrategyIndicatorTraceParquetWriterTest, OpenFailsWhenOutputAlreadyExists) {
#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    GTEST_SKIP() << "Arrow parquet writer is disabled in this build";
#else
    const std::filesystem::path path = UniqueTracePath("sub_strategy_trace_existing");
    std::ofstream existing(path);
    existing << "occupied";
    existing.close();

    SubStrategyIndicatorTraceParquetWriter writer;
    std::string error;
    EXPECT_FALSE(writer.Open(path.string(), &error));
    EXPECT_NE(error.find("already exists"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove(path, ec);
#endif
}

TEST(SubStrategyIndicatorTraceParquetWriterTest, WritesRowsWithNullableIndicatorsWhenEnabled) {
#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    GTEST_SKIP() << "Arrow parquet writer is disabled in this build";
#else
    const std::filesystem::path path = UniqueTracePath("sub_strategy_trace_enabled");
    std::error_code ec;
    std::filesystem::remove(path, ec);

    SubStrategyIndicatorTraceParquetWriter writer;
    std::string error;
    ASSERT_TRUE(writer.Open(path.string(), &error)) << error;

    SubStrategyIndicatorTraceRow row0;
    row0.instrument_id = "rb2405";
    row0.ts_ns = 1700000000000000000LL;
    row0.strategy_id = "open_1";
    row0.strategy_type = "TrendStrategy";
    row0.bar_open = 100.0;
    row0.bar_high = 101.0;
    row0.bar_low = 99.0;
    row0.bar_close = 100.5;
    row0.bar_volume = 10.0;
    row0.market_regime = MarketRegime::kUnknown;
    row0.dt_utc = "2023-11-14 22:13:20";
    ASSERT_TRUE(writer.Append(row0, &error)) << error;

    SubStrategyIndicatorTraceRow row1 = row0;
    row1.ts_ns += 60'000'000'000LL;
    row1.kama = 100.8;
    row1.atr = 1.2;
    row1.adx = 25.4;
    row1.er = 0.55;
    row1.stop_loss_price = 98.5;
    row1.take_profit_price = 105.0;
    row1.market_regime = MarketRegime::kWeakTrend;
    row1.dt_utc = "2023-11-14 22:14:20.123";
    ASSERT_TRUE(writer.Append(row1, &error)) << error;

    EXPECT_EQ(writer.rows_written(), 2);
    ASSERT_TRUE(writer.Close(&error)) << error;
    ASSERT_TRUE(std::filesystem::exists(path));

    auto input_result = arrow::io::ReadableFile::Open(path.string());
    ASSERT_TRUE(input_result.ok()) << input_result.status().ToString();
    std::shared_ptr<arrow::io::ReadableFile> input = input_result.ValueOrDie();

    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    ASSERT_TRUE(OpenParquetReaderCompat(input, &parquet_reader, 0));
    std::shared_ptr<arrow::Table> table;
    ASSERT_TRUE(parquet_reader->ReadTable(&table).ok());
    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->num_rows(), 2);
    ASSERT_EQ(table->num_columns(), 18);

    const std::shared_ptr<arrow::Schema> schema = table->schema();
    ASSERT_NE(schema, nullptr);
    ASSERT_EQ(schema->num_fields(), 18);
    EXPECT_EQ(schema->field(2)->name(), "dt_utc");
    EXPECT_EQ(schema->field(3)->name(), "timeframe_minutes");
    EXPECT_EQ(schema->field(4)->name(), "strategy_id");
    EXPECT_EQ(schema->field(5)->name(), "strategy_type");
    EXPECT_EQ(schema->field(12)->name(), "atr");
    EXPECT_EQ(schema->field(15)->name(), "stop_loss_price");
    EXPECT_EQ(schema->field(16)->name(), "take_profit_price");
    EXPECT_EQ(schema->field(17)->name(), "market_regime");

    const auto dt_utc_column = table->GetColumnByName("dt_utc");
    ASSERT_NE(dt_utc_column, nullptr);
    const auto dt_utc = std::static_pointer_cast<arrow::StringArray>(dt_utc_column->chunk(0));
    ASSERT_EQ(dt_utc->length(), 2);
    EXPECT_TRUE(IsDateTimeSecondPrecision(dt_utc->GetString(0)));
    EXPECT_FALSE(dt_utc->GetString(0).empty());
    EXPECT_EQ(dt_utc->GetString(0), row0.dt_utc);
    EXPECT_EQ(dt_utc->GetString(1), row1.dt_utc);

    const auto timeframe_column = table->GetColumnByName("timeframe_minutes");
    const auto strategy_id_column = table->GetColumnByName("strategy_id");
    const auto kama_column = table->GetColumnByName("kama");
    const auto atr_column = table->GetColumnByName("atr");
    const auto stop_loss_price_column = table->GetColumnByName("stop_loss_price");
    const auto take_profit_price_column = table->GetColumnByName("take_profit_price");
    const auto regime_column = table->GetColumnByName("market_regime");
    ASSERT_NE(timeframe_column, nullptr);
    ASSERT_NE(strategy_id_column, nullptr);
    ASSERT_NE(kama_column, nullptr);
    ASSERT_NE(atr_column, nullptr);
    ASSERT_NE(stop_loss_price_column, nullptr);
    ASSERT_NE(take_profit_price_column, nullptr);
    ASSERT_NE(regime_column, nullptr);
    const auto timeframe =
        std::static_pointer_cast<arrow::Int32Array>(timeframe_column->chunk(0));
    const auto strategy_id =
        std::static_pointer_cast<arrow::StringArray>(strategy_id_column->chunk(0));
    const auto kama = std::static_pointer_cast<arrow::DoubleArray>(kama_column->chunk(0));
    const auto atr = std::static_pointer_cast<arrow::DoubleArray>(atr_column->chunk(0));
    const auto stop_loss_price =
        std::static_pointer_cast<arrow::DoubleArray>(stop_loss_price_column->chunk(0));
    const auto take_profit_price =
        std::static_pointer_cast<arrow::DoubleArray>(take_profit_price_column->chunk(0));
    const auto regime = std::static_pointer_cast<arrow::UInt8Array>(regime_column->chunk(0));

    EXPECT_EQ(timeframe->Value(0), 1);
    EXPECT_EQ(timeframe->Value(1), 1);
    EXPECT_EQ(strategy_id->GetString(0), "open_1");
    EXPECT_TRUE(kama->IsNull(0));
    EXPECT_TRUE(atr->IsNull(0));
    EXPECT_TRUE(stop_loss_price->IsNull(0));
    EXPECT_TRUE(take_profit_price->IsNull(0));
    EXPECT_DOUBLE_EQ(kama->Value(1), 100.8);
    EXPECT_DOUBLE_EQ(atr->Value(1), 1.2);
    EXPECT_DOUBLE_EQ(stop_loss_price->Value(1), 98.5);
    EXPECT_DOUBLE_EQ(take_profit_price->Value(1), 105.0);
    EXPECT_EQ(regime->Value(0), static_cast<std::uint8_t>(MarketRegime::kUnknown));
    EXPECT_EQ(regime->Value(1), static_cast<std::uint8_t>(MarketRegime::kWeakTrend));

    std::filesystem::remove(path, ec);
#endif
}

}  // namespace
}  // namespace quant_hft
