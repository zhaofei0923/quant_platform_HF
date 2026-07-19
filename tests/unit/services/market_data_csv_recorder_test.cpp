#include "quant_hft/services/market_data_csv_recorder.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace quant_hft {
namespace {

std::string ReadTextFile(const std::filesystem::path& path) {
    std::ifstream in(path);
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

std::size_t CountOccurrences(const std::string& text, const std::string& needle) {
    std::size_t count = 0;
    std::size_t pos = 0;
    while ((pos = text.find(needle, pos)) != std::string::npos) {
        ++count;
        pos += needle.size();
    }
    return count;
}

std::size_t CountColumns(const std::string& row) {
    return static_cast<std::size_t>(std::count(row.begin(), row.end(), ',')) + 1;
}

TEST(MarketDataCsvRecorderTest, WritesTickAndBarCsvFiles) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root = std::filesystem::temp_directory_path() / ("quant_hft_market_data_" + token);

    MarketDataRecordingConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.run_id = "ignored-run";
    config.flush_each_write = true;

    MarketDataCsvRecorder recorder;
    std::string error;
    ASSERT_TRUE(recorder.Open(config, &error)) << error;

    MarketSnapshot tick;
    tick.instrument_id = "SHFE.ag2406";
    tick.exchange_id = "SHFE";
    tick.trading_day = "20260429";
    tick.action_day = "20260429";
    tick.update_time = "09:30:01";
    tick.update_millisec = 500;
    tick.last_price = 6123.5;
    tick.bid_price_1 = 6123.0;
    tick.ask_price_1 = 6124.0;
    tick.bid_volume_1 = 10;
    tick.ask_volume_1 = 11;
    tick.volume = 100;
    tick.open_interest = 200;
    tick.exchange_ts_ns = 111;
    tick.recv_ts_ns = 222;
    tick.average_price_norm_valid = true;
    ASSERT_TRUE(recorder.AppendTick(tick, &error)) << error;

    BarSnapshot bar;
    bar.instrument_id = "SHFE.ag2406";
    bar.exchange_id = "SHFE";
    bar.trading_day = "20260429";
    bar.action_day = "20260429";
    bar.minute = "20260429 09:30";
    bar.open = 6120.0;
    bar.high = 6125.0;
    bar.low = 6119.0;
    bar.close = 6123.5;
    bar.analysis_open = bar.open;
    bar.analysis_high = bar.high;
    bar.analysis_low = bar.low;
    bar.analysis_close = bar.close;
    bar.volume = 8;
    bar.ts_ns = 333;
    bar.period_end_ts_ns = 444;
    bar.finalized_ts_ns = 555;
    bar.expected_source_bars = 5;
    bar.observed_source_bars = 4;
    bar.is_complete = false;
    bar.strategy_eligible = false;
    bar.volume_complete = false;
    bar.has_conflict = true;
    bar.is_recovery_replay = true;
    ASSERT_TRUE(recorder.AppendBar(bar, &error)) << error;
    ASSERT_TRUE(recorder.Close(&error)) << error;

    EXPECT_EQ(recorder.ticks_written(), 1);
    EXPECT_EQ(recorder.bars_written(), 1);
    EXPECT_FALSE(std::filesystem::exists(root / "ignored-run"));
    const auto tick_text = ReadTextFile(root / "trading_day=20260429" / "ticks.csv");
    const auto bar_text = ReadTextFile(root / "trading_day=20260429" / "bars_1m.csv");
    EXPECT_NE(tick_text.find("instrument_id,exchange_id,trading_day"), std::string::npos);
    EXPECT_NE(tick_text.find("SHFE.ag2406,SHFE,20260429,20260429,09:30:01"), std::string::npos);
    EXPECT_NE(tick_text.find("recv_ts_ns,average_price_norm_valid"), std::string::npos);
    EXPECT_NE(tick_text.find(",111,222,1\n"), std::string::npos);
    EXPECT_NE(bar_text.find("instrument_id,exchange_id,trading_day"), std::string::npos);
    EXPECT_NE(bar_text.find("period_end_ts_ns,finalized_ts_ns,expected_source_bars"),
              std::string::npos);
    EXPECT_NE(bar_text.find(",333,444,555,5,4,0,0,0,0,1,1\n"), std::string::npos);
    EXPECT_NE(bar_text.find("SHFE.ag2406,SHFE,20260429,20260429,20260429 09:30"),
              std::string::npos);
    const auto tick_header_end = tick_text.find('\n');
    const auto tick_row_end = tick_text.find('\n', tick_header_end + 1);
    ASSERT_NE(tick_header_end, std::string::npos);
    ASSERT_NE(tick_row_end, std::string::npos);
    EXPECT_EQ(
        CountColumns(tick_text.substr(0, tick_header_end)),
        CountColumns(tick_text.substr(tick_header_end + 1, tick_row_end - tick_header_end - 1)));

    std::filesystem::remove_all(root);
}

TEST(MarketDataCsvRecorderTest, AppendsSameTradingDayWithoutDuplicateHeaders) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root =
        std::filesystem::temp_directory_path() / ("quant_hft_market_data_append_" + token);

    MarketDataRecordingConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.flush_each_write = true;
    config.partition_by_product = true;

    MarketSnapshot tick;
    tick.instrument_id = "SHFE.rb2405";
    tick.exchange_id = "SHFE";
    tick.trading_day = "20260429";
    tick.action_day = "20260429";
    tick.update_time = "09:30:01";
    tick.last_price = 3500.0;

    std::string error;
    {
        MarketDataCsvRecorder recorder;
        ASSERT_TRUE(recorder.Open(config, &error)) << error;
        ASSERT_TRUE(recorder.AppendTick(tick, &error)) << error;
        ASSERT_TRUE(recorder.Close(&error)) << error;
    }
    {
        MarketDataCsvRecorder recorder;
        tick.update_time = "09:30:02";
        tick.last_price = 3501.0;
        ASSERT_TRUE(recorder.Open(config, &error)) << error;
        ASSERT_TRUE(recorder.AppendTick(tick, &error)) << error;
        ASSERT_TRUE(recorder.Close(&error)) << error;
    }

    const auto tick_text =
        ReadTextFile(root / "trading_day=20260429" / "varieties" / "rb" / "market" / "ticks.csv");
    EXPECT_EQ(CountOccurrences(tick_text, "instrument_id,exchange_id,trading_day"), 1U);
    EXPECT_NE(tick_text.find("09:30:01"), std::string::npos);
    EXPECT_NE(tick_text.find("09:30:02"), std::string::npos);

    std::filesystem::remove_all(root);
}

TEST(MarketDataCsvRecorderTest, WritesDifferentTradingDaysToSeparateDirectories) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root =
        std::filesystem::temp_directory_path() / ("quant_hft_market_data_days_" + token);

    MarketDataRecordingConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.flush_each_write = true;
    config.partition_by_product = true;

    MarketDataCsvRecorder recorder;
    std::string error;
    ASSERT_TRUE(recorder.Open(config, &error)) << error;

    MarketSnapshot tick;
    tick.instrument_id = "DCE.c2607";
    tick.exchange_id = "DCE";
    tick.trading_day = "20260429";
    tick.action_day = "20260429";
    tick.update_time = "09:30:01";
    tick.last_price = 2300.0;
    ASSERT_TRUE(recorder.AppendTick(tick, &error)) << error;

    tick.trading_day = "20260430";
    tick.action_day = "20260430";
    tick.update_time = "09:30:02";
    tick.last_price = 2301.0;
    ASSERT_TRUE(recorder.AppendTick(tick, &error)) << error;
    ASSERT_TRUE(recorder.Close(&error)) << error;

    EXPECT_NE(
        ReadTextFile(root / "trading_day=20260429" / "varieties" / "c" / "market" / "ticks.csv")
            .find("09:30:01"),
        std::string::npos);
    EXPECT_NE(
        ReadTextFile(root / "trading_day=20260430" / "varieties" / "c" / "market" / "ticks.csv")
            .find("09:30:02"),
        std::string::npos);

    std::filesystem::remove_all(root);
}

TEST(MarketDataCsvRecorderTest, WritesHeaderWhenExistingFileIsEmpty) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root =
        std::filesystem::temp_directory_path() / ("quant_hft_market_data_empty_" + token);
    const auto tick_path =
        root / "trading_day=20260429" / "varieties" / "rb" / "market" / "ticks.csv";
    std::filesystem::create_directories(tick_path.parent_path());
    std::ofstream(tick_path).close();

    MarketDataRecordingConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.flush_each_write = true;
    config.partition_by_product = true;

    MarketDataCsvRecorder recorder;
    std::string error;
    ASSERT_TRUE(recorder.Open(config, &error)) << error;

    MarketSnapshot tick;
    tick.instrument_id = "SHFE.rb2405";
    tick.exchange_id = "SHFE";
    tick.trading_day = "20260429";
    tick.action_day = "20260429";
    tick.update_time = "09:30:01";
    tick.last_price = 3500.0;
    ASSERT_TRUE(recorder.AppendTick(tick, &error)) << error;
    ASSERT_TRUE(recorder.Close(&error)) << error;

    const auto tick_text = ReadTextFile(tick_path);
    EXPECT_EQ(CountOccurrences(tick_text, "instrument_id,exchange_id,trading_day"), 1U);
    EXPECT_NE(tick_text.find("SHFE.rb2405,SHFE,20260429"), std::string::npos);

    std::filesystem::remove_all(root);
}

TEST(MarketDataCsvRecorderTest, WritesPartitionedTickAndBarCsvFilesByProduct) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root =
        std::filesystem::temp_directory_path() / ("quant_hft_market_data_partitioned_" + token);

    MarketDataRecordingConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.run_id = "ignored-run";
    config.flush_each_write = true;
    config.partition_by_product = true;

    MarketDataCsvRecorder recorder;
    std::string error;
    ASSERT_TRUE(recorder.Open(config, &error)) << error;

    MarketSnapshot tick;
    tick.instrument_id = "SHFE.rb2405";
    tick.exchange_id = "SHFE";
    tick.trading_day = "20260429";
    tick.action_day = "20260429";
    tick.update_time = "09:30:01";
    tick.last_price = 3500.0;
    ASSERT_TRUE(recorder.AppendTick(tick, &error)) << error;

    BarSnapshot bar;
    bar.instrument_id = "SHFE.rb2405";
    bar.exchange_id = "SHFE";
    bar.trading_day = "20260429";
    bar.action_day = "20260429";
    bar.minute = "20260429 09:30";
    bar.open = 3500.0;
    bar.high = 3501.0;
    bar.low = 3499.0;
    bar.close = 3500.5;
    bar.analysis_open = bar.open;
    bar.analysis_high = bar.high;
    bar.analysis_low = bar.low;
    bar.analysis_close = bar.close;
    ASSERT_TRUE(recorder.AppendBar(bar, &error)) << error;
    ASSERT_TRUE(recorder.Close(&error)) << error;

    EXPECT_FALSE(std::filesystem::exists(root / "trading_day=20260429" / "ticks.csv"));
    EXPECT_FALSE(std::filesystem::exists(root / "trading_day=20260429" / "bars_1m.csv"));
    const auto tick_text =
        ReadTextFile(root / "trading_day=20260429" / "varieties" / "rb" / "market" / "ticks.csv");
    const auto bar_text =
        ReadTextFile(root / "trading_day=20260429" / "varieties" / "rb" / "market" / "bars_1m.csv");
    EXPECT_NE(tick_text.find("SHFE.rb2405,SHFE,20260429"), std::string::npos);
    EXPECT_NE(bar_text.find("SHFE.rb2405,SHFE,20260429"), std::string::npos);

    std::filesystem::remove_all(root);
}

TEST(MarketDataCsvRecorderTest, WritesPartitionedTimeframeBarCsvFile) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root =
        std::filesystem::temp_directory_path() / ("quant_hft_market_data_5m_" + token);

    MarketDataRecordingConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.run_id = "ignored-run";
    config.flush_each_write = true;
    config.partition_by_product = true;

    MarketDataCsvRecorder recorder;
    std::string error;
    ASSERT_TRUE(recorder.Open(config, &error)) << error;

    BarSnapshot bar;
    bar.instrument_id = "SHFE.hc2405";
    bar.exchange_id = "SHFE";
    bar.trading_day = "20260429";
    bar.action_day = "20260429";
    bar.minute = "20260429 09:00";
    bar.open = 3600.0;
    bar.high = 3610.0;
    bar.low = 3595.0;
    bar.close = 3608.0;
    bar.analysis_open = bar.open;
    bar.analysis_high = bar.high;
    bar.analysis_low = bar.low;
    bar.analysis_close = bar.close;
    bar.volume = 50;
    bar.ts_ns = 12345;
    ASSERT_TRUE(recorder.AppendTimeframeBar(bar, 5, &error)) << error;
    ASSERT_TRUE(recorder.Close(&error)) << error;

    const auto bar_text =
        ReadTextFile(root / "trading_day=20260429" / "varieties" / "hc" / "market" / "bars_5m.csv");
    EXPECT_NE(bar_text.find("instrument_id,exchange_id,trading_day"), std::string::npos);
    EXPECT_NE(bar_text.find("SHFE.hc2405,SHFE,20260429,20260429,20260429 09:00"),
              std::string::npos);

    std::filesystem::remove_all(root);
}

TEST(MarketDataCsvRecorderTest, FiltersTicksAndBarsToAllowedInstruments) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root =
        std::filesystem::temp_directory_path() / ("quant_hft_market_data_filtered_" + token);

    MarketDataRecordingConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.run_id = "ignored-run";
    config.flush_each_write = true;
    config.partition_by_product = true;

    MarketDataCsvRecorder recorder;
    std::string error;
    ASSERT_TRUE(recorder.Open(config, &error)) << error;
    recorder.SetAllowedInstrumentIds({"SHFE.rb2405"});

    MarketSnapshot allowed_tick;
    allowed_tick.instrument_id = "SHFE.rb2405";
    allowed_tick.exchange_id = "SHFE";
    allowed_tick.trading_day = "20260429";
    allowed_tick.action_day = "20260429";
    allowed_tick.update_time = "09:30:01";
    allowed_tick.last_price = 3500.0;
    ASSERT_TRUE(recorder.AppendTick(allowed_tick, &error)) << error;

    MarketSnapshot filtered_tick = allowed_tick;
    filtered_tick.instrument_id = "SHFE.rb2406";
    filtered_tick.last_price = 3510.0;
    ASSERT_TRUE(recorder.AppendTick(filtered_tick, &error)) << error;

    BarSnapshot allowed_bar;
    allowed_bar.instrument_id = "SHFE.rb2405";
    allowed_bar.exchange_id = "SHFE";
    allowed_bar.trading_day = "20260429";
    allowed_bar.action_day = "20260429";
    allowed_bar.minute = "20260429 09:30";
    allowed_bar.open = 3500.0;
    allowed_bar.high = 3501.0;
    allowed_bar.low = 3499.0;
    allowed_bar.close = 3500.5;
    allowed_bar.analysis_open = allowed_bar.open;
    allowed_bar.analysis_high = allowed_bar.high;
    allowed_bar.analysis_low = allowed_bar.low;
    allowed_bar.analysis_close = allowed_bar.close;
    ASSERT_TRUE(recorder.AppendBar(allowed_bar, &error)) << error;

    BarSnapshot filtered_bar = allowed_bar;
    filtered_bar.instrument_id = "SHFE.rb2406";
    filtered_bar.close = 3510.5;
    ASSERT_TRUE(recorder.AppendBar(filtered_bar, &error)) << error;
    ASSERT_TRUE(recorder.Close(&error)) << error;

    EXPECT_EQ(recorder.ticks_written(), 1);
    EXPECT_EQ(recorder.bars_written(), 1);
    const auto tick_text =
        ReadTextFile(root / "trading_day=20260429" / "varieties" / "rb" / "market" / "ticks.csv");
    const auto bar_text =
        ReadTextFile(root / "trading_day=20260429" / "varieties" / "rb" / "market" / "bars_1m.csv");
    EXPECT_NE(tick_text.find("SHFE.rb2405,SHFE,20260429"), std::string::npos);
    EXPECT_EQ(tick_text.find("SHFE.rb2406"), std::string::npos);
    EXPECT_NE(bar_text.find("SHFE.rb2405,SHFE,20260429"), std::string::npos);
    EXPECT_EQ(bar_text.find("SHFE.rb2406"), std::string::npos);

    std::filesystem::remove_all(root);
}

}  // namespace
}  // namespace quant_hft
