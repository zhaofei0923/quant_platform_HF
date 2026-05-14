#include "quant_hft/services/market_data_csv_recorder.h"

#include <gtest/gtest.h>

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

TEST(MarketDataCsvRecorderTest, WritesTickAndBarCsvFiles) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root = std::filesystem::temp_directory_path() / ("quant_hft_market_data_" + token);

    MarketDataRecordingConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.run_id = "unit-run";
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
    ASSERT_TRUE(recorder.AppendBar(bar, &error)) << error;
    ASSERT_TRUE(recorder.Close(&error)) << error;

    EXPECT_EQ(recorder.ticks_written(), 1);
    EXPECT_EQ(recorder.bars_written(), 1);
    const auto tick_text = ReadTextFile(root / "unit-run" / "ticks.csv");
    const auto bar_text = ReadTextFile(root / "unit-run" / "bars_1m.csv");
    EXPECT_NE(tick_text.find("instrument_id,exchange_id,trading_day"), std::string::npos);
    EXPECT_NE(tick_text.find("SHFE.ag2406,SHFE,20260429,20260429,09:30:01"), std::string::npos);
    EXPECT_NE(bar_text.find("instrument_id,exchange_id,trading_day"), std::string::npos);
    EXPECT_NE(bar_text.find("SHFE.ag2406,SHFE,20260429,20260429,20260429 09:30"),
              std::string::npos);

    std::filesystem::remove_all(root);
}

TEST(MarketDataCsvRecorderTest, WritesPartitionedTickAndBarCsvFilesByProduct) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root =
        std::filesystem::temp_directory_path() / ("quant_hft_market_data_partitioned_" + token);

    MarketDataRecordingConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.run_id = "unit-run";
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

    EXPECT_FALSE(std::filesystem::exists(root / "unit-run" / "ticks.csv"));
    EXPECT_FALSE(std::filesystem::exists(root / "unit-run" / "bars_1m.csv"));
    const auto tick_text =
        ReadTextFile(root / "unit-run" / "varieties" / "rb" / "market" / "ticks.csv");
    const auto bar_text =
        ReadTextFile(root / "unit-run" / "varieties" / "rb" / "market" / "bars_1m.csv");
    EXPECT_NE(tick_text.find("SHFE.rb2405,SHFE,20260429"), std::string::npos);
    EXPECT_NE(bar_text.find("SHFE.rb2405,SHFE,20260429"), std::string::npos);

    std::filesystem::remove_all(root);
}

TEST(MarketDataCsvRecorderTest, FiltersTicksAndBarsToAllowedInstruments) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root =
        std::filesystem::temp_directory_path() / ("quant_hft_market_data_filtered_" + token);

    MarketDataRecordingConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.run_id = "unit-run";
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
        ReadTextFile(root / "unit-run" / "varieties" / "rb" / "market" / "ticks.csv");
    const auto bar_text =
        ReadTextFile(root / "unit-run" / "varieties" / "rb" / "market" / "bars_1m.csv");
    EXPECT_NE(tick_text.find("SHFE.rb2405,SHFE,20260429"), std::string::npos);
    EXPECT_EQ(tick_text.find("SHFE.rb2406"), std::string::npos);
    EXPECT_NE(bar_text.find("SHFE.rb2405,SHFE,20260429"), std::string::npos);
    EXPECT_EQ(bar_text.find("SHFE.rb2406"), std::string::npos);

    std::filesystem::remove_all(root);
}

}  // namespace
}  // namespace quant_hft