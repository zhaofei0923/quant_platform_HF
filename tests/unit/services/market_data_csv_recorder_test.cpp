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

}  // namespace
}  // namespace quant_hft