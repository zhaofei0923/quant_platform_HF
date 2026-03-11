#include "quant_hft/backtest/indicator_trace_csv_writer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace quant_hft {
namespace {

std::filesystem::path UniqueTracePath(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           (stem + "_" + std::to_string(stamp) + ".csv");
}

std::vector<std::string> ReadLines(const std::filesystem::path& path) {
    std::vector<std::string> lines;
    std::ifstream input(path);
    std::string line;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        lines.push_back(line);
    }
    return lines;
}

TEST(IndicatorTraceCsvWriterTest, OpenFailsWhenOutputAlreadyExists) {
    const std::filesystem::path path = UniqueTracePath("indicator_trace_csv_existing");
    std::ofstream existing(path);
    existing << "occupied";
    existing.close();

    IndicatorTraceCsvWriter writer;
    std::string error;
    EXPECT_FALSE(writer.Open(path.string(), &error));
    EXPECT_NE(error.find("already exists"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove(path, ec);
}

TEST(IndicatorTraceCsvWriterTest, WritesHeaderAndNullableIndicatorColumns) {
    const std::filesystem::path path = UniqueTracePath("indicator_trace_csv_enabled");
    std::error_code ec;
    std::filesystem::remove(path, ec);

    IndicatorTraceCsvWriter writer;
    std::string error;
    ASSERT_TRUE(writer.Open(path.string(), &error)) << error;

    IndicatorTraceRow row0;
    row0.instrument_id = "rb2405";
    row0.ts_ns = 1700000000000000000LL;
    row0.dt_utc = "2023-11-14 22:13:20";
    row0.bar_open = 100.0;
    row0.bar_high = 101.0;
    row0.bar_low = 99.0;
    row0.bar_close = 100.5;
    row0.bar_volume = 10.0;
    row0.market_regime = MarketRegime::kUnknown;
    ASSERT_TRUE(writer.Append(row0, &error)) << error;

    IndicatorTraceRow row1 = row0;
    row1.ts_ns += 60'000'000'000LL;
    row1.dt_utc = "2023-11-14 22:14:20";
    row1.kama = 100.8;
    row1.atr = 1.2;
    row1.adx = 25.4;
    row1.er = 0.55;
    row1.market_regime = MarketRegime::kWeakTrend;
    ASSERT_TRUE(writer.Append(row1, &error)) << error;

    ASSERT_TRUE(writer.Close(&error)) << error;

    const std::vector<std::string> lines = ReadLines(path);
    ASSERT_EQ(lines.size(), 3U);
    EXPECT_EQ(lines[0],
              "instrument_id,ts_ns,dt_utc,timeframe_minutes,bar_open,bar_high,bar_low,bar_close,"
              "bar_volume,kama,atr,adx,er,market_regime");
    EXPECT_EQ(lines[1],
              "rb2405,1700000000000000000,2023-11-14 22:13:20,1,100,101,99,100.5,10,,,,,kUnknown");
    EXPECT_EQ(lines[2],
              "rb2405,1700000060000000000,2023-11-14 22:14:20,1,100,101,99,100.5,10,100.8,1.2,"
              "25.4,0.55,kWeakTrend");

    std::filesystem::remove(path, ec);
}

}  // namespace
}  // namespace quant_hft
