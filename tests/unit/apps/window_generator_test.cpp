#include "quant_hft/rolling/window_generator.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace quant_hft::rolling {
namespace {

std::filesystem::path MakeTempDir(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto dir = std::filesystem::temp_directory_path() /
                     (stem + "_" + std::to_string(stamp));
    std::filesystem::create_directories(dir);
    return dir;
}

std::filesystem::path WriteManifest(const std::filesystem::path& dataset_root,
                                    const std::vector<std::string>& trading_days) {
    const auto manifest = dataset_root / "_manifest" / "partitions.jsonl";
    std::filesystem::create_directories(manifest.parent_path());
    std::ofstream out(manifest);
    int file_index = 0;
    for (const std::string& day : trading_days) {
        out << "{\"file_path\":\"source=rb/trading_day=" << day
            << "/instrument_id=rb2405/part-" << file_index++ << ".parquet\",";
        out << "\"source\":\"rb\",\"trading_day\":\"" << day
            << "\",\"instrument_id\":\"rb2405\",\"min_ts_ns\":1,\"max_ts_ns\":2,\"row_count\":1}\n";
    }
    out.close();
    return manifest;
}

RollingConfig BuildConfig(const std::filesystem::path& dataset_root,
                          const std::filesystem::path& manifest,
                          const std::string& type) {
    RollingConfig config;
    config.mode = "fixed_params";
    config.backtest_base.engine_mode = "parquet";
    config.backtest_base.dataset_root = dataset_root.string();
    config.backtest_base.dataset_manifest = manifest.string();
    config.backtest_base.strategy_factory = "demo";
    config.window.type = type;
    config.window.train_length_days = 2;
    config.window.min_train_days = 3;
    config.window.test_length_days = 2;
    config.window.step_days = 2;
    config.window.start_date = "20230101";
    config.window.end_date = "20230131";
    return config;
}

TEST(WindowGeneratorTest, BuildsTradingDayCalendarFromManifest) {
    const auto dir = MakeTempDir("window_generator_calendar");
    const auto dataset_root = dir / "data";
    const auto manifest = WriteManifest(dataset_root, {"20230103", "20230101", "20230103", "20230102"});

    RollingConfig config = BuildConfig(dataset_root, manifest, "rolling");

    std::vector<std::string> trading_days;
    std::string error;
    ASSERT_TRUE(BuildTradingDayCalendar(config, &trading_days, &error)) << error;
    ASSERT_EQ(trading_days.size(), 3U);
    EXPECT_EQ(trading_days[0], "20230101");
    EXPECT_EQ(trading_days[1], "20230102");
    EXPECT_EQ(trading_days[2], "20230103");

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(WindowGeneratorTest, GeneratesRollingWindowsAndDropsTail) {
    const auto dir = MakeTempDir("window_generator_rolling");
    const auto dataset_root = dir / "data";
    const auto manifest = WriteManifest(dataset_root,
                                        {"20230101", "20230102", "20230103", "20230104",
                                         "20230105", "20230106", "20230107"});

    RollingConfig config = BuildConfig(dataset_root, manifest, "rolling");

    std::vector<std::string> trading_days;
    std::string error;
    ASSERT_TRUE(BuildTradingDayCalendar(config, &trading_days, &error)) << error;

    std::vector<Window> windows;
    ASSERT_TRUE(GenerateWindows(config, trading_days, &windows, &error)) << error;
    ASSERT_EQ(windows.size(), 2U);
    EXPECT_EQ(windows[0].train_start, "20230101");
    EXPECT_EQ(windows[0].train_end, "20230102");
    EXPECT_EQ(windows[0].test_start, "20230103");
    EXPECT_EQ(windows[0].test_end, "20230104");

    EXPECT_EQ(windows[1].train_start, "20230103");
    EXPECT_EQ(windows[1].train_end, "20230104");
    EXPECT_EQ(windows[1].test_start, "20230105");
    EXPECT_EQ(windows[1].test_end, "20230106");

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(WindowGeneratorTest, GeneratesExpandingWindows) {
    const auto dir = MakeTempDir("window_generator_expanding");
    const auto dataset_root = dir / "data";
    const auto manifest = WriteManifest(dataset_root,
                                        {"20230101", "20230102", "20230103", "20230104",
                                         "20230105", "20230106", "20230107"});

    RollingConfig config = BuildConfig(dataset_root, manifest, "expanding");

    std::vector<std::string> trading_days;
    std::string error;
    ASSERT_TRUE(BuildTradingDayCalendar(config, &trading_days, &error)) << error;

    std::vector<Window> windows;
    ASSERT_TRUE(GenerateWindows(config, trading_days, &windows, &error)) << error;
    ASSERT_EQ(windows.size(), 2U);

    EXPECT_EQ(windows[0].train_start, "20230101");
    EXPECT_EQ(windows[0].train_end, "20230103");
    EXPECT_EQ(windows[0].test_start, "20230104");
    EXPECT_EQ(windows[0].test_end, "20230105");

    EXPECT_EQ(windows[1].train_start, "20230101");
    EXPECT_EQ(windows[1].train_end, "20230105");
    EXPECT_EQ(windows[1].test_start, "20230106");
    EXPECT_EQ(windows[1].test_end, "20230107");

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

}  // namespace
}  // namespace quant_hft::rolling

