#include "quant_hft/rolling/rolling_config.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft::rolling {
namespace {

std::filesystem::path MakeTempDir(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto dir = std::filesystem::temp_directory_path() /
                     (stem + "_" + std::to_string(stamp));
    std::filesystem::create_directories(dir);
    return dir;
}

std::filesystem::path WriteFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

std::string ManifestLine(const std::string& trading_day) {
    return "{\"file_path\":\"source=rb/trading_day=" + trading_day +
           "/instrument_id=rb2405/part-0000.parquet\","
           "\"source\":\"rb\",\"trading_day\":\"" +
           trading_day +
           "\",\"instrument_id\":\"rb2405\",\"min_ts_ns\":1,\"max_ts_ns\":2,\"row_count\":1}\n";
}

TEST(RollingConfigTest, LoadsValidConfigAndResolvesPaths) {
    const auto dir = MakeTempDir("rolling_config_valid");
    const auto dataset_root = dir / "data";
    const auto manifest = dataset_root / "_manifest" / "partitions.jsonl";
    WriteFile(manifest, ManifestLine("20230103"));

    const auto report_dir = dir / "report";
    std::filesystem::create_directories(report_dir);

    const auto config_path = dir / "rolling.yaml";
    WriteFile(config_path,
              "mode: fixed_params\n"
              "backtest_base:\n"
              "  engine_mode: parquet\n"
              "  dataset_root: " +
                  dataset_root.string() +
                  "\n"
                  "  strategy_factory: demo\n"
                  "window:\n"
                  "  type: rolling\n"
                  "  train_length_days: 2\n"
                  "  test_length_days: 1\n"
                  "  step_days: 1\n"
                  "  min_train_days: 2\n"
                  "  start_date: 20230101\n"
                  "  end_date: 20230131\n"
                  "output:\n"
                  "  report_json: " +
                  (report_dir / "r.json").string() +
                  "\n"
                  "  report_md: " +
                  (report_dir / "r.md").string() +
                  "\n");

    RollingConfig config;
    std::string error;
    ASSERT_TRUE(LoadRollingConfig(config_path.string(), &config, &error)) << error;

    EXPECT_EQ(config.mode, "fixed_params");
    EXPECT_EQ(config.backtest_base.engine_mode, "parquet");
    EXPECT_EQ(config.backtest_base.dataset_manifest, manifest.string());
    EXPECT_EQ(config.output.report_json, (report_dir / "r.json").string());
    EXPECT_EQ(config.output.report_md, (report_dir / "r.md").string());

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(RollingConfigTest, RejectsNonParquetEngineMode) {
    const auto dir = MakeTempDir("rolling_config_mode");
    const auto dataset_root = dir / "data";
    const auto manifest = dataset_root / "_manifest" / "partitions.jsonl";
    WriteFile(manifest, ManifestLine("20230103"));

    const auto config_path = dir / "rolling.yaml";
    WriteFile(config_path,
              "mode: fixed_params\n"
              "backtest_base:\n"
              "  engine_mode: csv\n"
              "  dataset_root: " +
                  dataset_root.string() +
                  "\n"
                  "  strategy_factory: demo\n"
                  "window:\n"
                  "  type: rolling\n"
                  "  train_length_days: 2\n"
                  "  test_length_days: 1\n"
                  "  step_days: 1\n"
                  "  min_train_days: 2\n"
                  "  start_date: 20230101\n"
                  "  end_date: 20230131\n"
                  "output:\n"
                  "  report_json: " +
                  (dir / "r.json").string() +
                  "\n"
                  "  report_md: " +
                  (dir / "r.md").string() +
                  "\n");

    RollingConfig config;
    std::string error;
    EXPECT_FALSE(LoadRollingConfig(config_path.string(), &config, &error));
    EXPECT_NE(error.find("engine_mode must be parquet"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(RollingConfigTest, RejectsMissingManifest) {
    const auto dir = MakeTempDir("rolling_config_manifest");
    const auto dataset_root = dir / "data";
    std::filesystem::create_directories(dataset_root);

    const auto config_path = dir / "rolling.yaml";
    WriteFile(config_path,
              "mode: fixed_params\n"
              "backtest_base:\n"
              "  engine_mode: parquet\n"
              "  dataset_root: " +
                  dataset_root.string() +
                  "\n"
                  "  strategy_factory: demo\n"
                  "window:\n"
                  "  type: rolling\n"
                  "  train_length_days: 2\n"
                  "  test_length_days: 1\n"
                  "  step_days: 1\n"
                  "  min_train_days: 2\n"
                  "  start_date: 20230101\n"
                  "  end_date: 20230131\n"
                  "output:\n"
                  "  report_json: " +
                  (dir / "r.json").string() +
                  "\n"
                  "  report_md: " +
                  (dir / "r.md").string() +
                  "\n");

    RollingConfig config;
    std::string error;
    EXPECT_FALSE(LoadRollingConfig(config_path.string(), &config, &error));
    EXPECT_NE(error.find("dataset_manifest does not exist"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

}  // namespace
}  // namespace quant_hft::rolling

