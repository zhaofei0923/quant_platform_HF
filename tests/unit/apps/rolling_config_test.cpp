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
    const auto config_dir = dir / "rolling_assets";
    const auto composite = WriteFile(config_dir / "main_backtest_strategy.yaml", "run_type: backtest\n");
    const auto products = WriteFile(config_dir / "products_info.yaml", "products:\n");
    const auto calendar = WriteFile(config_dir / "contract_expiry_calendar.yaml", "contracts:\n");

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
                  "  strategy_factory: composite\n"
                  "  strategy_composite_config: ./rolling_assets/main_backtest_strategy.yaml\n"
                  "  product_config_path: ./rolling_assets/products_info.yaml\n"
                  "  contract_expiry_calendar_path: ./rolling_assets/contract_expiry_calendar.yaml\n"
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
    EXPECT_EQ(config.backtest_base.strategy_composite_config, composite.string());
    EXPECT_EQ(config.backtest_base.product_config_path, products.string());
    EXPECT_EQ(config.backtest_base.contract_expiry_calendar_path, calendar.string());
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

TEST(RollingConfigTest, LoadsRepoOpsRollingOptimizeConfig) {
    const auto repo_root = std::filesystem::path(__FILE__)
                               .parent_path()
                               .parent_path()
                               .parent_path()
                               .parent_path();
    const auto path = repo_root / "configs" / "ops" / "rolling_optimize_kama.yaml";

    RollingConfig config;
    std::string error;
    ASSERT_TRUE(LoadRollingConfig(path.string(), &config, &error)) << error;

    EXPECT_EQ(config.mode, "rolling_optimize");
    EXPECT_EQ(config.optimization.algorithm, "grid");
    EXPECT_EQ(config.optimization.metric, "hf_standard.risk_metrics.calmar_ratio");
    EXPECT_TRUE(config.optimization.maximize);
    ASSERT_TRUE(config.optimization.preserve_top_k_trials.has_value());
    EXPECT_EQ(*config.optimization.preserve_top_k_trials, 10);
    EXPECT_EQ(config.optimization.max_trials, 48);
    EXPECT_EQ(config.optimization.parallel, 2);
    EXPECT_EQ(config.window.type, "rolling");
    EXPECT_EQ(config.window.train_length_days, 120);
    EXPECT_EQ(config.window.test_length_days, 30);
    EXPECT_EQ(config.window.step_days, 30);
    EXPECT_EQ(config.window.min_train_days, 120);
    EXPECT_EQ(config.window.start_date, "20240102");
    EXPECT_EQ(config.window.end_date, "20241231");
    EXPECT_EQ(config.output.root_dir,
              (repo_root / "runtime" / "rolling_optimize_kama").string());
    EXPECT_EQ(config.output.report_json,
              (repo_root / "runtime" / "rolling_optimize_kama" /
               "rolling_optimize_report.json")
                  .string());
    EXPECT_EQ(config.output.report_md,
              (repo_root / "runtime" / "rolling_optimize_kama" /
               "rolling_optimize_report.md")
                  .string());
    EXPECT_EQ(config.output.best_params_dir,
              (repo_root / "runtime" / "rolling_optimize_kama" / "best_params").string());
    EXPECT_TRUE(std::filesystem::exists(config.optimization.param_space));
    EXPECT_TRUE(std::filesystem::exists(config.backtest_base.strategy_composite_config));
}

TEST(RollingConfigTest, LoadsRollingOptimizeRandomConfigWithSeed) {
    const auto dir = MakeTempDir("rolling_config_random_optimize");
    const auto dataset_root = dir / "data";
    const auto manifest = dataset_root / "_manifest" / "partitions.jsonl";
    WriteFile(manifest, ManifestLine("20230103"));
    const auto composite = WriteFile(dir / "composite.yaml", "run_type: backtest\n");
    const auto sub_config = WriteFile(dir / "sub_strategy.yaml", "params:\n");
    const auto products = WriteFile(dir / "instrument_info.json", "{\"products\":{}}\n");
    const auto calendar = WriteFile(dir / "contract_expiry_calendar.yaml", "contracts:\n");
    const auto param_space = WriteFile(
        dir / "param_space.yaml",
        "composite_config_path: " + composite.string() +
            "\n"
            "target_sub_config_path: " + sub_config.string() +
            "\n"
            "backtest_args:\n"
            "  engine_mode: parquet\n"
            "  dataset_root: " +
            dataset_root.string() +
            "\n"
            "optimization:\n"
            "  algorithm: random\n"
            "  metric_path: profit_factor\n"
            "  max_trials: 12\n"
            "  random_seed: 1001\n"
            "parameters:\n"
            "  - name: default_volume\n"
            "    type: int\n"
            "    values: [1, 2, 3]\n");

    const auto config_path = dir / "rolling.yaml";
    WriteFile(config_path,
              "mode: rolling_optimize\n"
              "backtest_base:\n"
              "  engine_mode: parquet\n"
              "  dataset_root: " +
                  dataset_root.string() +
                  "\n"
                  "  dataset_manifest: " +
                  manifest.string() +
                  "\n"
                  "  strategy_factory: composite\n"
                  "  strategy_composite_config: " +
                  composite.string() +
                  "\n"
                  "  product_config_path: " +
                  products.string() +
                  "\n"
                  "  contract_expiry_calendar_path: " +
                  calendar.string() +
                  "\n"
                  "window:\n"
                  "  type: rolling\n"
                  "  train_length_days: 2\n"
                  "  test_length_days: 1\n"
                  "  step_days: 1\n"
                  "  min_train_days: 2\n"
                  "  start_date: 20230101\n"
                  "  end_date: 20230131\n"
                  "optimization:\n"
                  "  algorithm: random\n"
                  "  metric: profit_factor\n"
                  "  max_trials: 12\n"
                  "  random_seed: 4242\n"
                  "  parallel: 2\n"
                  "  param_space: " +
                  param_space.string() +
                  "\n"
                  "  target_sub_config_path: " +
                  sub_config.string() +
                  "\n"
                  "output:\n"
                  "  report_json: " +
                  (dir / "report.json").string() +
                  "\n"
                  "  report_md: " +
                  (dir / "report.md").string() +
                  "\n");

    RollingConfig config;
    std::string error;
    ASSERT_TRUE(LoadRollingConfig(config_path.string(), &config, &error)) << error;

    EXPECT_EQ(config.mode, "rolling_optimize");
    EXPECT_EQ(config.optimization.algorithm, "random");
    ASSERT_TRUE(config.optimization.random_seed.has_value());
    EXPECT_EQ(*config.optimization.random_seed, 4242ULL);
    EXPECT_EQ(config.optimization.max_trials, 12);
    EXPECT_EQ(config.optimization.parallel, 2);
    EXPECT_EQ(config.optimization.param_space, param_space.string());

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

}  // namespace
}  // namespace quant_hft::rolling
