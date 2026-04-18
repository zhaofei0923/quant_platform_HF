#include "quant_hft/optim/parameter_space.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft::optim {
namespace {

std::filesystem::path MakeTempDir(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      (stem + "_" + std::to_string(stamp));
    std::filesystem::create_directories(path);
    return path;
}

std::filesystem::path WriteFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

TEST(ParameterSpaceTest, LoadsValidConfig) {
    const auto dir = MakeTempDir("quant_hft_parameter_space_valid");
    const auto composite = WriteFile(dir / "space_assets" / "strategies" / "main_backtest_strategy.yaml",
                                     "composite:\n");
    const auto target = WriteFile(dir / "space_assets" / "strategies" / "sub" / "kama_trend_1.yaml",
                                  "params:\n");
    const auto path = WriteFile(
        dir / "param_space.yaml",
        "composite_config_path: ./space_assets/strategies/main_backtest_strategy.yaml\n"
        "target_sub_config_path: ./sub/kama_trend_1.yaml\n"
        "backtest_args:\n"
        "  engine_mode: parquet\n"
        "  dataset_root: backtest_data/parquet_v2\n"
        "optimization:\n"
        "  algorithm: grid\n"
        "  metric_path: hf_standard.profit_factor\n"
        "  maximize: true\n"
        "  max_trials: 20\n"
        "  parallel: 2\n"
        "parameters:\n"
        "  - name: take_profit_atr_multiplier\n"
        "    type: double\n"
        "    range: [3.0, 20.0]\n"
        "    step: 1.0\n"
        "  - name: default_volume\n"
        "    type: int\n"
        "    values: [1, 2, 3]\n");

    ParameterSpace space;
    std::string error;
    EXPECT_TRUE(LoadParameterSpace(path.string(), &space, &error)) << error;
    EXPECT_EQ(space.composite_config_path, composite.string());
    EXPECT_EQ(space.target_sub_config_path, target.string());
    EXPECT_EQ(space.backtest_args.at("engine_mode"), "parquet");
    EXPECT_EQ(space.backtest_args.at("dataset_root"), "backtest_data/parquet_v2");
    EXPECT_EQ(space.optimization.algorithm, "grid");
    EXPECT_EQ(space.optimization.metric_path, "hf_standard.profit_factor");
    EXPECT_EQ(space.optimization.max_trials, 20);
    EXPECT_EQ(space.optimization.batch_size, 2);
    ASSERT_EQ(space.parameters.size(), 2U);
    EXPECT_EQ(space.parameters[0].name, "take_profit_atr_multiplier");
    EXPECT_EQ(space.parameters[1].name, "default_volume");

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(ParameterSpaceTest, RejectsInvalidSchema) {
    const auto dir = MakeTempDir("quant_hft_parameter_space_invalid");
    const auto path = WriteFile(
        dir / "param_space.yaml",
        "composite_config_path: configs/strategies/main_backtest_strategy.yaml\n"
        "target_sub_config_path: ./sub/kama_trend_1.yaml\n"
        "parameters:\n"
        "  - name: bad_param\n"
        "    type: number\n"
        "    range: [1, 2]\n");

    ParameterSpace space;
    std::string error;
    EXPECT_FALSE(LoadParameterSpace(path.string(), &space, &error));
    EXPECT_NE(error.find("unsupported parameter type"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(ParameterSpaceTest, UsesDefaultsWhenOptimizationMissing) {
    const auto dir = MakeTempDir("quant_hft_parameter_space_defaults");
    const auto composite = WriteFile(dir / "space_assets" / "strategies" / "main_backtest_strategy.yaml",
                                     "composite:\n");
    const auto target = WriteFile(dir / "space_assets" / "strategies" / "sub" / "kama_trend_1.yaml",
                                  "params:\n");
    const auto path = WriteFile(
        dir / "param_space.yaml",
        "composite_config_path: ./space_assets/strategies/main_backtest_strategy.yaml\n"
        "target_sub_config_path: ./sub/kama_trend_1.yaml\n"
        "backtest_args:\n"
        "  dataset_root: backtest_data/parquet_v2\n"
        "parameters:\n"
        "  - name: default_volume\n"
        "    type: int\n"
        "    range: [1, 2]\n");

    ParameterSpace space;
    std::string error;
    EXPECT_TRUE(LoadParameterSpace(path.string(), &space, &error)) << error;
    EXPECT_EQ(space.composite_config_path, composite.string());
    EXPECT_EQ(space.target_sub_config_path, target.string());
    EXPECT_EQ(space.optimization.algorithm, "grid");
    EXPECT_EQ(space.optimization.metric_path, "hf_standard.profit_factor");
    EXPECT_GT(space.optimization.batch_size, 0);
    EXPECT_EQ(space.optimization.max_trials, 100);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

}  // namespace
}  // namespace quant_hft::optim
