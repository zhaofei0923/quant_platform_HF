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
        "  preserve_top_k_trials: 3\n"
        "  export_heatmap: true\n"
        "  constraints:\n"
        "    - \"max_drawdown_pct < 5.0\"\n"
        "    - \"total_trades >= 100\"\n"
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
    EXPECT_EQ(space.optimization.preserve_top_k_trials, 3);
    EXPECT_TRUE(space.optimization.export_heatmap);
    ASSERT_EQ(space.optimization.constraints.size(), 2U);
    EXPECT_EQ(space.optimization.constraints[0].raw_expression, "max_drawdown_pct < 5.0");
    EXPECT_EQ(space.optimization.constraints[0].metric_name, "max_drawdown_pct");
    EXPECT_EQ(space.optimization.constraints[1].raw_expression, "total_trades >= 100");
    EXPECT_EQ(space.optimization.constraints[1].metric_name, "total_trades");
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

TEST(ParameterSpaceTest, RejectsInvalidConstraintExpression) {
    const auto dir = MakeTempDir("quant_hft_parameter_space_invalid_constraint");
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
        "  constraints:\n"
        "    - \"unknown_metric > 1.0\"\n"
        "parameters:\n"
        "  - name: default_volume\n"
        "    type: int\n"
        "    values: [1, 2]\n");

    ParameterSpace space;
    std::string error;
    EXPECT_FALSE(LoadParameterSpace(path.string(), &space, &error));
    EXPECT_NE(error.find("unsupported constraint metric"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(ParameterSpaceTest, LoadsRandomSearchConfigWithSeed) {
    const auto dir = MakeTempDir("quant_hft_parameter_space_random_seed");
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
        "  algorithm: random\n"
        "  metric_path: profit_factor\n"
        "  max_trials: 20\n"
        "  random_seed: 20260420\n"
        "parameters:\n"
        "  - name: default_volume\n"
        "    type: int\n"
        "    values: [1, 2, 3]\n");

    ParameterSpace space;
    std::string error;
    EXPECT_TRUE(LoadParameterSpace(path.string(), &space, &error)) << error;
    EXPECT_EQ(space.composite_config_path, composite.string());
    EXPECT_EQ(space.target_sub_config_path, target.string());
    EXPECT_EQ(space.optimization.algorithm, "random");
    ASSERT_TRUE(space.optimization.random_seed.has_value());
    EXPECT_EQ(*space.optimization.random_seed, 20260420ULL);
    EXPECT_EQ(space.optimization.max_trials, 20);

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
    EXPECT_EQ(space.optimization.preserve_top_k_trials, 0);
    EXPECT_FALSE(space.optimization.export_heatmap);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(ParameterSpaceTest, LoadsWeightedObjectivesConfig) {
    const auto dir = MakeTempDir("quant_hft_parameter_space_weighted");
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
        "optimization:\n"
        "  algorithm: grid\n"
        "  objectives:\n"
        "    - path: summary.total_pnl\n"
        "      weight: 0.6\n"
        "      maximize: true\n"
        "      scale_by_initial_equity: true\n"
        "    - path: summary.max_drawdown\n"
        "      weight: 0.4\n"
        "      maximize: false\n"
        "      scale_by_initial_equity: true\n"
        "  max_trials: 8\n"
        "  batch_size: 2\n"
        "parameters:\n"
        "  - name: kama_filter\n"
        "    type: double\n"
        "    values: [0.5, 1.0]\n");

    ParameterSpace space;
    std::string error;
    EXPECT_TRUE(LoadParameterSpace(path.string(), &space, &error)) << error;
    EXPECT_EQ(space.composite_config_path, composite.string());
    EXPECT_EQ(space.target_sub_config_path, target.string());
    EXPECT_TRUE(space.optimization.metric_path.empty());
    ASSERT_EQ(space.optimization.objectives.size(), 2U);
    EXPECT_EQ(space.optimization.objectives[0].metric_path, "summary.total_pnl");
    EXPECT_DOUBLE_EQ(space.optimization.objectives[0].weight, 0.6);
    EXPECT_TRUE(space.optimization.objectives[0].maximize);
    EXPECT_TRUE(space.optimization.objectives[0].scale_by_initial_equity);
    EXPECT_EQ(space.optimization.objectives[1].metric_path, "summary.max_drawdown");
    EXPECT_DOUBLE_EQ(space.optimization.objectives[1].weight, 0.4);
    EXPECT_FALSE(space.optimization.objectives[1].maximize);
    EXPECT_TRUE(space.optimization.objectives[1].scale_by_initial_equity);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(ParameterSpaceTest, LoadsRepoOpsParameterOptimConfig) {
    const auto repo_root = std::filesystem::path(__FILE__)
                               .parent_path()
                               .parent_path()
                               .parent_path()
                               .parent_path();
    const auto path = repo_root / "configs" / "ops" / "parameter_optim.yaml";

    ParameterSpace space;
    std::string error;
    EXPECT_TRUE(LoadParameterSpace(path.string(), &space, &error)) << error;
    EXPECT_EQ(std::filesystem::path(space.composite_config_path).filename(),
              "main_backtest_strategy.yaml");
    EXPECT_EQ(std::filesystem::path(space.target_sub_config_path).filename(),
              "kama_trend_production.yaml");
    EXPECT_EQ(space.backtest_args.at("dataset_root"), "backtest_data/parquet_v2");
    EXPECT_EQ(space.backtest_args.at("product_config_path"),
              "configs/strategies/instrument_info.json");
    EXPECT_EQ(space.backtest_args.at("contract_expiry_calendar_path"),
              "configs/strategies/contract_expiry_calendar.yaml");
    EXPECT_EQ(space.backtest_args.at("symbols"), "c");
    EXPECT_EQ(space.backtest_args.at("emit_trades"), "true");
    EXPECT_EQ(space.optimization.algorithm, "grid");
    EXPECT_EQ(space.optimization.metric_path, "profit_factor");
    EXPECT_TRUE(space.optimization.objectives.empty());
    EXPECT_EQ(space.optimization.max_trials, 48);
    EXPECT_EQ(space.optimization.batch_size, 2);
    EXPECT_EQ(space.optimization.preserve_top_k_trials, 10);
    EXPECT_EQ(space.optimization.output_json, "docs/results/opts/parameter_optim_report.json");
    EXPECT_EQ(space.optimization.output_md, "docs/results/opts/parameter_optim_report.md");
    EXPECT_EQ(space.optimization.best_params_yaml,
              "docs/results/opts/parameter_optim_best_params.yaml");
    EXPECT_TRUE(space.optimization.export_heatmap);
    ASSERT_EQ(space.parameters.size(), 3U);
    EXPECT_EQ(space.parameters[0].name, "kama_filter");
    EXPECT_EQ(space.parameters[1].name, "stop_loss_atr_multiplier");
    EXPECT_EQ(space.parameters[2].name, "risk_per_trade_pct");
}

}  // namespace
}  // namespace quant_hft::optim
