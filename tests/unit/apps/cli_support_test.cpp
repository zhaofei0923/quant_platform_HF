#include "quant_hft/apps/cli_support.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft::apps {
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

TEST(CliSupportTest, UsesOpsAndRuntimeDefaultConfigPaths) {
    EXPECT_EQ(DefaultParameterOptimConfigPath(), "configs/ops/parameter_optim.yaml");
    EXPECT_EQ(DefaultRollingBacktestConfigPath(), "configs/ops/rolling_backtest.yaml");
}

TEST(CliSupportTest, ResolveConfigPathWithDefaultUsesExplicitConfigWhenProvided) {
    ArgMap args;
    args["config"] = "/tmp/custom.yaml";

    const ResolvedConfigPath resolved =
        ResolveConfigPathWithDefault(args, "config", DefaultParameterOptimConfigPath());

    EXPECT_EQ(resolved.path, "/tmp/custom.yaml");
    EXPECT_FALSE(resolved.used_default);
}

TEST(CliSupportTest, ResolveConfigPathWithDefaultFallsBackToDefaultWhenMissing) {
    const ResolvedConfigPath resolved =
        ResolveConfigPathWithDefault(ArgMap{}, "config", DefaultRollingBacktestConfigPath());

    EXPECT_EQ(resolved.path, DefaultRollingBacktestConfigPath());
    EXPECT_TRUE(resolved.used_default);
}

TEST(CliSupportTest, ResolveBacktestOutputPathsUsesExplicitPathsWhenProvided) {
    ArgMap args;
    args["output_json"] = "runtime/out/result.json";
    args["output_md"] = "runtime/out/report.md";
    args["export_csv_dir"] = "runtime/out/csv";
    args["output"] = "runtime/out/ignored";

    const BacktestOutputPaths resolved = ResolveBacktestOutputPaths(args);
    EXPECT_EQ(resolved.output_json, "runtime/out/result.json");
    EXPECT_EQ(resolved.output_md, "runtime/out/report.md");
    EXPECT_EQ(resolved.export_csv_dir, "runtime/out/csv");
}

TEST(CliSupportTest, ResolveBacktestOutputPathsExpandsOutputDirectoryAlias) {
    ArgMap args;
    args["output"] = "runtime/verify_oos/train_2023";

    const BacktestOutputPaths resolved = ResolveBacktestOutputPaths(args);
    EXPECT_EQ(resolved.output_json, "runtime/verify_oos/train_2023/result.json");
    EXPECT_EQ(resolved.output_md, "runtime/verify_oos/train_2023/report.md");
    EXPECT_EQ(resolved.export_csv_dir, "runtime/verify_oos/train_2023/csv");
}

TEST(CliSupportTest, BacktestCliFallbackPrefersSiblingBinary) {
    const auto dir = MakeTempDir("cli_support_sibling");
    const auto app_dir = dir / "build-gcc";
    const auto sibling = WriteFile(app_dir / "backtest_cli", "");

    EXPECT_EQ(DetectDefaultBacktestCliPath((app_dir / "parameter_optim_cli").string(), dir),
              sibling.string());

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(CliSupportTest, BacktestCliFallbackPrefersBuildGccOverBuild) {
    const auto dir = MakeTempDir("cli_support_build");
    const auto build = WriteFile(dir / "build" / "backtest_cli", "");
    const auto build_gcc = WriteFile(dir / "build-gcc" / "backtest_cli", "");

    EXPECT_EQ(DetectDefaultBacktestCliPath("parameter_optim_cli", dir), build_gcc.string());
    EXPECT_NE(build.string(), build_gcc.string());

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

}  // namespace
}  // namespace quant_hft::apps
