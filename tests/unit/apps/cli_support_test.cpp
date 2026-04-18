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

TEST(CliSupportTest, UsesRuntimeOptimDefaultConfigPaths) {
    EXPECT_EQ(DefaultParameterOptimConfigPath(), "runtime/optim/c_kama_param_space.yaml");
    EXPECT_EQ(DefaultRollingBacktestConfigPath(), "runtime/optim/c_kama_rolling_optimize.yaml");
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
