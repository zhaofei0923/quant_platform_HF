#include "quant_hft/rolling/rolling_runner.h"

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

TEST(RollingRunnerFixedTest, RunsWindowsAndAggregatesSummary) {
    const auto dir = MakeTempDir("rolling_runner_fixed");
    const auto dataset_root = dir / "data";
    const auto manifest =
        WriteManifest(dataset_root,
                      {"20230101", "20230102", "20230103", "20230104", "20230105", "20230106"});

    RollingConfig config;
    config.mode = "fixed_params";
    config.backtest_base.engine_mode = "parquet";
    config.backtest_base.dataset_root = dataset_root.string();
    config.backtest_base.dataset_manifest = manifest.string();
    config.backtest_base.strategy_factory = "demo";
    config.window.type = "rolling";
    config.window.train_length_days = 2;
    config.window.test_length_days = 1;
    config.window.step_days = 1;
    config.window.min_train_days = 2;
    config.window.start_date = "20230101";
    config.window.end_date = "20230131";
    config.optimization.metric = "hf_standard.profit_factor";
    config.output.window_parallel = 2;

    auto fake_run_fn = [](const quant_hft::apps::BacktestCliSpec& spec,
                          quant_hft::apps::BacktestCliResult* out,
                          std::string* error) {
        (void)error;
        quant_hft::apps::BacktestCliResult result;
        result.run_id = spec.run_id;
        result.engine_mode = spec.engine_mode;
        result.mode = "backtest";
        result.data_source = "parquet";
        result.spec = spec;

        const int day = std::stoi(spec.start_date.substr(spec.start_date.size() - 2));
        result.advanced_summary.profit_factor = static_cast<double>(day);

        result.has_deterministic = true;
        result.deterministic.performance.total_pnl = static_cast<double>(day) * 10.0;
        result.deterministic.performance.max_drawdown = -static_cast<double>(day);
        result.final_equity = 1000000.0 + result.deterministic.performance.total_pnl;
        *out = std::move(result);
        return true;
    };

    RollingReport report;
    std::string error;
    ASSERT_TRUE(RunRollingBacktest(config, &report, &error, fake_run_fn)) << error;

    ASSERT_EQ(report.windows.size(), 4U);
    EXPECT_EQ(report.success_count, 4);
    EXPECT_EQ(report.failed_count, 0);
    EXPECT_FALSE(report.interrupted);
    EXPECT_DOUBLE_EQ(report.windows[0].objective, 3.0);
    EXPECT_DOUBLE_EQ(report.windows[3].objective, 6.0);
    EXPECT_DOUBLE_EQ(report.mean_objective, 4.5);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

}  // namespace
}  // namespace quant_hft::rolling

