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

std::filesystem::path WriteFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
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

std::string ReadConfigPathFromComposite(const std::string& composite_path) {
    std::ifstream in(composite_path);
    std::string line;
    while (std::getline(in, line)) {
        const auto pos = line.find("config_path:");
        if (pos == std::string::npos) {
            continue;
        }
        std::string value = line.substr(pos + std::string("config_path:").size());
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
            value.erase(value.begin());
        }
        if (value.size() >= 2 &&
            ((value.front() == '"' && value.back() == '"') ||
             (value.front() == '\'' && value.back() == '\''))) {
            value = value.substr(1, value.size() - 2);
        }
        return value;
    }
    return "";
}

int ReadDefaultVolumeFromSubConfig(const std::string& sub_config_path) {
    std::ifstream in(sub_config_path);
    std::string line;
    while (std::getline(in, line)) {
        const auto pos = line.find("default_volume:");
        if (pos == std::string::npos) {
            continue;
        }
        std::string value = line.substr(pos + std::string("default_volume:").size());
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
            value.erase(value.begin());
        }
        return std::stoi(value);
    }
    return 0;
}

TEST(RollingRunnerOptimizeTest, SelectsBestTrialAndEvaluatesOnTestWindow) {
    const auto dir = MakeTempDir("rolling_runner_optimize");
    const auto dataset_root = dir / "data";
    const auto manifest = WriteManifest(dataset_root, {"20230101", "20230102", "20230103", "20230104"});

    const auto sub_config = WriteFile(dir / "sub_strategy.yaml",
                                      "params:\n"
                                      "  id: trend_1\n"
                                      "  default_volume: 1\n");

    const auto composite_config = WriteFile(dir / "composite.yaml",
                                            "composite:\n"
                                            "  merge_rule: kPriority\n"
                                            "  sub_strategies:\n"
                                            "    - id: trend_1\n"
                                            "      enabled: true\n"
                                            "      type: TrendStrategy\n"
                                            "      config_path: " +
                                                sub_config.string() + "\n");

    const auto param_space = WriteFile(
        dir / "param_space.yaml",
        "composite_config_path: " + composite_config.string() +
            "\n"
            "target_sub_config_path: " + sub_config.string() +
            "\n"
            "backtest_args:\n"
            "  engine_mode: parquet\n"
            "  dataset_root: " +
            dataset_root.string() +
            "\n"
            "optimization:\n"
            "  algorithm: grid\n"
            "  metric_path: hf_standard.profit_factor\n"
            "  maximize: true\n"
            "  max_trials: 10\n"
            "  parallel: 2\n"
            "parameters:\n"
            "  - name: default_volume\n"
            "    type: int\n"
            "    values: [1, 2]\n");

    RollingConfig config;
    config.mode = "rolling_optimize";
    config.backtest_base.engine_mode = "parquet";
    config.backtest_base.dataset_root = dataset_root.string();
    config.backtest_base.dataset_manifest = manifest.string();
    config.backtest_base.strategy_factory = "composite";
    config.backtest_base.strategy_composite_config = composite_config.string();
    config.window.type = "rolling";
    config.window.train_length_days = 2;
    config.window.test_length_days = 2;
    config.window.step_days = 2;
    config.window.min_train_days = 2;
    config.window.start_date = "20230101";
    config.window.end_date = "20230131";

    config.optimization.algorithm = "grid";
    config.optimization.metric = "hf_standard.profit_factor";
    config.optimization.maximize = true;
    config.optimization.max_trials = 10;
    config.optimization.parallel = 2;
    config.optimization.param_space = param_space.string();
    config.optimization.target_sub_config_path = sub_config.string();

    config.output.best_params_dir = (dir / "best").string();
    config.output.keep_temp_files = false;
    config.output.window_parallel = 3;

    auto fake_run_fn = [](const quant_hft::apps::BacktestCliSpec& spec,
                          quant_hft::apps::BacktestCliResult* out,
                          std::string* error) {
        (void)error;
        quant_hft::apps::BacktestCliResult result;
        result.run_id = spec.run_id;
        result.spec = spec;
        result.mode = "backtest";
        result.engine_mode = spec.engine_mode;
        result.data_source = "parquet";

        const std::string sub_config_path = ReadConfigPathFromComposite(spec.strategy_composite_config);
        const int default_volume = ReadDefaultVolumeFromSubConfig(sub_config_path);

        const bool is_train = spec.run_id.find("-train-") != std::string::npos;
        result.advanced_summary.profit_factor =
            is_train ? static_cast<double>(default_volume) : 100.0 + static_cast<double>(default_volume);

        result.has_deterministic = true;
        result.deterministic.performance.total_pnl = result.advanced_summary.profit_factor * 10.0;
        result.deterministic.performance.max_drawdown = -1.0;
        result.final_equity = 1000000.0 + result.deterministic.performance.total_pnl;

        *out = std::move(result);
        return true;
    };

    RollingReport report;
    std::string error;
    ASSERT_TRUE(RunRollingBacktest(config, &report, &error, fake_run_fn)) << error;

    ASSERT_EQ(report.windows.size(), 1U);
    EXPECT_EQ(report.success_count, 1);
    EXPECT_EQ(report.failed_count, 0);
    EXPECT_TRUE(report.windows[0].success);
    EXPECT_DOUBLE_EQ(report.windows[0].objective, 102.0);
    EXPECT_FALSE(report.windows[0].best_params_yaml.empty());
    EXPECT_TRUE(std::filesystem::exists(report.windows[0].best_params_yaml));

    std::ifstream best_in(report.windows[0].best_params_yaml);
    std::string best_text((std::istreambuf_iterator<char>(best_in)), std::istreambuf_iterator<char>());
    EXPECT_NE(best_text.find("default_volume: 2"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

}  // namespace
}  // namespace quant_hft::rolling

