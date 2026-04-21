#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace {

int RunCommand(const std::string& command) { return std::system(command.c_str()); }

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto path =
        std::filesystem::temp_directory_path() / ("quant_hft_run_rolling_test_" + suffix);
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path;
}

void WriteFile(const std::filesystem::path& path, const std::string& payload) {
    if (!path.parent_path().empty()) {
        std::filesystem::create_directories(path.parent_path());
    }
    std::ofstream out(path);
    out << payload;
}

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

std::string EscapePathForShell(const std::filesystem::path& path) {
    std::string text = path.string();
    std::string escaped;
    escaped.reserve(text.size() + 8);
    for (const char ch : text) {
        if (ch == '\'') {
            escaped += "'\\''";
        } else {
            escaped.push_back(ch);
        }
    }
    return escaped;
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

const std::string* FindArgValue(const std::vector<std::string>& args, const std::string& flag) {
    for (std::size_t index = 0; index + 1 < args.size(); ++index) {
        if (args[index] == flag) {
            return &args[index + 1];
        }
    }
    return nullptr;
}

TEST(RunRollingBacktestScriptTest, DryRunUsesDefaultConfigWithoutArgs) {
    const auto root = MakeTempDir("dry_run");
    const auto log_file = root / "dry_run.log";

    const std::string command = "bash scripts/build/run_rolling_backtest.sh --dry-run >'" +
                                EscapePathForShell(log_file) + "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("rolling_backtest_cli"), std::string::npos);
    EXPECT_NE(payload.find("configs/ops/rolling_backtest.yaml"), std::string::npos);
    EXPECT_NE(payload.find("cmake --build"), std::string::npos);
}

TEST(RunRollingBacktestScriptTest, SkipBuildPassesExplicitConfigToRollingCli) {
    const auto root = MakeTempDir("skip_build");
    const auto build_dir = root / "build-gcc";
    const auto fake_cli = build_dir / "rolling_backtest_cli";
    const auto args_log = root / "captured_args.txt";
    const auto config_path = root / "rolling.yaml";

    std::filesystem::create_directories(build_dir);
    WriteFile(config_path,
              "mode: fixed_params\n"
              "backtest_base:\n"
              "  engine_mode: parquet\n"
              "  dataset_root: backtest_data/parquet_v2\n"
              "window:\n"
              "  type: rolling\n"
              "  train_length_days: 10\n"
              "  test_length_days: 5\n"
              "  step_days: 5\n"
              "  min_train_days: 10\n"
              "  start_date: 20240101\n"
              "  end_date: 20240131\n"
              "output:\n"
              "  report_json: /tmp/rolling.json\n"
              "  report_md: /tmp/rolling.md\n");

    WriteFile(fake_cli,
              "#!/usr/bin/env bash\n"
              "set -euo pipefail\n"
              "args_file='" +
                  args_log.string() +
                  "'\n"
                  ": >\"${args_file}\"\n"
                  "for arg in \"$@\"; do\n"
                  "  printf '%s\\n' \"${arg}\" >>\"${args_file}\"\n"
                  "done\n");
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    const std::string command = "bash scripts/build/run_rolling_backtest.sh --skip-build --build-dir '" +
                                EscapePathForShell(build_dir) + "' --config '" +
                                EscapePathForShell(config_path) + "'";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::vector<std::string> args = ReadLines(args_log);
    ASSERT_NE(FindArgValue(args, "--config"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--config"), config_path.string());
}

}  // namespace