#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

namespace {

int RunCommand(const std::string& command) {
    return std::system(command.c_str());
}

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto path =
        std::filesystem::temp_directory_path() / ("quant_hft_run_backtest_cfg_test_" + suffix);
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

TEST(RunBacktestFromConfigScriptTest, DryRunSucceedsWithMinimalConfig) {
    const auto root = MakeTempDir("dry_run");
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto output_json = root / "out" / "result.json";
    const auto output_md = root / "out" / "result.md";
    const auto log_file = root / "dry_run.log";

    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main,
              "run_type: backtest\n"
              "backtest:\n"
              "  initial_equity: 100000\n"
              "  symbols: [c]\n"
              "  start_date: 20240101\n"
              "  end_date: 20240102\n");
    WriteFile(config_path,
              "engine_mode: parquet\n"
              "dataset_root: " +
                  dataset_root.string() +
                  "\n"
                  "strategy_main_config_path: " +
                  strategy_main.string() +
                  "\n"
                  "output_json: " +
                  output_json.string() +
                  "\n"
                  "output_md: " +
                  output_md.string() +
                  "\n");

    const std::string command =
        "bash scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' --dry-run >'" + EscapePathForShell(log_file) +
        "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("backtest_cli"), std::string::npos);
    EXPECT_NE(payload.find("--engine_mode"), std::string::npos);
    EXPECT_NE(payload.find("parquet"), std::string::npos);
}

TEST(RunBacktestFromConfigScriptTest, MissingRequiredFieldFailsFast) {
    const auto root = MakeTempDir("missing_required");
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto log_file = root / "missing_required.log";

    WriteFile(strategy_main, "run_type: backtest\n");
    WriteFile(config_path,
              "engine_mode: parquet\n"
              "strategy_main_config_path: " +
                  strategy_main.string() +
                  "\n"
                  "output_json: " +
                  (root / "result.json").string() +
                  "\n"
                  "output_md: " +
                  (root / "result.md").string() +
                  "\n");

    const std::string command =
        "bash scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' >'" + EscapePathForShell(log_file) + "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_NE(rc, 0);

    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("required config keys"), std::string::npos);
}

TEST(RunBacktestFromConfigScriptTest, SkipBuildPassesThroughOptionalArgs) {
    const auto root = MakeTempDir("skip_build_passthrough");
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto export_csv_dir = root / "csv_export";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto args_log = root / "captured_args.txt";
    const auto output_json = root / "result" / "result.json";
    const auto output_md = root / "result" / "result.md";
    const auto fake_cli = build_dir / "backtest_cli";

    std::filesystem::create_directories(build_dir);
    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main, "run_type: backtest\n");

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
    std::filesystem::permissions(fake_cli,
                                 std::filesystem::perms::owner_exec |
                                     std::filesystem::perms::group_exec |
                                     std::filesystem::perms::others_exec |
                                     std::filesystem::perms::owner_read |
                                     std::filesystem::perms::group_read |
                                     std::filesystem::perms::others_read |
                                     std::filesystem::perms::owner_write,
                                 std::filesystem::perm_options::replace);

    WriteFile(config_path,
              "build_dir: " +
                  build_dir.string() +
                  "\n"
                  "engine_mode: parquet\n"
                  "dataset_root: " +
                  dataset_root.string() +
                  "\n"
                  "strategy_main_config_path: " +
                  strategy_main.string() +
                  "\n"
                  "output_json: " +
                  output_json.string() +
                  "\n"
                  "output_md: " +
                  output_md.string() +
                  "\n"
                  "export_csv_dir: " +
                  export_csv_dir.string() +
                  "\n"
                  "run_id: passthrough-run\n"
                  "max_ticks: 123\n"
                  "start_date: 20240101\n"
                  "end_date: 20240131\n"
                  "emit_position_history: true\n");

    const std::string command =
        "bash scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' --skip-build";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::string payload = ReadFile(args_log);
    EXPECT_NE(payload.find("--engine_mode"), std::string::npos);
    EXPECT_NE(payload.find("parquet"), std::string::npos);
    EXPECT_NE(payload.find("--dataset_root"), std::string::npos);
    EXPECT_NE(payload.find(dataset_root.string()), std::string::npos);
    EXPECT_NE(payload.find("--strategy_main_config_path"), std::string::npos);
    EXPECT_NE(payload.find(strategy_main.string()), std::string::npos);
    EXPECT_NE(payload.find("--output_json"), std::string::npos);
    EXPECT_NE(payload.find(output_json.string()), std::string::npos);
    EXPECT_NE(payload.find("--output_md"), std::string::npos);
    EXPECT_NE(payload.find(output_md.string()), std::string::npos);
    EXPECT_NE(payload.find("--export_csv_dir"), std::string::npos);
    EXPECT_NE(payload.find(export_csv_dir.string()), std::string::npos);
    EXPECT_NE(payload.find("--run_id"), std::string::npos);
    EXPECT_NE(payload.find("passthrough-run"), std::string::npos);
    EXPECT_NE(payload.find("--max_ticks"), std::string::npos);
    EXPECT_NE(payload.find("123"), std::string::npos);
    EXPECT_NE(payload.find("--start_date"), std::string::npos);
    EXPECT_NE(payload.find("20240101"), std::string::npos);
    EXPECT_NE(payload.find("--end_date"), std::string::npos);
    EXPECT_NE(payload.find("20240131"), std::string::npos);
    EXPECT_NE(payload.find("--emit_position_history"), std::string::npos);
    EXPECT_NE(payload.find("true"), std::string::npos);
    EXPECT_TRUE(std::filesystem::exists(export_csv_dir));
}

}  // namespace

