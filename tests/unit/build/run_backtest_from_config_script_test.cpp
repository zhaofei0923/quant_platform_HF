#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

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

TEST(RunBacktestFromConfigScriptTest, DryRunSucceedsWithMinimalConfig) {
    const auto root = MakeTempDir("dry_run");
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto output_json = root / "out" / "result.json";
    const auto output_md = root / "out" / "result.md";
    const auto output_root_dir = root / "backtest_runs";
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
                  "output_root_dir: " +
                  output_root_dir.string() +
                  "\n"
                  "timestamp_timezone: utc\n"
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
    EXPECT_NE(payload.find("output_root_dir="), std::string::npos);
    EXPECT_NE(payload.find("run_dir="), std::string::npos);
    EXPECT_NE(payload.find("timestamp_timezone=utc"), std::string::npos);
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
    const auto output_root_dir = root / "backtest_runs";
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
                  "output_root_dir: " +
                  output_root_dir.string() +
                  "\n"
                  "timestamp_timezone: utc\n"
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

    const std::vector<std::string> args = ReadLines(args_log);
    ASSERT_NE(FindArgValue(args, "--engine_mode"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--engine_mode"), "parquet");
    ASSERT_NE(FindArgValue(args, "--dataset_root"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--dataset_root"), dataset_root.string());
    ASSERT_NE(FindArgValue(args, "--strategy_main_config_path"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--strategy_main_config_path"), strategy_main.string());
    ASSERT_NE(FindArgValue(args, "--output_json"), nullptr);
    ASSERT_NE(FindArgValue(args, "--output_md"), nullptr);
    ASSERT_NE(FindArgValue(args, "--export_csv_dir"), nullptr);
    ASSERT_NE(FindArgValue(args, "--run_id"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--run_id"), "passthrough-run");
    ASSERT_NE(FindArgValue(args, "--max_ticks"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--max_ticks"), "123");
    ASSERT_NE(FindArgValue(args, "--start_date"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--start_date"), "20240101");
    ASSERT_NE(FindArgValue(args, "--end_date"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--end_date"), "20240131");
    ASSERT_NE(FindArgValue(args, "--emit_position_history"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--emit_position_history"), "true");

    const std::filesystem::path output_json_path(*FindArgValue(args, "--output_json"));
    const std::filesystem::path output_md_path(*FindArgValue(args, "--output_md"));
    const std::filesystem::path export_csv_dir_path(*FindArgValue(args, "--export_csv_dir"));
    const std::filesystem::path run_dir = output_json_path.parent_path();

    EXPECT_EQ(run_dir.parent_path(), output_root_dir);
    EXPECT_EQ(output_md_path.parent_path(), run_dir);
    EXPECT_EQ(export_csv_dir_path.parent_path(), run_dir);
    EXPECT_EQ(output_json_path.filename(), output_json.filename());
    EXPECT_EQ(output_md_path.filename(), output_md.filename());
    EXPECT_EQ(export_csv_dir_path.filename(), export_csv_dir.filename());

    const std::regex run_dir_pattern("^passthrough-run_[0-9]{8}T[0-9]{6}Z$");
    EXPECT_TRUE(std::regex_match(run_dir.filename().string(), run_dir_pattern));
}

TEST(RunBacktestFromConfigScriptTest, SkipBuildRoutesAllArtifactsIntoSingleRunDir) {
    const auto root = MakeTempDir("single_run_dir");
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto args_log = root / "captured_args.txt";
    const auto output_root_dir = root / "backtest_runs";
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
                  "output_root_dir: " +
                  output_root_dir.string() +
                  "\n"
                  "timestamp_timezone: utc\n"
                  "output_json: /tmp/templates/custom_result.json\n"
                  "output_md: /tmp/templates/custom_result.md\n"
                  "export_csv_dir: /tmp/templates/custom_csv\n"
                  "run_id: single-run\n"
                  "emit_indicator_trace: true\n"
                  "indicator_trace_path: /tmp/templates/custom_trace.parquet\n"
                  "emit_sub_strategy_indicator_trace: true\n"
                  "sub_strategy_indicator_trace_path: /tmp/templates/custom_sub_trace.parquet\n");

    const std::string command =
        "bash scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' --skip-build";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::vector<std::string> args = ReadLines(args_log);
    ASSERT_NE(FindArgValue(args, "--output_json"), nullptr);
    ASSERT_NE(FindArgValue(args, "--output_md"), nullptr);
    ASSERT_NE(FindArgValue(args, "--export_csv_dir"), nullptr);
    ASSERT_NE(FindArgValue(args, "--indicator_trace_path"), nullptr);
    ASSERT_NE(FindArgValue(args, "--sub_strategy_indicator_trace_path"), nullptr);
    const std::filesystem::path output_json_path(*FindArgValue(args, "--output_json"));
    const std::filesystem::path run_dir = output_json_path.parent_path();

    EXPECT_EQ(run_dir.parent_path(), output_root_dir);
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--output_md")).parent_path(), run_dir);
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--export_csv_dir")).parent_path(), run_dir);
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--indicator_trace_path")).parent_path(),
              run_dir);
    EXPECT_EQ(
        std::filesystem::path(*FindArgValue(args, "--sub_strategy_indicator_trace_path")).parent_path(),
        run_dir);
    EXPECT_EQ(output_json_path.filename(), "custom_result.json");
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--output_md")).filename(),
              "custom_result.md");
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--export_csv_dir")).filename(),
              "custom_csv");
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--indicator_trace_path")).filename(),
              "custom_trace.parquet");
    EXPECT_EQ(
        std::filesystem::path(*FindArgValue(args, "--sub_strategy_indicator_trace_path")).filename(),
        "custom_sub_trace.parquet");

    const std::regex run_dir_pattern("^single-run_[0-9]{8}T[0-9]{6}Z$");
    EXPECT_TRUE(std::regex_match(run_dir.filename().string(), run_dir_pattern));
}

TEST(RunBacktestFromConfigScriptTest, SkipBuildDefaultsCsvDirWhenExportCsvDirEmpty) {
    const auto root = MakeTempDir("default_csv_dir");
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto args_log = root / "captured_args.txt";
    const auto output_root_dir = root / "backtest_runs";
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
                  "output_root_dir: " +
                  output_root_dir.string() +
                  "\n"
                  "timestamp_timezone: utc\n"
                  "output_json: /tmp/templates/default_csv_result.json\n"
                  "output_md: /tmp/templates/default_csv_result.md\n"
                  "run_id: default-csv\n");

    const std::string command =
        "bash scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' --skip-build";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::vector<std::string> args = ReadLines(args_log);
    ASSERT_NE(FindArgValue(args, "--export_csv_dir"), nullptr);
    ASSERT_NE(FindArgValue(args, "--output_json"), nullptr);
    const std::filesystem::path output_json_path(*FindArgValue(args, "--output_json"));
    const std::filesystem::path export_csv_dir(*FindArgValue(args, "--export_csv_dir"));

    EXPECT_EQ(export_csv_dir.parent_path(), output_json_path.parent_path());
    EXPECT_EQ(export_csv_dir.filename(), "csv");
}

TEST(RunBacktestFromConfigScriptTest, SkipBuildPassesIndicatorTraceArgsWhenEnabled) {
    const auto root = MakeTempDir("trace_passthrough");
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto args_log = root / "captured_args.txt";
    const auto output_root_dir = root / "backtest_runs";
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
                  "output_root_dir: " +
                  output_root_dir.string() +
                  "\n"
                  "timestamp_timezone: utc\n"
                  "output_json: /tmp/templates/trace_result.json\n"
                  "output_md: /tmp/templates/trace_result.md\n"
                  "run_id: trace-run\n"
                  "emit_indicator_trace: true\n"
                  "indicator_trace_path: /tmp/templates/main_trace.parquet\n"
                  "emit_sub_strategy_indicator_trace: true\n"
                  "sub_strategy_indicator_trace_path: /tmp/templates/sub_trace.parquet\n");

    const std::string command =
        "bash scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' --skip-build";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::vector<std::string> args = ReadLines(args_log);
    ASSERT_NE(FindArgValue(args, "--emit_indicator_trace"), nullptr);
    ASSERT_NE(FindArgValue(args, "--emit_sub_strategy_indicator_trace"), nullptr);
    ASSERT_NE(FindArgValue(args, "--indicator_trace_path"), nullptr);
    ASSERT_NE(FindArgValue(args, "--sub_strategy_indicator_trace_path"), nullptr);
    ASSERT_NE(FindArgValue(args, "--output_json"), nullptr);

    EXPECT_EQ(*FindArgValue(args, "--emit_indicator_trace"), "true");
    EXPECT_EQ(*FindArgValue(args, "--emit_sub_strategy_indicator_trace"), "true");

    const std::filesystem::path run_dir =
        std::filesystem::path(*FindArgValue(args, "--output_json")).parent_path();
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--indicator_trace_path")).parent_path(),
              run_dir);
    EXPECT_EQ(
        std::filesystem::path(*FindArgValue(args, "--sub_strategy_indicator_trace_path")).parent_path(),
        run_dir);
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--indicator_trace_path")).filename(),
              "main_trace.parquet");
    EXPECT_EQ(
        std::filesystem::path(*FindArgValue(args, "--sub_strategy_indicator_trace_path")).filename(),
        "sub_trace.parquet");
}

TEST(RunBacktestFromConfigScriptTest, InvalidTimestampTimezoneFailsFast) {
    const auto root = MakeTempDir("invalid_timezone");
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto output_json = root / "result" / "result.json";
    const auto output_md = root / "result" / "result.md";
    const auto log_file = root / "timezone_error.log";
    const auto fake_cli = build_dir / "backtest_cli";

    std::filesystem::create_directories(build_dir);
    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main, "run_type: backtest\n");
    WriteFile(fake_cli, "#!/usr/bin/env bash\nset -euo pipefail\n");
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
                  "timestamp_timezone: moon\n");

    const std::string command =
        "bash scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' --skip-build >'" + EscapePathForShell(log_file) +
        "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_NE(rc, 0);
    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("timestamp_timezone must be one of: local, utc"), std::string::npos);
}

TEST(RunBacktestFromConfigScriptTest, DryRunPrintsResolvedRunDirSummary) {
    const auto root = MakeTempDir("dry_run_summary");
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto output_root_dir = root / "backtest_runs";
    const auto log_file = root / "dry_run_summary.log";

    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main, "run_type: backtest\n");
    WriteFile(config_path,
              "engine_mode: parquet\n"
              "dataset_root: " +
                  dataset_root.string() +
                  "\n"
                  "strategy_main_config_path: " +
                  strategy_main.string() +
                  "\n"
                  "output_root_dir: " +
                  output_root_dir.string() +
                  "\n"
                  "timestamp_timezone: utc\n"
                  "output_json: /tmp/templates/report.json\n"
                  "output_md: /tmp/templates/report.md\n"
                  "emit_indicator_trace: true\n"
                  "emit_sub_strategy_indicator_trace: true\n");

    const std::string command =
        "bash scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' --dry-run >'" + EscapePathForShell(log_file) +
        "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("output_root_dir="), std::string::npos);
    EXPECT_NE(payload.find("timestamp_timezone=utc"), std::string::npos);
    EXPECT_NE(payload.find("run_id="), std::string::npos);
    EXPECT_NE(payload.find("run_dir="), std::string::npos);
    EXPECT_NE(payload.find("export_csv_dir="), std::string::npos);
    EXPECT_NE(payload.find("indicator_trace_path="), std::string::npos);
    EXPECT_NE(payload.find("sub_strategy_indicator_trace_path="), std::string::npos);
}

TEST(RunBacktestFromConfigScriptTest, SkipBuildShowsProgressAndCanSilenceBacktestStdout) {
    const auto root = MakeTempDir("skip_build_progress");
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto output_json = root / "result" / "result.json";
    const auto output_md = root / "result" / "result.md";
    const auto fake_cli = build_dir / "backtest_cli";
    const auto log_file = root / "progress.log";

    std::filesystem::create_directories(build_dir);
    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main, "run_type: backtest\n");

    WriteFile(fake_cli,
              "#!/usr/bin/env bash\n"
              "set -euo pipefail\n"
              "sleep 0.2\n"
              "echo '{\"status\":\"ok\",\"pnl\":123}'\n");
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
                  "quiet_backtest_stdout: true\n"
                  "progress_only: true\n");

    const std::string command =
        "bash scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' --skip-build >'" + EscapePathForShell(log_file) +
        "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("%"), std::string::npos);
    EXPECT_NE(payload.find("100%"), std::string::npos);
    EXPECT_EQ(payload.find("{\"status\":\"ok\",\"pnl\":123}"), std::string::npos);
    EXPECT_EQ(payload.find("=== backtest run summary ==="), std::string::npos);
    EXPECT_EQ(payload.find("config_path="), std::string::npos);
    EXPECT_EQ(payload.find("--engine_mode"), std::string::npos);
}

TEST(RunBacktestFromConfigScriptTest, SkipBuildShowsErrorWhenProgressOnlyEnabled) {
    const auto root = MakeTempDir("skip_build_progress_error");
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto output_json = root / "result" / "result.json";
    const auto output_md = root / "result" / "result.md";
    const auto fake_cli = build_dir / "backtest_cli";
    const auto log_file = root / "progress_error.log";

    std::filesystem::create_directories(build_dir);
    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main, "run_type: backtest\n");

    WriteFile(fake_cli,
              "#!/usr/bin/env bash\n"
              "set -euo pipefail\n"
              "echo 'fatal: replay failed at row 12' >&2\n"
              "exit 7\n");
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
                  "quiet_backtest_stdout: true\n"
                  "progress_only: true\n");

    const std::string command =
        "bash scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' --skip-build >'" + EscapePathForShell(log_file) +
        "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_NE(rc, 0);

    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("fatal: replay failed at row 12"), std::string::npos);
}

}  // namespace
