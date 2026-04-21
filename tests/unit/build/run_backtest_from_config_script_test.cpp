#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

namespace {

int RunCommand(const std::string& command) { return std::system(command.c_str()); }

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
                  output_md.string() + "\n");

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' --dry-run >'" +
                                EscapePathForShell(log_file) + "' 2>&1";
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

TEST(RunBacktestFromConfigScriptTest, DryRunPrintsFormalAnalysisCommandWhenEnabled) {
    const auto root = MakeTempDir("dry_run_formal_report");
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto output_root_dir = root / "backtest_runs";
    const auto log_file = root / "dry_run.log";

    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main,
              "run_type: backtest\n"
              "backtest:\n"
              "  initial_equity: 100000\n"
              "  symbols: [c]\n");
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
                  "output_json: result.json\n"
                  "output_md: result.md\n"
                  "emit_formal_analysis_report: true\n"
                  "formal_analysis_report_path: formal_report.md\n"
                  "formal_analysis_report_date: 20260420\n"
                  "formal_analysis_sample_trades: 12\n");

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' --dry-run >'" +
                                EscapePathForShell(log_file) + "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("backtest_analysis_report.py"), std::string::npos);
    EXPECT_NE(payload.find("--run-dir"), std::string::npos);
    EXPECT_NE(payload.find("formal_report.md"), std::string::npos);
    EXPECT_NE(payload.find("--report-date"), std::string::npos);
    EXPECT_NE(payload.find("20260420"), std::string::npos);
    EXPECT_NE(payload.find("--sample-trades"), std::string::npos);
    EXPECT_NE(payload.find("12"), std::string::npos);
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
                  (root / "result.md").string() + "\n");

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' >'" +
                                EscapePathForShell(log_file) + "' 2>&1";
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
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path, "build_dir: " + build_dir.string() +
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

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
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
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path,
              "build_dir: " + build_dir.string() +
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
                  "indicator_trace_path: /tmp/templates/custom_trace.csv\n"
                  "emit_sub_strategy_indicator_trace: true\n"
                  "sub_strategy_indicator_trace_path: /tmp/templates/custom_sub_trace.csv\n");

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
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
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--export_csv_dir")).parent_path(),
              run_dir);
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--indicator_trace_path")).parent_path(),
              run_dir);
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--sub_strategy_indicator_trace_path"))
                  .parent_path(),
              run_dir);
    EXPECT_EQ(output_json_path.filename(), "custom_result.json");
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--output_md")).filename(),
              "custom_result.md");
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--export_csv_dir")).filename(),
              "custom_csv");
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--indicator_trace_path")).filename(),
              "custom_trace.csv");
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--sub_strategy_indicator_trace_path"))
                  .filename(),
              "custom_sub_trace.csv");

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
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path, "build_dir: " + build_dir.string() +
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

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
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
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path, "build_dir: " + build_dir.string() +
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
                               "indicator_trace_path: /tmp/templates/main_trace.csv\n"
                               "emit_sub_strategy_indicator_trace: true\n"
                               "sub_strategy_indicator_trace_path: /tmp/templates/sub_trace.csv\n");

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
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
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--sub_strategy_indicator_trace_path"))
                  .parent_path(),
              run_dir);
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--indicator_trace_path")).filename(),
              "main_trace.csv");
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--sub_strategy_indicator_trace_path"))
                  .filename(),
              "sub_trace.csv");
}

TEST(RunBacktestFromConfigScriptTest, SkipBuildPassesDetectorConfigArgWhenConfigured) {
    const auto root = MakeTempDir("detector_passthrough");
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto detector_config = root / "detector.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto args_log = root / "captured_args.txt";
    const auto fake_cli = build_dir / "backtest_cli";

    std::filesystem::create_directories(build_dir);
    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main, "run_type: backtest\n");
    WriteFile(detector_config,
              "market_state_detector:\n"
              "  adx_period: 7\n"
              "  atr_period: 5\n");

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

    WriteFile(config_path, "build_dir: " + build_dir.string() +
                               "\n"
                               "engine_mode: parquet\n"
                               "dataset_root: " +
                               dataset_root.string() +
                               "\n"
                               "strategy_main_config_path: " +
                               strategy_main.string() +
                               "\n"
                               "output_json: /tmp/templates/detector_result.json\n"
                               "output_md: /tmp/templates/detector_result.md\n"
                               "detector_config: " +
                               detector_config.string() + "\n");

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' --skip-build";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::vector<std::string> args = ReadLines(args_log);
    ASSERT_NE(FindArgValue(args, "--detector_config"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--detector_config"), detector_config.string());
}

TEST(RunBacktestFromConfigScriptTest, SkipBuildDefaultsTraceOutputFormatToCsvAndCsvTracePaths) {
    const auto root = MakeTempDir("trace_output_format_default");
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
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path, "build_dir: " + build_dir.string() +
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
                               "emit_sub_strategy_indicator_trace: true\n");

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' --skip-build";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::vector<std::string> args = ReadLines(args_log);
    ASSERT_NE(FindArgValue(args, "--trace_output_format"), nullptr);
    ASSERT_NE(FindArgValue(args, "--indicator_trace_path"), nullptr);
    ASSERT_NE(FindArgValue(args, "--sub_strategy_indicator_trace_path"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--trace_output_format"), "csv");
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--indicator_trace_path")).filename(),
              "indicator_trace.csv");
    EXPECT_EQ(std::filesystem::path(*FindArgValue(args, "--sub_strategy_indicator_trace_path"))
                  .filename(),
              "sub_strategy_indicator_trace.csv");
}

TEST(RunBacktestFromConfigScriptTest, SkipBuildPassesTraceOutputFormatWhenConfigured) {
    const auto root = MakeTempDir("trace_output_format_passthrough");
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto args_log = root / "captured_args.txt";
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
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path, "build_dir: " + build_dir.string() +
                               "\n"
                               "engine_mode: parquet\n"
                               "dataset_root: " +
                               dataset_root.string() +
                               "\n"
                               "strategy_main_config_path: " +
                               strategy_main.string() +
                               "\n"
                               "output_json: /tmp/templates/trace_result.json\n"
                               "output_md: /tmp/templates/trace_result.md\n"
                               "trace_output_format: both\n");

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' --skip-build";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::vector<std::string> args = ReadLines(args_log);
    ASSERT_NE(FindArgValue(args, "--trace_output_format"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--trace_output_format"), "both");
}

TEST(RunBacktestFromConfigScriptTest, SkipBuildInvokesFormalAnalysisWhenEnabled) {
    const auto root = MakeTempDir("formal_report_passthrough");
    const auto build_dir = root / "build-gcc";
    const auto fake_bin_dir = root / "fake_bin";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto backtest_args_log = root / "backtest_args.txt";
    const auto python_args_log = root / "python_args.txt";
    const auto output_root_dir = root / "backtest_runs";
    const auto fake_cli = build_dir / "backtest_cli";
    const auto fake_python = fake_bin_dir / "python3";

    std::filesystem::create_directories(build_dir);
    std::filesystem::create_directories(fake_bin_dir);
    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main, "run_type: backtest\n");

    WriteFile(fake_cli,
              "#!/usr/bin/env bash\n"
              "set -euo pipefail\n"
              "args_file='" +
                  backtest_args_log.string() +
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

    WriteFile(fake_python,
              "#!/usr/bin/env bash\n"
              "set -euo pipefail\n"
              "args_file='" +
                  python_args_log.string() +
                  "'\n"
                  ": >\"${args_file}\"\n"
                  "output_path=''\n"
                  "previous=''\n"
                  "for arg in \"$@\"; do\n"
                  "  printf '%s\\n' \"${arg}\" >>\"${args_file}\"\n"
                  "  if [[ \"${previous}\" == '--output' ]]; then\n"
                  "    output_path=\"${arg}\"\n"
                  "  fi\n"
                  "  previous=\"${arg}\"\n"
                  "done\n"
                  "if [[ -n \"${output_path}\" ]]; then\n"
                  "  mkdir -p \"$(dirname \"${output_path}\")\"\n"
                  "  printf '# formal report\\n' >\"${output_path}\"\n"
                  "fi\n");
    std::filesystem::permissions(
        fake_python,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path,
              "build_dir: " + build_dir.string() +
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
                  "output_json: result.json\n"
                  "output_md: result.md\n"
                  "run_id: formal-report-run\n"
                  "emit_formal_analysis_report: true\n"
                  "formal_analysis_report_path: formal_report.md\n"
                  "formal_analysis_report_date: 20260420\n"
                  "formal_analysis_sample_trades: 12\n");

    const std::string command =
        "PATH='" + EscapePathForShell(fake_bin_dir) + "':\"$PATH\" bash "
        "scripts/build/run_backtest_from_config.sh --config '" +
        EscapePathForShell(config_path) + "' --skip-build";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::vector<std::string> backtest_args = ReadLines(backtest_args_log);
    ASSERT_NE(FindArgValue(backtest_args, "--output_json"), nullptr);
    const std::filesystem::path run_dir =
        std::filesystem::path(*FindArgValue(backtest_args, "--output_json")).parent_path();

    const std::vector<std::string> python_args = ReadLines(python_args_log);
    ASSERT_FALSE(python_args.empty());
    EXPECT_EQ(std::filesystem::path(python_args.front()).filename(), "backtest_analysis_report.py");
    ASSERT_NE(FindArgValue(python_args, "--run-dir"), nullptr);
    EXPECT_EQ(*FindArgValue(python_args, "--run-dir"), run_dir.string());
    ASSERT_NE(FindArgValue(python_args, "--output"), nullptr);
    EXPECT_EQ(*FindArgValue(python_args, "--output"), (run_dir / "formal_report.md").string());
    ASSERT_NE(FindArgValue(python_args, "--report-date"), nullptr);
    EXPECT_EQ(*FindArgValue(python_args, "--report-date"), "20260420");
    ASSERT_NE(FindArgValue(python_args, "--sample-trades"), nullptr);
    EXPECT_EQ(*FindArgValue(python_args, "--sample-trades"), "12");
    EXPECT_TRUE(std::filesystem::exists(run_dir / "formal_report.md"));
}

TEST(RunBacktestFromConfigScriptTest, ValidationReportReadsCsvTraceWithoutPyarrow) {
    const auto root = MakeTempDir("validation_csv_trace");
    const auto run_dir = root / "run";
    const auto trace_path = run_dir / "sub_strategy_indicator_trace.csv";
    const auto report_path = run_dir / "validation_report.md";
    const auto json_path = run_dir / "backtest_auto.json";
    const auto log_path = run_dir / "validation.log";

    std::filesystem::create_directories(run_dir);
    WriteFile(trace_path,
              "instrument_id,ts_ns,dt_utc,timeframe_minutes,strategy_id,strategy_type,bar_open,"
              "bar_high,bar_low,bar_close,bar_volume,kama,atr,adx,er,stop_loss_price,"
              "take_profit_price,market_regime\n"
              "rb2405,1704186000000000000,2024-01-02 09:00:00,1,open_1,TrendStrategy,100,101,99,"
              "100.5,10,100.1,1.2,25.0,0.5,95,110,kWeakTrend\n"
              "rb2405,1704186060000000000,2024-01-02 09:01:00,1,open_1,TrendStrategy,101,102,100,"
              "101.5,11,100.4,1.3,26.0,0.6,95,110,kWeakTrend\n");
    WriteFile(json_path,
              "{\n"
              "  \"run_id\": \"validation-csv-trace\",\n"
              "  \"engine_mode\": \"parquet\",\n"
              "  \"initial_equity\": 1000000,\n"
              "  \"final_equity\": 1000010,\n"
              "  \"spec\": {\"strategy_factory\": \"composite\", \"symbols\": [\"rb2405\"]},\n"
              "  \"replay\": {\"ticks_read\": 10, \"bars_emitted\": 2},\n"
              "  \"deterministic\": {\n"
              "    \"intents_processed\": 1,\n"
              "    \"order_events_emitted\": 1,\n"
              "    \"invariant_violations\": [],\n"
              "    \"performance\": {\n"
              "      \"total_commission\": 1,\n"
              "      \"total_realized_pnl\": 12,\n"
              "      \"total_unrealized_pnl\": -1,\n"
              "      \"total_pnl_after_cost\": 10,\n"
              "      \"margin_clipped_orders\": 0,\n"
              "      \"margin_rejected_orders\": 0\n"
              "    },\n"
              "    \"trades\": [\n"
              "      {\"commission\": 1, \"signal_type\": \"kOpen\"}\n"
              "    ],\n"
              "    \"orders\": [\n"
              "      {\"order_id\": \"order-1\"}\n"
              "    ]\n"
              "  },\n"
              "  \"sub_strategy_indicator_trace\": {\n"
              "    \"enabled\": true,\n"
              "    \"path\": \"" +
                  trace_path.string() +
                  "\",\n"
                  "    \"rows\": 2\n"
                  "  }\n"
                  "}\n");

    const std::string command =
        "python3 scripts/analysis/backtest_validation_report.py --run-dir '" +
        EscapePathForShell(run_dir) + "' --output '" + EscapePathForShell(report_path) +
        "' --strict >'" + EscapePathForShell(log_path) + "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const std::string report = ReadFile(report_path);
    EXPECT_NE(report.find("[PASS] 子策略追踪行数一致"), std::string::npos);
    EXPECT_NE(report.find("- rows: 2"), std::string::npos);
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
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path, "build_dir: " + build_dir.string() +
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

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' --skip-build >'" +
                                EscapePathForShell(log_file) + "' 2>&1";
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

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' --dry-run >'" +
                                EscapePathForShell(log_file) + "' 2>&1";
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

TEST(RunBacktestFromConfigScriptTest, InvalidDetectorConfigPathFailsFast) {
    const auto root = MakeTempDir("invalid_detector_config");
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto config_path = root / "backtest_run.yaml";
    const auto output_json = root / "result" / "result.json";
    const auto output_md = root / "result" / "result.md";
    const auto log_file = root / "invalid_detector.log";
    const auto fake_cli = build_dir / "backtest_cli";
    const auto detector_config = root / "missing_detector.yaml";

    std::filesystem::create_directories(build_dir);
    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main, "run_type: backtest\n");
    WriteFile(fake_cli, "#!/usr/bin/env bash\nset -euo pipefail\n");
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path, "build_dir: " + build_dir.string() +
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
                               "detector_config: " +
                               detector_config.string() + "\n");

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' --skip-build >'" +
                                EscapePathForShell(log_file) + "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_NE(rc, 0);

    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("detector_config does not exist"), std::string::npos);
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
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path, "build_dir: " + build_dir.string() +
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

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' --skip-build >'" +
                                EscapePathForShell(log_file) + "' 2>&1";
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
    std::filesystem::permissions(
        fake_cli,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);

    WriteFile(config_path, "build_dir: " + build_dir.string() +
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

    const std::string command = "bash scripts/build/run_backtest_from_config.sh --config '" +
                                EscapePathForShell(config_path) + "' --skip-build >'" +
                                EscapePathForShell(log_file) + "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_NE(rc, 0);

    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("fatal: replay failed at row 12"), std::string::npos);
}

}  // namespace
