#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <sys/wait.h>
#include <vector>

namespace {

int RunCommand(const std::string& command) {
    const int status = std::system(command.c_str());
    if (status == -1) {
        return -1;
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status)) {
        return 128 + WTERMSIG(status);
    }
    return status;
}

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto path =
        std::filesystem::temp_directory_path() /
        ("quant_hft_run_backtest_with_validation_test_" + suffix);
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

void WriteExecutable(const std::filesystem::path& path, const std::string& payload) {
    WriteFile(path, payload);
    std::filesystem::permissions(
        path,
        std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
            std::filesystem::perms::others_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::group_read | std::filesystem::perms::others_read |
            std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace);
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

void WriteFakeBacktestCli(const std::filesystem::path& path, const std::filesystem::path& args_log) {
    WriteExecutable(
        path,
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "args_file='" +
            args_log.string() +
            "'\n"
            ": >\"${args_file}\"\n"
            "output_json=''\n"
            "output_md=''\n"
            "export_csv_dir=''\n"
            "while [[ $# -gt 0 ]]; do\n"
            "  printf '%s\\n' \"$1\" >>\"${args_file}\"\n"
            "  case \"$1\" in\n"
            "    --output_json)\n"
            "      output_json=\"$2\"\n"
            "      shift 2\n"
            "      ;;\n"
            "    --output_md)\n"
            "      output_md=\"$2\"\n"
            "      shift 2\n"
            "      ;;\n"
            "    --export_csv_dir)\n"
            "      export_csv_dir=\"$2\"\n"
            "      shift 2\n"
            "      ;;\n"
            "    *)\n"
            "      shift\n"
            "      ;;\n"
            "  esac\n"
            "done\n"
            "mkdir -p \"$(dirname \"${output_json}\")\" \"$(dirname \"${output_md}\")\" \"${export_csv_dir}\"\n"
            "printf '{}' >\"${output_json}\"\n"
            "printf '# backtest\\n' >\"${output_md}\"\n"
            "printf 'date,capital,daily_return_pct,cumulative_return_pct,drawdown_pct,position_value,trades_count,turnover,market_regime\\n' >\"${export_csv_dir}/daily_equity.csv\"\n"
            "printf 'fill_seq,trade_id,order_id,symbol,side,offset,volume,price,timestamp_ns,timestamp_dt_local,commission,slippage,realized_pnl,strategy_id,signal_type,regime_at_entry,risk_budget_r\\n' >\"${export_csv_dir}/trades.csv\"\n"
            "printf 'order_id,status,created_at_ns,last_update_ns\\n' >\"${export_csv_dir}/orders.csv\"\n"
            "printf 'timestamp_ns,symbol,net_position,avg_price,unrealized_pnl\\n' >\"${export_csv_dir}/position_history.csv\"\n");
}

void WriteFakePython(const std::filesystem::path& path, const std::filesystem::path& args_log) {
    WriteExecutable(
        path,
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "args_file='" +
            args_log.string() +
            "'\n"
            "script_path=\"$1\"\n"
            "shift\n"
            "printf '%s\\n' \"$(basename \"${script_path}\")\" >>\"${args_file}\"\n"
            "run_dir=''\n"
            "output=''\n"
            "while [[ $# -gt 0 ]]; do\n"
            "  printf '%s\\n' \"$1\" >>\"${args_file}\"\n"
            "  case \"$1\" in\n"
            "    --run-dir)\n"
            "      run_dir=\"$2\"\n"
            "      printf '%s\\n' \"$2\" >>\"${args_file}\"\n"
            "      shift 2\n"
            "      ;;\n"
            "    --output)\n"
            "      output=\"$2\"\n"
            "      printf '%s\\n' \"$2\" >>\"${args_file}\"\n"
            "      shift 2\n"
            "      ;;\n"
            "    *)\n"
            "      shift\n"
            "      ;;\n"
            "  esac\n"
            "done\n"
            "case \"$(basename \"${script_path}\")\" in\n"
            "  backtest_analysis_report.py)\n"
            "    if [[ -z \"${output}\" ]]; then\n"
            "      output=\"${run_dir}/strategy_回测分析报告_unit.md\"\n"
            "    fi\n"
            "    mkdir -p \"$(dirname \"${output}\")\"\n"
            "    printf 'formal report\\n' >\"${output}\"\n"
            "    printf '[report] 生成完成: %s\\n' \"${output}\"\n"
            "    ;;\n"
            "  backtest_validation_report.py)\n"
            "    if [[ -z \"${output}\" ]]; then\n"
            "      output=\"${run_dir}/validation_report.md\"\n"
            "    fi\n"
            "    mkdir -p \"$(dirname \"${output}\")\"\n"
            "    printf 'validation report\\n' >\"${output}\"\n"
            "    printf '[report] 生成完成: %s\\n' \"${output}\"\n"
            "    printf '[report] 检查结果: PASS\\n'\n"
            "    ;;\n"
            "  *)\n"
            "    printf 'unexpected python entry: %s\\n' \"${script_path}\" >&2\n"
            "    exit 2\n"
            "    ;;\n"
            "esac\n");
}

std::filesystem::path WriteConfig(const std::filesystem::path& root,
                                  const std::filesystem::path& build_dir,
                                  const std::filesystem::path& dataset_root,
                                  const std::filesystem::path& strategy_main,
                                  const std::filesystem::path& output_root_dir) {
    const auto config_path = root / "backtest_run.yaml";
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
                  "output_json: backtest_auto.json\n"
                  "output_md: backtest_auto.md\n"
                  "emit_indicator_trace: true\n"
                  "indicator_trace_path: my_trace.csv\n"
                  "emit_sub_strategy_indicator_trace: true\n"
                  "sub_strategy_indicator_trace_path: my_sub_trace.csv\n"
                  "emit_formal_analysis_report: true\n"
                  "formal_analysis_report_path: strategy_回测分析报告_unit.md\n");
    return config_path;
}

TEST(RunBacktestWithValidationScriptTest, FastModeDisablesHeavyOutputsAndSkipsFormalReport) {
    const auto root = MakeTempDir("fast_mode");
    const auto fake_bin_dir = root / "fake-bin";
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto output_root_dir = root / "backtest_runs";
    const auto args_log = root / "backtest_cli_args.txt";
    const auto python_log = root / "python_args.txt";
    const auto log_file = root / "run.log";

    std::filesystem::create_directories(fake_bin_dir);
    std::filesystem::create_directories(build_dir);
    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main, "run_type: backtest\n");
    WriteFakeBacktestCli(build_dir / "backtest_cli", args_log);
    WriteFakePython(fake_bin_dir / "python3", python_log);
    const auto config_path = WriteConfig(root, build_dir, dataset_root, strategy_main, output_root_dir);

    const std::string command =
        "PATH='" + EscapePathForShell(fake_bin_dir) +
        "':\"$PATH\" /bin/bash scripts/build/run_backtest_with_validation.sh --config '" +
        EscapePathForShell(config_path) + "' --fast --fast-start-date 20240201 --fast-end-date 20240205 "
        "--fast-max-ticks 321 --skip-build >'" + EscapePathForShell(log_file) + "' 2>&1";
    const int rc = RunCommand(command);
    ASSERT_EQ(rc, 0) << ReadFile(log_file);

    const std::string payload = ReadFile(log_file);
    EXPECT_NE(payload.find("fast 模式已启用"), std::string::npos);
    EXPECT_NE(payload.find("validation_report.md"), std::string::npos);
    EXPECT_EQ(payload.find("正式分析报告:"), std::string::npos);

    const std::vector<std::string> args = ReadLines(args_log);
    ASSERT_NE(FindArgValue(args, "--start_date"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--start_date"), "20240201");
    ASSERT_NE(FindArgValue(args, "--end_date"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--end_date"), "20240205");
    ASSERT_NE(FindArgValue(args, "--max_ticks"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--max_ticks"), "321");
    EXPECT_EQ(FindArgValue(args, "--emit_indicator_trace"), nullptr);
    EXPECT_EQ(FindArgValue(args, "--emit_sub_strategy_indicator_trace"), nullptr);

    const std::string python_invocations = ReadFile(python_log);
    EXPECT_NE(python_invocations.find("backtest_validation_report.py"), std::string::npos);
    EXPECT_EQ(python_invocations.find("backtest_analysis_report.py"), std::string::npos);
}

TEST(RunBacktestWithValidationScriptTest, DefaultModeKeepsHeavyOutputsAndPublishesFormalReport) {
    const auto root = MakeTempDir("default_mode");
    const auto fake_bin_dir = root / "fake-bin";
    const auto build_dir = root / "build-gcc";
    const auto dataset_root = root / "parquet_v2";
    const auto strategy_main = root / "main_backtest_strategy.yaml";
    const auto output_root_dir = root / "backtest_runs";
    const auto args_log = root / "backtest_cli_args.txt";
    const auto python_log = root / "python_args.txt";
    const auto log_file = root / "run.log";

    std::filesystem::create_directories(fake_bin_dir);
    std::filesystem::create_directories(build_dir);
    std::filesystem::create_directories(dataset_root);
    WriteFile(strategy_main, "run_type: backtest\n");
    WriteFakeBacktestCli(build_dir / "backtest_cli", args_log);
    WriteFakePython(fake_bin_dir / "python3", python_log);
    const auto config_path = WriteConfig(root, build_dir, dataset_root, strategy_main, output_root_dir);

    const std::string command =
        "PATH='" + EscapePathForShell(fake_bin_dir) +
        "':\"$PATH\" /bin/bash scripts/build/run_backtest_with_validation.sh --config '" +
        EscapePathForShell(config_path) + "' --skip-build >'" + EscapePathForShell(log_file) + "' 2>&1";
    const int rc = RunCommand(command);
    ASSERT_EQ(rc, 0) << ReadFile(log_file);

    const std::string payload = ReadFile(log_file);
    EXPECT_EQ(payload.find("fast 模式已启用"), std::string::npos);
    EXPECT_NE(payload.find("正式分析报告:"), std::string::npos);
    EXPECT_NE(payload.find("strategy_回测分析报告_unit.md"), std::string::npos);

    const std::vector<std::string> args = ReadLines(args_log);
    ASSERT_NE(FindArgValue(args, "--emit_indicator_trace"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--emit_indicator_trace"), "true");
    ASSERT_NE(FindArgValue(args, "--emit_sub_strategy_indicator_trace"), nullptr);
    EXPECT_EQ(*FindArgValue(args, "--emit_sub_strategy_indicator_trace"), "true");

    const std::string python_invocations = ReadFile(python_log);
    EXPECT_NE(python_invocations.find("backtest_validation_report.py"), std::string::npos);
    EXPECT_NE(python_invocations.find("backtest_analysis_report.py"), std::string::npos);
}

}  // namespace