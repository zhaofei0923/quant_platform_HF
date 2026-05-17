#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace {

int RunCommand(const std::string& command) { return std::system(command.c_str()); }

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto path =
        std::filesystem::temp_directory_path() / ("quant_hft_simnow_preflight_test_" + suffix);
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path;
}

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

void WriteFile(const std::filesystem::path& path, const std::string& payload) {
    if (!path.parent_path().empty()) {
        std::filesystem::create_directories(path.parent_path());
    }
    std::ofstream out(path);
    out << payload;
}

void WriteExecutable(const std::filesystem::path& path) {
    WriteFile(path, "#!/usr/bin/env bash\nexit 0\n");
    std::filesystem::permissions(
        path,
        std::filesystem::perms::owner_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::owner_write | std::filesystem::perms::group_exec |
            std::filesystem::perms::group_read | std::filesystem::perms::others_exec |
            std::filesystem::perms::others_read,
        std::filesystem::perm_options::replace);
}

std::string EscapeForShell(const std::string& text) {
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

std::string EscapePathForShell(const std::filesystem::path& path) {
    return EscapeForShell(path.string());
}

struct FixturePaths {
    std::filesystem::path root;
    std::filesystem::path env_file;
    std::filesystem::path config_file;
    std::filesystem::path build_dir;
    std::filesystem::path run_root;
    std::filesystem::path market_dir;
    std::filesystem::path wal_file;
    std::filesystem::path report_root;
    std::filesystem::path export_root;
    std::filesystem::path reconcile_root;
    std::filesystem::path output_root;
};

FixturePaths MakeFixture(const std::string& suffix) {
    FixturePaths paths;
    paths.root = MakeTempDir(suffix);
    paths.env_file = paths.root / "simnow.env";
    paths.config_file = paths.root / "ctp_sim_trade_candidates.yaml";
    paths.build_dir = paths.root / "build-gcc";
    paths.run_root = paths.root / "runs";
    paths.market_dir = paths.root / "market";
    paths.wal_file = paths.root / "wal" / "events.wal";
    paths.report_root = paths.root / "reports";
    paths.export_root = paths.root / "exports";
    paths.reconcile_root = paths.root / "reconcile";
    paths.output_root = paths.root / "preflight";

    WriteFile(paths.env_file,
              "CTP_SIM_IS_PRODUCTION_MODE=true\n"
              "CTP_SIM_ENABLE_REAL_API=true\n"
              "CTP_SIM_MARKET_FRONT=tcp://182.254.243.31:30011\n"
              "CTP_SIM_TRADER_FRONT=tcp://182.254.243.31:30001\n"
              "CTP_SIM_BROKER_ID=broker\n"
              "CTP_SIM_USER_ID=user\n"
              "CTP_SIM_INVESTOR_ID=investor\n"
              "CTP_SIM_PASSWORD=password\n"
              "CTP_SIM_AUTH_CODE=auth\n"
              "CTP_SIM_APP_ID=app\n"
              "SIMNOW_TRADING_WINDOWS=night=20:50-02:35,day_am=08:50-11:35,day_pm=13:20-15:20\n"
              "SIMNOW_PROBE_SECONDS=1\n"
              "SIMNOW_PROBE_TIMEOUT_SECONDS=2\n"
              "SIMNOW_INSTRUMENT_TIMEOUT_SECONDS=1\n");
    WriteFile(paths.config_file,
              "ctp:\n"
              "  product_ids: \"c,hc\"\n");
    WriteFile(paths.build_dir / "CMakeCache.txt",
              "QUANT_HFT_BUILD_TESTS:BOOL=ON\n"
              "QUANT_HFT_ENABLE_CTP_REAL_API:BOOL=ON\n");
    WriteExecutable(paths.build_dir / "core_engine");
    WriteExecutable(paths.build_dir / "simnow_probe");
    return paths;
}

std::string CommonArgs(const FixturePaths& paths) {
    return " --test-date 20260518 --env-file '" + EscapePathForShell(paths.env_file) +
           "' --config '" + EscapePathForShell(paths.config_file) + "' --build-dir '" +
           EscapePathForShell(paths.build_dir) + "' --run-root '" +
           EscapePathForShell(paths.run_root) + "' --market-data-dir '" +
           EscapePathForShell(paths.market_dir) + "' --wal-file '" +
           EscapePathForShell(paths.wal_file) + "' --report-root '" +
           EscapePathForShell(paths.report_root) + "' --export-root '" +
           EscapePathForShell(paths.export_root) + "' --reconcile-root '" +
           EscapePathForShell(paths.reconcile_root) + "' --output-root '" +
           EscapePathForShell(paths.output_root) + "' --allow-dirty";
}

TEST(SimnowPreflightScriptTest, PrestartLocalChecksPassWithoutRealProbe) {
    const auto paths = MakeFixture("prestart");
    const auto output_file = paths.root / "prestart.out";

    const std::string command = "bash scripts/ops/run_simnow_preflight_check.sh --phase prestart" +
                                CommonArgs(paths) +
                                " --skip-build --skip-gates --skip-tests --skip-probe > '" +
                                EscapePathForShell(output_file) + "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0) << ReadFile(output_file);

    const std::string output = ReadFile(output_file);
    EXPECT_NE(output.find("[ok] prestart checks passed"), std::string::npos) << output;
    EXPECT_NE(output.find("schedule_day_am"), std::string::npos) << output;
    EXPECT_NE(output.find("schedule_eod"), std::string::npos) << output;
    EXPECT_NE(output.find("probe_only"), std::string::npos) << output;
}

TEST(SimnowPreflightScriptTest, EodChecksRequireMarkerAndArtifacts) {
    const auto paths = MakeFixture("eod");
    const auto output_file = paths.root / "eod.out";
    const auto day_report_dir = paths.report_root / "20260518";
    WriteFile(paths.run_root / "eod" / "20260518.done", "done_at=2026-05-18T15:30:00+08:00\n");
    WriteFile(day_report_dir / "daily_settlement_evidence.json", "{}\n");
    WriteFile(day_report_dir / "settlement_diff.json", "{}\n");
    WriteFile(day_report_dir / "simnow_daily_report.md", "# report\n");
    WriteFile(day_report_dir / "ops_health_report.json", "{}\n");
    WriteFile(day_report_dir / "ops_alert_report.json", "{}\n");
    WriteFile(day_report_dir / "daily_settlement.log", "settlement ok\n");
    WriteFile(day_report_dir / "simnow_export.log", "export ok\n");

    const std::string command = "bash scripts/ops/run_simnow_preflight_check.sh --phase eod" +
                                CommonArgs(paths) + " > '" + EscapePathForShell(output_file) +
                                "' 2>&1";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0) << ReadFile(output_file);

    const std::string output = ReadFile(output_file);
    EXPECT_NE(output.find("[ok] eod checks passed"), std::string::npos) << output;
}

}  // namespace
