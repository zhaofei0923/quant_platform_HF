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
    std::filesystem::path fake_bin;
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
    paths.fake_bin = paths.root / "fake-bin";

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
              "SIMNOW_TRADING_WINDOWS=night=21:00-02:35,day_am=09:00-11:35,day_pm=13:30-15:20\n"
              "SIMNOW_PROBE_SECONDS=1\n"
              "SIMNOW_PROBE_TIMEOUT_SECONDS=2\n"
              "SIMNOW_INSTRUMENT_TIMEOUT_SECONDS=1\n"
              "SIMNOW_INSTRUMENT_CACHE_ROOT=" +
                  paths.root.string() + "/instrument-cache\n");
    WriteFile(paths.config_file,
              "ctp:\n"
              "  product_ids: \"c,hc\"\n");
    WriteFile(paths.build_dir / "CMakeCache.txt",
              "QUANT_HFT_BUILD_TESTS:BOOL=ON\n"
              "QUANT_HFT_ENABLE_CTP_REAL_API:BOOL=ON\n");
    WriteExecutable(paths.build_dir / "core_engine");
    WriteExecutable(paths.build_dir / "simnow_probe");
    WriteExecutable(paths.build_dir / "simnow_contract_universe_refresh");
    WriteExecutable(paths.fake_bin / "ps");
    return paths;
}

std::string IsolatedPathPrefix(const FixturePaths& paths) {
    return "PATH='" + EscapePathForShell(paths.fake_bin) + "':\"${PATH}\" ";
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

    const std::string command = IsolatedPathPrefix(paths) +
                                "bash scripts/ops/run_simnow_preflight_check.sh --phase prestart" +
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

TEST(SimnowPreflightScriptTest, PrestartProbeUsesPreopenConnectivityOnlyMode) {
    const auto paths = MakeFixture("preopen_probe");
    const auto output_file = paths.root / "prestart.out";
    const auto args_file = paths.root / "probe.args";
    WriteFile(paths.build_dir / "simnow_probe",
              "#!/usr/bin/env bash\n"
              "printf '%s\\n' \"$*\" > '" +
                  EscapePathForShell(args_file) +
                  "'\n"
                  "echo 'ts_ns=1 level=info app=simnow_probe "
                  "event=dominant_contract_selection_deferred product_id=\"c\" "
                  "reason=\"outside_session\"'\n"
                  "echo 'ts_ns=2 level=info app=simnow_probe "
                  "event=dominant_contract_probe_summary mode=\"preopen_connectivity_only\" "
                  "selected_count=\"0\" deferred_count=\"2\" product_count=\"2\"'\n"
                  "echo 'ts_ns=3 level=info app=simnow_probe event=health_status "
                  "state=\"healthy\"'\n"
                  "echo 'ts_ns=4 level=info app=simnow_probe event=probe_completed'\n");
    std::filesystem::permissions(
        paths.build_dir / "simnow_probe",
        std::filesystem::perms::owner_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::owner_write | std::filesystem::perms::group_exec |
            std::filesystem::perms::group_read | std::filesystem::perms::others_exec |
            std::filesystem::perms::others_read,
        std::filesystem::perm_options::replace);

    const std::string command = IsolatedPathPrefix(paths) +
                                "bash scripts/ops/run_simnow_preflight_check.sh --phase prestart" +
                                CommonArgs(paths) + " --skip-build --skip-gates --skip-tests > '" +
                                EscapePathForShell(output_file) + "' 2>&1";
    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output_file);
    EXPECT_NE(ReadFile(args_file).find("--preopen-connectivity-only"), std::string::npos);
    EXPECT_NE(ReadFile(output_file).find("[ok] prestart checks passed"), std::string::npos);
}

TEST(SimnowPreflightScriptTest, ConnectivityOnlyRequiresProbeOnly) {
    const auto paths = MakeFixture("connectivity_requires_probe_only");
    const auto output_file = paths.root / "start.out";
    const std::string command =
        "bash scripts/ops/start_simnow_trading.sh --preopen-connectivity-only --dry-run > '" +
        EscapePathForShell(output_file) + "' 2>&1";

    EXPECT_NE(RunCommand(command), 0);
    EXPECT_NE(ReadFile(output_file).find("requires --probe-only"), std::string::npos);
}

TEST(SimnowPreflightScriptTest, StrictProbeDryRunDoesNotUseConnectivityOnlyMode) {
    const auto paths = MakeFixture("strict_probe");
    const auto output_file = paths.root / "start.out";
    const std::string command =
        IsolatedPathPrefix(paths) + "bash scripts/ops/start_simnow_trading.sh --env-file '" +
        EscapePathForShell(paths.env_file) + "' --config '" +
        EscapePathForShell(paths.config_file) + "' --build-dir '" +
        EscapePathForShell(paths.build_dir) + "' --run-root '" +
        EscapePathForShell(paths.run_root) + "' --wal-file '" + EscapePathForShell(paths.wal_file) +
        "' --run-id strict-probe --probe-only --dry-run > '" + EscapePathForShell(output_file) +
        "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output_file);
    const std::string output = ReadFile(output_file);
    EXPECT_NE(output.find("[dry-run] contract refresh:"), std::string::npos) << output;
    const auto probe_line = output.find("[dry-run] probe:");
    ASSERT_NE(probe_line, std::string::npos) << output;
    EXPECT_EQ(output.find("--preopen-connectivity-only", probe_line), std::string::npos) << output;
}

TEST(SimnowPreflightScriptTest, ExistingManifestSkipsStartupRefresh) {
    const auto paths = MakeFixture("existing_manifest");
    const auto output_file = paths.root / "start.out";
    const auto refresh_marker = paths.root / "refresh-called";
    WriteFile(paths.root / "instrument-cache" / "contract_universe_manifest.json", "{}\n");
    WriteFile(paths.build_dir / "simnow_contract_universe_refresh",
              "#!/usr/bin/env bash\n"
              "touch '" +
                  EscapePathForShell(refresh_marker) +
                  "'\n"
                  "exit 99\n");
    std::filesystem::permissions(
        paths.build_dir / "simnow_contract_universe_refresh",
        std::filesystem::perms::owner_exec | std::filesystem::perms::owner_read |
            std::filesystem::perms::owner_write | std::filesystem::perms::group_exec |
            std::filesystem::perms::group_read | std::filesystem::perms::others_exec |
            std::filesystem::perms::others_read,
        std::filesystem::perm_options::replace);

    const std::string command =
        IsolatedPathPrefix(paths) + "bash scripts/ops/start_simnow_trading.sh --env-file '" +
        EscapePathForShell(paths.env_file) + "' --config '" +
        EscapePathForShell(paths.config_file) + "' --build-dir '" +
        EscapePathForShell(paths.build_dir) + "' --run-root '" +
        EscapePathForShell(paths.run_root) + "' --wal-file '" + EscapePathForShell(paths.wal_file) +
        "' --run-id existing-manifest --probe-only > '" + EscapePathForShell(output_file) +
        "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output_file);
    EXPECT_FALSE(std::filesystem::exists(refresh_marker));
    EXPECT_NE(ReadFile(output_file).find("strict probe will validate Broker trading day"),
              std::string::npos);
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
