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
        std::filesystem::temp_directory_path() / ("quant_hft_simnow_supervisor_test_" + suffix);
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

std::string RunSupervisorDryRun(const std::string& suffix, const std::string& fake_now) {
    const auto temp_root = MakeTempDir(suffix);
    const auto env_file = temp_root / "simnow.env";
    const auto output_file = temp_root / "dry_run.out";
    WriteFile(env_file, "# test env intentionally empty\n");

    const std::string command =
        "SIMNOW_FAKE_NOW='" + EscapeForShell(fake_now) + "' " + "SIMNOW_LOCK_DIR='" +
        EscapePathForShell(temp_root / "locks") + "' " +
        "bash scripts/ops/supervise_simnow_trading.sh " + "--env-file '" +
        EscapePathForShell(env_file) + "' " + "--run-root '" +
        EscapePathForShell(temp_root / "runs") + "' " + "--market-data-dir '" +
        EscapePathForShell(temp_root / "market") + "' " + "--wal-file '" +
        EscapePathForShell(temp_root / "wal" / "events.wal") + "' " + "--report-root '" +
        EscapePathForShell(temp_root / "reports") + "' " + "--export-root '" +
        EscapePathForShell(temp_root / "exports") + "' " + "--reconcile-root '" +
        EscapePathForShell(temp_root / "reconcile") + "' " +
        "--windows 'night=20:50-02:35,day_am=08:50-11:35,day_pm=13:20-15:20' " +
        "--no-eod --dry-run > '" + EscapePathForShell(output_file) + "' 2>&1";

    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0) << ReadFile(output_file);
    return ReadFile(output_file);
}

TEST(SimnowSupervisorScriptTest, FridayNightMapsToNextAllowedTradingDay) {
    const std::string output = RunSupervisorDryRun("friday_night", "2026-05-15 21:01:00");

    EXPECT_NE(
        output.find("[dry-run] decision=start_or_keep_alive session=night trading_day=20260518 "
                    "range=20:50-02:35"),
        std::string::npos)
        << output;
}

TEST(SimnowSupervisorScriptTest, SaturdayEarlyMorningContinuesFridayNightSession) {
    const std::string output = RunSupervisorDryRun("saturday_early", "2026-05-16 01:01:00");

    EXPECT_NE(
        output.find("[dry-run] decision=start_or_keep_alive session=night trading_day=20260518 "
                    "range=20:50-02:35"),
        std::string::npos)
        << output;
}

TEST(SimnowSupervisorScriptTest, WeekendNightDoesNotStartANewSession) {
    const std::string output = RunSupervisorDryRun("weekend_night", "2026-05-16 21:01:00");

    EXPECT_NE(output.find("[dry-run] decision=outside_trading_window"), std::string::npos)
        << output;
}

TEST(SimnowSupervisorScriptTest, SignalMonitorHeartbeatIsSessionAwareWhenCoreIsStopped) {
    const auto temp_root = MakeTempDir("monitor_stopped");
    const auto csv_file = temp_root / "market" / "trading_day=20260710" / "varieties" / "rb" /
                          "strategy" / "kama_5m.csv";
    const auto output_file = temp_root / "monitor.out";
    const auto heartbeat_file = temp_root / "monitor" / "heartbeat.json";
    WriteFile(csv_file,
              "minute,instrument,sub_strategy_id,raw_signal,blocked_reason,ts_ns\n"
              "20260710 09:25,rb2405,kama_candidate_rb,buy,none,1783646700000000000\n");

    const std::string command =
        "bash scripts/ops/monitor_simnow_signal_execution.sh "
        "--run-root '" +
        EscapePathForShell(temp_root / "runs") + "' --market-data-dir '" +
        EscapePathForShell(temp_root / "market") + "' --wal-file '" +
        EscapePathForShell(temp_root / "wal" / "events.wal") + "' --monitor-root '" +
        EscapePathForShell(temp_root / "monitor") + "' --heartbeat-file '" +
        EscapePathForShell(heartbeat_file) +
        "' --signal-to-order-timeout 0 --status-interval-seconds 0 --replay-existing "
        "--once > '" +
        EscapePathForShell(output_file) + "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output_file);
    const std::string heartbeat = ReadFile(heartbeat_file);
    EXPECT_NE(heartbeat.find("\"core_state\":\"stopped\""), std::string::npos) << heartbeat;
    EXPECT_NE(heartbeat.find("\"session_key\":\"none\""), std::string::npos) << heartbeat;
    EXPECT_TRUE(std::filesystem::is_empty(temp_root / "monitor" / "incidents"));
}

TEST(SimnowSupervisorScriptTest, SettlementDryRunDoesNotFabricateEvidence) {
    const auto temp_root = MakeTempDir("settlement_dry_run");
    const auto output_file = temp_root / "settlement.out";
    const auto evidence_file = temp_root / "evidence.json";
    const auto diff_file = temp_root / "diff.json";
    const std::string command =
        "bash scripts/ops/run_daily_settlement.sh --trading-day 20260710 "
        "--settlement-bin '/not/used/in/dry-run' --evidence-json '" +
        EscapePathForShell(evidence_file) + "' --diff-json '" + EscapePathForShell(diff_file) +
        "' > '" + EscapePathForShell(output_file) + "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output_file);
    EXPECT_FALSE(std::filesystem::exists(evidence_file));
    EXPECT_FALSE(std::filesystem::exists(diff_file));
    EXPECT_NE(ReadFile(output_file).find("--strict-order-trade-backfill"), std::string::npos);
}

TEST(SimnowSupervisorScriptTest, IndependentSignalMonitorUnitRestartsAlways) {
    const std::string unit = ReadFile("infra/systemd/quant-hft-simnow-signal-monitor.service");
    EXPECT_NE(unit.find("Restart=always"), std::string::npos) << unit;
    EXPECT_NE(unit.find("monitor_simnow_signal_execution.sh"), std::string::npos) << unit;
    EXPECT_NE(unit.find("--heartbeat-file"), std::string::npos) << unit;
}

}  // namespace
