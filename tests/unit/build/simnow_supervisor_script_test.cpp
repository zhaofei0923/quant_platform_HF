#include <gtest/gtest.h>
#include <unistd.h>

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

std::string StandardSessionCalendar() {
    return "natural_date,session,trading_day,exchange,product\n"
           "# product_scope=hc:SHFE\n"
           "2026-05-15,night,2026-05-18,SHFE,hc\n"
           "2026-09-07,day_am,2026-09-07,SHFE,hc\n"
           "2026-09-07,day_pm,2026-09-07,SHFE,hc\n"
           "2026-09-07,night,2026-09-08,SHFE,hc\n";
}

struct SupervisorDryRunResult {
    int rc{0};
    std::string output;
};

SupervisorDryRunResult RunSupervisorDryRun(
    const std::string& suffix, const std::string& fake_now,
    const std::string& calendar_payload = StandardSessionCalendar(), bool pass_calendar = true,
    const std::string& windows =
        "night=21:00-23:05,day_am=09:00-11:35,day_pm=13:30-15:20") {
    const auto temp_root = MakeTempDir(suffix);
    const auto env_file = temp_root / "simnow.env";
    const auto calendar_file = temp_root / "session_calendar.csv";
    const auto output_file = temp_root / "dry_run.out";
    WriteFile(env_file, "# test env intentionally empty\n");
    WriteFile(calendar_file, calendar_payload);

    std::string command =
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
        "--windows '" + EscapeForShell(windows) + "' " +
        "--prewarm-windows "
        "'night=20:45-21:00,day_am=08:45-09:00,day_pm=13:25-13:30' ";
    if (pass_calendar) {
        command += "--session-calendar-file '" + EscapePathForShell(calendar_file) + "' ";
    }
    command += "--no-eod --dry-run > '" + EscapePathForShell(output_file) + "' 2>&1";

    const int rc = RunCommand(command);
    return SupervisorDryRunResult{rc, ReadFile(output_file)};
}

std::string MonitorCommand(const std::filesystem::path& temp_root, const std::string& fake_now,
                           bool strict = false, bool replay_existing = false) {
    const auto sessions = std::filesystem::current_path() / "configs" / "trading_sessions.yaml";
    std::string command =
        "QUANT_ROOT='" + EscapePathForShell(temp_root) + "' SIMNOW_MONITOR_FAKE_NOW='" +
        EscapeForShell(fake_now) + "' bash scripts/ops/monitor_simnow_signal_execution.sh " +
        "--run-root '" + EscapePathForShell(temp_root / "runs") + "' --market-data-dir '" +
        EscapePathForShell(temp_root / "market") + "' --wal-file '" +
        EscapePathForShell(temp_root / "wal" / "events.wal") + "' --monitor-root '" +
        EscapePathForShell(temp_root / "monitor") + "' --trading-sessions-config '" +
        EscapePathForShell(sessions) + "' --products c --status-interval-seconds 0 --once";
    if (strict) {
        command += " --strict-exit";
    }
    if (replay_existing) {
        command += " --replay-existing";
    }
    return command;
}

void WriteHealthyPipelineFixture(const std::filesystem::path& root) {
    WriteFile(root / "runtime" / "ctp_instruments" / "c_dominant_contract.json",
              "{\"product_id\":\"c\",\"current_instrument_id\":\"c2609\"," +
                  std::string("\"instrument_id\":\"c2609\",\"exchange_id\":\"DCE\",") +
                  "\"eligible_count\":1,\"baseline_count\":1}\n");
    WriteFile(root / "runs" / "current_core_engine.pid", std::to_string(getpid()) + "\n");
    WriteFile(root / "monitor" / "readiness.json",
              "{\"mode\":\"Ready\",\"generation\":3,\"recovery_complete\":true," +
                  std::string("\"trader_ready\":true,\"gateway_healthy\":true,") +
                  "\"settlement_confirmed\":true,\"pending_exit_count\":0," +
                  "\"unresolved_mapping_count\":0}\n");
    WriteFile(root / "market" / "trading_day=20260720" / "varieties" / "c" / "market" / "ticks.csv",
              "instrument_id,exchange_id,trading_day,action_day,update_time,update_millisec," +
                  std::string("last_price,bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,") +
                  "volume,open_interest,settlement_price,average_price_raw,average_price_norm," +
                  "is_valid_settlement,exchange_ts_ns,recv_ts_ns,average_price_norm_valid\n" +
                  "c2609,DCE,20260720,20260720,09:31:04,0,2285,2284,2285,10,11,100,1000," +
                  "0,22850,2285,0,0,0,1\n");
    const std::string bar_header =
        "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close," +
        std::string("analysis_open,analysis_high,analysis_low,analysis_close,") +
        "analysis_price_offset,volume,ts_ns,period_end_ts_ns,finalized_ts_ns," +
        "expected_source_bars,observed_source_bars,is_complete,is_session_endpoint," +
        "strategy_eligible,volume_complete,has_conflict,is_recovery_replay\n";
    WriteFile(
        root / "market" / "trading_day=20260720" / "varieties" / "c" / "market" / "bars_1m.csv",
        bar_header + "c2609,DCE,20260720,20260720,20260720 09:30,2284,2286,2283,2285," +
            "2284,2286,2283,2285,0,10,1,2,3,1,1,1,0,1,1,0,0\n");
    WriteFile(
        root / "market" / "trading_day=20260720" / "varieties" / "c" / "market" / "bars_5m.csv",
        bar_header + "c2609,DCE,20260720,20260720,20260720 09:25,2280,2286,2279,2285," +
            "2280,2286,2279,2285,0,50,1,2,3,5,5,1,0,1,1,0,0\n");
    WriteFile(
        root / "market" / "trading_day=20260720" / "varieties" / "c" / "strategy" / "kama_5m.csv",
        "minute,instrument,sub_strategy_id,raw_signal,blocked_reason,ts_ns\n"
        "20260720 09:25,c2609,kama_c,,no_raw_signal,1\n");
    WriteFile(root / "wal" / "events.wal", "");
}

void WriteAllowedTraceFixture(const std::filesystem::path& root, int contract_generation) {
    WriteHealthyPipelineFixture(root);
    constexpr const char* kEventTs = "1784510999000000000";
    const std::string bar_header =
        "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close," +
        std::string("analysis_open,analysis_high,analysis_low,analysis_close,") +
        "analysis_price_offset,volume,ts_ns,period_end_ts_ns,finalized_ts_ns," +
        "expected_source_bars,observed_source_bars,is_complete,is_session_endpoint," +
        "strategy_eligible,volume_complete,has_conflict,is_recovery_replay\n";
    WriteFile(root / "market" / "trading_day=20260720" / "varieties" / "c" / "market" / "ticks.csv",
              "instrument_id,exchange_id,trading_day,action_day,update_time,update_millisec," +
                  std::string("last_price,bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,") +
                  "volume,open_interest,settlement_price,average_price_raw,average_price_norm," +
                  "is_valid_settlement,exchange_ts_ns,recv_ts_ns,average_price_norm_valid\n" +
                  "c2609,DCE,20260720,20260720,09:31:04,0,2285,2284,2285,10,11,100,1000," +
                  "0,22850,2285,0,1784511064000000000,1784511064001000000,1\n");
    WriteFile(
        root / "market" / "trading_day=20260720" / "varieties" / "c" / "market" / "bars_1m.csv",
        bar_header + "c2609,DCE,20260720,20260720,20260720 09:30,2284,2286,2283,2285," +
            "2284,2286,2283,2285,0,10," + kEventTs +
            ",1784511000000000000,1784511004000000000,1,1,1,0,1,1,0,0\n");
    WriteFile(
        root / "market" / "trading_day=20260720" / "varieties" / "c" / "market" / "bars_5m.csv",
        bar_header + "c2609,DCE,20260720,20260720,20260720 09:25,2280,2286,2279,2285," +
            "2280,2286,2279,2285,0,50," + kEventTs +
            ",1784511000000000000,1784511004000000000,5,5,1,0,1,1,0,0\n");
    WriteFile(
        root / "market" / "trading_day=20260720" / "varieties" / "c" / "strategy" / "kama_5m.csv",
        "minute,instrument,sub_strategy_id,raw_signal,blocked_reason,ts_ns\n"
        "20260720 09:25,c2609,kama_c,buy,none," +
            std::string(kEventTs) + "\n");
    const auto core_log = root / "runs" / "simnow-test-run" / "core_engine.log";
    WriteFile(root / "runs" / "current_run_dir",
              (root / "runs" / "simnow-test-run").string() + "\n");
    WriteFile(root / "runs" / "current_core_engine_log", core_log.string() + "\n");
    const std::string trace = "kama_c-open-c2609-" + std::string(kEventTs);
    WriteFile(core_log,
              "ts_ns=1784511005000000000 level=info app=kama event=signal_candidate "
              "event_ts_ns=\"" +
                  std::string(kEventTs) + "\" trace_id=\"" + trace + "\"\n" +
                  "ts_ns=1784511006000000000 level=info app=composite event=strategy_decision "
                  "event_ts_ns=\"" +
                  kEventTs + "\" disposition=\"allowed\" trace_id=\"" + trace + "\"\n" +
                  "ts_ns=1784511007000000000 level=info app=core event=order_submitted "
                  "event_ts_ns=\"" +
                  kEventTs +
                  "\" instrument_id=\"c2609\" strategy_id=\"kama_c\" "
                  "side=\"buy\" client_order_id=\"order-1\" trace_id=\"" +
                  trace + "\"\n" +
                  "ts_ns=1784511008000000000 level=info app=ctp event=ctp_order_submitted "
                  "client_order_id=\"order-1\" order_ref=\"1\" request_id=\"1\"\n" +
                  "ts_ns=1784511007000000000 level=info app=core event=execution_disposition "
                  "disposition=\"ctp_submitted\" contract_generation=\"" +
                  std::to_string(contract_generation) + "\" trace_id=\"" + trace + "\"\n");
    WriteFile(root / "wal" / "events.wal",
              "{\"seq\":1,\"event_type\":\"order_update\",\"run_id\":\"simnow-test-run\"," +
                  std::string("\"ts_ns\":1784511009000000000,\"trading_day\":\"20260720\",") +
                  "\"account_id\":\"sim\",\"exchange_id\":\"DCE\",\"trace_id\":\"" + trace +
                  "\",\"client_order_id\":\"order-1\",\"status\":4,\"filled_volume\":0}\n");
}

TEST(SimnowSupervisorScriptTest, FridayNightUsesExplicitCalendarTradingDay) {
    const auto result = RunSupervisorDryRun("friday_night", "2026-05-15 21:01:00");

    ASSERT_EQ(result.rc, 0) << result.output;
    EXPECT_NE(
        result.output.find("[dry-run] decision=start_or_keep_alive session=night "
                           "natural_date=20260515 trading_day=20260518 range=21:00-23:05"),
        std::string::npos)
        << result.output;
}

TEST(SimnowSupervisorScriptTest, NightSessionStopsBeforeSaturdayEarlyMorning) {
    const auto result = RunSupervisorDryRun("saturday_early", "2026-05-16 01:01:00");

    ASSERT_EQ(result.rc, 0) << result.output;
    EXPECT_NE(result.output.find("[dry-run] decision=outside_trading_window"), std::string::npos)
        << result.output;
}

TEST(SimnowSupervisorScriptTest, LongNightSessionUsesPreviousNaturalDateAfterMidnight) {
    const auto result = RunSupervisorDryRun(
        "long_night_after_midnight", "2026-09-08 01:01:00", StandardSessionCalendar(), true,
        "night=21:00-02:35,day_am=09:00-11:35,day_pm=13:30-15:20");

    ASSERT_EQ(result.rc, 0) << result.output;
    EXPECT_NE(result.output.find("decision=start_or_keep_alive session=night "), std::string::npos)
        << result.output;
    EXPECT_NE(result.output.find("natural_date=20260907 trading_day=20260908 range=21:00-02:35"),
              std::string::npos)
        << result.output;
}

TEST(SimnowSupervisorScriptTest, CalendarOutOfCoverageFailsClosedForNewSession) {
    const auto result = RunSupervisorDryRun("weekend_night", "2026-05-16 21:01:00");

    EXPECT_NE(result.rc, 0) << result.output;
    EXPECT_NE(result.output.find("decision=fail_closed"), std::string::npos) << result.output;
    EXPECT_NE(result.output.find("reason=calendar_session_out_of_coverage"), std::string::npos)
        << result.output;
}

TEST(SimnowSupervisorScriptTest, PrewarmStartsEngineBeforeSessionGateAllowsOpening) {
    struct Case {
        const char* suffix;
        const char* fake_now;
        const char* decision;
        const char* range;
        bool starts_engine;
    };
    const Case cases[] = {
        {"day_prewarm", "2026-09-07 08:45:00",
         "decision=prewarm_start_or_keep_alive session=day_am", "range=08:45-09:00", true},
        {"day_start", "2026-09-07 09:00:00", "decision=start_or_keep_alive session=day_am",
         "range=09:00-11:35", true},
        {"day_stop", "2026-09-07 11:35:00", "decision=outside_trading_window", "", false},
        {"pm_prewarm", "2026-09-07 13:25:00",
         "decision=prewarm_start_or_keep_alive session=day_pm", "range=13:25-13:30", true},
        {"pm_start", "2026-09-07 13:30:00", "decision=start_or_keep_alive session=day_pm",
         "range=13:30-15:20", true},
        {"pm_stop", "2026-09-07 15:20:00", "decision=outside_trading_window", "", false},
        {"night_prewarm", "2026-09-07 20:45:00",
         "decision=prewarm_start_or_keep_alive session=night", "range=20:45-21:00", true},
        {"night_start", "2026-09-07 21:00:00",
         "decision=start_or_keep_alive session=night", "range=21:00-23:05", true},
        {"night_stop", "2026-09-07 23:05:00", "decision=outside_trading_window", "", false},
    };

    for (const auto& test_case : cases) {
        const auto result = RunSupervisorDryRun(test_case.suffix, test_case.fake_now);
        ASSERT_EQ(result.rc, 0) << test_case.suffix << "\n" << result.output;
        EXPECT_NE(result.output.find(test_case.decision), std::string::npos)
            << test_case.suffix << "\n"
            << result.output;
        if (test_case.range[0] != '\0') {
            EXPECT_NE(result.output.find(test_case.range), std::string::npos)
                << test_case.suffix << "\n"
                << result.output;
        }
        EXPECT_EQ(result.output.find("[dry-run] start:") != std::string::npos,
                  test_case.starts_engine)
            << test_case.suffix << "\n"
            << result.output;
    }
}

TEST(SimnowSupervisorScriptTest, ActiveStartAlwaysKeepsSafeProbe) {
    const auto result = RunSupervisorDryRun("active_safe_probe", "2026-09-07 09:00:00");

    ASSERT_EQ(result.rc, 0) << result.output;
    EXPECT_NE(result.output.find("[dry-run] start:"), std::string::npos) << result.output;
    EXPECT_NE(result.output.find("--probe-seconds"), std::string::npos) << result.output;
    EXPECT_EQ(result.output.find("--skip-probe"), std::string::npos) << result.output;
}

TEST(SimnowSupervisorScriptTest, PrewarmPersistsAuthorizationAndStartsCoreInCloseOnlyWindow) {
    const auto root = MakeTempDir("local_prewarm");
    const auto env_file = root / "simnow.env";
    const auto calendar_file = root / "session_calendar.csv";
    const auto build_dir = root / "build-real-server";
    const auto start_script = root / "start_simnow_trading.sh";
    const auto output_file = root / "prewarm.out";
    WriteFile(env_file, "# test env intentionally empty\n");
    WriteFile(calendar_file,
              "natural_date,session,trading_day,exchange,product\n"
              "# product_scope=hc:SHFE\n"
              "2026-09-07,day_am,2026-09-07,SHFE,hc\n");
    WriteExecutable(build_dir / "core_engine");
    WriteExecutable(build_dir / "simnow_probe");
    WriteExecutable(start_script);

    const std::string command =
        "SIMNOW_FAKE_NOW='2026-09-07 08:45:00' SIMNOW_RESTART_DELAY_SECONDS=1 "
        "SIMNOW_START_SCRIPT='" +
        EscapePathForShell(start_script) + "' SIMNOW_LOCK_DIR='" +
        EscapePathForShell(root / "locks") +
        "' bash scripts/ops/supervise_simnow_trading.sh --env-file '" +
        EscapePathForShell(env_file) + "' --build-dir '" + EscapePathForShell(build_dir) +
        "' --run-root '" + EscapePathForShell(root / "runs") + "' --market-data-dir '" +
        EscapePathForShell(root / "market") + "' --wal-file '" +
        EscapePathForShell(root / "wal" / "events.wal") + "' --report-root '" +
        EscapePathForShell(root / "reports") + "' --export-root '" +
        EscapePathForShell(root / "exports") + "' --reconcile-root '" +
        EscapePathForShell(root / "reconcile") + "' --session-calendar-file '" +
        EscapePathForShell(calendar_file) +
        "' --min-free-mb 1 --no-eod --once > '" + EscapePathForShell(output_file) +
        "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output_file);
    const auto marker = root / "runs" / "prewarm" / "20260907.day_am.20260907.ok";
    ASSERT_TRUE(std::filesystem::exists(marker)) << ReadFile(output_file);
    const std::string marker_payload = ReadFile(marker);
    EXPECT_NE(marker_payload.find("product_scope=hc:SHFE"), std::string::npos) << marker_payload;
    EXPECT_NE(marker_payload.find("local_checks=passed"), std::string::npos) << marker_payload;
    EXPECT_NE(ReadFile(output_file).find("[step] starting session=day_am"), std::string::npos)
        << ReadFile(output_file);
}

TEST(SimnowSupervisorScriptTest, ColdStartDiscardsStaleCorePidWithoutFalseCrashAlert) {
    const auto root = MakeTempDir("stale_core_pid");
    const auto env_file = root / "simnow.env";
    const auto config_file = root / "ctp_sim_trade_hc.yaml";
    const auto calendar_file = root / "session_calendar.csv";
    const auto build_dir = root / "build-real-server";
    const auto start_script = root / "start_simnow_trading.sh";
    const auto output_file = root / "cold_start.out";
    const auto current_pid_file = root / "runs" / "current_core_engine.pid";
    constexpr const char* kStalePid = "2147483646";

    WriteFile(env_file, "# test env intentionally empty\n");
    WriteFile(config_file, "settlement_confirm_required: true\n");
    WriteFile(calendar_file,
              "natural_date,session,trading_day,exchange,product\n"
              "# product_scope=hc:SHFE\n"
              "2026-09-07,day_am,2026-09-07,SHFE,hc\n");
    WriteExecutable(build_dir / "core_engine");
    WriteExecutable(build_dir / "simnow_probe");
    WriteExecutable(start_script);
    WriteFile(current_pid_file, std::string(kStalePid) + "\n");

    const std::string command =
        "SIMNOW_FAKE_NOW='2026-09-07 09:00:00' SIMNOW_LOCK_DIR='" +
        EscapePathForShell(root / "locks") + "' SIMNOW_START_SCRIPT='" +
        EscapePathForShell(start_script) + "' SIMNOW_RESTART_DELAY_SECONDS=1 " +
        "bash scripts/ops/supervise_simnow_trading.sh --env-file '" +
        EscapePathForShell(env_file) + "' --config '" + EscapePathForShell(config_file) +
        "' --build-dir '" + EscapePathForShell(build_dir) + "' --run-root '" +
        EscapePathForShell(root / "runs") + "' --market-data-dir '" +
        EscapePathForShell(root / "market") + "' --wal-file '" +
        EscapePathForShell(root / "wal" / "events.wal") + "' --report-root '" +
        EscapePathForShell(root / "reports") + "' --export-root '" +
        EscapePathForShell(root / "exports") + "' --reconcile-root '" +
        EscapePathForShell(root / "reconcile") + "' --session-calendar-file '" +
        EscapePathForShell(calendar_file) + "' --min-free-mb 1 --no-eod --once > '" +
        EscapePathForShell(output_file) + "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output_file);
    const std::string output = ReadFile(output_file);
    EXPECT_NE(output.find("[step] starting session=day_am"), std::string::npos) << output;
    EXPECT_EQ(output.find(std::string("[alert:critical] core_engine pid=") + kStalePid),
              std::string::npos)
        << output;
    EXPECT_FALSE(std::filesystem::exists(current_pid_file)) << output;
}

TEST(SimnowSupervisorScriptTest, MissingCalendarFailsClosedForNewSession) {
    const auto result =
        RunSupervisorDryRun("missing_calendar", "2026-09-07 09:00:00", "", false);

    EXPECT_NE(result.rc, 0) << result.output;
    EXPECT_NE(result.output.find("decision=fail_closed"), std::string::npos) << result.output;
    EXPECT_NE(result.output.find("reason=calendar_file_missing"), std::string::npos)
        << result.output;
}

TEST(SimnowSupervisorScriptTest, InvalidCalendarFailsClosedForNewSession) {
    const auto result = RunSupervisorDryRun(
        "invalid_calendar", "2026-09-07 09:00:00",
        "natural_date,session,trading_day,exchange_id,product\n"
        "2026-09-07,day_am,2026-09-07,SHFE,hc\n");

    EXPECT_NE(result.rc, 0) << result.output;
    EXPECT_NE(result.output.find("reason=calendar_header_invalid"), std::string::npos)
        << result.output;
}

TEST(SimnowSupervisorScriptTest, IncompleteHcShfeCalendarGroupFailsClosed) {
    const auto result = RunSupervisorDryRun(
        "incomplete_hc_group", "2026-09-07 09:00:00",
        "natural_date,session,trading_day,exchange,product\n"
        "# product_scope=hc:SHFE,c:DCE\n"
        "2026-09-07,day_am,2026-09-07,DCE,c\n");

    EXPECT_NE(result.rc, 0) << result.output;
    EXPECT_NE(result.output.find("reason=calendar_group_incomplete:required=hc:SHFE"),
              std::string::npos)
        << result.output;
}

TEST(SimnowSupervisorScriptTest, ConfiguredMultiProductCalendarRequiresEveryScopeMember) {
    const std::string calendar =
        "natural_date,session,trading_day,exchange,product\n"
        "# product_scope=hc:SHFE,c:DCE\n"
        "2026-09-07,day_am,2026-09-07,SHFE,hc\n"
        "2026-09-07,day_am,2026-09-07,DCE,c\n";
    const auto complete = RunSupervisorDryRun(
        "complete_multi_product", "2026-09-07 09:00:00", calendar);
    ASSERT_EQ(complete.rc, 0) << complete.output;
    EXPECT_NE(complete.output.find("product_scope=hc:SHFE,c:DCE"), std::string::npos)
        << complete.output;

    const auto incomplete = RunSupervisorDryRun(
        "incomplete_multi_product", "2026-09-07 09:00:00",
        "natural_date,session,trading_day,exchange,product\n"
        "# product_scope=hc:SHFE,c:DCE\n"
        "2026-09-07,day_am,2026-09-07,SHFE,hc\n");
    EXPECT_NE(incomplete.rc, 0) << incomplete.output;
    EXPECT_NE(incomplete.output.find("reason=calendar_group_incomplete:required=c:DCE"),
              std::string::npos)
        << incomplete.output;
}

TEST(SimnowSupervisorScriptTest, NightSessionScopeExcludesConfiguredNoNightProduct) {
    const std::string calendar =
        "natural_date,session,trading_day,exchange,product\n"
        "# product_scope=hc:SHFE,si:GFEX\n"
        "# session_scope.night=hc:SHFE\n"
        "2026-09-07,night,2026-09-08,SHFE,hc\n";
    const auto result =
        RunSupervisorDryRun("mixed_night_scope", "2026-09-07 21:01:00", calendar);

    ASSERT_EQ(result.rc, 0) << result.output;
    EXPECT_NE(result.output.find("decision=start_or_keep_alive session=night"), std::string::npos)
        << result.output;
    EXPECT_NE(result.output.find("trading_day=20260908"), std::string::npos) << result.output;
}

TEST(SimnowSupervisorScriptTest, NightSessionScopeRejectsNoNightProductRow) {
    const std::string calendar =
        "natural_date,session,trading_day,exchange,product\n"
        "# product_scope=hc:SHFE,si:GFEX\n"
        "# session_scope.night=hc:SHFE\n"
        "2026-09-07,night,2026-09-08,SHFE,hc\n"
        "2026-09-07,night,2026-09-08,GFEX,si\n";
    const auto result =
        RunSupervisorDryRun("invalid_no_night_row", "2026-09-07 21:01:00", calendar);

    EXPECT_NE(result.rc, 0) << result.output;
    EXPECT_NE(result.output.find("reason=calendar_product_scope_mismatch"), std::string::npos)
        << result.output;
}

TEST(SimnowSupervisorScriptTest, LegacyCalendarStillRequiresEveryProductAtNight) {
    const std::string calendar =
        "natural_date,session,trading_day,exchange,product\n"
        "# product_scope=hc:SHFE,si:GFEX\n"
        "2026-09-07,night,2026-09-08,SHFE,hc\n";
    const auto result =
        RunSupervisorDryRun("legacy_mixed_night", "2026-09-07 21:01:00", calendar);

    EXPECT_NE(result.rc, 0) << result.output;
    EXPECT_NE(result.output.find("reason=calendar_group_incomplete:required=si:GFEX"),
              std::string::npos)
        << result.output;
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

TEST(SimnowSupervisorScriptTest, PipelineMonitorTreatsWeekendAsInactive) {
    const auto root = MakeTempDir("pipeline_weekend_inactive");
    const auto output = root / "monitor.out";
    const std::string command = MonitorCommand(root, "2026-07-18 21:05:00", true) + " > '" +
                                EscapePathForShell(output) + "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output);
    const std::string health = ReadFile(root / "monitor" / "pipeline_health.json");
    EXPECT_NE(health.find("\"schema_version\": 3"), std::string::npos) << health;
    EXPECT_NE(health.find("\"overall_status\": \"inactive\""), std::string::npos) << health;
    EXPECT_NE(health.find("\"session\": \"closed\""), std::string::npos) << health;
}

TEST(SimnowSupervisorScriptTest, PipelineMonitorTreatsPostEndpointLunchAsInactive) {
    const auto root = MakeTempDir("pipeline_lunch_inactive");
    const auto output = root / "monitor.out";
    WriteHealthyPipelineFixture(root);
    const std::string command = "find '" + EscapePathForShell(root) +
                                "' -type f -exec touch -d '2026-07-20 11:30:20' {} + && " +
                                MonitorCommand(root, "2026-07-20 11:30:20", true) + " > '" +
                                EscapePathForShell(output) + "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output);
    const std::string health = ReadFile(root / "monitor" / "pipeline_health.json");
    EXPECT_NE(health.find("\"overall_status\": \"inactive\""), std::string::npos) << health;
    EXPECT_NE(health.find("\"session\": \"closed\""), std::string::npos) << health;
}

TEST(SimnowSupervisorScriptTest, PipelineMonitorMarksActiveSessionWithoutCoreUnhealthy) {
    const auto root = MakeTempDir("pipeline_active_core_missing");
    const auto output = root / "monitor.out";
    WriteFile(
        root / "runtime/ctp_instruments/c_dominant_contract.json",
        R"({"schema_version":2,"trading_day":"20260720","current_instrument":"c2609","exchange_id":"DCE"})");
    const std::string command = MonitorCommand(root, "2026-07-20 09:31:05", true) + " > '" +
                                EscapePathForShell(output) + "' 2>&1";

    EXPECT_NE(RunCommand(command), 0) << ReadFile(output);
    const std::string health = ReadFile(root / "monitor" / "pipeline_health.json");
    EXPECT_NE(health.find("\"overall_status\": \"unhealthy\""), std::string::npos) << health;
    EXPECT_NE(health.find("core_engine_stopped_in_trading_session"), std::string::npos) << health;
}

TEST(SimnowSupervisorScriptTest, PipelineMonitorAcceptsCompleteBarsAndNoCandidate) {
    const auto root = MakeTempDir("pipeline_healthy_no_candidate");
    const auto output = root / "monitor.out";
    WriteHealthyPipelineFixture(root);
    const std::string touch_command = "find '" + EscapePathForShell(root) +
                                      "' -type f -exec touch -d '2026-07-20 09:31:05' {} + && ";
    const std::string command = touch_command +
                                MonitorCommand(root, "2026-07-20 09:31:05", true, true) + " > '" +
                                EscapePathForShell(output) + "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output);
    const std::string health = ReadFile(root / "monitor" / "pipeline_health.json");
    const std::string checkpoint = ReadFile(root / "monitor" / "pipeline_checkpoint_v3.tsv");
    EXPECT_NE(health.find("\"overall_status\": \"healthy\""), std::string::npos) << health;
    EXPECT_NE(health.find("\"strategy_status\": \"healthy\""), std::string::npos) << health;
    EXPECT_NE(health.find("\"candidates\":0"), std::string::npos) << health;
    EXPECT_NE(checkpoint.find("schema\t3"), std::string::npos) << checkpoint;
    EXPECT_NE(checkpoint.find("cursor\t"), std::string::npos) << checkpoint;
}

TEST(SimnowSupervisorScriptTest, PipelineMonitorCorrelatesAllowedTraceAndLatencies) {
    const auto root = MakeTempDir("pipeline_allowed_trace");
    const auto output = root / "monitor.out";
    WriteAllowedTraceFixture(root, 3);
    const std::string command = "find '" + EscapePathForShell(root) +
                                "' -type f -exec touch -d '2026-07-20 09:31:05' {} + && " +
                                MonitorCommand(root, "2026-07-20 09:31:05", true, true) + " > '" +
                                EscapePathForShell(output) + "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output);
    const std::string health = ReadFile(root / "monitor" / "pipeline_health.json");
    EXPECT_NE(health.find("\"overall_status\": \"healthy\""), std::string::npos) << health;
    EXPECT_NE(health.find("\"candidate_to_disposition_p50\":2000"), std::string::npos) << health;
    EXPECT_NE(health.find("\"ctp_to_callback_p50\":1000"), std::string::npos) << health;
    EXPECT_NE(health.find("\"bar_finalize_p50\":4000"), std::string::npos) << health;
    EXPECT_NE(health.find("\"bar_to_decision_p50\":2000"), std::string::npos) << health;
    EXPECT_NE(health.find("\"status\":\"canceled\""), std::string::npos) << health;
}

TEST(SimnowSupervisorScriptTest, PipelineMonitorRejectsOldGenerationSubmission) {
    const auto root = MakeTempDir("pipeline_old_generation");
    const auto output = root / "monitor.out";
    WriteAllowedTraceFixture(root, 2);
    const std::string command = "find '" + EscapePathForShell(root) +
                                "' -type f -exec touch -d '2026-07-20 09:31:05' {} + && " +
                                MonitorCommand(root, "2026-07-20 09:31:05", true, true) + " > '" +
                                EscapePathForShell(output) + "' 2>&1";

    EXPECT_NE(RunCommand(command), 0) << ReadFile(output);
    const std::string health = ReadFile(root / "monitor" / "pipeline_health.json");
    EXPECT_NE(health.find("\"overall_status\": \"unhealthy\""), std::string::npos) << health;
    EXPECT_NE(health.find("\"generation_mismatch_submission_count\": 1"), std::string::npos)
        << health;
    EXPECT_NE(ReadFile(root / "monitor" / "signal_execution_watch.jsonl")
                  .find("old_generation_ctp_submission"),
              std::string::npos);
}

TEST(SimnowSupervisorScriptTest, PipelineMonitorDefersHalfLineAndResumesFromCheckpoint) {
    const auto root = MakeTempDir("pipeline_half_line");
    const auto first_output = root / "monitor-first.out";
    const auto second_output = root / "monitor-second.out";
    WriteHealthyPipelineFixture(root);
    const auto core_log = root / "runs" / "simnow-half-line" / "core_engine.log";
    WriteFile(root / "runs" / "current_run_dir",
              (root / "runs" / "simnow-half-line").string() + "\n");
    WriteFile(root / "runs" / "current_core_engine_log", core_log.string() + "\n");
    const std::string partial_line =
        "ts_ns=1784511005000000000 level=info app=kama event=signal_candidate "
        "event_ts_ns=\"1\" trace_id=\"half-line-trace\"";
    WriteFile(core_log, partial_line);
    const std::string touch_command = "find '" + EscapePathForShell(root) +
                                      "' -type f -exec touch -d '2026-07-20 09:31:05' {} + && ";
    const std::string first_command = touch_command +
                                      MonitorCommand(root, "2026-07-20 09:31:05", true, true) +
                                      " > '" + EscapePathForShell(first_output) + "' 2>&1";

    ASSERT_EQ(RunCommand(first_command), 0) << ReadFile(first_output);
    EXPECT_EQ(ReadFile(root / "monitor" / "signal_execution_watch.jsonl")
                  .find("candidate_without_strategy_decision"),
              std::string::npos);

    WriteFile(core_log, partial_line + "\n");
    const std::string second_command = MonitorCommand(root, "2026-07-20 09:31:05", true, true) +
                                       " > '" + EscapePathForShell(second_output) + "' 2>&1";
    EXPECT_NE(RunCommand(second_command), 0) << ReadFile(second_output);
    EXPECT_NE(ReadFile(root / "monitor" / "signal_execution_watch.jsonl")
                  .find("candidate_without_strategy_decision"),
              std::string::npos);
}

TEST(SimnowSupervisorScriptTest, PipelineMonitorIgnoresCorruptCheckpointAndRewritesV3) {
    const auto root = MakeTempDir("pipeline_corrupt_checkpoint");
    const auto output = root / "monitor.out";
    WriteHealthyPipelineFixture(root);
    WriteFile(root / "monitor" / "pipeline_checkpoint_v3.tsv", "not-a-checkpoint\n");
    const std::string command = "find '" + EscapePathForShell(root) +
                                "' -type f -exec touch -d '2026-07-20 09:31:05' {} + && " +
                                MonitorCommand(root, "2026-07-20 09:31:05", true, true) + " > '" +
                                EscapePathForShell(output) + "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output);
    EXPECT_NE(ReadFile(output).find("ignoring invalid monitor checkpoint"), std::string::npos);
    EXPECT_EQ(ReadFile(root / "monitor" / "pipeline_checkpoint_v3.tsv").rfind("schema\t3", 0), 0U);
}

TEST(SimnowSupervisorScriptTest, PipelineMonitorRejectsIncompleteFiveMinuteBar) {
    const auto root = MakeTempDir("pipeline_incomplete_5m");
    const auto output = root / "monitor.out";
    WriteHealthyPipelineFixture(root);
    const auto bar_path =
        root / "market" / "trading_day=20260720" / "varieties" / "c" / "market" / "bars_5m.csv";
    std::string bar = ReadFile(bar_path);
    const std::string complete_suffix = ",5,5,1,0,1,1,0,0\n";
    const auto suffix_pos = bar.find(complete_suffix);
    ASSERT_NE(suffix_pos, std::string::npos);
    bar.replace(suffix_pos, complete_suffix.size(), ",5,4,0,0,0,0,0,0\n");
    WriteFile(bar_path, bar);
    const std::string touch_command = "find '" + EscapePathForShell(root) +
                                      "' -type f -exec touch -d '2026-07-20 09:31:05' {} + && ";
    const std::string command = touch_command +
                                MonitorCommand(root, "2026-07-20 09:31:05", true, true) + " > '" +
                                EscapePathForShell(output) + "' 2>&1";

    EXPECT_NE(RunCommand(command), 0) << ReadFile(output);
    const std::string health = ReadFile(root / "monitor" / "pipeline_health.json");
    EXPECT_NE(health.find("\"overall_status\": \"unhealthy\""), std::string::npos) << health;
    EXPECT_NE(health.find("latest_bar_5m_incomplete"), std::string::npos) << health;
    EXPECT_NE(health.find("\"incomplete_bar_count\": 1"), std::string::npos) << health;
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

TEST(SimnowSupervisorScriptTest, NewEpochSkipsEndOfDayBeforeItsFirstTradingDay) {
    const auto root = MakeTempDir("epoch_eod_skip");
    const auto env_file = root / "empty.env";
    const auto calendar_file = root / "calendar.csv";
    const auto output_file = root / "supervisor.out";
    WriteFile(env_file, "# intentionally empty\n");
    WriteFile(calendar_file, StandardSessionCalendar());
    const std::string command =
        "SIMNOW_FAKE_NOW='2026-09-07 17:00:00' "
        "SIMNOW_EPOCH_FIRST_TRADING_DAY=20260908 SIMNOW_LOCK_DIR='" +
        EscapePathForShell(root / "locks") +
        "' bash scripts/ops/supervise_simnow_trading.sh --env-file '" +
        EscapePathForShell(env_file) + "' --run-root '" +
        EscapePathForShell(root / "runs") + "' --market-data-dir '" +
        EscapePathForShell(root / "market") + "' --wal-file '" +
        EscapePathForShell(root / "wal" / "events.wal") + "' --report-root '" +
        EscapePathForShell(root / "reports") + "' --export-root '" +
        EscapePathForShell(root / "exports") + "' --reconcile-root '" +
        EscapePathForShell(root / "reconcile") + "' --session-calendar-file '" +
        EscapePathForShell(calendar_file) +
        "' --product-scope hc:SHFE --min-free-mb 1 --dry-run > '" +
        EscapePathForShell(output_file) + "' 2>&1";

    ASSERT_EQ(RunCommand(command), 0) << ReadFile(output_file);
    EXPECT_NE(ReadFile(output_file).find("eod_due=false"), std::string::npos)
        << ReadFile(output_file);
}

TEST(SimnowSupervisorScriptTest, PrewarmAndActiveUseSeparateRestartBudgets) {
    const auto supervisor = ReadFile("scripts/ops/supervise_simnow_trading.sh");
    EXPECT_NE(supervisor.find(
                  "session_key=\"${trading_day}.${session_label}.${natural_date}.${phase}\""),
              std::string::npos)
        << supervisor;
}

TEST(SimnowSupervisorScriptTest, IndependentSignalMonitorUnitRestartsAlways) {
    const std::string unit = ReadFile("infra/systemd/quant-hft-simnow-signal-monitor.service");
    EXPECT_NE(unit.find("Restart=always"), std::string::npos) << unit;
    EXPECT_NE(unit.find("monitor_simnow_signal_execution.sh"), std::string::npos) << unit;
    EXPECT_EQ(unit.find("runtime/trading/monitor/simnow"), std::string::npos) << unit;
    const auto monitor = ReadFile("scripts/ops/monitor_simnow_signal_execution.sh");
    EXPECT_NE(monitor.find("load_runtime_path_defaults"), std::string::npos);
    EXPECT_NE(monitor.find("pipeline_checkpoint_v3.tsv"), std::string::npos);
}

TEST(SimnowSupervisorScriptTest,
     IdentityDefaultsSurviveRunIdsAndSeparateAccountsWithoutClaimingState) {
    const auto root = MakeTempDir("identity_defaults_" + std::to_string(getpid()));
    const auto config = root / "config.yaml";
    const auto env_file = root / "empty.env";
    WriteFile(env_file, "# Synthetic test environment; no broker is contacted.\n");
    WriteFile(config,
              "ctp:\n  environment: sim\n  enable_real_api: false\n"
              "  is_production_mode: false\n  broker_id: b\n"
              "  user_id: ${PATH_TEST_ACCOUNT}\n  password: fixture-only\n"
              "  market_front: tcp://127.0.0.1:40011\n"
              "  trader_front: tcp://127.0.0.1:40001\n");
    const auto runtime = root / "runtime";
    const auto run = [&](const std::string& account, const std::string& run_id) {
        const auto output = root / (account + run_id + ".out");
        const std::string command =
            "env -u SIMNOW_WAL_FILE -u QUANT_HFT_WAL_FILE -u SIMNOW_RUN_ROOT "
            "-u SIMNOW_MARKET_DATA_DIR -u QUANT_HFT_MARKET_DATA_DIR "
            "-u SIMNOW_REPORT_ROOT -u SIMNOW_EXPORT_ROOT -u SIMNOW_RECONCILE_ROOT "
            "-u QUANT_HFT_READINESS_FILE QUANT_HFT_INSTANCE=default "
            "QUANT_HFT_RUNTIME_ROOT='" +
            EscapePathForShell(runtime) +
            "' "
            "PATH_TEST_ACCOUNT='" +
            account + "' SIMNOW_RUN_ID='" + run_id +
            "' "
            "SIMNOW_FAKE_NOW='2026-07-20 12:00:00' "
            "bash scripts/ops/supervise_simnow_trading.sh --env-file '" +
            EscapePathForShell(env_file) + "' --config '" + EscapePathForShell(config) +
            "' --build-dir '" + EscapeForShell(QUANT_HFT_BUILD_DIR) + "' --no-eod --dry-run > '" +
            EscapePathForShell(output) + "' 2>&1";
        EXPECT_EQ(RunCommand(command), 0) << ReadFile(output);
        return ReadFile(output);
    };
    const auto first = run("a", "run1");
    const auto restart = run("a", "run2");
    const auto other = run("other", "run1");
    const auto wal_a = (runtime / "sim/b/a/default/wal/events.wal").string();
    const auto wal_other = (runtime / "sim/b/other/default/wal/events.wal").string();
    EXPECT_NE(first.find("wal_file=" + wal_a), std::string::npos) << first;
    EXPECT_NE(restart.find("wal_file=" + wal_a), std::string::npos) << restart;
    EXPECT_NE(other.find("wal_file=" + wal_other), std::string::npos) << other;
    EXPECT_EQ(other.find("wal_file=" + wal_a), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(runtime / "sim"));
    EXPECT_TRUE(std::filesystem::is_directory(runtime / "runs/sim/b/a/default"));
    EXPECT_TRUE(std::filesystem::is_directory(runtime / "runs/sim/b/other/default"));
    const auto monitor_output = root / "monitor.out";
    const std::string monitor_command =
        "env -u SIMNOW_WAL_FILE -u QUANT_HFT_WAL_FILE -u SIMNOW_RUN_ROOT "
        "-u SIMNOW_MARKET_DATA_DIR -u QUANT_HFT_MARKET_DATA_DIR "
        "-u SIMNOW_SIGNAL_MONITOR_ROOT -u QUANT_HFT_READINESS_FILE "
        "QUANT_HFT_INSTANCE=default PATH_TEST_ACCOUNT=a "
        "QUANT_HFT_RUNTIME_ROOT='" +
        EscapePathForShell(runtime) +
        "' "
        "CTP_CONFIG_PATH='" +
        EscapePathForShell(config) +
        "' "
        "SIMNOW_MONITOR_FAKE_NOW='2026-07-19 12:00:00' "
        "bash scripts/ops/monitor_simnow_signal_execution.sh --build-dir '" +
        EscapeForShell(QUANT_HFT_BUILD_DIR) + "' --once > '" + EscapePathForShell(monitor_output) +
        "' 2>&1";
    ASSERT_EQ(RunCommand(monitor_command), 0) << ReadFile(monitor_output);
    const auto monitor = ReadFile(monitor_output);
    EXPECT_NE(monitor.find("wal_file=" + wal_a), std::string::npos) << monitor;
    EXPECT_NE(monitor.find("core_readiness_file=" +
                           (runtime / "sim/b/a/default/monitor/readiness.json").string()),
              std::string::npos);
    EXPECT_TRUE(std::filesystem::exists(runtime / "runs/sim/b/a/default/monitor/heartbeat.json"));
    EXPECT_FALSE(std::filesystem::exists(runtime / "sim"));
}

}  // namespace
