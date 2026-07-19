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

TEST(SimnowSupervisorScriptTest, IndependentSignalMonitorUnitRestartsAlways) {
    const std::string unit = ReadFile("infra/systemd/quant-hft-simnow-signal-monitor.service");
    EXPECT_NE(unit.find("Restart=always"), std::string::npos) << unit;
    EXPECT_NE(unit.find("monitor_simnow_signal_execution.sh"), std::string::npos) << unit;
    EXPECT_NE(unit.find("--heartbeat-file"), std::string::npos) << unit;
    EXPECT_NE(unit.find("--health-snapshot-file"), std::string::npos) << unit;
    EXPECT_NE(unit.find("pipeline_checkpoint_v3.tsv"), std::string::npos) << unit;
}

}  // namespace
