#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#ifndef QUANT_HFT_BUILD_DIR
#error "QUANT_HFT_BUILD_DIR is required"
#endif

namespace {

std::filesystem::path BuildDir() { return std::filesystem::path(QUANT_HFT_BUILD_DIR); }

std::filesystem::path BinaryPath(const std::string& name) { return BuildDir() / name; }

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

void WriteFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    out << content;
}

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto base =
        std::filesystem::temp_directory_path() / ("quant_hft_simnow_dashboard_cli_test_" + suffix);
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);
    return base;
}

int RunCommandCapture(const std::string& command, const std::filesystem::path& output_file) {
    const std::string shell_command = command + " > \"" + output_file.string() + "\" 2>&1";
    return std::system(shell_command.c_str());
}

std::string DashboardCommand(const std::filesystem::path& root,
                             const std::filesystem::path& output_dir) {
    return "\"" + BinaryPath("simnow_dashboard_cli").string() +
           "\""
           " --run-root \"" +
           (root / "runs").string() +
           "\""
           " --market-data-dir \"" +
           (root / "market").string() +
           "\""
           " --wal-file \"" +
           (root / "wal" / "events.wal").string() +
           "\""
           " --report-root \"" +
           (root / "reports").string() +
           "\""
           " --export-root \"" +
           (root / "exports").string() +
           "\""
           " --monitor-root \"" +
           (root / "monitor").string() +
           "\""
           " --ctp-instrument-dir \"" +
           (root / "ctp_instruments").string() +
           "\""
           " --probe-log-dir \"" +
           (root / "verify_simnow_login").string() +
           "\""
           " --output-dir \"" +
           output_dir.string() + "\"";
}

void WriteSampleMarketData(const std::filesystem::path& root) {
    WriteFile(root / "market" / "trading_day=20260515" / "varieties" / "c" / "market" / "ticks.csv",
              "instrument_id,exchange_id,trading_day,action_day,update_time,update_millisec,"
              "last_price,bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,volume,"
              "open_interest,settlement_price,average_price_raw,average_price_norm,"
              "is_valid_settlement,exchange_ts_ns,recv_ts_ns\n"
              "c2607,DCE,20260515,20260514,21:00:01,0,2365,2364,2365,91,14,2821,"
              "1223500,0,23658,23658,0,0,1778763619820649828\n");
    WriteFile(
        root / "market" / "trading_day=20260515" / "varieties" / "c" / "market" / "bars_1m.csv",
        "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close,"
        "analysis_open,analysis_high,analysis_low,analysis_close,analysis_price_offset,"
        "volume,ts_ns\n"
        "c2607,DCE,20260515,20260514,20260515 21:00,2365,2366,2363,2365,"
        "2365,2366,2363,2365,0,16655,1778763659736134827\n");
}

void WriteSampleReports(const std::filesystem::path& root) {
    WriteFile(root / "reports" / "20260514" / "simnow_daily_report.json",
              "{\"trading_day\":\"20260514\",\"tick_rows\":0,\"bar_rows\":0,"
              "\"wal_order_events\":0,\"wal_trade_events\":0}\n");
    WriteFile(root / "reports" / "20260514" / "ops_health_report.json",
              "{\"generated_ts_ns\":1,\"overall_healthy\":false,\"slis\":[]}\n");
    WriteFile(root / "reports" / "20260514" / "ops_alert_report.json",
              "{\"alerts\":[{\"severity\":\"critical\"},{\"severity\":\"warn\"}]}\n");
    WriteFile(root / "exports" / "20260514" / "summary.json",
              "{\"trading_day\":\"20260514\",\"wal_missing\":true,\"parse_errors\":0,"
              "\"order_events\":0,\"trade_fills\":0}\n");
    WriteFile(root / "exports" / "20260514" / "orders.csv",
              "seq,event_type,run_id,trading_day,client_order_id\n");
    WriteFile(root / "exports" / "20260514" / "trade_fills.csv",
              "seq,event_type,run_id,trading_day,trade_id\n");
}

void WriteSampleSignalMonitor(const std::filesystem::path& root) {
    WriteFile(
        root / "monitor" / "signal_execution_watch.jsonl",
        "{\"ts\":\"2026-05-27T09:00:00+08:00\",\"event\":\"signal_passed\","
        "\"trace_id\":\"kama_candidate_c-open-c2607-1\",\"message\":\"composite gate passed\"}\n"
        "{\"ts\":\"2026-05-27T09:00:31+08:00\",\"event\":\"incident\","
        "\"trace_id\":\"kama_candidate_c-open-c2607-1\","
        "\"message\":\"signal_without_order_submitted\"}\n"
        "{\"ts\":\"2026-05-27T09:01:00+08:00\",\"event\":\"summary\","
        "\"message\":\"signals=1 active=0 filled=0 incidents=1\","
        "\"signals\":1,\"active\":0,\"filled\":0,\"incidents\":1}\n");
    WriteFile(root / "monitor" / "incidents" / "20260527T090031-kama_candidate_c-open-c2607-1.md",
              "# incident\n");
}

void WriteSampleCtpRuntime(const std::filesystem::path& root,
                           const std::filesystem::path& core_log) {
    WriteFile(root / "ctp_instruments" / "c_dominant_contract.json",
              "{\"product_id\":\"c\",\"instrument_id\":\"c2607\","
              "\"exchange_id\":\"DCE\",\"selection_metric\":\"open_interest\"}\n");
    WriteFile(root / "verify_simnow_login" / "simnow_probe_real.log",
              "ts_ns=1 level=info app=simnow_probe event=session_snapshot state=logged_in\n"
              "ts_ns=2 level=info app=simnow_probe event=probe_completed state=healthy\n");
    WriteFile(root / "runs" / "current_core_engine.pid", std::to_string(getpid()) + "\n");
    WriteFile(root / "runs" / "current_core_engine_log", core_log.string() + "\n");
    WriteFile(core_log,
              "ts_ns=1 level=info app=core_engine event=ctp_td_front_connected\n"
              "ts_ns=2 level=info app=core_engine event=ctp_md_front_connected\n"
              "ts_ns=3 level=info app=core_engine event=ctp_login_response error_id=0\n"
              "ts_ns=4 level=info app=core_engine event=ctp_settlement_confirmed "
              "settlement_confirm_required=true\n"
              "ts_ns=5 level=info app=core_engine event=order_submitted "
              "trace_id=kama_candidate_c-open-c2607-1 "
              "client_order_id=kama_candidate_c-0001-1 instrument_id=c2607\n"
              "ts_ns=6 level=info app=core_engine event=ctp_order_submitted "
              "trace_id=kama_candidate_c-open-c2607-1 "
              "client_order_id=kama_candidate_c-0001-1 order_ref=0001 request_id=7\n"
              "ts_ns=7 level=error app=core_engine event=order_rejected "
              "reason=\"order_insert_error (ErrorID=42, ErrorMsg=CTP:?????????????\?)\" "
              "client_order_id=kama_candidate_c-0001-1\n");
}

void WriteSampleCtpSignalMonitor(const std::filesystem::path& root) {
    WriteFile(root / "monitor" / "signal_execution_watch.jsonl",
              "{\"ts\":\"2026-05-27T09:00:00+08:00\",\"event\":\"signal_passed\","
              "\"trace_id\":\"kama_candidate_c-open-c2607-1\","
              "\"message\":\"composite gate passed\"}\n"
              "{\"ts\":\"2026-05-27T09:00:01+08:00\",\"event\":\"summary\","
              "\"message\":\"signals=1 active=0 filled=1 incidents=0\","
              "\"signals\":1,\"active\":0,\"filled\":1,\"incidents\":0}\n");
}

void WriteSampleCtpWal(const std::filesystem::path& root) {
    WriteFile(root / "wal" / "events.wal",
              "{\"seq\":1,\"schema_version\":2,\"kind\":\"order\","
              "\"event_type\":\"order_update\","
              "\"trace_id\":\"kama_candidate_c-open-c2607-1\","
              "\"client_order_id\":\"kama_candidate_c-0001-1\","
              "\"order_ref\":\"0001\",\"status\":5,\"error_id\":42,"
              "\"reason\":\"结算结果未确认\"}\n"
              "{\"seq\":2,\"schema_version\":2,\"kind\":\"trade\","
              "\"event_type\":\"trade_fill\","
              "\"trace_id\":\"kama_candidate_c-open-c2607-2\","
              "\"client_order_id\":\"kama_candidate_c-0002-2\","
              "\"trade_id\":\"trade-1\"}\n");
}

TEST(SimnowDashboardCli, EmptyWalAndDailyReportsRenderHtmlAndState) {
    const auto root = MakeTempDir("happy_path");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto core_log = root / "runs" / "run_1" / "core_engine.log";

    WriteFile(root / "wal" / "events.wal", "");
    WriteFile(root / "runs" / "current_run_dir", (root / "runs" / "run_1").string() + "\n");
    WriteFile(root / "runs" / "current_core_engine_log", core_log.string() + "\n");
    WriteFile(core_log,
              "ts_ns=1 level=warn app=core_engine event=order_reject "
              "account_id=\"secret-account\"\n");
    WriteSampleMarketData(root);
    WriteSampleReports(root);
    WriteSampleSignalMonitor(root);

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"status\": \"missing_pid\""), std::string::npos);
    EXPECT_NE(json.find("\"wal_order_events\": 0"), std::string::npos);
    EXPECT_NE(json.find("\"trading_day\": \"20260514\""), std::string::npos);
    EXPECT_NE(json.find("\"critical_alerts\": 1"), std::string::npos);
    EXPECT_NE(json.find("\"signal_monitor\""), std::string::npos);
    EXPECT_NE(json.find("\"signals\": 1"), std::string::npos);
    EXPECT_NE(json.find("signal_without_order_submitted"), std::string::npos);
    EXPECT_NE(json.find("<redacted>"), std::string::npos);
    EXPECT_NE(html.find("SimNow Dashboard"), std::string::npos);
    EXPECT_NE(html.find("Signal Execution"), std::string::npos);
    EXPECT_NE(html.find("signal_without_order_submitted"), std::string::npos);
    EXPECT_NE(html.find("missing_pid"), std::string::npos);
    EXPECT_NE(html.find("20260514"), std::string::npos);
}

TEST(SimnowDashboardCli, RendersCtpConnectionAndOrderFlowPanels) {
    const auto root = MakeTempDir("ctp_panels");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto core_log = root / "runs" / "run_1" / "core_engine.log";

    WriteSampleCtpRuntime(root, core_log);
    WriteSampleCtpSignalMonitor(root);
    WriteSampleCtpWal(root);

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"ctp_connection\""), std::string::npos);
    EXPECT_NE(json.find("\"status\": \"connected\""), std::string::npos);
    EXPECT_NE(json.find("\"td_front\": \"connected\""), std::string::npos);
    EXPECT_NE(json.find("\"settlement_status\": \"confirmed\""), std::string::npos);
    EXPECT_NE(json.find("ctp_settlement_confirmed"), std::string::npos);
    EXPECT_NE(json.find("\"active_instruments\": [\"c2607\"]"), std::string::npos);
    EXPECT_NE(json.find("\"ctp_order_flow\""), std::string::npos);
    EXPECT_NE(json.find("\"ctp_submitted\": 1"), std::string::npos);
    EXPECT_NE(json.find("\"ctp_callbacks\": 1"), std::string::npos);
    EXPECT_NE(json.find("\"wal_rejected\": 1"), std::string::npos);
    EXPECT_NE(json.find("\"wal_fills\": 1"), std::string::npos);
    EXPECT_NE(json.find("\"last_error_id\": \"42\""), std::string::npos);
    EXPECT_NE(json.find("\"last_reject_reason\": \"settlement_unconfirmed\""), std::string::npos);
    EXPECT_NE(html.find("CTP Connection"), std::string::npos);
    EXPECT_NE(html.find("Signal To CTP Flow"), std::string::npos);
    EXPECT_NE(html.find("CTP Orders And Rejects"), std::string::npos);
    EXPECT_NE(html.find("settlement_unconfirmed"), std::string::npos);
    EXPECT_NE(html.find("结算结果未确认"), std::string::npos);
    EXPECT_EQ(html.find("????????"), std::string::npos);
    EXPECT_EQ(html.find("auth_code"), std::string::npos);
}

TEST(SimnowDashboardCli, LiveHealthIgnoresStaleOrderFlowBeforeCurrentMonitorEpoch) {
    const auto root = MakeTempDir("stale_order_flow");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto run_dir = root / "runs" / "simnow-auto-20260528-day_pm-142549-r1";
    const auto core_log = run_dir / "core_engine.log";

    WriteSampleMarketData(root);
    WriteFile(root / "ctp_instruments" / "c_dominant_contract.json",
              "{\"product_id\":\"c\",\"instrument_id\":\"c2607\","
              "\"exchange_id\":\"DCE\",\"selection_metric\":\"open_interest\"}\n");
    WriteFile(root / "runs" / "current_core_engine.pid", std::to_string(getpid()) + "\n");
    WriteFile(root / "runs" / "current_run_dir", run_dir.string() + "\n");
    WriteFile(root / "runs" / "current_core_engine_log", core_log.string() + "\n");
    WriteFile(core_log,
              "ts_ns=1 level=info app=core_engine event=ctp_td_front_connected\n"
              "ts_ns=2 level=info app=core_engine event=ctp_md_front_connected\n"
              "ts_ns=3 level=info app=core_engine event=ctp_login_response error_id=0\n"
              "ts_ns=4 level=info app=core_engine event=ctp_settlement_confirmed "
              "settlement_confirm_required=true\n");
    WriteFile(root / "monitor" / "signal_execution_watch.jsonl",
              "{\"ts\":\"2026-05-28T13:20:00+08:00\",\"event\":\"signal_passed\","
              "\"trace_id\":\"old-signal\",\"message\":\"old signal\"}\n"
              "{\"ts\":\"2026-05-28T13:20:01+08:00\",\"event\":\"incident\","
              "\"trace_id\":\"old-signal\",\"message\":\"old incident\"}\n"
              "{\"ts\":\"2026-05-28T14:25:49+08:00\",\"event\":\"monitor_started\","
              "\"message\":\"SimNow signal execution monitor started\"}\n"
              "{\"ts\":\"2026-05-28T14:26:53+08:00\",\"event\":\"summary\","
              "\"message\":\"signals=0 active=0 filled=0 incidents=0\","
              "\"signals\":0,\"active\":0,\"filled\":0,\"incidents\":0}\n");
    WriteFile(root / "wal" / "events.wal",
              "{\"seq\":1,\"schema_version\":2,\"kind\":\"order\","
              "\"event_type\":\"order_update\","
              "\"run_id\":\"simnow-auto-20260527-day_pm-133111-r2\","
              "\"client_order_id\":\"old-order\",\"status\":5,\"error_id\":42,"
              "\"reason\":\"结算结果未确认\"}\n");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"live_healthy\": true"), std::string::npos);
    EXPECT_NE(json.find("\"overall_healthy\": true"), std::string::npos);
    EXPECT_NE(json.find("\"live_warning_count\": 0"), std::string::npos);
    EXPECT_NE(json.find("\"signals\": 0"), std::string::npos);
    EXPECT_NE(json.find("\"ctp_submit_rejected\": 0"), std::string::npos);
    EXPECT_NE(json.find("\"wal_rejected\": 0"), std::string::npos);
    EXPECT_NE(html.find("live healthy"), std::string::npos);
    EXPECT_EQ(html.find("live unhealthy"), std::string::npos);
}

TEST(SimnowDashboardCli, HidesStaleIncidentFilesAfterMonitorReset) {
    const auto root = MakeTempDir("fresh_monitor");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";

    WriteFile(root / "wal" / "events.wal", "");
    WriteFile(root / "monitor" / "signal_execution_watch.jsonl",
              "{\"ts\":\"2026-05-28T12:40:17+08:00\",\"event\":\"monitor_started\","
              "\"message\":\"SimNow signal execution monitor started\"}\n"
              "{\"ts\":\"2026-05-28T12:41:18+08:00\",\"event\":\"summary\","
              "\"message\":\"signals=0 active=0 filled=0 incidents=0\","
              "\"signals\":0,\"active\":0,\"filled\":0,\"incidents\":0}\n");
    WriteFile(
        root / "monitor" / "incidents" / "20260528T095805-kama_candidate_hc-open-hc2610-old.md",
        "# old incident\n");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"incidents\": 0"), std::string::npos);
    EXPECT_EQ(json.find("kama_candidate_hc-open-hc2610-old"), std::string::npos);
    EXPECT_EQ(html.find("kama_candidate_hc-open-hc2610-old"), std::string::npos);
}

TEST(SimnowDashboardCli, StrictExitReturnsNonZeroForUnhealthyState) {
    const auto root = MakeTempDir("strict_exit");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";

    WriteFile(root / "wal" / "events.wal", "");
    WriteFile(root / "runs" / "current_core_engine.pid", "999999\n");
    WriteSampleMarketData(root);

    const int rc =
        RunCommandCapture(DashboardCommand(root, output_dir) + " --strict-exit", stdout_log);

    EXPECT_NE(rc, 0);
    EXPECT_NE(ReadFile(output_dir / "dashboard_state.json").find("\"overall_healthy\": false"),
              std::string::npos);
}

TEST(SimnowDashboardCli, MissingInputsStillProduceDashboardFiles) {
    const auto root = MakeTempDir("missing_inputs");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"exists\": false"), std::string::npos);
    EXPECT_NE(json.find("\"recent_alerts\""), std::string::npos);
    EXPECT_NE(html.find("No market CSV files found"), std::string::npos);
}

}  // namespace
