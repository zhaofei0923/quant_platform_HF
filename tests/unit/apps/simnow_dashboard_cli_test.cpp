#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
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

std::size_t CountOccurrences(const std::string& text, const std::string& needle) {
    std::size_t count = 0;
    std::size_t pos = 0;
    while ((pos = text.find(needle, pos)) != std::string::npos) {
        ++count;
        pos += needle.size();
    }
    return count;
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

void WritePipelineHealth(const std::filesystem::path& root, const std::string& status,
                         const std::string& product_status = "healthy",
                         const std::string& reason = "all_checks_passed") {
    const bool inactive = status == "inactive";
    const std::string stage_status = inactive ? "inactive" : status;
    WriteFile(root / "monitor" / "pipeline_health.json",
              "{\"schema_version\":3,\"overall_status\":\"" + status + "\",\"session\":\"" +
                  (inactive ? "closed" : "day") +
                  "\",\"trading_day\":\"20260720\",\"readiness_mode\":\"Ready\"," +
                  "\"generation\":7,\"warning_count\":0,\"critical_count\":0," +
                  "\"last_change_epoch\":1784511065,\"pending_exit_count\":0," +
                  "\"unresolved_mapping_count\":0,\"late_tick_count\":0," +
                  "\"duplicate_bar_count\":0,\"conflict_bar_count\":0," +
                  "\"incomplete_bar_count\":0,\"missing_strategy_evaluation_count\":0," +
                  "\"duplicate_disposition_count\":0,\"unresolved_trace_count\":0," +
                  "\"runtime_status\":\"" + stage_status + "\",\"runtime_reason\":\"" + reason +
                  "\",\"market_data_status\":\"" + stage_status + "\",\"market_data_reason\":\"" +
                  reason + "\",\"bar_1m_status\":\"" + stage_status + "\",\"bar_1m_reason\":\"" +
                  reason + "\",\"bar_5m_status\":\"" + stage_status + "\",\"bar_5m_reason\":\"" +
                  reason + "\",\"strategy_status\":\"" + stage_status +
                  "\",\"strategy_reason\":\"" + reason + "\",\"execution_status\":\"" +
                  stage_status + "\",\"execution_reason\":\"" + reason +
                  "\",\"latencies_ms\":{\"bar_finalize_p50\":4," +
                  "\"bar_finalize_p95\":5,\"bar_finalize_p99\":6," +
                  "\"bar_to_decision_p50\":1,\"bar_to_decision_p95\":2," +
                  "\"bar_to_decision_p99\":3,\"candidate_to_disposition_p50\":1," +
                  "\"candidate_to_disposition_p95\":2," +
                  "\"candidate_to_disposition_p99\":3,\"ctp_to_callback_p50\":10," +
                  "\"ctp_to_callback_p95\":20,\"ctp_to_callback_p99\":30}," +
                  "\"products\":[{\"product_id\":\"c\",\"instrument_id\":\"c2609\"," +
                  "\"exchange_id\":\"DCE\",\"status\":\"" + product_status + "\",\"reason\":\"" +
                  reason + "\",\"schema\":\"1m:v2,5m:v2\",\"tick_age_seconds\":1," +
                  "\"tick_delay_ms\":2,\"last_1m\":\"20260720 09:30\"," +
                  "\"last_5m\":\"20260720 09:25\",\"bar_5m_complete\":1," +
                  "\"strategy_minute\":\"20260720 09:25\",\"strategy_evaluations\":8," +
                  "\"candidates\":1,\"allowed\":1,\"pending_traces\":0}]," +
                  "\"recent_traces\":[{\"trace_id\":\"trace-1\",\"instrument_id\":\"c2609\"," +
                  "\"strategy_id\":\"kama_c\",\"status\":\"filled\"," +
                  "\"last_event\":\"trade_fill\",\"client_order_id\":\"order-1\"}]," +
                  "\"issues\":[],\"recent_recoveries\":[]}\n");
    WriteFile(root / "monitor" / "heartbeat.json",
              "{\"schema_version\":3,\"pipeline_status\":\"" + status + "\"}\n");
}

void WriteHealthyReadiness(const std::filesystem::path& root) {
    WriteFile(root / "monitor" / "readiness.json",
              "{\"mode\":\"Ready\",\"generation\":7,\"recovery_complete\":true," +
                  std::string("\"trader_ready\":true,\"gateway_healthy\":true,") +
                  "\"settlement_confirmed\":true,\"pending_exit_count\":0," +
                  "\"unresolved_mapping_count\":0}\n");
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

void WriteStaleTwoProductMarketData(const std::filesystem::path& root) {
    const auto c_tick_file =
        root / "market" / "trading_day=20260529" / "varieties" / "c" / "market" / "ticks.csv";
    const auto c_bar_file =
        root / "market" / "trading_day=20260529" / "varieties" / "c" / "market" / "bars_1m.csv";
    const auto hc_tick_file =
        root / "market" / "trading_day=20260529" / "varieties" / "hc" / "market" / "ticks.csv";
    const auto hc_bar_file =
        root / "market" / "trading_day=20260529" / "varieties" / "hc" / "market" / "bars_1m.csv";

    WriteFile(c_tick_file,
              "instrument_id,exchange_id,trading_day,action_day,update_time,update_millisec,"
              "last_price,bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,volume,"
              "open_interest,settlement_price,average_price_raw,average_price_norm,"
              "is_valid_settlement,exchange_ts_ns,recv_ts_ns\n"
              "c2607,DCE,20260529,20260529,23:00:01,0,2324,2323,2324,91,14,326725,"
              "1223500,0,23658,23658,0,0,1778763619820649828\n");
    WriteFile(c_bar_file,
              "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close,"
              "analysis_open,analysis_high,analysis_low,analysis_close,analysis_price_offset,"
              "volume,ts_ns\n"
              "c2607,DCE,20260529,20260529,20260529 23:00,2320,2324,2319,2324,"
              "2320,2324,2319,2324,0,326725,1778763659736134827\n");
    WriteFile(hc_tick_file,
              "instrument_id,exchange_id,trading_day,action_day,update_time,update_millisec,"
              "last_price,bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,volume,"
              "open_interest,settlement_price,average_price_raw,average_price_norm,"
              "is_valid_settlement,exchange_ts_ns,recv_ts_ns\n"
              "hc2610,SHFE,20260529,20260529,23:00:02,0,3369,3368,3369,91,14,107698,"
              "1223500,0,33690,33690,0,0,1778763619820649828\n");
    WriteFile(hc_bar_file,
              "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close,"
              "analysis_open,analysis_high,analysis_low,analysis_close,analysis_price_offset,"
              "volume,ts_ns\n"
              "hc2610,SHFE,20260529,20260529,20260529 23:00,3365,3370,3364,3369,"
              "3365,3370,3364,3369,0,107698,1778763659736134827\n");

    const auto stale_time = std::filesystem::file_time_type::clock::now() - std::chrono::minutes(5);
    std::filesystem::last_write_time(c_tick_file, stale_time);
    std::filesystem::last_write_time(c_bar_file, stale_time);
    std::filesystem::last_write_time(hc_tick_file, stale_time);
    std::filesystem::last_write_time(hc_bar_file, stale_time);
}

void WriteDayCloseMarketData(const std::filesystem::path& root) {
    const auto tick_file =
        root / "market" / "trading_day=20260528" / "varieties" / "c" / "market" / "ticks.csv";
    const auto bar_file =
        root / "market" / "trading_day=20260528" / "varieties" / "c" / "market" / "bars_1m.csv";
    WriteFile(tick_file,
              "instrument_id,exchange_id,trading_day,action_day,update_time,update_millisec,"
              "last_price,bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,volume,"
              "open_interest,settlement_price,average_price_raw,average_price_norm,"
              "is_valid_settlement,exchange_ts_ns,recv_ts_ns\n"
              "c2607,DCE,20260528,20260528,15:00:00,0,2365,2364,2365,91,14,2821,"
              "1223500,0,23658,23658,0,0,1778763619820649828\n");
    WriteFile(bar_file,
              "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close,"
              "analysis_open,analysis_high,analysis_low,analysis_close,analysis_price_offset,"
              "volume,ts_ns\n"
              "c2607,DCE,20260528,20260528,20260528 15:00,2365,2366,2363,2365,"
              "2365,2366,2363,2365,0,16655,1778763659736134827\n");
    const auto stale_time = std::filesystem::file_time_type::clock::now() - std::chrono::minutes(5);
    std::filesystem::last_write_time(tick_file, stale_time);
    std::filesystem::last_write_time(bar_file, stale_time);
}

void WritePostCloseTick(const std::filesystem::path& root) {
    const auto tick_file =
        root / "market" / "trading_day=20260528" / "varieties" / "c" / "market" / "ticks.csv";
    WriteFile(tick_file,
              "instrument_id,exchange_id,trading_day,action_day,update_time,update_millisec,"
              "last_price,bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,volume,"
              "open_interest,settlement_price,average_price_raw,average_price_norm,"
              "is_valid_settlement,exchange_ts_ns,recv_ts_ns\n"
              "c2607,DCE,20260528,20260528,15:15:38,0,2365,2364,2365,91,14,2821,"
              "1223500,0,23658,23658,0,0,1778763619820649828\n");
    const auto stale_time = std::filesystem::file_time_type::clock::now() - std::chrono::minutes(5);
    std::filesystem::last_write_time(tick_file, stale_time);
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
              "\"strategy_id\":\"kama_trend_production\","
              "\"trace_id\":\"kama_trend_production-open-c2607-2\","
              "\"client_order_id\":\"kama_trend_production-0002-2\","
              "\"instrument_id\":\"c2607\",\"exchange_id\":\"DCE\","
              "\"side\":0,\"offset\":0,\"last_trade_volume\":1,"
              "\"avg_fill_price\":2320,\"order_ref\":\"0002\","
              "\"trade_id\":\"trade-1\",\"ts_ns\":1780291227262046132}\n");
}

void WriteCtpReplayDuplicateWal(const std::filesystem::path& root,
                                const std::string& current_run_id) {
    std::ostringstream wal;
    wal << "{\"seq\":1,\"schema_version\":2,\"kind\":\"trade\","
           "\"event_type\":\"trade_fill\","
           "\"run_id\":\"simnow-auto-20260602-day_am-090000-r1\","
           "\"strategy_id\":\"kama_trend_production\","
           "\"trace_id\":\"kama_trend_production-open-c2607-1\","
           "\"client_order_id\":\"kama_trend_production-0001-1\","
           "\"instrument_id\":\"c2607\",\"exchange_id\":\"DCE\","
           "\"side\":0,\"offset\":0,\"last_trade_volume\":1,"
           "\"avg_fill_price\":2320,\"order_ref\":\"0001\","
           "\"trade_id\":\"trade-dup\",\"ts_ns\":1780291227262046132}\n";
    wal << "{\"seq\":2,\"schema_version\":2,\"kind\":\"trade\","
           "\"event_type\":\"trade_fill\",\"run_id\":\""
        << current_run_id
        << "\",\"strategy_id\":\"kama_trend_production\","
           "\"trace_id\":\"kama_trend_production-open-c2607-1\","
           "\"client_order_id\":\"kama_trend_production-0001-1\","
           "\"instrument_id\":\"c2607\",\"exchange_id\":\"DCE\","
           "\"side\":0,\"offset\":0,\"last_trade_volume\":1,"
           "\"avg_fill_price\":2320,\"order_ref\":\"0001\","
           "\"trade_id\":\"trade-dup\",\"ts_ns\":1780291327262046132}\n";
    wal << "{\"seq\":3,\"schema_version\":2,\"kind\":\"trade\","
           "\"event_type\":\"trade_fill\",\"run_id\":\""
        << current_run_id
        << "\",\"strategy_id\":\"kama_trend_production\","
           "\"trace_id\":\"kama_trend_production-open-c2607-2\","
           "\"client_order_id\":\"kama_trend_production-0002-2\","
           "\"instrument_id\":\"c2607\",\"exchange_id\":\"DCE\","
           "\"side\":0,\"offset\":0,\"last_trade_volume\":1,"
           "\"avg_fill_price\":2321,\"order_ref\":\"0002\","
           "\"trade_id\":\"trade-new\",\"ts_ns\":1780291427262046132}\n";
    WriteFile(root / "wal" / "events.wal", wal.str());
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
    EXPECT_NE(html.find("SimNow Trading Cockpit"), std::string::npos);
    EXPECT_NE(html.find("Price Board"), std::string::npos);
    EXPECT_NE(html.find("System Status"), std::string::npos);
    EXPECT_NE(html.find("Orders And Fills"), std::string::npos);
    EXPECT_NE(html.find("missing_pid"), std::string::npos);
    EXPECT_NE(html.find("20260514"), std::string::npos);
}

TEST(SimnowDashboardCli, RendersStructuredDominantContractV2State) {
    const auto root = MakeTempDir("dominant_v2");
    const auto output = root / "dashboard";
    WriteFile(root / "ctp_instruments" / "c_dominant_contract.json",
              "{\n"
              "  \"schema_version\": 2,\n"
              "  \"trading_day\": \"20260719\",\n"
              "  \"product_id\": \"c\",\n"
              "  \"current_instrument_id\": \"c2609\",\n"
              "  \"candidate_instrument_id\": \"c2611\",\n"
              "  \"phase\": \"warming\",\n"
              "  \"generation\": 4,\n"
              "  \"selection_metric\": \"open_interest\",\n"
              "  \"lead_ratio\": 0.18,\n"
              "  \"eligible_count\": 3,\n"
              "  \"baseline_count\": 3,\n"
              "  \"broker_position\": 0,\n"
              "  \"broker_frozen\": 0,\n"
              "  \"active_open_orders\": 0,\n"
              "  \"active_close_orders\": 0,\n"
              "  \"warmup_observed_bars\": 12,\n"
              "  \"warmup_required_bars\": 30,\n"
              "  \"generation_rejections\": 2,\n"
              "  \"phase_started_ts_ns\": 1,\n"
              "  \"last_error\": \"\"\n"
              "}\n");
    WriteFile(root / "ctp_instruments" / "c_contracts.json",
              "{\"schema_version\":2,\"broker_trading_day\":\"20260719\"}\n");

    const int rc = RunCommandCapture(DashboardCommand(root, output), root / "stdout.log");
    EXPECT_EQ(rc, 0);
    const std::string state = ReadFile(output / "dashboard_state.json");
    EXPECT_NE(state.find("\"instrument_id\": \"c2609\""), std::string::npos);
    EXPECT_NE(state.find("\"candidate_instrument_id\": \"c2611\""), std::string::npos);
    EXPECT_NE(state.find("\"phase\": \"warming\""), std::string::npos);
    EXPECT_NE(state.find("\"generation\": 4"), std::string::npos);
    EXPECT_NE(state.find("\"warmup_observed_bars\": 12"), std::string::npos);
    EXPECT_NE(state.find("\"generation_rejections\": 2"), std::string::npos);
    EXPECT_NE(state.find("\"cache_trading_day_mismatch\": false"), std::string::npos);
    const std::string html = ReadFile(output / "index.html");
    EXPECT_NE(html.find("Dominant Contract State"), std::string::npos);
    EXPECT_NE(html.find("c2609"), std::string::npos);
    EXPECT_NE(html.find("12/30"), std::string::npos);
    EXPECT_NE(html.find("Generation Rejects"), std::string::npos);
    std::filesystem::remove_all(root);
}

TEST(SimnowDashboardCli, PriceBoardDoesNotRenderBarStatusAsBadge) {
    const auto root = MakeTempDir("price_board_stale_badges");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";

    WriteFile(root / "wal" / "events.wal", "");
    WriteStaleTwoProductMarketData(root);

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_EQ(CountOccurrences(html, "<span class=\"badge bad\">stale</span>"), 2U);
    EXPECT_EQ(html.find("Bar <span class=\"badge bad\">stale</span>"), std::string::npos);
    EXPECT_NE(html.find("Bar stale at 20260529 23:00"), std::string::npos);
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
    EXPECT_NE(json.find("\"wal_fills_raw\": 1"), std::string::npos);
    EXPECT_NE(json.find("\"wal_duplicate_fills\": 0"), std::string::npos);
    EXPECT_NE(json.find("\"last_error_id\": \"42\""), std::string::npos);
    EXPECT_NE(json.find("\"last_reject_reason\": \"settlement_unconfirmed\""), std::string::npos);
    EXPECT_NE(json.find("\"trade_fills\""), std::string::npos);
    EXPECT_NE(json.find("\"instrument_id\": \"c2607\""), std::string::npos);
    EXPECT_NE(json.find("\"side\": \"buy\""), std::string::npos);
    EXPECT_NE(json.find("\"offset\": \"open\""), std::string::npos);
    EXPECT_NE(json.find("\"price\": \"2320\""), std::string::npos);
    EXPECT_NE(json.find("\"exchange_id\": \"DCE\""), std::string::npos);
    EXPECT_NE(json.find("\"trade_id\": \"trade-1\""), std::string::npos);
    EXPECT_NE(json.find("\"strategy_id\": \"kama_trend_production\""), std::string::npos);
    EXPECT_NE(json.find("\"attribution\": \"strategy_matched\""), std::string::npos);
    EXPECT_NE(html.find("System Status"), std::string::npos);
    EXPECT_NE(html.find("Orders And Fills"), std::string::npos);
    EXPECT_NE(html.find("Recent Fills"), std::string::npos);
    EXPECT_NE(html.find("kama_trend_production"), std::string::npos);
    EXPECT_NE(html.find("2320"), std::string::npos);
    EXPECT_NE(html.find("buy"), std::string::npos);
    EXPECT_NE(html.find("open"), std::string::npos);
    EXPECT_EQ(html.find("Signal To CTP Flow"), std::string::npos);
    EXPECT_NE(html.find("结算结果未确认"), std::string::npos);
    EXPECT_EQ(html.find("????????"), std::string::npos);
    EXPECT_EQ(html.find("auth_code"), std::string::npos);
}

TEST(SimnowDashboardCli, RendersInternalOffsetCodesAsCloseNames) {
    const auto root = MakeTempDir("offset_codes");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto core_log = root / "runs" / "run_1" / "core_engine.log";

    WriteSampleCtpRuntime(root, core_log);
    WriteSampleCtpSignalMonitor(root);
    WriteFile(root / "wal" / "events.wal",
              "{\"seq\":1,\"schema_version\":2,\"kind\":\"trade\","
              "\"event_type\":\"trade_fill\","
              "\"strategy_id\":\"kama_trend_production\","
              "\"trace_id\":\"kama_trend_production-close-today\","
              "\"client_order_id\":\"kama_trend_production-close-today\","
              "\"instrument_id\":\"hc2610\",\"exchange_id\":\"SHFE\","
              "\"side\":1,\"offset\":2,\"last_trade_volume\":21,"
              "\"avg_fill_price\":3374,\"order_ref\":\"0001\","
              "\"trade_id\":\"trade-close-today\",\"ts_ns\":1781244400855416785}\n"
              "{\"seq\":2,\"schema_version\":2,\"kind\":\"trade\","
              "\"event_type\":\"trade_fill\","
              "\"strategy_id\":\"kama_trend_production\","
              "\"trace_id\":\"kama_trend_production-close-yesterday\","
              "\"client_order_id\":\"kama_trend_production-close-yesterday\","
              "\"instrument_id\":\"hc2610\",\"exchange_id\":\"SHFE\","
              "\"side\":0,\"offset\":3,\"last_trade_volume\":17,"
              "\"avg_fill_price\":3372,\"order_ref\":\"0002\","
              "\"trade_id\":\"trade-close-yesterday\",\"ts_ns\":1781228444190765562}\n");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    EXPECT_NE(json.find("\"trade_id\": \"trade-close-today\""), std::string::npos);
    EXPECT_NE(json.find("\"offset\": \"close_today\""), std::string::npos);
    EXPECT_NE(json.find("\"trade_id\": \"trade-close-yesterday\""), std::string::npos);
    EXPECT_NE(json.find("\"offset\": \"close_yesterday\""), std::string::npos);
    EXPECT_EQ(json.find("\"offset\": \"2\""), std::string::npos);
    EXPECT_EQ(json.find("\"offset\": \"3\""), std::string::npos);
}

TEST(SimnowDashboardCli, DeduplicatesCtpReplayFillsAcrossRuns) {
    const auto root = MakeTempDir("ctp_replay_dedup");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto run_id = std::string("simnow-auto-20260602-day_pm-150000-r2");
    const auto run_dir = root / "runs" / run_id;
    const auto core_log = run_dir / "core_engine.log";

    WriteSampleCtpRuntime(root, core_log);
    WriteSampleCtpSignalMonitor(root);
    WriteFile(root / "runs" / "current_run_dir", run_dir.string() + "\n");
    WriteCtpReplayDuplicateWal(root, run_id);

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"wal_fills\": 1"), std::string::npos);
    EXPECT_NE(json.find("\"wal_fills_raw\": 2"), std::string::npos);
    EXPECT_NE(json.find("\"wal_duplicate_fills\": 1"), std::string::npos);
    EXPECT_NE(json.find("\"replay_duplicate_fills\""), std::string::npos);
    EXPECT_NE(json.find("\"trade_id\": \"trade-new\""), std::string::npos);
    EXPECT_NE(json.find("\"trade_id\": \"trade-dup\""), std::string::npos);
    EXPECT_NE(html.find("Recent Fills"), std::string::npos);
    EXPECT_NE(html.find("1 replay duplicates filtered"), std::string::npos);
    EXPECT_NE(html.find("c2607"), std::string::npos);
    EXPECT_NE(html.find("2321"), std::string::npos);
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

TEST(SimnowDashboardCli, LiveHealthIgnoresStaleSignalIncidentBeforeCurrentCoreEpoch) {
    const auto root = MakeTempDir("stale_signal_incident_core_epoch");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto run_dir = root / "runs" / "simnow-auto-20260528-day_pm-142549-r2";
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
              "{\"ts\":\"2026-05-28T11:09:47+08:00\",\"event\":\"monitor_started\","
              "\"message\":\"SimNow signal execution monitor started\"}\n"
              "{\"ts\":\"2026-05-28T11:09:48+08:00\",\"event\":\"order_submitted\","
              "\"trace_id\":\"old-take-profit\",\"message\":\"submitted\"}\n"
              "{\"ts\":\"2026-05-28T11:09:54+08:00\",\"event\":\"incident\","
              "\"trace_id\":\"old-take-profit\",\"message\":\"order_canceled_without_fill\"}\n"
              "{\"ts\":\"2026-05-28T14:25:49+08:00\",\"event\":\"core_engine_running\","
              "\"message\":\"pid=12345\",\"pid\":\"12345\"}\n"
              "{\"ts\":\"2026-05-28T14:26:53+08:00\",\"event\":\"summary\","
              "\"message\":\"signals=18 active=0 filled=17 incidents=1\","
              "\"signals\":18,\"active\":0,\"filled\":17,\"incidents\":1}\n");
    WriteFile(root / "monitor" / "incidents" / "20260528T110954-old-take-profit.md",
              "# old incident\n");
    WriteFile(root / "wal" / "events.wal", "");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"live_healthy\": true"), std::string::npos);
    EXPECT_NE(json.find("\"overall_healthy\": true"), std::string::npos);
    EXPECT_NE(json.find("\"incidents\": 0"), std::string::npos);
    EXPECT_NE(json.find("\"monitor_incidents\": 0"), std::string::npos);
    EXPECT_EQ(json.find("old-take-profit"), std::string::npos);
    EXPECT_NE(html.find("live healthy"), std::string::npos);
    EXPECT_EQ(html.find("live unhealthy"), std::string::npos);
    EXPECT_EQ(html.find("old-take-profit"), std::string::npos);
}

TEST(SimnowDashboardCli, LiveHealthIgnoresStaleSignalMonitorPidWhenOnlyHistoryExists) {
    const auto root = MakeTempDir("stale_signal_monitor_pid");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto run_dir = root / "runs" / "simnow-auto-20260528-day_pm-142549-r3";
    const auto core_log = run_dir / "core_engine.log";

    WriteSampleMarketData(root);
    WriteFile(root / "ctp_instruments" / "c_dominant_contract.json",
              "{\"product_id\":\"c\",\"instrument_id\":\"c2607\","
              "\"exchange_id\":\"DCE\",\"selection_metric\":\"open_interest\"}\n");
    WriteFile(root / "runs" / "current_core_engine.pid", std::to_string(getpid()) + "\n");
    WriteFile(root / "runs" / "current_run_dir", run_dir.string() + "\n");
    WriteFile(root / "runs" / "current_core_engine_log", core_log.string() + "\n");
    WriteFile(root / "runs" / "signal_execution_monitor.pid", "999999\n");
    WriteFile(core_log,
              "ts_ns=1 level=info app=core_engine event=ctp_td_front_connected\n"
              "ts_ns=2 level=info app=core_engine event=ctp_md_front_connected\n"
              "ts_ns=3 level=info app=core_engine event=ctp_login_response error_id=0\n"
              "ts_ns=4 level=info app=core_engine event=ctp_settlement_confirmed "
              "settlement_confirm_required=true\n");
    WriteFile(root / "monitor" / "signal_execution_watch.jsonl",
              "{\"ts\":\"2026-05-28T14:25:49+08:00\",\"event\":\"monitor_started\","
              "\"message\":\"SimNow signal execution monitor started\"}\n"
              "{\"ts\":\"2026-05-28T14:26:53+08:00\",\"event\":\"summary\","
              "\"message\":\"signals=0 active=0 filled=0 incidents=0\","
              "\"signals\":0,\"active\":0,\"filled\":0,\"incidents\":0}\n");
    WriteFile(root / "wal" / "events.wal", "");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"live_healthy\": true"), std::string::npos);
    EXPECT_NE(json.find("\"status\": \"history_only\""), std::string::npos);
    EXPECT_NE(json.find("\"live_warning_count\": 0"), std::string::npos);
    EXPECT_NE(html.find("live healthy"), std::string::npos);
    EXPECT_EQ(html.find("live unhealthy"), std::string::npos);
}

TEST(SimnowDashboardCli, LiveHealthTreatsStartupCtpConnectionLossAsRecovered) {
    const auto root = MakeTempDir("startup_ctp_connection_loss_recovered");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto run_dir = root / "runs" / "simnow-auto-20260528-day_pm-142549-r4";
    const auto core_log = run_dir / "core_engine.log";

    WriteSampleMarketData(root);
    WriteFile(root / "ctp_instruments" / "c_dominant_contract.json",
              "{\"product_id\":\"c\",\"instrument_id\":\"c2607\","
              "\"exchange_id\":\"DCE\",\"selection_metric\":\"open_interest\"}\n");
    WriteFile(root / "runs" / "current_core_engine.pid", std::to_string(getpid()) + "\n");
    WriteFile(root / "runs" / "current_run_dir", run_dir.string() + "\n");
    WriteFile(root / "runs" / "current_core_engine_log", core_log.string() + "\n");
    WriteFile(core_log,
              "ts_ns=1 level=warn app=ctp_trader_adapter event=ctp_trader_connection_lost "
              "previous_state=\"disconnected\" will_reconnect=\"false\"\n"
              "ts_ns=2 level=info app=ctp_gateway_adapter "
              "event=ctp_front_candidate_connect_success "
              "md_front=\"tcp://md.example:30011\" td_front=\"tcp://td.example:30001\"\n"
              "ts_ns=3 level=info app=core_engine event=ctp_settlement_confirmed "
              "settlement_confirm_required=\"true\"\n");
    WriteFile(root / "monitor" / "signal_execution_watch.jsonl",
              "{\"ts\":\"2026-05-28T14:25:49+08:00\",\"event\":\"monitor_started\","
              "\"message\":\"SimNow signal execution monitor started\"}\n"
              "{\"ts\":\"2026-05-28T14:26:53+08:00\",\"event\":\"summary\","
              "\"message\":\"signals=0 active=0 filled=0 incidents=0\","
              "\"signals\":0,\"active\":0,\"filled\":0,\"incidents\":0}\n");
    WriteFile(root / "wal" / "events.wal", "");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"live_healthy\": true"), std::string::npos);
    EXPECT_NE(json.find("\"status\": \"connected\""), std::string::npos);
    EXPECT_NE(json.find("\"td_front\": \"connected\""), std::string::npos);
    EXPECT_NE(json.find("\"md_front\": \"connected\""), std::string::npos);
    EXPECT_NE(json.find("\"ctp_errors\": 0"), std::string::npos);
    EXPECT_NE(json.find("\"live_warning_count\": 0"), std::string::npos);
    EXPECT_NE(html.find("live healthy"), std::string::npos);
    EXPECT_EQ(html.find("live unhealthy"), std::string::npos);
}

TEST(SimnowDashboardCli, LiveHealthTreatsCommodityDayCloseMarketDataAsClosed) {
    const auto root = MakeTempDir("day_close_market");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto run_dir = root / "runs" / "simnow-auto-20260528-day_pm-150335-r2";
    const auto core_log = run_dir / "core_engine.log";

    WriteDayCloseMarketData(root);
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
              "{\"ts\":\"2026-05-28T15:03:35+08:00\",\"event\":\"monitor_started\","
              "\"message\":\"SimNow signal execution monitor started\"}\n"
              "{\"ts\":\"2026-05-28T15:03:55+08:00\",\"event\":\"summary\","
              "\"message\":\"signals=0 active=0 filled=0 incidents=0\","
              "\"signals\":0,\"active\":0,\"filled\":0,\"incidents\":0}\n");
    WriteFile(root / "wal" / "events.wal", "");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"live_healthy\": true"), std::string::npos);
    EXPECT_NE(json.find("\"tick_status\": \"closed\""), std::string::npos);
    EXPECT_NE(json.find("\"bar_status\": \"closed\""), std::string::npos);
    EXPECT_NE(html.find("live healthy"), std::string::npos);
    EXPECT_NE(html.find("closed"), std::string::npos);
    EXPECT_EQ(html.find("live unhealthy"), std::string::npos);
}

TEST(SimnowDashboardCli, LiveHealthTreatsPostCloseTickAndDayCloseBarAsClosed) {
    const auto root = MakeTempDir("post_close_tick");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto run_dir = root / "runs" / "simnow-auto-20260528-day_pm-150335-r2";
    const auto core_log = run_dir / "core_engine.log";

    WriteDayCloseMarketData(root);
    WritePostCloseTick(root);
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
              "{\"ts\":\"2026-05-28T15:03:35+08:00\",\"event\":\"monitor_started\","
              "\"message\":\"SimNow signal execution monitor started\"}\n"
              "{\"ts\":\"2026-05-28T15:03:55+08:00\",\"event\":\"summary\","
              "\"message\":\"signals=0 active=0 filled=0 incidents=0\","
              "\"signals\":0,\"active\":0,\"filled\":0,\"incidents\":0}\n");
    WriteFile(root / "wal" / "events.wal", "");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"live_healthy\": true"), std::string::npos);
    EXPECT_NE(json.find("\"tick_status\": \"closed\""), std::string::npos);
    EXPECT_NE(json.find("\"bar_status\": \"closed\""), std::string::npos);
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

TEST(SimnowDashboardCli, PositionsRenderPriceLevelsFromStrategyState) {
    const auto root = MakeTempDir("position_price_levels");
    const auto output_dir = root / "dashboard";
    const auto state_dir = root / "state";
    const auto stdout_log = root / "stdout.log";

    WriteFile(root / "wal" / "events.wal", "");
    // A persisted strategy state carrying a live net position plus the flat
    // per-instrument risk price levels surfaced for the dashboard.
    WriteFile(state_dir / "strategy_state__kama_trend_production.json",
              "{\n"
              "  \"saved_epoch_seconds\": 1780291427,\n"
              "  \"account_id\": \"acct\",\n"
              "  \"strategy_id\": \"kama_trend_production\",\n"
              "  \"state\": {\n"
              "    \"net_pos.rb\": \"3\",\n"
              "    \"avg_open.rb\": \"4500.500000\",\n"
              "    \"init_stop.rb\": \"4460.000000\",\n"
              "    \"trailing_stop.rb\": \"4488.250000\",\n"
              "    \"take_profit.rb\": \"4560.750000\",\n"
              "    \"atomic.kama_trend_production.trailing_stop.count\": \"1\",\n"
              "    \"atomic.kama_trend_production.trailing_stop.0.instrument\": \"rb\",\n"
              "    \"atomic.kama_trend_production.trailing_stop.0.price\": \"9999.000000\"\n"
              "  }\n"
              "}\n");

    const std::string command =
        DashboardCommand(root, output_dir) + " --state-dir \"" + state_dir.string() + "\"";
    const int rc = RunCommandCapture(command, stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");

    // JSON exposes the four price levels for the live position.
    EXPECT_NE(json.find("\"instrument_id\": \"rb\""), std::string::npos);
    EXPECT_NE(json.find("\"avg_open\": 4500.5000"), std::string::npos);
    EXPECT_NE(json.find("\"initial_stop\": 4460.0000"), std::string::npos);
    EXPECT_NE(json.find("\"trailing_stop\": 4488.2500"), std::string::npos);
    EXPECT_NE(json.find("\"take_profit\": 4560.7500"), std::string::npos);

    // HTML table gains the new price columns with formatted values.
    EXPECT_NE(html.find("Init Stop"), std::string::npos);
    EXPECT_NE(html.find("Take Profit"), std::string::npos);
    EXPECT_NE(html.find("Trailing Stop"), std::string::npos);
    EXPECT_NE(html.find("4500.50"), std::string::npos);
    EXPECT_NE(html.find("4460.00"), std::string::npos);
    EXPECT_NE(html.find("4488.25"), std::string::npos);
    EXPECT_NE(html.find("4560.75"), std::string::npos);
    // The nested atomic trailing-stop value must not leak into the position row.
    EXPECT_EQ(html.find("9999.00"), std::string::npos);
}

TEST(SimnowDashboardCli, StructuredReadinessOverridesLogInference) {
    const auto root = MakeTempDir("structured_readiness");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    const auto run_dir = root / "runs" / "simnow-structured-readiness";
    const auto core_log = run_dir / "core_engine.log";

    WriteFile(root / "runs" / "current_core_engine.pid", std::to_string(getpid()) + "\n");
    WriteFile(root / "runs" / "current_run_dir", run_dir.string() + "\n");
    WriteFile(root / "runs" / "current_core_engine_log", core_log.string() + "\n");
    WriteFile(core_log,
              "ts_ns=1 level=error app=core_engine event=ctp_td_front_disconnected\n"
              "ts_ns=2 level=error app=core_engine event=ctp_settlement_unconfirmed\n");
    WriteFile(root / "monitor" / "readiness.json",
              "{\"schema_version\":2,\"heartbeat_ts_ns\":1784419200000000000,"
              "\"mode\":\"Ready\",\"generation\":7,\"recovery_complete\":true,"
              "\"trader_ready\":true,\"gateway_healthy\":true,"
              "\"settlement_confirmed\":true,\"pending_exit_count\":1,"
              "\"unresolved_mapping_count\":0}\n");
    WriteFile(root / "wal" / "events.wal", "");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0) << ReadFile(stdout_log);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    EXPECT_NE(json.find("\"readiness\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"mode\": \"Ready\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"generation\": 7"), std::string::npos) << json;
    EXPECT_NE(json.find("\"pending_exit_count\": 1"), std::string::npos) << json;
    EXPECT_NE(json.find("\"status\": \"connected\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"settlement_status\": \"confirmed\""), std::string::npos) << json;
}

TEST(SimnowDashboardCli, PipelineV3IsAuthoritativeAndRendersFullChain) {
    const auto root = MakeTempDir("pipeline_v3");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    WriteFile(root / "runs" / "current_core_engine.pid", std::to_string(getpid()) + "\n");
    WriteFile(root / "wal" / "events.wal", "");
    WriteHealthyReadiness(root);
    WritePipelineHealth(root, "healthy");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    ASSERT_EQ(rc, 0) << ReadFile(stdout_log);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"pipeline_health\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"authoritative\": true"), std::string::npos) << json;
    EXPECT_NE(json.find("\"status\": \"healthy\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"product_id\": \"c\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"trace_id\": \"trace-1\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"overall_healthy\": true"), std::string::npos) << json;
    EXPECT_NE(html.find("Pipeline Stages"), std::string::npos) << html;
    EXPECT_NE(html.find("Product Pipeline Health"), std::string::npos) << html;
    EXPECT_NE(html.find("Recent Execution Traces"), std::string::npos) << html;
    EXPECT_NE(html.find("candidate_to_disposition"), std::string::npos) << html;
    EXPECT_EQ(html.find("Legacy Monitoring"), std::string::npos) << html;
}

TEST(SimnowDashboardCli, PipelineInactiveIsHealthyWithoutLiveCore) {
    const auto root = MakeTempDir("pipeline_inactive");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    WriteFile(root / "wal" / "events.wal", "");
    WritePipelineHealth(root, "inactive", "inactive", "outside_trading_session");

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    ASSERT_EQ(rc, 0) << ReadFile(stdout_log);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"status\": \"inactive\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"overall_healthy\": true"), std::string::npos) << json;
    EXPECT_NE(html.find("inactive"), std::string::npos) << html;
}

TEST(SimnowDashboardCli, StrictExitAllowsDegradedPipeline) {
    const auto root = MakeTempDir("pipeline_degraded_strict");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    WriteFile(root / "runs" / "current_core_engine.pid", std::to_string(getpid()) + "\n");
    WriteFile(root / "wal" / "events.wal", "");
    WriteHealthyReadiness(root);
    WritePipelineHealth(root, "degraded", "degraded", "candidate_tick_baseline_incomplete");

    const int rc =
        RunCommandCapture(DashboardCommand(root, output_dir) + " --strict-exit", stdout_log);

    ASSERT_EQ(rc, 0) << ReadFile(stdout_log);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    EXPECT_NE(json.find("\"live_status\": \"degraded\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"overall_healthy\": false"), std::string::npos) << json;
}

TEST(SimnowDashboardCli, V3HeartbeatWithoutSnapshotCannotFallBackToLegacy) {
    const auto root = MakeTempDir("pipeline_missing");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    WriteFile(root / "monitor" / "heartbeat.json",
              "{\"schema_version\":3,\"pipeline_status\":\"healthy\"}\n");
    WriteFile(root / "wal" / "events.wal", "");

    const int rc =
        RunCommandCapture(DashboardCommand(root, output_dir) + " --strict-exit", stdout_log);

    EXPECT_NE(rc, 0) << ReadFile(stdout_log);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    EXPECT_NE(json.find("\"authoritative\": true"), std::string::npos) << json;
    EXPECT_NE(json.find("\"status\": \"missing\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"overall_healthy\": false"), std::string::npos) << json;
}

TEST(SimnowDashboardCli, V3HeartbeatWithLegacySnapshotCannotFallBackToLegacy) {
    const auto root = MakeTempDir("pipeline_legacy_with_v3_heartbeat");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    WriteFile(root / "monitor" / "heartbeat.json",
              "{\"schema_version\":3,\"pipeline_status\":\"healthy\"}\n");
    WriteFile(root / "monitor" / "pipeline_health.json",
              "{\"schema_version\":2,\"overall_status\":\"healthy\"}\n");
    WriteFile(root / "wal" / "events.wal", "");

    const int rc =
        RunCommandCapture(DashboardCommand(root, output_dir) + " --strict-exit", stdout_log);

    EXPECT_NE(rc, 0) << ReadFile(stdout_log);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    EXPECT_NE(json.find("\"authoritative\": true"), std::string::npos) << json;
    EXPECT_NE(json.find("\"status\": \"invalid\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"live_status\": \"unhealthy\""), std::string::npos) << json;
}

TEST(SimnowDashboardCli, PipelineContentIsHtmlEscaped) {
    const auto root = MakeTempDir("pipeline_escape");
    const auto output_dir = root / "dashboard";
    const auto stdout_log = root / "stdout.log";
    WriteFile(root / "wal" / "events.wal", "");
    WritePipelineHealth(root, "degraded", "degraded", "<script>alert(1)</script>");

    ASSERT_EQ(RunCommandCapture(DashboardCommand(root, output_dir), stdout_log), 0)
        << ReadFile(stdout_log);
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_EQ(html.find("<script>alert(1)</script>"), std::string::npos) << html;
    EXPECT_NE(html.find("&lt;script&gt;alert(1)&lt;/script&gt;"), std::string::npos) << html;
}

}  // namespace
