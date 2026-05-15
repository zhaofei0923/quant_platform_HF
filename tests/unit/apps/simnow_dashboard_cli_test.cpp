#include <gtest/gtest.h>

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
           " --output-dir \"" +
           output_dir.string() + "\"";
}

void WriteSampleMarketData(const std::filesystem::path& root) {
    WriteFile(root / "market" / "run_1" / "varieties" / "c" / "market" / "ticks.csv",
              "instrument_id,exchange_id,trading_day,action_day,update_time,update_millisec,"
              "last_price,bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,volume,"
              "open_interest,settlement_price,average_price_raw,average_price_norm,"
              "is_valid_settlement,exchange_ts_ns,recv_ts_ns\n"
              "c2607,DCE,20260515,20260514,21:00:01,0,2365,2364,2365,91,14,2821,"
              "1223500,0,23658,23658,0,0,1778763619820649828\n");
    WriteFile(root / "market" / "run_1" / "varieties" / "c" / "market" / "bars_1m.csv",
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

    const int rc = RunCommandCapture(DashboardCommand(root, output_dir), stdout_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_dir / "dashboard_state.json");
    const std::string html = ReadFile(output_dir / "index.html");
    EXPECT_NE(json.find("\"status\": \"missing_pid\""), std::string::npos);
    EXPECT_NE(json.find("\"wal_order_events\": 0"), std::string::npos);
    EXPECT_NE(json.find("\"trading_day\": \"20260514\""), std::string::npos);
    EXPECT_NE(json.find("\"critical_alerts\": 1"), std::string::npos);
    EXPECT_NE(json.find("<redacted>"), std::string::npos);
    EXPECT_NE(html.find("SimNow Dashboard"), std::string::npos);
    EXPECT_NE(html.find("missing_pid"), std::string::npos);
    EXPECT_NE(html.find("20260514"), std::string::npos);
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
