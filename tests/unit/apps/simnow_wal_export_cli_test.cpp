#include <gtest/gtest.h>

#include <algorithm>
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

void WriteFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    out << content;
}

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto base =
        std::filesystem::temp_directory_path() / ("quant_hft_simnow_wal_export_cli_test_" + suffix);
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);
    return base;
}

int RunCommandCapture(const std::string& command, const std::filesystem::path& output_file) {
    const std::string shell_command = command + " > \"" + output_file.string() + "\" 2>&1";
    return std::system(shell_command.c_str());
}

// One WAL trade_fill line. offset: 0=open, 1=close, 2=close_today.
std::string TradeFillLine(std::int64_t seq, const std::string& instrument,
                          const std::string& exchange, int side, int offset, int volume,
                          double price, const std::string& trade_id) {
    std::ostringstream out;
    out << "{\"seq\":" << seq
        << ",\"schema_version\":2,\"kind\":\"trade\",\"event_type\":\"trade_fill\","
        << "\"run_id\":\"simnow-auto-20260710-day_am-085018-r1\",\"trading_day\":\"20260710\","
        << "\"account_id\":\"1\","
        << "\"strategy_id\":\"s\",\"client_order_id\":\"c" << seq << "\","
        << "\"instrument_id\":\"" << instrument << "\",\"exchange_id\":\"" << exchange << "\","
        << "\"side\":" << side << ",\"offset\":" << offset << ",\"status\":3,"
        << "\"filled_volume\":" << volume << ",\"last_trade_volume\":" << volume << ","
        << "\"avg_fill_price\":" << price << ",\"trade_id\":\"" << trade_id << "\","
        << "\"event_source\":\"OnRtnTrade\",\"ts_ns\":" << seq << "}";
    return out.str();
}

std::string ExportCommand(const std::filesystem::path& root,
                          const std::filesystem::path& fee_config) {
    return "\"" + BinaryPath("simnow_wal_export_cli").string() +
           "\""
           " --trading-day 20260710"
           " --wal-file \"" +
           (root / "events.wal").string() +
           "\""
           " --export-root \"" +
           (root / "exports").string() +
           "\""
           " --reconcile-root \"" +
           (root / "reconcile").string() +
           "\""
           " --summary-json \"" +
           (root / "summary.json").string() +
           "\""
           " --summary-md \"" +
           (root / "summary.md").string() +
           "\""
           " --reconcile-json \"" +
           (root / "reconcile.json").string() +
           "\""
           " --reconcile-md \"" +
           (root / "reconcile.md").string() +
           "\""
           " --fee-config \"" +
           fee_config.string() + "\"";
}

// Fee config: RB volume_multiple 10; open by-money 0.0001; close by-volume 1.5.
constexpr const char* kFeeConfig = R"({
  "RB": {
    "product": "RB",
    "volume_multiple": 10,
    "long_margin_ratio": 0.1,
    "short_margin_ratio": 0.1,
    "commission": {
      "open_ratio_by_money": 0.0001,
      "open_ratio_by_volume": 0.0,
      "close_ratio_by_money": 0.0,
      "close_ratio_by_volume": 1.5,
      "close_today_ratio_by_money": 0.0,
      "close_today_ratio_by_volume": 3.0
    }
  }
})";

}  // namespace

TEST(SimnowWalExportCli, ComputesCommissionFromFeeConfig) {
    const auto root = MakeTempDir("commission");
    const auto stdout_log = root / "stdout.log";

    // Open 2 lots of rb at 4000, then close 2 lots at 4010.
    std::ostringstream wal;
    wal << TradeFillLine(1, "rb2405", "SHFE", 0, 0, 2, 4000.0, "T1") << '\n'
        << TradeFillLine(2, "rb2405", "SHFE", 1, 1, 2, 4010.0, "T2") << '\n';
    WriteFile(root / "events.wal", wal.str());

    const auto fee_config = root / "fee.json";
    WriteFile(fee_config, kFeeConfig);

    const int rc = RunCommandCapture(ExportCommand(root, fee_config), stdout_log);
    ASSERT_EQ(rc, 0) << ReadFile(stdout_log);

    const std::string json = ReadFile(root / "summary.json");
    // Open commission: notional 4000*2*10=80000 * 0.0001 = 8.0
    // Close commission: 2 lots * 1.5 = 3.0
    // total = 11.0
    EXPECT_NE(json.find("\"total_commission\": 11"), std::string::npos) << json;
    EXPECT_NE(json.find("\"unpriced_fills\": 0"), std::string::npos) << json;
    EXPECT_NE(json.find("\"fee_config_loaded\": true"), std::string::npos) << json;
    EXPECT_NE(json.find("\"commission_by_instrument\": {\"rb2405\":11"), std::string::npos) << json;

    // trade_fills.csv must contain the commission column.
    const std::string csv = ReadFile(root / "exports" / "20260710" / "trade_fills.csv");
    EXPECT_NE(csv.find("avg_fill_price,commission,event_source"), std::string::npos) << csv;
}

TEST(SimnowWalExportCli, CountsUnpricedFillsWhenInstrumentMissing) {
    const auto root = MakeTempDir("unpriced");
    const auto stdout_log = root / "stdout.log";

    // Instrument "xx9999" is not in the fee config.
    WriteFile(root / "events.wal", TradeFillLine(1, "xx9999", "SHFE", 0, 0, 1, 100.0, "T1") + "\n");

    const auto fee_config = root / "fee.json";
    WriteFile(fee_config, kFeeConfig);

    const int rc = RunCommandCapture(ExportCommand(root, fee_config), stdout_log);
    ASSERT_EQ(rc, 0) << ReadFile(stdout_log);

    const std::string json = ReadFile(root / "summary.json");
    EXPECT_NE(json.find("\"total_commission\": 0"), std::string::npos) << json;
    EXPECT_NE(json.find("\"unpriced_fills\": 1"), std::string::npos) << json;
}

TEST(SimnowWalExportCli, DeduplicatesCanonicalTradeBeforePricingAndExport) {
    const auto root = MakeTempDir("canonical_dedup");
    const auto stdout_log = root / "stdout.log";

    std::ostringstream wal;
    wal << TradeFillLine(1, "rb2405", "SHFE", 0, 0, 2, 4000.0, "T1") << '\n'
        << TradeFillLine(2, "rb2405", "SHFE", 0, 0, 2, 4000.0, "T1") << '\n';
    WriteFile(root / "events.wal", wal.str());
    const auto fee_config = root / "fee.json";
    WriteFile(fee_config, kFeeConfig);

    const int rc = RunCommandCapture(ExportCommand(root, fee_config), stdout_log);
    ASSERT_EQ(rc, 0) << ReadFile(stdout_log);

    const std::string summary = ReadFile(root / "summary.json");
    EXPECT_NE(summary.find("\"raw_trade_fill_records\": 2"), std::string::npos) << summary;
    EXPECT_NE(summary.find("\"trade_fills\": 1"), std::string::npos) << summary;
    EXPECT_NE(summary.find("\"duplicate_trade_fills\": 1"), std::string::npos) << summary;
    EXPECT_NE(summary.find("\"total_commission\": 8"), std::string::npos) << summary;

    const std::string csv = ReadFile(root / "exports" / "20260710" / "trade_fills.csv");
    EXPECT_EQ(std::count(csv.begin(), csv.end(), '\n'), 2) << csv;
}

TEST(SimnowWalExportCli, MissingTradeIdIsUnresolvedAndStrictReconcileFailsWhenDbNotQueried) {
    const auto root = MakeTempDir("unresolved_strict");
    const auto stdout_log = root / "stdout.log";
    WriteFile(root / "events.wal", TradeFillLine(1, "rb2405", "SHFE", 0, 0, 1, 4000.0, "") + "\n");
    const auto fee_config = root / "fee.json";
    WriteFile(fee_config, kFeeConfig);

    const int rc =
        RunCommandCapture(ExportCommand(root, fee_config) + " --strict-reconcile 1", stdout_log);
    ASSERT_NE(rc, 0) << ReadFile(stdout_log);

    const std::string summary = ReadFile(root / "summary.json");
    EXPECT_NE(summary.find("\"trade_fills\": 0"), std::string::npos) << summary;
    EXPECT_NE(summary.find("\"unresolved_trade_fills\": 1"), std::string::npos) << summary;

    const std::string reconcile = ReadFile(root / "reconcile.json");
    EXPECT_NE(reconcile.find("\"status\": \"incomplete\""), std::string::npos) << reconcile;
    EXPECT_NE(reconcile.find("\"complete\": false"), std::string::npos) << reconcile;
    EXPECT_NE(reconcile.find("\"db_queried\": false"), std::string::npos) << reconcile;
}
