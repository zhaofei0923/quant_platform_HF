#include "quant_hft/apps/backtest_replay_support.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <vector>

namespace quant_hft::apps {
namespace {

std::vector<std::string> ReadDataLines(const std::filesystem::path& path) {
    std::ifstream in(path);
    std::vector<std::string> lines;
    std::string line;
    bool skip_header = true;
    while (std::getline(in, line)) {
        if (skip_header) {
            skip_header = false;
            continue;
        }
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    return lines;
}

std::string ReadFileText(const std::filesystem::path& path) {
    std::ifstream in(path);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

TradeRecord MakeTrade(std::int64_t fill_seq, const std::string& trade_id, EpochNanos timestamp_ns,
                      const std::string& timestamp_dt_utc) {
    TradeRecord row;
    row.fill_seq = fill_seq;
    row.trade_id = trade_id;
    row.order_id = "order-" + trade_id;
    row.symbol = "rb2405";
    row.side = "Buy";
    row.offset = "Open";
    row.volume = 1;
    row.price = 100.0;
    row.timestamp_ns = timestamp_ns;
    row.timestamp_dt_utc = timestamp_dt_utc;
    row.strategy_id = "s1";
    row.signal_type = "kOpen";
    row.regime_at_entry = "kUnknown";
    return row;
}

OrderRecord MakeOrder(std::int64_t order_seq, const std::string& order_id, EpochNanos created_at_ns,
                      EpochNanos last_update_ns) {
    OrderRecord row;
    row.order_seq = order_seq;
    row.order_id = order_id;
    row.client_order_id = "client-" + order_id;
    row.symbol = "rb2405";
    row.type = "Market";
    row.side = "Buy";
    row.offset = "Open";
    row.price = 100.0;
    row.volume = 1;
    row.status = "Filled";
    row.filled_volume = 1;
    row.avg_fill_price = 100.0;
    row.created_at_ns = created_at_ns;
    row.created_at_dt_utc = "2024-01-03 14:55:00";
    row.last_update_ns = last_update_ns;
    row.last_update_dt_utc = "2024-01-03 14:58:57";
    row.strategy_id = "s1";
    return row;
}

}  // namespace

TEST(BacktestResultExportTest, TradesCsvUsesChronologicalOrderingWhenFillSequenceDiffers) {
    BacktestCliResult result;
    result.spec.emit_trades = true;
    result.trades = {
        MakeTrade(2, "trade-2", 100, "2024-01-03 14:55:00"),
        MakeTrade(1, "trade-1", 200, "2024-01-03 14:58:57"),
    };

    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::filesystem::path out_dir =
        std::filesystem::temp_directory_path() /
        ("quant_hft_backtest_export_test_" + std::to_string(stamp));

    std::string error;
    ASSERT_TRUE(ExportBacktestCsv(result, out_dir.string(), &error)) << error;

    const std::vector<std::string> lines = ReadDataLines(out_dir / "trades.csv");
    ASSERT_EQ(lines.size(), 2U);
    EXPECT_NE(lines[0].find("trade-2"), std::string::npos);
    EXPECT_NE(lines[1].find("trade-1"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(out_dir, ec);
}

TEST(BacktestResultExportTest, OrdersCsvUsesExecutionTimestampWhenSequenceDiffers) {
    BacktestCliResult result;
    result.spec.emit_orders = true;
    result.orders = {
        MakeOrder(2, "order-2", 100, 100),
        MakeOrder(1, "order-1", 200, 200),
    };

    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::filesystem::path out_dir =
        std::filesystem::temp_directory_path() /
        ("quant_hft_backtest_export_orders_test_" + std::to_string(stamp));

    std::string error;
    ASSERT_TRUE(ExportBacktestCsv(result, out_dir.string(), &error)) << error;

    const std::vector<std::string> lines = ReadDataLines(out_dir / "orders.csv");
    ASSERT_EQ(lines.size(), 2U);
    EXPECT_NE(lines[0].find("order-2"), std::string::npos);
    EXPECT_NE(lines[1].find("order-1"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(out_dir, ec);
}

TEST(BacktestResultExportTest, TradesCsvSortsByTradingDaySessionAndTimestamp) {
    BacktestCliResult result;
    result.spec.emit_trades = true;
    result.trades = {
        MakeTrade(1,
                  "trade-day",
                  detail::ToEpochNs("20240110", "09:44:11", 0),
                  "2024-01-10 09:44:11"),
        MakeTrade(2,
                  "trade-night",
                  detail::ToEpochNs("20240110", "22:51:06", 0),
                  "2024-01-09 22:51:06"),
    };
    result.trades[0].signal_ts_ns = result.trades[0].timestamp_ns;
    result.trades[0].trading_day = "20240110";
    result.trades[0].action_day = "20240110";
    result.trades[0].update_time = "09:44:11";
    result.trades[0].timestamp_dt_local = "2024-01-10 09:44:11";
    result.trades[1].signal_ts_ns = result.trades[1].timestamp_ns;
    result.trades[1].trading_day = "20240110";
    result.trades[1].action_day = "20240109";
    result.trades[1].update_time = "22:51:06";
    result.trades[1].timestamp_dt_local = "2024-01-09 22:51:06";

    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::filesystem::path out_dir =
        std::filesystem::temp_directory_path() /
        ("quant_hft_backtest_export_trade_day_sort_test_" + std::to_string(stamp));

    std::string error;
    ASSERT_TRUE(ExportBacktestCsv(result, out_dir.string(), &error)) << error;

    const std::vector<std::string> lines = ReadDataLines(out_dir / "trades.csv");
    ASSERT_EQ(lines.size(), 2U);
    EXPECT_NE(lines[0].find("trade-night"), std::string::npos);
    EXPECT_NE(lines[1].find("trade-day"), std::string::npos);

    const std::string csv_text = ReadFileText(out_dir / "trades.csv");
    EXPECT_NE(csv_text.find("signal_ts_ns"), std::string::npos);
    EXPECT_NE(csv_text.find("trading_day"), std::string::npos);
    EXPECT_NE(csv_text.find("action_day"), std::string::npos);
    EXPECT_NE(csv_text.find("timestamp_dt_local"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(out_dir, ec);
}

TEST(BacktestResultExportTest, RenderBacktestJsonSortsTradesAndOrdersChronologically) {
    BacktestCliResult result;
    result.trades = {
        MakeTrade(2, "trade-2", 100, "2024-01-03 14:55:00"),
        MakeTrade(1, "trade-1", 200, "2024-01-03 14:58:57"),
    };
    result.orders = {
        MakeOrder(2, "order-2", 100, 100),
        MakeOrder(1, "order-1", 200, 200),
    };
    result.trades[0].signal_dt_local = "2024-01-03 14:54:00";
    result.trades[1].signal_dt_local = "2024-01-03 14:57:00";
    result.trades[0].timestamp_dt_local = "2024-01-03 14:55:00";
    result.trades[1].timestamp_dt_local = "2024-01-03 14:58:57";

    const std::string json = RenderBacktestJson(result);
    const std::size_t trade_2_pos = json.find("\"trade_id\":\"trade-2\"");
    const std::size_t trade_1_pos = json.find("\"trade_id\":\"trade-1\"");
    const std::size_t order_2_pos = json.find("\"order_id\":\"order-2\"");
    const std::size_t order_1_pos = json.find("\"order_id\":\"order-1\"");

    ASSERT_NE(trade_1_pos, std::string::npos);
    ASSERT_NE(trade_2_pos, std::string::npos);
    ASSERT_NE(order_1_pos, std::string::npos);
    ASSERT_NE(order_2_pos, std::string::npos);
    EXPECT_LT(trade_2_pos, trade_1_pos);
    EXPECT_LT(order_2_pos, order_1_pos);
    EXPECT_NE(json.find("\"signal_dt_local\":\"2024-01-03 14:54:00\""), std::string::npos);
    EXPECT_NE(json.find("\"timestamp_dt_local\":\"2024-01-03 14:55:00\""), std::string::npos);
}

}  // namespace quant_hft::apps
