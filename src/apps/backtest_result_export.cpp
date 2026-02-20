#include "quant_hft/apps/backtest_replay_support.h"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>

namespace quant_hft::apps {

namespace {

std::string CsvEscape(const std::string& text) {
    if (text.find_first_of(",\"\n\r") == std::string::npos) {
        return text;
    }
    std::string escaped;
    escaped.reserve(text.size() + 2);
    escaped.push_back('"');
    for (const char ch : text) {
        if (ch == '"') {
            escaped.push_back('"');
            escaped.push_back('"');
        } else {
            escaped.push_back(ch);
        }
    }
    escaped.push_back('"');
    return escaped;
}

std::string CsvDouble(double value) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(8) << value;
    return oss.str();
}

bool WriteDailyCsv(const BacktestCliResult& result, const std::filesystem::path& out_path,
                   std::string* error) {
    std::ofstream out(out_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "unable to open output file: " + out_path.string();
        }
        return false;
    }
    out << "date,capital,daily_return_pct,cumulative_return_pct,drawdown_pct,position_value,"
           "trades_count,turnover,market_regime\n";
    for (const DailyPerformance& row : result.daily) {
        out << CsvEscape(row.date) << ',' << CsvDouble(row.capital) << ','
            << CsvDouble(row.daily_return_pct) << ',' << CsvDouble(row.cumulative_return_pct)
            << ',' << CsvDouble(row.drawdown_pct) << ',' << CsvDouble(row.position_value) << ','
            << row.trades_count << ',' << CsvDouble(row.turnover) << ','
            << CsvEscape(row.market_regime) << '\n';
    }
    return true;
}

bool WriteTradesCsv(const BacktestCliResult& result, const std::filesystem::path& out_path,
                    std::string* error) {
    std::ofstream out(out_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "unable to open output file: " + out_path.string();
        }
        return false;
    }
    out << "trade_id,order_id,symbol,exchange,side,offset,volume,price,timestamp_ns,commission,"
           "slippage,realized_pnl,strategy_id,signal_type,regime_at_entry\n";
    for (const TradeRecord& row : result.trades) {
        out << CsvEscape(row.trade_id) << ',' << CsvEscape(row.order_id) << ','
            << CsvEscape(row.symbol) << ',' << CsvEscape(row.exchange) << ','
            << CsvEscape(row.side) << ',' << CsvEscape(row.offset) << ',' << row.volume << ','
            << CsvDouble(row.price) << ',' << row.timestamp_ns << ',' << CsvDouble(row.commission)
            << ',' << CsvDouble(row.slippage) << ',' << CsvDouble(row.realized_pnl) << ','
            << CsvEscape(row.strategy_id) << ',' << CsvEscape(row.signal_type) << ','
            << CsvEscape(row.regime_at_entry) << '\n';
    }
    return true;
}

bool WriteOrdersCsv(const BacktestCliResult& result, const std::filesystem::path& out_path,
                    std::string* error) {
    std::ofstream out(out_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "unable to open output file: " + out_path.string();
        }
        return false;
    }
    out << "order_id,client_order_id,symbol,type,side,offset,price,volume,status,filled_volume,"
           "avg_fill_price,created_at_ns,last_update_ns,strategy_id,cancel_reason\n";
    for (const OrderRecord& row : result.orders) {
        out << CsvEscape(row.order_id) << ',' << CsvEscape(row.client_order_id) << ','
            << CsvEscape(row.symbol) << ',' << CsvEscape(row.type) << ',' << CsvEscape(row.side)
            << ',' << CsvEscape(row.offset) << ',' << CsvDouble(row.price) << ',' << row.volume
            << ',' << CsvEscape(row.status) << ',' << row.filled_volume << ','
            << CsvDouble(row.avg_fill_price) << ',' << row.created_at_ns << ','
            << row.last_update_ns << ',' << CsvEscape(row.strategy_id) << ','
            << CsvEscape(row.cancel_reason) << '\n';
    }
    return true;
}

bool WritePositionCsv(const BacktestCliResult& result, const std::filesystem::path& out_path,
                      std::string* error) {
    std::ofstream out(out_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "unable to open output file: " + out_path.string();
        }
        return false;
    }
    out << "timestamp_ns,symbol,net_position,avg_price,unrealized_pnl\n";
    for (const PositionSnapshot& row : result.position_history) {
        out << row.timestamp_ns << ',' << CsvEscape(row.symbol) << ',' << row.net_position << ','
            << CsvDouble(row.avg_price) << ',' << CsvDouble(row.unrealized_pnl) << '\n';
    }
    return true;
}

}  // namespace

bool ExportBacktestCsv(const BacktestCliResult& result, const std::string& out_dir,
                       std::string* error) {
    if (out_dir.empty()) {
        return true;
    }
    try {
        const std::filesystem::path base_dir(out_dir);
        std::filesystem::create_directories(base_dir);

        if (!WriteDailyCsv(result, base_dir / "daily_equity.csv", error)) {
            return false;
        }
        if (result.spec.emit_trades &&
            !WriteTradesCsv(result, base_dir / "trades.csv", error)) {
            return false;
        }
        if (result.spec.emit_orders &&
            !WriteOrdersCsv(result, base_dir / "orders.csv", error)) {
            return false;
        }
        if (result.spec.emit_position_history &&
            !WritePositionCsv(result, base_dir / "position_history.csv", error)) {
            return false;
        }
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = ex.what();
        }
        return false;
    }
    return true;
}

}  // namespace quant_hft::apps
