#include "quant_hft/services/market_data_csv_recorder.h"

#include <exception>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <utility>

namespace quant_hft {
namespace {

bool SetError(const std::string& message, std::string* error) {
    if (error != nullptr) {
        *error = message;
    }
    return false;
}

std::string FormatNumber(double value) {
    std::ostringstream out;
    out << std::setprecision(12) << value;
    return out.str();
}

std::string CsvEscape(std::string value) {
    const bool requires_quotes =
        value.find(',') != std::string::npos || value.find('"') != std::string::npos ||
        value.find('\n') != std::string::npos || value.find('\r') != std::string::npos;
    if (!requires_quotes) {
        return value;
    }
    std::string escaped;
    escaped.reserve(value.size() + 8);
    escaped.push_back('"');
    for (const char ch : value) {
        if (ch == '"') {
            escaped += "\"\"";
        } else {
            escaped.push_back(ch);
        }
    }
    escaped.push_back('"');
    return escaped;
}

std::string ResolveRunId(const std::string& configured_run_id) {
    if (!configured_run_id.empty()) {
        return configured_run_id;
    }
    return "run_" + std::to_string(NowEpochNanos());
}

}  // namespace

MarketDataCsvRecorder::~MarketDataCsvRecorder() {
    std::string ignored_error;
    (void)Close(&ignored_error);
}

bool MarketDataCsvRecorder::Open(MarketDataRecordingConfig config, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_open_) {
        return SetError("market data csv recorder is already open", error);
    }
    config_ = std::move(config);
    if (!config_.enabled) {
        return true;
    }
    if (config_.output_dir.empty()) {
        return SetError("market data recording output_dir is empty", error);
    }

    try {
        const std::filesystem::path base_dir(config_.output_dir);
        const auto run_dir = base_dir / ResolveRunId(config_.run_id);
        std::filesystem::create_directories(run_dir);
        output_dir_ = run_dir.string();
        tick_path_ = (run_dir / "ticks.csv").string();
        bar_path_ = (run_dir / "bars_1m.csv").string();
    } catch (const std::exception& ex) {
        return SetError(std::string("failed to prepare market data recording dir: ") + ex.what(),
                        error);
    }

    tick_out_.open(tick_path_, std::ios::out | std::ios::trunc);
    if (!tick_out_.is_open()) {
        return SetError("failed to open tick csv output: " + tick_path_, error);
    }
    bar_out_.open(bar_path_, std::ios::out | std::ios::trunc);
    if (!bar_out_.is_open()) {
        tick_out_.close();
        return SetError("failed to open bar csv output: " + bar_path_, error);
    }

    tick_out_ << "instrument_id,exchange_id,trading_day,action_day,update_time,update_millisec,"
                 "last_price,bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,volume,"
                 "open_interest,settlement_price,average_price_raw,average_price_norm,"
                 "is_valid_settlement,exchange_ts_ns,recv_ts_ns\n";
    bar_out_ << "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close,"
                "analysis_open,analysis_high,analysis_low,analysis_close,analysis_price_offset,"
                "volume,ts_ns\n";
    if (!tick_out_.good() || !bar_out_.good()) {
        tick_out_.close();
        bar_out_.close();
        return SetError("failed to write market data csv headers", error);
    }

    ticks_written_ = 0;
    bars_written_ = 0;
    is_open_ = true;
    return true;
}

bool MarketDataCsvRecorder::AppendTick(const MarketSnapshot& snapshot, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!config_.enabled) {
        return true;
    }
    if (!is_open_) {
        return SetError("market data csv recorder is not open", error);
    }
    if (snapshot.instrument_id.empty()) {
        return SetError("market tick instrument_id is empty", error);
    }

    tick_out_ << CsvEscape(snapshot.instrument_id) << ',' << CsvEscape(snapshot.exchange_id) << ','
              << CsvEscape(snapshot.trading_day) << ',' << CsvEscape(snapshot.action_day) << ','
              << CsvEscape(snapshot.update_time) << ',' << snapshot.update_millisec << ','
              << FormatNumber(snapshot.last_price) << ',' << FormatNumber(snapshot.bid_price_1)
              << ',' << FormatNumber(snapshot.ask_price_1) << ',' << snapshot.bid_volume_1 << ','
              << snapshot.ask_volume_1 << ',' << snapshot.volume << ',' << snapshot.open_interest
              << ',' << FormatNumber(snapshot.settlement_price) << ','
              << FormatNumber(snapshot.average_price_raw) << ','
              << FormatNumber(snapshot.average_price_norm) << ','
              << (snapshot.is_valid_settlement ? 1 : 0) << ',' << snapshot.exchange_ts_ns << ','
              << snapshot.recv_ts_ns << '\n';
    if (!tick_out_.good()) {
        return SetError("failed to append market tick csv row", error);
    }
    if (config_.flush_each_write) {
        tick_out_.flush();
        if (!tick_out_.good()) {
            return SetError("failed to flush market tick csv output", error);
        }
    }
    ++ticks_written_;
    return true;
}

bool MarketDataCsvRecorder::AppendBar(const BarSnapshot& bar, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!config_.enabled) {
        return true;
    }
    if (!is_open_) {
        return SetError("market data csv recorder is not open", error);
    }
    if (bar.instrument_id.empty()) {
        return SetError("market bar instrument_id is empty", error);
    }

    bar_out_ << CsvEscape(bar.instrument_id) << ',' << CsvEscape(bar.exchange_id) << ','
             << CsvEscape(bar.trading_day) << ',' << CsvEscape(bar.action_day) << ','
             << CsvEscape(bar.minute) << ',' << FormatNumber(bar.open) << ','
             << FormatNumber(bar.high) << ',' << FormatNumber(bar.low) << ','
             << FormatNumber(bar.close) << ',' << FormatNumber(bar.analysis_open) << ','
             << FormatNumber(bar.analysis_high) << ',' << FormatNumber(bar.analysis_low) << ','
             << FormatNumber(bar.analysis_close) << ',' << FormatNumber(bar.analysis_price_offset)
             << ',' << bar.volume << ',' << bar.ts_ns << '\n';
    if (!bar_out_.good()) {
        return SetError("failed to append market bar csv row", error);
    }
    if (config_.flush_each_write) {
        bar_out_.flush();
        if (!bar_out_.good()) {
            return SetError("failed to flush market bar csv output", error);
        }
    }
    ++bars_written_;
    return true;
}

bool MarketDataCsvRecorder::Close(std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!is_open_) {
        return true;
    }
    tick_out_.flush();
    bar_out_.flush();
    const bool ok = tick_out_.good() && bar_out_.good();
    tick_out_.close();
    bar_out_.close();
    is_open_ = false;
    if (!ok) {
        return SetError("failed to flush market data csv output", error);
    }
    return true;
}

}  // namespace quant_hft