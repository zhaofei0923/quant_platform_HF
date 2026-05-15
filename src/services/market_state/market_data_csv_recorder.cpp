#include "quant_hft/services/market_data_csv_recorder.h"

#include <exception>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <utility>

#include "quant_hft/contracts/instrument_utils.h"

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

void WriteTickHeader(std::ostream& out) {
    out << "instrument_id,exchange_id,trading_day,action_day,update_time,update_millisec,"
           "last_price,bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,volume,"
           "open_interest,settlement_price,average_price_raw,average_price_norm,"
           "is_valid_settlement,exchange_ts_ns,recv_ts_ns\n";
}

void WriteBarHeader(std::ostream& out) {
    out << "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close,"
           "analysis_open,analysis_high,analysis_low,analysis_close,analysis_price_offset,"
           "volume,ts_ns\n";
}

std::string TimeframeBarFilename(std::int32_t timeframe_minutes) {
    return "bars_" + std::to_string(timeframe_minutes) + "m.csv";
}

void WriteTickRow(std::ostream& out, const MarketSnapshot& snapshot) {
    out << CsvEscape(snapshot.instrument_id) << ',' << CsvEscape(snapshot.exchange_id) << ','
        << CsvEscape(snapshot.trading_day) << ',' << CsvEscape(snapshot.action_day) << ','
        << CsvEscape(snapshot.update_time) << ',' << snapshot.update_millisec << ','
        << FormatNumber(snapshot.last_price) << ',' << FormatNumber(snapshot.bid_price_1) << ','
        << FormatNumber(snapshot.ask_price_1) << ',' << snapshot.bid_volume_1 << ','
        << snapshot.ask_volume_1 << ',' << snapshot.volume << ',' << snapshot.open_interest << ','
        << FormatNumber(snapshot.settlement_price) << ','
        << FormatNumber(snapshot.average_price_raw) << ','
        << FormatNumber(snapshot.average_price_norm) << ','
        << (snapshot.is_valid_settlement ? 1 : 0) << ',' << snapshot.exchange_ts_ns << ','
        << snapshot.recv_ts_ns << '\n';
}

void WriteBarRow(std::ostream& out, const BarSnapshot& bar) {
    out << CsvEscape(bar.instrument_id) << ',' << CsvEscape(bar.exchange_id) << ','
        << CsvEscape(bar.trading_day) << ',' << CsvEscape(bar.action_day) << ','
        << CsvEscape(bar.minute) << ',' << FormatNumber(bar.open) << ',' << FormatNumber(bar.high)
        << ',' << FormatNumber(bar.low) << ',' << FormatNumber(bar.close) << ','
        << FormatNumber(bar.analysis_open) << ',' << FormatNumber(bar.analysis_high) << ','
        << FormatNumber(bar.analysis_low) << ',' << FormatNumber(bar.analysis_close) << ','
        << FormatNumber(bar.analysis_price_offset) << ',' << bar.volume << ',' << bar.ts_ns << '\n';
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
        if (!config_.partition_by_product || config_.write_global_copy) {
            tick_path_ = (run_dir / "ticks.csv").string();
            bar_path_ = (run_dir / "bars_1m.csv").string();
        } else {
            tick_path_.clear();
            bar_path_.clear();
        }
    } catch (const std::exception& ex) {
        return SetError(std::string("failed to prepare market data recording dir: ") + ex.what(),
                        error);
    }

    if (!tick_path_.empty()) {
        tick_out_.open(tick_path_, std::ios::out | std::ios::trunc);
        if (!tick_out_.is_open()) {
            return SetError("failed to open tick csv output: " + tick_path_, error);
        }
        bar_out_.open(bar_path_, std::ios::out | std::ios::trunc);
        if (!bar_out_.is_open()) {
            tick_out_.close();
            return SetError("failed to open bar csv output: " + bar_path_, error);
        }

        WriteTickHeader(tick_out_);
        WriteBarHeader(bar_out_);
        if (!tick_out_.good() || !bar_out_.good()) {
            tick_out_.close();
            bar_out_.close();
            return SetError("failed to write market data csv headers", error);
        }
    }

    ticks_written_ = 0;
    bars_written_ = 0;
    is_open_ = true;
    return true;
}

void MarketDataCsvRecorder::SetAllowedInstrumentIds(
    const std::vector<std::string>& instrument_ids) {
    std::lock_guard<std::mutex> lock(mutex_);
    allowed_instrument_ids_.clear();
    for (const auto& instrument_id : instrument_ids) {
        if (!instrument_id.empty()) {
            allowed_instrument_ids_.insert(instrument_id);
        }
    }
    restrict_to_allowed_instruments_ = true;
}

void MarketDataCsvRecorder::ClearAllowedInstrumentIds() {
    std::lock_guard<std::mutex> lock(mutex_);
    allowed_instrument_ids_.clear();
    restrict_to_allowed_instruments_ = false;
}

bool MarketDataCsvRecorder::ShouldRecordInstrumentLocked(const std::string& instrument_id) const {
    return !restrict_to_allowed_instruments_ ||
           allowed_instrument_ids_.find(instrument_id) != allowed_instrument_ids_.end();
}

MarketDataCsvRecorder::ProductStreams* MarketDataCsvRecorder::EnsureProductStreams(
    const std::string& instrument_id, std::string* error) {
    std::string product_id = ExtractProductIdFromInstrumentId(instrument_id);
    if (product_id.empty()) {
        product_id = "unknown";
    }
    const auto existing = product_streams_.find(product_id);
    if (existing != product_streams_.end()) {
        return existing->second.get();
    }

    auto streams = std::make_unique<ProductStreams>();
    try {
        const std::filesystem::path product_dir =
            std::filesystem::path(output_dir_) / "varieties" / product_id / "market";
        std::filesystem::create_directories(product_dir);
        streams->tick_path = (product_dir / "ticks.csv").string();
        streams->bar_path = (product_dir / "bars_1m.csv").string();
    } catch (const std::exception& ex) {
        SetError(std::string("failed to prepare product market data dir: ") + ex.what(), error);
        return nullptr;
    }

    streams->tick_out.open(streams->tick_path, std::ios::out | std::ios::trunc);
    if (!streams->tick_out.is_open()) {
        SetError("failed to open product tick csv output: " + streams->tick_path, error);
        return nullptr;
    }
    streams->bar_out.open(streams->bar_path, std::ios::out | std::ios::trunc);
    if (!streams->bar_out.is_open()) {
        streams->tick_out.close();
        SetError("failed to open product bar csv output: " + streams->bar_path, error);
        return nullptr;
    }
    WriteTickHeader(streams->tick_out);
    WriteBarHeader(streams->bar_out);
    if (!streams->tick_out.good() || !streams->bar_out.good()) {
        SetError("failed to write product market data csv headers", error);
        return nullptr;
    }

    auto [inserted, _] = product_streams_.emplace(product_id, std::move(streams));
    return inserted->second.get();
}

std::ofstream* MarketDataCsvRecorder::EnsureProductTimeframeBarStream(
    ProductStreams* streams, const std::string& instrument_id, std::int32_t timeframe_minutes,
    std::string* error) {
    if (streams == nullptr) {
        SetError("product streams are null", error);
        return nullptr;
    }
    const auto existing = streams->timeframe_bar_outs.find(timeframe_minutes);
    if (existing != streams->timeframe_bar_outs.end()) {
        return existing->second.get();
    }

    std::string product_id = ExtractProductIdFromInstrumentId(instrument_id);
    if (product_id.empty()) {
        product_id = "unknown";
    }
    std::string path;
    try {
        const std::filesystem::path product_dir =
            std::filesystem::path(output_dir_) / "varieties" / product_id / "market";
        std::filesystem::create_directories(product_dir);
        path = (product_dir / TimeframeBarFilename(timeframe_minutes)).string();
    } catch (const std::exception& ex) {
        SetError(std::string("failed to prepare product timeframe bar dir: ") + ex.what(), error);
        return nullptr;
    }

    auto output = std::make_unique<std::ofstream>(path, std::ios::out | std::ios::trunc);
    if (!output->is_open()) {
        SetError("failed to open product timeframe bar csv output: " + path, error);
        return nullptr;
    }
    WriteBarHeader(*output);
    if (!output->good()) {
        SetError("failed to write product timeframe bar csv header: " + path, error);
        return nullptr;
    }
    streams->timeframe_bar_paths[timeframe_minutes] = path;
    auto [inserted, _] = streams->timeframe_bar_outs.emplace(timeframe_minutes, std::move(output));
    return inserted->second.get();
}

std::ofstream* MarketDataCsvRecorder::EnsureGlobalTimeframeBarStream(std::int32_t timeframe_minutes,
                                                                     std::string* error) {
    const auto existing = timeframe_bar_outs_.find(timeframe_minutes);
    if (existing != timeframe_bar_outs_.end()) {
        return existing->second.get();
    }

    std::string path;
    try {
        const std::filesystem::path run_dir(output_dir_);
        std::filesystem::create_directories(run_dir);
        path = (run_dir / TimeframeBarFilename(timeframe_minutes)).string();
    } catch (const std::exception& ex) {
        SetError(std::string("failed to prepare timeframe bar dir: ") + ex.what(), error);
        return nullptr;
    }
    auto output = std::make_unique<std::ofstream>(path, std::ios::out | std::ios::trunc);
    if (!output->is_open()) {
        SetError("failed to open timeframe bar csv output: " + path, error);
        return nullptr;
    }
    WriteBarHeader(*output);
    if (!output->good()) {
        SetError("failed to write timeframe bar csv header: " + path, error);
        return nullptr;
    }
    timeframe_bar_paths_[timeframe_minutes] = path;
    auto [inserted, _] = timeframe_bar_outs_.emplace(timeframe_minutes, std::move(output));
    return inserted->second.get();
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
    if (!ShouldRecordInstrumentLocked(snapshot.instrument_id)) {
        return true;
    }

    if (config_.partition_by_product) {
        ProductStreams* streams = EnsureProductStreams(snapshot.instrument_id, error);
        if (streams == nullptr) {
            return false;
        }
        WriteTickRow(streams->tick_out, snapshot);
        if (!streams->tick_out.good()) {
            return SetError("failed to append product market tick csv row", error);
        }
        if (config_.flush_each_write) {
            streams->tick_out.flush();
            if (!streams->tick_out.good()) {
                return SetError("failed to flush product market tick csv output", error);
            }
        }
    }

    if (!config_.partition_by_product || config_.write_global_copy) {
        WriteTickRow(tick_out_, snapshot);
        if (!tick_out_.good()) {
            return SetError("failed to append market tick csv row", error);
        }
        if (config_.flush_each_write) {
            tick_out_.flush();
            if (!tick_out_.good()) {
                return SetError("failed to flush market tick csv output", error);
            }
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
    if (!ShouldRecordInstrumentLocked(bar.instrument_id)) {
        return true;
    }

    if (config_.partition_by_product) {
        ProductStreams* streams = EnsureProductStreams(bar.instrument_id, error);
        if (streams == nullptr) {
            return false;
        }
        WriteBarRow(streams->bar_out, bar);
        if (!streams->bar_out.good()) {
            return SetError("failed to append product market bar csv row", error);
        }
        if (config_.flush_each_write) {
            streams->bar_out.flush();
            if (!streams->bar_out.good()) {
                return SetError("failed to flush product market bar csv output", error);
            }
        }
    }

    if (!config_.partition_by_product || config_.write_global_copy) {
        WriteBarRow(bar_out_, bar);
        if (!bar_out_.good()) {
            return SetError("failed to append market bar csv row", error);
        }
        if (config_.flush_each_write) {
            bar_out_.flush();
            if (!bar_out_.good()) {
                return SetError("failed to flush market bar csv output", error);
            }
        }
    }
    ++bars_written_;
    return true;
}

bool MarketDataCsvRecorder::AppendTimeframeBar(const BarSnapshot& bar,
                                               std::int32_t timeframe_minutes, std::string* error) {
    if (timeframe_minutes <= 1) {
        return AppendBar(bar, error);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (!config_.enabled) {
        return true;
    }
    if (!is_open_) {
        return SetError("market data csv recorder is not open", error);
    }
    if (bar.instrument_id.empty()) {
        return SetError("market timeframe bar instrument_id is empty", error);
    }
    if (!ShouldRecordInstrumentLocked(bar.instrument_id)) {
        return true;
    }

    if (config_.partition_by_product) {
        ProductStreams* streams = EnsureProductStreams(bar.instrument_id, error);
        if (streams == nullptr) {
            return false;
        }
        std::ofstream* output =
            EnsureProductTimeframeBarStream(streams, bar.instrument_id, timeframe_minutes, error);
        if (output == nullptr) {
            return false;
        }
        WriteBarRow(*output, bar);
        if (!output->good()) {
            return SetError("failed to append product timeframe bar csv row", error);
        }
        if (config_.flush_each_write) {
            output->flush();
            if (!output->good()) {
                return SetError("failed to flush product timeframe bar csv output", error);
            }
        }
    }

    if (!config_.partition_by_product || config_.write_global_copy) {
        std::ofstream* output = EnsureGlobalTimeframeBarStream(timeframe_minutes, error);
        if (output == nullptr) {
            return false;
        }
        WriteBarRow(*output, bar);
        if (!output->good()) {
            return SetError("failed to append timeframe bar csv row", error);
        }
        if (config_.flush_each_write) {
            output->flush();
            if (!output->good()) {
                return SetError("failed to flush timeframe bar csv output", error);
            }
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
    bool ok = true;
    if (tick_out_.is_open()) {
        tick_out_.flush();
        ok = ok && tick_out_.good();
        tick_out_.close();
    }
    if (bar_out_.is_open()) {
        bar_out_.flush();
        ok = ok && bar_out_.good();
        bar_out_.close();
    }
    for (auto& [timeframe_minutes, output] : timeframe_bar_outs_) {
        (void)timeframe_minutes;
        if (output == nullptr) {
            continue;
        }
        output->flush();
        ok = ok && output->good();
        output->close();
    }
    timeframe_bar_outs_.clear();
    timeframe_bar_paths_.clear();
    for (auto& [product_id, streams] : product_streams_) {
        (void)product_id;
        if (streams == nullptr) {
            continue;
        }
        streams->tick_out.flush();
        streams->bar_out.flush();
        ok = ok && streams->tick_out.good() && streams->bar_out.good();
        streams->tick_out.close();
        streams->bar_out.close();
        for (auto& [timeframe_minutes, output] : streams->timeframe_bar_outs) {
            (void)timeframe_minutes;
            if (output == nullptr) {
                continue;
            }
            output->flush();
            ok = ok && output->good();
            output->close();
        }
    }
    product_streams_.clear();
    is_open_ = false;
    if (!ok) {
        return SetError("failed to flush market data csv output", error);
    }
    return true;
}

}  // namespace quant_hft
