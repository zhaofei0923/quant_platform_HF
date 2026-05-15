#include "quant_hft/services/market_data_csv_recorder.h"

#include <cctype>
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

std::string NormalizeTradingDayToken(const std::string& value) {
    std::string normalized;
    normalized.reserve(value.size());
    for (const char ch : value) {
        const auto uch = static_cast<unsigned char>(ch);
        if (std::isalnum(uch) != 0 || ch == '_') {
            normalized.push_back(ch);
        } else if (ch == '-' || std::isspace(uch) != 0) {
            continue;
        } else {
            normalized.push_back('_');
        }
    }
    return normalized.empty() ? std::string("unknown") : normalized;
}

std::string ResolveTradingDay(const std::string& trading_day, const std::string& action_day) {
    if (!trading_day.empty()) {
        return NormalizeTradingDayToken(trading_day);
    }
    if (!action_day.empty()) {
        return NormalizeTradingDayToken(action_day);
    }
    return "unknown";
}

std::filesystem::path TradingDayDir(const std::filesystem::path& root,
                                    const std::string& trading_day) {
    return root / ("trading_day=" + NormalizeTradingDayToken(trading_day));
}

std::string StreamKey(const std::string& trading_day, const std::string& product_id) {
    return NormalizeTradingDayToken(trading_day) + "|" + product_id;
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

using HeaderWriter = void (*)(std::ostream&);

bool OpenAppendCsv(const std::filesystem::path& path, HeaderWriter write_header,
                   const std::string& description, std::ofstream* output, std::string* error) {
    if (output == nullptr) {
        return SetError("csv output pointer is null", error);
    }
    bool needs_header = true;
    std::error_code ec;
    if (std::filesystem::exists(path, ec) && !ec) {
        const auto size = std::filesystem::file_size(path, ec);
        needs_header = ec || size == 0;
    }
    try {
        std::filesystem::create_directories(path.parent_path());
    } catch (const std::exception& ex) {
        return SetError("failed to prepare " + description + " csv dir: " + ex.what(), error);
    }
    output->open(path, std::ios::out | std::ios::app);
    if (!output->is_open()) {
        return SetError("failed to open " + description + " csv output: " + path.string(), error);
    }
    if (needs_header) {
        write_header(*output);
        if (!output->good()) {
            output->close();
            return SetError("failed to write " + description + " csv header: " + path.string(),
                            error);
        }
    }
    return true;
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
        std::filesystem::create_directories(base_dir);
        output_dir_ = base_dir.string();
        tick_path_.clear();
        bar_path_.clear();
    } catch (const std::exception& ex) {
        return SetError(std::string("failed to prepare market data recording dir: ") + ex.what(),
                        error);
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

MarketDataCsvRecorder::CsvStreams* MarketDataCsvRecorder::EnsureProductStreams(
    const std::string& instrument_id, const std::string& trading_day, std::string* error) {
    std::string product_id = ExtractProductIdFromInstrumentId(instrument_id);
    if (product_id.empty()) {
        product_id = "unknown";
    }
    const std::string day = NormalizeTradingDayToken(trading_day);
    const auto key = StreamKey(day, product_id);
    const auto existing = product_streams_.find(key);
    if (existing != product_streams_.end()) {
        return existing->second.get();
    }

    auto streams = std::make_unique<CsvStreams>();
    try {
        const std::filesystem::path product_dir =
            TradingDayDir(std::filesystem::path(output_dir_), day) / "varieties" / product_id /
            "market";
        std::filesystem::create_directories(product_dir);
        streams->trading_day = day;
        streams->product_id = product_id;
        streams->tick_path = (product_dir / "ticks.csv").string();
        streams->bar_path = (product_dir / "bars_1m.csv").string();
    } catch (const std::exception& ex) {
        SetError(std::string("failed to prepare product market data dir: ") + ex.what(), error);
        return nullptr;
    }

    if (!OpenAppendCsv(streams->tick_path, WriteTickHeader, "product tick", &streams->tick_out,
                       error)) {
        return nullptr;
    }
    if (!OpenAppendCsv(streams->bar_path, WriteBarHeader, "product bar", &streams->bar_out,
                       error)) {
        streams->tick_out.close();
        return nullptr;
    }

    auto [inserted, _] = product_streams_.emplace(key, std::move(streams));
    return inserted->second.get();
}

MarketDataCsvRecorder::CsvStreams* MarketDataCsvRecorder::EnsureGlobalStreams(
    const std::string& trading_day, std::string* error) {
    const std::string day = NormalizeTradingDayToken(trading_day);
    const auto existing = global_streams_.find(day);
    if (existing != global_streams_.end()) {
        return existing->second.get();
    }

    auto streams = std::make_unique<CsvStreams>();
    try {
        const std::filesystem::path day_dir =
            TradingDayDir(std::filesystem::path(output_dir_), day);
        std::filesystem::create_directories(day_dir);
        streams->trading_day = day;
        streams->product_id = "global";
        streams->tick_path = (day_dir / "ticks.csv").string();
        streams->bar_path = (day_dir / "bars_1m.csv").string();
    } catch (const std::exception& ex) {
        SetError(std::string("failed to prepare global market data dir: ") + ex.what(), error);
        return nullptr;
    }

    if (!OpenAppendCsv(streams->tick_path, WriteTickHeader, "tick", &streams->tick_out, error)) {
        return nullptr;
    }
    if (!OpenAppendCsv(streams->bar_path, WriteBarHeader, "bar", &streams->bar_out, error)) {
        streams->tick_out.close();
        return nullptr;
    }
    tick_path_ = streams->tick_path;
    bar_path_ = streams->bar_path;
    auto [inserted, _] = global_streams_.emplace(day, std::move(streams));
    return inserted->second.get();
}

std::ofstream* MarketDataCsvRecorder::EnsureProductTimeframeBarStream(
    CsvStreams* streams, std::int32_t timeframe_minutes, std::string* error) {
    if (streams == nullptr) {
        SetError("product streams are null", error);
        return nullptr;
    }
    const auto existing = streams->timeframe_bar_outs.find(timeframe_minutes);
    if (existing != streams->timeframe_bar_outs.end()) {
        return existing->second.get();
    }

    std::string path;
    try {
        const std::filesystem::path product_dir =
            std::filesystem::path(streams->tick_path).parent_path();
        std::filesystem::create_directories(product_dir);
        path = (product_dir / TimeframeBarFilename(timeframe_minutes)).string();
    } catch (const std::exception& ex) {
        SetError(std::string("failed to prepare product timeframe bar dir: ") + ex.what(), error);
        return nullptr;
    }

    auto output = std::make_unique<std::ofstream>();
    if (!OpenAppendCsv(path, WriteBarHeader, "product timeframe bar", output.get(), error)) {
        return nullptr;
    }
    streams->timeframe_bar_paths[timeframe_minutes] = path;
    auto [inserted, _] = streams->timeframe_bar_outs.emplace(timeframe_minutes, std::move(output));
    return inserted->second.get();
}

std::ofstream* MarketDataCsvRecorder::EnsureGlobalTimeframeBarStream(std::int32_t timeframe_minutes,
                                                                     const std::string& trading_day,
                                                                     std::string* error) {
    CsvStreams* streams = EnsureGlobalStreams(trading_day, error);
    if (streams == nullptr) {
        return nullptr;
    }
    const auto existing = streams->timeframe_bar_outs.find(timeframe_minutes);
    if (existing != streams->timeframe_bar_outs.end()) {
        return existing->second.get();
    }

    std::string path;
    try {
        const std::filesystem::path day_dir =
            std::filesystem::path(streams->tick_path).parent_path();
        std::filesystem::create_directories(day_dir);
        path = (day_dir / TimeframeBarFilename(timeframe_minutes)).string();
    } catch (const std::exception& ex) {
        SetError(std::string("failed to prepare timeframe bar dir: ") + ex.what(), error);
        return nullptr;
    }
    auto output = std::make_unique<std::ofstream>();
    if (!OpenAppendCsv(path, WriteBarHeader, "timeframe bar", output.get(), error)) {
        return nullptr;
    }
    streams->timeframe_bar_paths[timeframe_minutes] = path;
    auto [inserted, _] = streams->timeframe_bar_outs.emplace(timeframe_minutes, std::move(output));
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
    const std::string trading_day = ResolveTradingDay(snapshot.trading_day, snapshot.action_day);

    if (config_.partition_by_product) {
        CsvStreams* streams = EnsureProductStreams(snapshot.instrument_id, trading_day, error);
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
        CsvStreams* streams = EnsureGlobalStreams(trading_day, error);
        if (streams == nullptr) {
            return false;
        }
        WriteTickRow(streams->tick_out, snapshot);
        if (!streams->tick_out.good()) {
            return SetError("failed to append market tick csv row", error);
        }
        if (config_.flush_each_write) {
            streams->tick_out.flush();
            if (!streams->tick_out.good()) {
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
    const std::string trading_day = ResolveTradingDay(bar.trading_day, bar.action_day);

    if (config_.partition_by_product) {
        CsvStreams* streams = EnsureProductStreams(bar.instrument_id, trading_day, error);
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
        CsvStreams* streams = EnsureGlobalStreams(trading_day, error);
        if (streams == nullptr) {
            return false;
        }
        WriteBarRow(streams->bar_out, bar);
        if (!streams->bar_out.good()) {
            return SetError("failed to append market bar csv row", error);
        }
        if (config_.flush_each_write) {
            streams->bar_out.flush();
            if (!streams->bar_out.good()) {
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
    const std::string trading_day = ResolveTradingDay(bar.trading_day, bar.action_day);

    if (config_.partition_by_product) {
        CsvStreams* streams = EnsureProductStreams(bar.instrument_id, trading_day, error);
        if (streams == nullptr) {
            return false;
        }
        std::ofstream* output = EnsureProductTimeframeBarStream(streams, timeframe_minutes, error);
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
        std::ofstream* output =
            EnsureGlobalTimeframeBarStream(timeframe_minutes, trading_day, error);
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
    auto close_streams = [&ok](CsvStreams* streams) {
        if (streams == nullptr) {
            return;
        }
        if (streams->tick_out.is_open()) {
            streams->tick_out.flush();
            ok = ok && streams->tick_out.good();
            streams->tick_out.close();
        }
        if (streams->bar_out.is_open()) {
            streams->bar_out.flush();
            ok = ok && streams->bar_out.good();
            streams->bar_out.close();
        }
        for (auto& [timeframe_minutes, output] : streams->timeframe_bar_outs) {
            (void)timeframe_minutes;
            if (output == nullptr) {
                continue;
            }
            output->flush();
            ok = ok && output->good();
            output->close();
        }
    };
    for (auto& [trading_day, streams] : global_streams_) {
        (void)trading_day;
        close_streams(streams.get());
    }
    global_streams_.clear();
    for (auto& [key, streams] : product_streams_) {
        (void)key;
        close_streams(streams.get());
    }
    product_streams_.clear();
    is_open_ = false;
    if (!ok) {
        return SetError("failed to flush market data csv output", error);
    }
    return true;
}

}  // namespace quant_hft
