#include "quant_hft/backtest/indicator_trace_csv_writer.h"

#include <exception>
#include <filesystem>
#include <iomanip>
#include <optional>
#include <sstream>
#include <string>

namespace quant_hft {
namespace {

bool SetError(const std::string& message, std::string* error) {
    if (error != nullptr) {
        *error = message;
    }
    return false;
}

std::string FormatNumber(double value) {
    std::ostringstream oss;
    oss << std::setprecision(12) << value;
    return oss.str();
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

std::string FormatOptional(const std::optional<double>& value) {
    return value.has_value() ? FormatNumber(*value) : "";
}

std::string MarketRegimeToLabel(MarketRegime regime) {
    switch (regime) {
        case MarketRegime::kUnknown:
            return "kUnknown";
        case MarketRegime::kStrongTrend:
            return "kStrongTrend";
        case MarketRegime::kWeakTrend:
            return "kWeakTrend";
        case MarketRegime::kRanging:
            return "kRanging";
        case MarketRegime::kFlat:
            return "kFlat";
        default:
            return "kUnknown";
    }
}

}  // namespace

bool IndicatorTraceCsvWriter::Open(const std::string& output_path, std::string* error) {
    if (is_open_) {
        return SetError("indicator trace csv writer is already open", error);
    }
    if (output_path.empty()) {
        return SetError("indicator trace csv output path is empty", error);
    }

    try {
        const std::filesystem::path path(output_path);
        if (std::filesystem::exists(path)) {
            return SetError("indicator trace csv output already exists: " + path.string(), error);
        }
        if (!path.parent_path().empty()) {
            std::filesystem::create_directories(path.parent_path());
        }
    } catch (const std::exception& ex) {
        return SetError(std::string("failed to prepare indicator trace csv path: ") + ex.what(),
                        error);
    }

    out_.open(output_path, std::ios::out | std::ios::trunc);
    if (!out_.is_open()) {
        return SetError("failed to open indicator trace csv output: " + output_path, error);
    }

    out_ << "instrument_id,ts_ns,dt_utc,timeframe_minutes,bar_open,bar_high,bar_low,bar_close,"
            "bar_volume,kama,atr,adx,er,market_regime\n";
    if (!out_.good()) {
        out_.close();
        return SetError("failed to write indicator trace csv header", error);
    }

    output_path_ = output_path;
    rows_written_ = 0;
    is_open_ = true;
    return true;
}

bool IndicatorTraceCsvWriter::Append(const IndicatorTraceRow& row, std::string* error) {
    if (!is_open_) {
        return SetError("indicator trace csv writer is not open", error);
    }
    if (row.instrument_id.empty()) {
        return SetError("indicator trace row instrument_id is empty", error);
    }

    out_ << CsvEscape(row.instrument_id) << ',' << row.ts_ns << ',' << CsvEscape(row.dt_utc) << ','
         << (row.timeframe_minutes > 0 ? row.timeframe_minutes : 1) << ','
         << FormatNumber(row.bar_open) << ',' << FormatNumber(row.bar_high) << ','
         << FormatNumber(row.bar_low) << ',' << FormatNumber(row.bar_close) << ','
         << FormatNumber(row.bar_volume) << ',' << FormatOptional(row.kama) << ','
         << FormatOptional(row.atr) << ',' << FormatOptional(row.adx) << ','
         << FormatOptional(row.er) << ',' << CsvEscape(MarketRegimeToLabel(row.market_regime))
         << '\n';
    if (!out_.good()) {
        return SetError("failed to append indicator trace csv row", error);
    }

    ++rows_written_;
    return true;
}

bool IndicatorTraceCsvWriter::Close(std::string* error) {
    if (!is_open_) {
        return true;
    }
    out_.flush();
    if (!out_.good()) {
        out_.close();
        is_open_ = false;
        return SetError("failed to flush indicator trace csv output", error);
    }
    out_.close();
    is_open_ = false;
    return true;
}

}  // namespace quant_hft
