#include "quant_hft/core/local_wal_regulatory_sink.h"

#include <fstream>
#include <iomanip>
#include <sstream>
#include <utility>

namespace quant_hft {

LocalWalRegulatorySink::LocalWalRegulatorySink(std::string wal_path)
    : wal_path_(std::move(wal_path)), stream_(wal_path_, std::ios::app) {
    seq_ = ComputeNextSeq();
}

LocalWalRegulatorySink::~LocalWalRegulatorySink() {
    Flush();
    if (stream_.is_open()) {
        stream_.close();
    }
}

bool LocalWalRegulatorySink::AppendOrderEvent(const OrderEvent& event) {
    return Append("order", event);
}

bool LocalWalRegulatorySink::AppendTradeEvent(const OrderEvent& event) {
    return Append("trade", event);
}

bool LocalWalRegulatorySink::Flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return false;
    }
    stream_.flush();
    return stream_.good();
}

bool LocalWalRegulatorySink::Append(const char* kind, const OrderEvent& event) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return false;
    }

    std::ostringstream oss;
    oss << "{"
        << "\"seq\":" << seq_++ << ","
        << "\"kind\":\"" << kind << "\","
        << "\"ts_ns\":" << event.ts_ns << ","
        << "\"account_id\":\"" << EscapeJsonString(event.account_id) << "\","
        << "\"client_order_id\":\"" << EscapeJsonString(event.client_order_id) << "\","
        << "\"exchange_order_id\":\"" << EscapeJsonString(event.exchange_order_id) << "\","
        << "\"instrument_id\":\"" << EscapeJsonString(event.instrument_id) << "\","
        << "\"status\":" << static_cast<int>(event.status) << ","
        << "\"total_volume\":" << event.total_volume << ","
        << "\"filled_volume\":" << event.filled_volume << ","
        << "\"avg_fill_price\":" << event.avg_fill_price << ","
        << "\"reason\":\"" << EscapeJsonString(event.reason) << "\","
        << "\"trace_id\":\"" << EscapeJsonString(event.trace_id) << "\""
        << "}\n";

    stream_ << oss.str();
    return stream_.good();
}

std::string LocalWalRegulatorySink::EscapeJsonString(const std::string& input) {
    std::string out;
    out.reserve(input.size());
    for (const char ch : input) {
        switch (ch) {
            case '\\':
                out.append("\\\\");
                break;
            case '"':
                out.append("\\\"");
                break;
            case '\n':
                out.append("\\n");
                break;
            case '\r':
                out.append("\\r");
                break;
            case '\t':
                out.append("\\t");
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    return out;
}

std::uint64_t LocalWalRegulatorySink::ComputeNextSeq() const {
    std::ifstream in(wal_path_);
    if (!in.is_open()) {
        return 0;
    }

    std::uint64_t max_seq = 0;
    std::string line;
    while (std::getline(in, line)) {
        const auto key_pos = line.find("\"seq\":");
        if (key_pos == std::string::npos) {
            continue;
        }
        std::size_t pos = key_pos + 6;
        std::size_t end = pos;
        while (end < line.size() && std::isdigit(static_cast<unsigned char>(line[end])) != 0) {
            ++end;
        }
        if (end == pos) {
            continue;
        }
        try {
            const auto parsed = static_cast<std::uint64_t>(
                std::stoull(line.substr(pos, end - pos)));
            if (parsed >= max_seq) {
                max_seq = parsed + 1;
            }
        } catch (...) {
            continue;
        }
    }
    return max_seq;
}

}  // namespace quant_hft
