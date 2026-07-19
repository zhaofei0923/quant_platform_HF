#include "quant_hft/core/local_wal_regulatory_sink.h"

#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <utility>

namespace quant_hft {

LocalWalRegulatorySink::LocalWalRegulatorySink(std::string wal_path)
    : wal_path_(std::move(wal_path)), run_id_(GetEnvOrEmpty("SIMNOW_RUN_ID")) {
    const std::filesystem::path path(wal_path_);
    if (const auto parent = path.parent_path(); !parent.empty()) {
        std::filesystem::create_directories(parent);
    }
    stream_.open(wal_path_, std::ios::app);
    seq_ = ComputeNextSeq();
}

LocalWalRegulatorySink::~LocalWalRegulatorySink() {
    Flush();
    if (stream_.is_open()) {
        stream_.close();
    }
}

bool LocalWalRegulatorySink::AppendOrderEvent(const OrderEvent& event) {
    return Append("order", "order_update", event);
}

bool LocalWalRegulatorySink::AppendTradeEvent(const OrderEvent& event) {
    return Append("trade", "trade_fill", event);
}

bool LocalWalRegulatorySink::AppendCtpOrderSubmitMapping(const CtpOrderSubmitMapping& mapping) {
    return AppendMapping(mapping);
}

bool LocalWalRegulatorySink::Flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return false;
    }
    stream_.flush();
    return stream_.good();
}

bool LocalWalRegulatorySink::Append(const char* kind, const char* event_type,
                                    const OrderEvent& event) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return false;
    }

    std::ostringstream oss;
    oss << "{"
        << "\"seq\":" << seq_++ << ","
        << "\"schema_version\":3,"
        << "\"kind\":\"" << kind << "\","
        << "\"event_type\":\"" << event_type << "\","
        << "\"run_id\":\"" << EscapeJsonString(run_id_) << "\","
        << "\"exchange_ts_ns\":" << event.exchange_ts_ns << ","
        << "\"recv_ts_ns\":" << event.recv_ts_ns << ","
        << "\"ts_ns\":" << event.ts_ns << ","
        << "\"account_id\":\"" << EscapeJsonString(event.account_id) << "\","
        << "\"strategy_id\":\"" << EscapeJsonString(event.strategy_id) << "\","
        << "\"client_order_id\":\"" << EscapeJsonString(event.client_order_id) << "\","
        << "\"exchange_order_id\":\"" << EscapeJsonString(event.exchange_order_id) << "\","
        << "\"instrument_id\":\"" << EscapeJsonString(event.instrument_id) << "\","
        << "\"exchange_id\":\"" << EscapeJsonString(event.exchange_id) << "\","
        << "\"trade_id\":\"" << EscapeJsonString(event.trade_id) << "\","
        << "\"raw_trade_id\":\"" << EscapeJsonString(event.raw_trade_id) << "\","
        << "\"trading_day\":\"" << EscapeJsonString(event.trading_day) << "\","
        << "\"event_source\":\"" << EscapeJsonString(event.event_source) << "\","
        << "\"side\":" << static_cast<int>(event.side) << ","
        << "\"offset\":" << static_cast<int>(event.offset) << ","
        << "\"status\":" << static_cast<int>(event.status) << ","
        << "\"total_volume\":" << event.total_volume << ","
        << "\"filled_volume\":" << event.filled_volume << ","
        << "\"last_trade_volume\":" << event.last_trade_volume << ","
        << "\"avg_fill_price\":" << event.avg_fill_price << ","
        << "\"reason\":\"" << EscapeJsonString(event.reason) << "\","
        << "\"status_msg\":\"" << EscapeJsonString(event.status_msg) << "\","
        << "\"order_submit_status\":\"" << EscapeJsonString(event.order_submit_status) << "\","
        << "\"order_ref\":\"" << EscapeJsonString(event.order_ref) << "\","
        << "\"front_id\":" << event.front_id << ","
        << "\"session_id\":" << event.session_id << ","
        << "\"query_request_id\":" << event.query_request_id << ","
        << "\"recovery_generation\":" << event.recovery_generation << ","
        << "\"trace_id\":\"" << EscapeJsonString(event.trace_id) << "\""
        << "}\n";

    stream_ << oss.str();
    return stream_.good();
}

bool LocalWalRegulatorySink::AppendMapping(const CtpOrderSubmitMapping& mapping) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return false;
    }

    const std::string run_id = mapping.run_id.empty() ? run_id_ : mapping.run_id;
    std::ostringstream oss;
    oss << "{"
        << "\"seq\":" << seq_++ << ","
        << "\"schema_version\":3,"
        << "\"kind\":\"ctp_order_submit_mapping\","
        << "\"event_type\":\"ctp_submit_mapping\","
        << "\"run_id\":\"" << EscapeJsonString(run_id) << "\","
        << "\"submit_ts_ns\":" << mapping.submit_ts_ns << ","
        << "\"account_id\":\"" << EscapeJsonString(mapping.account_id) << "\","
        << "\"strategy_id\":\"" << EscapeJsonString(mapping.strategy_id) << "\","
        << "\"trace_id\":\"" << EscapeJsonString(mapping.trace_id) << "\","
        << "\"client_order_id\":\"" << EscapeJsonString(mapping.client_order_id) << "\","
        << "\"instrument_id\":\"" << EscapeJsonString(mapping.instrument_id) << "\","
        << "\"exchange_id\":\"" << EscapeJsonString(mapping.exchange_id) << "\","
        << "\"side\":" << static_cast<int>(mapping.side) << ","
        << "\"offset\":" << static_cast<int>(mapping.offset) << ","
        << "\"volume\":" << mapping.volume << ","
        << "\"price\":" << mapping.price << ","
        << "\"order_ref\":\"" << EscapeJsonString(mapping.order_ref) << "\","
        << "\"front_id\":" << mapping.front_id << ","
        << "\"session_id\":" << mapping.session_id << ","
        << "\"request_id\":" << mapping.request_id << ","
        << "\"trading_day\":\"" << EscapeJsonString(mapping.trading_day) << "\","
        << "\"phase\":" << static_cast<int>(mapping.phase) << "}\n";

    stream_ << oss.str();
    return stream_.good();
}

std::string LocalWalRegulatorySink::GetEnvOrEmpty(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string() : std::string(value);
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
            const auto parsed =
                static_cast<std::uint64_t>(std::stoull(line.substr(pos, end - pos)));
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
