#include "quant_hft/core/local_wal_regulatory_sink.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <random>
#include <sstream>
#include <utility>

#include "quant_hft/core/wal_format.h"

namespace quant_hft {

LocalWalRegulatorySink::LocalWalRegulatorySink(std::string wal_path)
    : wal_path_(std::move(wal_path)), run_id_(GetEnvOrEmpty("SIMNOW_RUN_ID")) {
    const std::filesystem::path path(wal_path_);
    std::error_code ec;
    const bool existed = std::filesystem::exists(path, ec);
    if (ec) {
        error_ = "cannot inspect WAL: " + ec.message();
        return;
    }
    if (const auto parent = path.parent_path(); !parent.empty()) {
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            error_ = "cannot create WAL directory: " + ec.message();
            return;
        }
    }
    fd_ = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (fd_ < 0) {
        error_ = "cannot open WAL: " + std::string(std::strerror(errno));
        return;
    }
    if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
        error_ = "WAL writer already active or lock unavailable";
        ::close(fd_);
        fd_ = -1;
        return;
    }
    const auto validation = ValidateWalFile(wal_path_);
    if (!validation.valid) {
        error_ = validation.error;
        ::close(fd_);
        fd_ = -1;
        return;
    }
    seq_ = validation.next_sequence;
    stream_id_ = validation.stream_id;
    first_sequence_ = validation.first_sequence;
    if (stream_id_.empty()) {
        first_sequence_ = seq_;
        try {
            std::random_device random;
            std::ostringstream id;
            id << std::hex << std::setfill('0');
            for (int i = 0; i < 4; ++i) id << std::setw(8) << random();
            stream_id_ = id.str();
        } catch (const std::exception& error) {
            error_ = "cannot create WAL identity: " + std::string(error.what());
            ::close(fd_);
            fd_ = -1;
            return;
        }
    }
    sync_directory_ = !existed;
}

LocalWalRegulatorySink::~LocalWalRegulatorySink() {
    if (fd_ >= 0) ::close(fd_);
}

bool LocalWalRegulatorySink::AppendOrderEvent(const OrderEvent& event) {
    return CommitOrderEvent(event).durable;
}

bool LocalWalRegulatorySink::AppendTradeEvent(const OrderEvent& event) {
    return CommitTradeEvent(event).durable;
}

bool LocalWalRegulatorySink::AppendCtpOrderSubmitMapping(const CtpOrderSubmitMapping& mapping) {
    return CommitCtpOrderSubmitMapping(mapping).durable;
}

WalReceipt LocalWalRegulatorySink::CommitOrderEvent(const OrderEvent& event) {
    return Append("order", "order_update", event);
}

WalReceipt LocalWalRegulatorySink::CommitTradeEvent(const OrderEvent& event) {
    return Append("trade", "trade_fill", event);
}

WalReceipt LocalWalRegulatorySink::CommitCtpOrderSubmitMapping(
    const CtpOrderSubmitMapping& mapping) {
    return AppendMapping(mapping);
}

WalReceipt LocalWalRegulatorySink::LastReceipt() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_receipt_;
}

std::string LocalWalRegulatorySink::LastError() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return error_;
}

bool LocalWalRegulatorySink::Flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0 || !error_.empty()) {
        return false;
    }
    if (::fsync(fd_) != 0) {
        error_ = "WAL sync failed: " + std::string(std::strerror(errno));
        return false;
    }
    return true;
}

WalReceipt LocalWalRegulatorySink::Append(const char* kind, const char* event_type,
                                          const OrderEvent& event) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!std::isfinite(event.avg_fill_price)) {
        error_ = "non-finite WAL fill price";
    }
    std::ostringstream oss;
    oss << std::setprecision(std::numeric_limits<double>::max_digits10);
    oss << "{"
        << "\"seq\":" << seq_ << ","
        << "\"schema_version\":4,"
        << "\"stream_id\":\"" << stream_id_ << "\","
        << "\"stream_first_sequence\":" << first_sequence_ << ","
        << "\"kind\":\"" << kind << "\","
        << "\"event_type\":\"" << event_type << "\","
        << "\"run_id\":\"" << EscapeJsonString(run_id_) << "\","
        << "\"exchange_ts_ns\":" << event.exchange_ts_ns << ","
        << "\"recv_ts_ns\":" << event.recv_ts_ns << ","
        << "\"ts_ns\":" << event.ts_ns << ","
        << "\"account_id\":\"" << EscapeJsonString(event.account_id) << "\","
        << "\"broker_id\":\"" << EscapeJsonString(event.broker_id) << "\","
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
        << "}";
    return CommitRecordLocked(oss.str());
}

WalReceipt LocalWalRegulatorySink::AppendMapping(const CtpOrderSubmitMapping& mapping) {
    std::lock_guard<std::mutex> lock(mutex_);

    const std::string run_id = mapping.run_id.empty() ? run_id_ : mapping.run_id;
    if (!std::isfinite(mapping.price)) {
        error_ = "non-finite WAL mapping price";
    }
    std::ostringstream oss;
    oss << std::setprecision(std::numeric_limits<double>::max_digits10);
    oss << "{"
        << "\"seq\":" << seq_ << ","
        << "\"schema_version\":4,"
        << "\"stream_id\":\"" << stream_id_ << "\","
        << "\"stream_first_sequence\":" << first_sequence_ << ","
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
        << "\"phase\":" << static_cast<int>(mapping.phase) << "}";
    return CommitRecordLocked(oss.str());
}

WalReceipt LocalWalRegulatorySink::CommitRecordLocked(const std::string& record) {
    WalReceipt receipt;
    receipt.stream_id = stream_id_;
    receipt.sequence = seq_;
    receipt.first_sequence = first_sequence_;
    receipt.checksum = WalChecksum(record);
    if (record.size() + 22 > 4 * 1024 * 1024) {
        error_ = "WAL record exceeds size limit";
    }
    if (seq_ == std::numeric_limits<std::uint64_t>::max()) {
        error_ = "WAL sequence exhausted";
    }
    if (fd_ < 0 || !error_.empty()) {
        receipt.error = error_.empty() ? "WAL is closed" : error_;
        return receipt;
    }
    const auto bytes = record.substr(0, record.size() - 1) + ",\"checksum\":\"" +
                       WalChecksumHex(receipt.checksum) + "\"}\n";
    std::size_t written = 0;
    while (written < bytes.size()) {
        const auto count = ::write(fd_, bytes.data() + written, bytes.size() - written);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            error_ = "WAL write failed: " + std::string(std::strerror(errno));
            receipt.error = error_;
            return receipt;
        }
        written += static_cast<std::size_t>(count);
    }
    if (::fsync(fd_) != 0) {
        error_ = "WAL sync failed: " + std::string(std::strerror(errno));
        receipt.error = error_;
        return receipt;
    }
    if (sync_directory_) {
        // Sync every ancestor once: create_directories may have created several levels.
        auto parent = std::filesystem::absolute(wal_path_).parent_path();
        while (!parent.empty()) {
            const int directory = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            const bool synced = directory >= 0 && ::fsync(directory) == 0;
            if (directory >= 0) ::close(directory);
            if (!synced) {
                error_ = "WAL directory sync failed";
                receipt.error = error_;
                return receipt;
            }
            const auto next = parent.parent_path();
            if (next == parent) break;
            parent = next;
        }
        sync_directory_ = false;
    }
    ++seq_;
    receipt.durable = true;
    last_receipt_ = receipt;
    return receipt;
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
                if (static_cast<unsigned char>(ch) < 0x20) {
                    constexpr char kHex[] = "0123456789abcdef";
                    out.append("\\u00");
                    out.push_back(kHex[(static_cast<unsigned char>(ch) >> 4) & 0xf]);
                    out.push_back(kHex[static_cast<unsigned char>(ch) & 0xf]);
                } else {
                    out.push_back(ch);
                }
                break;
        }
    }
    return out;
}

}  // namespace quant_hft
