#include "quant_hft/services/pending_exit_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <tuple>
#include <utility>

#include "quant_hft/core/simple_json.h"

namespace quant_hft {
namespace {

constexpr int kSchemaVersion = 2;
constexpr const char* kNamespace = "__pending_exit_v2";

void SetError(std::string* error, const std::string& message) {
    if (error != nullptr) {
        *error = message;
    }
}

std::string JsonEscape(const std::string& value) {
    std::ostringstream out;
    for (const unsigned char ch : value) {
        switch (ch) {
            case '"':
                out << "\\\"";
                break;
            case '\\':
                out << "\\\\";
                break;
            case '\b':
                out << "\\b";
                break;
            case '\f':
                out << "\\f";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\r':
                out << "\\r";
                break;
            case '\t':
                out << "\\t";
                break;
            default:
                out << (ch < 0x20 ? ' ' : static_cast<char>(ch));
                break;
        }
    }
    return out.str();
}

const char* PositionSideName(PositionDirection direction) {
    return direction == PositionDirection::kLong ? "long" : "short";
}

const char* SignalTypeName(SignalType signal_type) {
    switch (signal_type) {
        case SignalType::kForceClose:
            return "force_close";
        case SignalType::kStopLoss:
            return "stop_loss";
        case SignalType::kTakeProfit:
            return "take_profit";
        case SignalType::kClose:
            return "close";
        case SignalType::kOpen:
        default:
            return "open";
    }
}

bool ParsePositionSide(const std::string& value, PositionDirection* out) {
    if (value == "long") {
        *out = PositionDirection::kLong;
        return true;
    }
    if (value == "short") {
        *out = PositionDirection::kShort;
        return true;
    }
    return false;
}

bool ParseSignalType(const std::string& value, SignalType* out) {
    if (value == "force_close") {
        *out = SignalType::kForceClose;
        return true;
    }
    if (value == "stop_loss") {
        *out = SignalType::kStopLoss;
        return true;
    }
    if (value == "take_profit") {
        *out = SignalType::kTakeProfit;
        return true;
    }
    if (value == "close") {
        *out = SignalType::kClose;
        return true;
    }
    return false;
}

bool ReadRequiredString(const simple_json::Value& root, const std::string& key, std::string* out,
                        std::string* error) {
    const auto* value = root.Find(key);
    if (value == nullptr || !value->IsString()) {
        SetError(error, "pending-exit record requires string field: " + key);
        return false;
    }
    *out = value->string_value;
    return true;
}

bool ReadEpochNanos(const simple_json::Value& root, const std::string& key, EpochNanos* out,
                    std::string* error) {
    std::string value;
    if (!ReadRequiredString(root, key, &value, error)) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const auto parsed = std::stoll(value, &consumed);
        if (consumed != value.size()) {
            throw std::invalid_argument("trailing epoch characters");
        }
        *out = parsed;
        return true;
    } catch (const std::exception&) {
        SetError(error, "invalid pending-exit epoch field: " + key);
        return false;
    }
}

bool ValidateIdentity(const PendingExitKey& key, std::string* error) {
    if (key.account_id.empty() || key.strategy_id.empty() || key.instrument_id.empty()) {
        SetError(error, "pending exit account, strategy and instrument are required");
        return false;
    }
    return true;
}

std::string SerializeKeyFields(const PendingExitKey& key) {
    std::ostringstream out;
    out << "\"account_id\":\"" << JsonEscape(key.account_id) << "\","
        << "\"strategy_id\":\"" << JsonEscape(key.strategy_id) << "\","
        << "\"instrument_id\":\"" << JsonEscape(key.instrument_id) << "\","
        << "\"position_side\":\"" << PositionSideName(key.position_side) << "\"";
    return out.str();
}

std::string SerializeUpsert(const PendingExit& pending_exit) {
    std::ostringstream out;
    out << "{\"schema_version\":" << kSchemaVersion << ",\"namespace\":\"" << kNamespace
        << "\",\"op\":\"upsert\"," << SerializeKeyFields(PendingExitStore::MakeKey(pending_exit))
        << ",\"signal_type\":\"" << SignalTypeName(pending_exit.signal_type) << "\",\"trace_id\":\""
        << JsonEscape(pending_exit.trace_id) << "\",\"trigger_ts_ns\":\""
        << pending_exit.trigger_ts_ns << "\"}\n";
    return out.str();
}

std::string SerializeRemove(const PendingExitKey& key, EpochNanos completed_ts_ns) {
    std::ostringstream out;
    out << "{\"schema_version\":" << kSchemaVersion << ",\"namespace\":\"" << kNamespace
        << "\",\"op\":\"remove\"," << SerializeKeyFields(key) << ",\"completed_ts_ns\":\""
        << completed_ts_ns << "\"}\n";
    return out.str();
}

bool ParseRecord(const std::string& line, std::string* op, PendingExitKey* key,
                 PendingExit* pending_exit, std::string* error) {
    simple_json::Value root;
    if (!simple_json::Parse(line, &root, error) || !root.IsObject()) {
        if (error != nullptr && error->empty()) {
            *error = "pending-exit record is not a JSON object";
        }
        return false;
    }
    const auto* schema = root.Find("schema_version");
    if (schema == nullptr || !schema->IsNumber() ||
        static_cast<int>(schema->number_value) != kSchemaVersion) {
        SetError(error, "unsupported pending-exit schema version");
        return false;
    }
    std::string name_space;
    std::string position_side;
    if (!ReadRequiredString(root, "namespace", &name_space, error) || name_space != kNamespace ||
        !ReadRequiredString(root, "op", op, error) ||
        !ReadRequiredString(root, "account_id", &key->account_id, error) ||
        !ReadRequiredString(root, "strategy_id", &key->strategy_id, error) ||
        !ReadRequiredString(root, "instrument_id", &key->instrument_id, error) ||
        !ReadRequiredString(root, "position_side", &position_side, error) ||
        !ParsePositionSide(position_side, &key->position_side) || !ValidateIdentity(*key, error)) {
        if (error != nullptr && error->empty()) {
            *error = "invalid pending-exit identity";
        }
        return false;
    }
    if (*op == "remove") {
        EpochNanos completed_ts_ns = 0;
        return ReadEpochNanos(root, "completed_ts_ns", &completed_ts_ns, error);
    }
    if (*op != "upsert") {
        SetError(error, "unsupported pending-exit operation");
        return false;
    }

    std::string signal_type;
    pending_exit->account_id = key->account_id;
    pending_exit->strategy_id = key->strategy_id;
    pending_exit->instrument_id = key->instrument_id;
    pending_exit->position_side = key->position_side;
    if (!ReadRequiredString(root, "signal_type", &signal_type, error) ||
        !ParseSignalType(signal_type, &pending_exit->signal_type) ||
        !ReadRequiredString(root, "trace_id", &pending_exit->trace_id, error) ||
        !ReadEpochNanos(root, "trigger_ts_ns", &pending_exit->trigger_ts_ns, error)) {
        if (error != nullptr && error->empty()) {
            *error = "invalid pending-exit upsert payload";
        }
        return false;
    }
    return true;
}

}  // namespace

bool PendingExitKey::operator==(const PendingExitKey& rhs) const {
    return account_id == rhs.account_id && strategy_id == rhs.strategy_id &&
           instrument_id == rhs.instrument_id && position_side == rhs.position_side;
}

bool PendingExitKey::operator<(const PendingExitKey& rhs) const {
    return std::tie(account_id, strategy_id, instrument_id, position_side) <
           std::tie(rhs.account_id, rhs.strategy_id, rhs.instrument_id, rhs.position_side);
}

PendingExitStore::PendingExitStore(std::string wal_path) : wal_path_(std::move(wal_path)) {}

bool PendingExitStore::Recover(std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::map<PendingExitKey, PendingExit> recovered;
    std::ifstream input(wal_path_, std::ios::binary);
    if (!input.is_open()) {
        if (std::filesystem::exists(wal_path_)) {
            SetError(error, "failed to open pending-exit WAL: " + wal_path_);
            recovered_ = false;
            return false;
        }
        pending_.clear();
        recovered_ = true;
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    std::ostringstream contents;
    contents << input.rdbuf();
    const std::string text = contents.str();
    std::size_t begin = 0;
    std::size_t line_number = 0;
    while (begin < text.size()) {
        ++line_number;
        const auto newline = text.find('\n', begin);
        const bool is_torn_tail = newline == std::string::npos;
        const auto end = is_torn_tail ? text.size() : newline;
        const auto line = text.substr(begin, end - begin);
        begin = is_torn_tail ? text.size() : newline + 1;
        if (line.empty()) {
            continue;
        }

        std::string op;
        PendingExitKey key;
        PendingExit pending_exit;
        std::string parse_error;
        if (!ParseRecord(line, &op, &key, &pending_exit, &parse_error)) {
            if (is_torn_tail) {
                break;
            }
            SetError(error, "invalid pending-exit WAL line " + std::to_string(line_number) + ": " +
                                parse_error);
            recovered_ = false;
            return false;
        }
        if (op == "remove") {
            recovered.erase(key);
        } else {
            const auto it = recovered.find(key);
            if (it == recovered.end() ||
                Priority(pending_exit.signal_type) > Priority(it->second.signal_type)) {
                recovered[key] = std::move(pending_exit);
            }
        }
    }

    pending_ = std::move(recovered);
    recovered_ = true;
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

bool PendingExitStore::IsRecovered() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return recovered_;
}

PendingExitUpsertResult PendingExitStore::Upsert(const PendingExit& pending_exit,
                                                 std::string* error) {
    const auto key = MakeKey(pending_exit);
    if (!ValidateIdentity(key, error) || Priority(pending_exit.signal_type) < 0) {
        if (Priority(pending_exit.signal_type) < 0) {
            SetError(error, "pending exit must use a close-like signal type");
        }
        return PendingExitUpsertResult::kFailed;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (!recovered_) {
        SetError(error, "pending-exit WAL must be recovered before mutation");
        return PendingExitUpsertResult::kFailed;
    }
    const auto it = pending_.find(key);
    if (it != pending_.end() &&
        Priority(pending_exit.signal_type) <= Priority(it->second.signal_type)) {
        if (error != nullptr) {
            error->clear();
        }
        return PendingExitUpsertResult::kAlreadyPending;
    }
    if (!AppendDurable(SerializeUpsert(pending_exit), error)) {
        return PendingExitUpsertResult::kFailed;
    }
    const bool inserted = it == pending_.end();
    pending_[key] = pending_exit;
    if (error != nullptr) {
        error->clear();
    }
    return inserted ? PendingExitUpsertResult::kInserted : PendingExitUpsertResult::kPriorityRaised;
}

bool PendingExitStore::RemoveAfterBrokerFlat(const PendingExitKey& key,
                                             std::int32_t broker_position_volume,
                                             EpochNanos completed_ts_ns, std::string* error) {
    if (!ValidateIdentity(key, error)) {
        return false;
    }
    if (broker_position_volume != 0) {
        SetError(error, "pending exit cannot be removed before broker position is flat");
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (!recovered_) {
        SetError(error, "pending-exit WAL must be recovered before mutation");
        return false;
    }
    const auto it = pending_.find(key);
    if (it == pending_.end()) {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }
    if (!AppendDurable(SerializeRemove(key, completed_ts_ns), error)) {
        return false;
    }
    pending_.erase(it);
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

std::optional<PendingExit> PendingExitStore::Get(const PendingExitKey& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = pending_.find(key);
    return it == pending_.end() ? std::nullopt : std::optional<PendingExit>(it->second);
}

std::vector<PendingExit> PendingExitStore::List() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PendingExit> out;
    out.reserve(pending_.size());
    for (const auto& [key, pending_exit] : pending_) {
        (void)key;
        out.push_back(pending_exit);
    }
    return out;
}

std::size_t PendingExitStore::Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_.size();
}

PendingExitKey PendingExitStore::MakeKey(const PendingExit& pending_exit) {
    return PendingExitKey{pending_exit.account_id, pending_exit.strategy_id,
                          pending_exit.instrument_id, pending_exit.position_side};
}

int PendingExitStore::Priority(SignalType signal_type) {
    switch (signal_type) {
        case SignalType::kForceClose:
            return 4;
        case SignalType::kStopLoss:
            return 3;
        case SignalType::kTakeProfit:
            return 2;
        case SignalType::kClose:
            return 1;
        case SignalType::kOpen:
        default:
            return -1;
    }
}

bool PendingExitStore::AppendDurable(const std::string& record, std::string* error) const {
    const std::filesystem::path path(wal_path_);
    std::error_code filesystem_error;
    if (const auto parent = path.parent_path(); !parent.empty()) {
        std::filesystem::create_directories(parent, filesystem_error);
        if (filesystem_error) {
            SetError(error,
                     "failed to create pending-exit WAL directory: " + filesystem_error.message());
            return false;
        }
    }
    const bool existed = std::filesystem::exists(path);
    const int fd = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (fd < 0) {
        SetError(error, "failed to open pending-exit WAL: " + std::string(std::strerror(errno)));
        return false;
    }

    std::size_t written = 0;
    while (written < record.size()) {
        const auto result = ::write(fd, record.data() + written, record.size() - written);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            const auto message = std::string(std::strerror(errno));
            ::close(fd);
            SetError(error, "failed to write pending-exit WAL: " + message);
            return false;
        }
        written += static_cast<std::size_t>(result);
    }
    if (::fsync(fd) != 0) {
        const auto message = std::string(std::strerror(errno));
        ::close(fd);
        SetError(error, "failed to fsync pending-exit WAL: " + message);
        return false;
    }
    if (::close(fd) != 0) {
        SetError(error, "failed to close pending-exit WAL: " + std::string(std::strerror(errno)));
        return false;
    }

    if (!existed && !path.parent_path().empty()) {
        const int directory_fd =
            ::open(path.parent_path().c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (directory_fd < 0 || ::fsync(directory_fd) != 0) {
            const auto message = std::string(std::strerror(errno));
            if (directory_fd >= 0) {
                ::close(directory_fd);
            }
            SetError(error, "failed to fsync pending-exit WAL directory: " + message);
            return false;
        }
        ::close(directory_fd);
    }
    return true;
}

}  // namespace quant_hft
