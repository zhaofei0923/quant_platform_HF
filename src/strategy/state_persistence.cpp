#include "quant_hft/strategy/state_persistence.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>
#include <utility>

#if !defined(_WIN32)
#include <fcntl.h>
#include <unistd.h>
#endif

#include "quant_hft/core/simple_json.h"

namespace quant_hft {
namespace {

void SetPersistenceError(std::string* error, const std::string& message) {
    if (error != nullptr) {
        *error = message;
    }
}

std::string SanitizePathPart(const std::string& value) {
    std::string out;
    out.reserve(value.size());
    for (unsigned char ch : value) {
        if (std::isalnum(ch) != 0 || ch == '.' || ch == '_' || ch == '-') {
            out.push_back(static_cast<char>(ch));
        } else {
            out.push_back('_');
        }
    }
    return out.empty() ? "empty" : out;
}

std::string JsonEscape(const std::string& value) {
    std::ostringstream out;
    for (unsigned char ch : value) {
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
                if (ch < 0x20) {
                    out << ' ';
                } else {
                    out << static_cast<char>(ch);
                }
                break;
        }
    }
    return out.str();
}

std::int64_t CurrentEpochSeconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string SerializeFileStateJson(const std::string& account_id, const std::string& strategy_id,
                                   const StrategyState& state) {
    std::ostringstream out;
    out << "{\n";
    out << "  \"saved_epoch_seconds\": " << CurrentEpochSeconds() << ",\n";
    out << "  \"account_id\": \"" << JsonEscape(account_id) << "\",\n";
    out << "  \"strategy_id\": \"" << JsonEscape(strategy_id) << "\",\n";
    out << "  \"state\": {";
    bool first = true;
    for (const auto& [key, value] : state) {
        out << (first ? "\n" : ",\n");
        first = false;
        out << "    \"" << JsonEscape(key) << "\": \"" << JsonEscape(value) << "\"";
    }
    if (!first) {
        out << "\n  ";
    }
    out << "}\n";
    out << "}\n";
    return out.str();
}

bool ReadFileToString(const std::filesystem::path& path, std::string* out, std::string* error) {
    std::ifstream input(path);
    if (!input.is_open()) {
        SetPersistenceError(error, "failed to open state file: " + path.string());
        return false;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    *out = buffer.str();
    return true;
}

const simple_json::Value* RequireJsonField(const simple_json::Value& object, const std::string& key,
                                           std::string* error) {
    const simple_json::Value* value = object.Find(key);
    if (value == nullptr) {
        SetPersistenceError(error, "missing json field: " + key);
    }
    return value;
}

}  // namespace

RedisStrategyStatePersistence::RedisStrategyStatePersistence(
    std::shared_ptr<IRedisHashClient> redis_client, std::string key_prefix, int ttl_seconds)
    : redis_client_(std::move(redis_client)),
      key_prefix_(key_prefix.empty() ? "strategy_state" : std::move(key_prefix)),
      ttl_seconds_(ttl_seconds) {}

bool RedisStrategyStatePersistence::SaveStrategyState(const std::string& account_id,
                                                      const std::string& strategy_id,
                                                      const StrategyState& state,
                                                      std::string* error) {
    if (redis_client_ == nullptr) {
        if (error != nullptr) {
            *error = "redis client is null";
        }
        return false;
    }
    if (account_id.empty() || strategy_id.empty()) {
        if (error != nullptr) {
            *error = "account_id and strategy_id must be non-empty";
        }
        return false;
    }
    const std::string key = BuildKey(account_id, strategy_id);
    if (!redis_client_->HSet(key, state, error)) {
        return false;
    }
    if (ttl_seconds_ > 0 && !redis_client_->Expire(key, ttl_seconds_, error)) {
        return false;
    }
    return true;
}

bool RedisStrategyStatePersistence::LoadStrategyState(const std::string& account_id,
                                                      const std::string& strategy_id,
                                                      StrategyState* state,
                                                      std::string* error) const {
    if (state == nullptr) {
        if (error != nullptr) {
            *error = "state output is null";
        }
        return false;
    }
    if (redis_client_ == nullptr) {
        if (error != nullptr) {
            *error = "redis client is null";
        }
        return false;
    }
    if (account_id.empty() || strategy_id.empty()) {
        if (error != nullptr) {
            *error = "account_id and strategy_id must be non-empty";
        }
        return false;
    }
    return redis_client_->HGetAll(BuildKey(account_id, strategy_id), state, error);
}

std::string RedisStrategyStatePersistence::BuildKey(const std::string& account_id,
                                                    const std::string& strategy_id) const {
    return key_prefix_ + ":" + account_id + ":" + strategy_id;
}

FileStrategyStatePersistence::FileStrategyStatePersistence(std::string state_dir,
                                                           std::string key_prefix, int ttl_seconds)
    : state_dir_(state_dir.empty() ? "runtime/trading/state" : std::move(state_dir)),
      key_prefix_(key_prefix.empty() ? "strategy_state" : std::move(key_prefix)),
      ttl_seconds_(ttl_seconds) {}

bool FileStrategyStatePersistence::SaveStrategyState(const std::string& account_id,
                                                     const std::string& strategy_id,
                                                     const StrategyState& state,
                                                     std::string* error) {
    if (error != nullptr) error->clear();
    if (account_id.empty() || strategy_id.empty()) {
        SetPersistenceError(error, "account_id and strategy_id must be non-empty");
        return false;
    }

    std::error_code ec;
    std::filesystem::create_directories(state_dir_, ec);
    if (ec) {
        SetPersistenceError(error, "failed to create state dir: " + ec.message());
        return false;
    }

#if defined(_WIN32)
    SetPersistenceError(error, "durable file state requires the supported POSIX runtime");
    return false;
#else
    const std::filesystem::path path(BuildPath(account_id, strategy_id));
    std::string temporary = path.string() + ".tmp.XXXXXX";
    const std::string data = SerializeFileStateJson(account_id, strategy_id, state);
    int fd = ::mkstemp(temporary.data());
    if (fd < 0) {
        SetPersistenceError(
            error, "failed to create state temp file: " + std::string(std::strerror(errno)));
        return false;
    }
    const auto fail = [&](const std::string& phase, int code) {
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
        ::unlink(temporary.c_str());
        SetPersistenceError(error, phase + ": " + std::strerror(code));
        return false;
    };
    if (::fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) return fail("state close-on-exec", errno);
    std::size_t written = 0;
    while (written < data.size()) {
        const auto count = ::write(fd, data.data() + written, data.size() - written);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return fail("state file write", count == 0 ? EIO : errno);
        written += static_cast<std::size_t>(count);
    }
    const auto sync = [](int descriptor) {
        int rc;
        do {
            rc = ::fsync(descriptor);
        } while (rc < 0 && errno == EINTR);
        return rc == 0;
    };
    if (!sync(fd)) return fail("state file fsync", errno);
    const int close_result = ::close(fd);
    fd = -1;
    if (close_result < 0) return fail("state file close", errno);
    // Atomic replacement in the same directory. A failed rename must never remove
    // the previous committed snapshot. Unique temp files also isolate concurrent saves.
    if (::rename(temporary.c_str(), path.c_str()) < 0) return fail("state file rename", errno);
    auto directory = std::filesystem::absolute(path.parent_path(), ec);
    if (ec) {
        SetPersistenceError(error, "state directory resolution: " + ec.message());
        return false;
    }
    // Synchronize ancestors too: create_directories may have created more than one
    // directory, including on an earlier failed attempt. Success covers their links.
    while (!directory.empty()) {
        const int directory_fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (directory_fd < 0) return fail("state directory open", errno);
        const bool synced = sync(directory_fd);
        const int saved_error = errno;
        const int closed = ::close(directory_fd);
        if (!synced) return fail("state directory fsync", saved_error);
        if (closed < 0) return fail("state directory close", errno);
        const auto parent = directory.parent_path();
        if (parent == directory) break;
        directory = parent;
    }
    return true;
#endif
}

bool FileStrategyStatePersistence::LoadStrategyState(const std::string& account_id,
                                                     const std::string& strategy_id,
                                                     StrategyState* state,
                                                     std::string* error) const {
    if (state == nullptr) {
        SetPersistenceError(error, "state output is null");
        return false;
    }
    if (account_id.empty() || strategy_id.empty()) {
        SetPersistenceError(error, "account_id and strategy_id must be non-empty");
        return false;
    }

    const std::filesystem::path path(BuildPath(account_id, strategy_id));
    if (!std::filesystem::exists(path)) {
        SetPersistenceError(error, "state file not found: " + path.string());
        return false;
    }

    std::string text;
    if (!ReadFileToString(path, &text, error)) {
        return false;
    }
    simple_json::Value root;
    if (!simple_json::Parse(text, &root, error) || !root.IsObject()) {
        if (error != nullptr && error->empty()) {
            *error = "state file root is not a json object";
        }
        return false;
    }

    const simple_json::Value* saved_ts = RequireJsonField(root, "saved_epoch_seconds", error);
    const simple_json::Value* stored_account = RequireJsonField(root, "account_id", error);
    const simple_json::Value* stored_strategy = RequireJsonField(root, "strategy_id", error);
    const simple_json::Value* stored_state = RequireJsonField(root, "state", error);
    if (saved_ts == nullptr || stored_account == nullptr || stored_strategy == nullptr ||
        stored_state == nullptr) {
        return false;
    }
    if (!saved_ts->IsNumber() || !stored_account->IsString() || !stored_strategy->IsString() ||
        !stored_state->IsObject()) {
        SetPersistenceError(error, "invalid state file schema: " + path.string());
        return false;
    }
    if (stored_account->string_value != account_id ||
        stored_strategy->string_value != strategy_id) {
        SetPersistenceError(error, "state file identity mismatch: " + path.string());
        return false;
    }
    if (ttl_seconds_ > 0) {
        const auto saved_seconds = static_cast<std::int64_t>(saved_ts->number_value);
        if (CurrentEpochSeconds() - saved_seconds > ttl_seconds_) {
            SetPersistenceError(error, "state file expired: " + path.string());
            return false;
        }
    }

    StrategyState loaded;
    for (const auto& [key, value] : stored_state->object_value) {
        if (!value.IsString()) {
            SetPersistenceError(error, "state value must be string for key: " + key);
            return false;
        }
        loaded[key] = value.string_value;
    }
    *state = std::move(loaded);
    return true;
}

std::string FileStrategyStatePersistence::BuildPath(const std::string& account_id,
                                                    const std::string& strategy_id) const {
    const std::string filename = SanitizePathPart(key_prefix_) + "__" +
                                 SanitizePathPart(account_id) + "__" +
                                 SanitizePathPart(strategy_id) + ".json";
    return (std::filesystem::path(state_dir_) / filename).string();
}

}  // namespace quant_hft
