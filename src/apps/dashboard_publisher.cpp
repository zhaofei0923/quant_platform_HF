#include "quant_hft/apps/dashboard_publisher.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "quant_hft/apps/dashboard_wal_validation.h"
#include "quant_hft/core/simple_json.h"
#include "quant_hft/core/wal_format.h"

namespace quant_hft::dashboard {
namespace {
namespace fs = std::filesystem;
using J = simple_json::Value;
constexpr std::int64_t kDayMs = 86'400'000;
constexpr std::int64_t kRealtimeStaleAfterMs = 5'000;
constexpr double kMaximumPublicPrice = 1.0e100;
constexpr double kMaximumExactJsonInteger = 9'007'199'254'740'991.0;
constexpr std::size_t kInputLimit = 16 * 1024 * 1024;
constexpr std::size_t kWalBatchBytes = 8 * 1024 * 1024;
constexpr std::size_t kWalLineLimit = 64 * 1024;

J Obj() {
    J j;
    j.type = J::Type::kObject;
    return j;
}
J Arr() {
    J j;
    j.type = J::Type::kArray;
    return j;
}
J Str(const std::string& s) {
    J j;
    j.type = J::Type::kString;
    j.string_value = s;
    return j;
}
J Num(double n) {
    J j;
    if (std::isfinite(n)) {
        j.type = J::Type::kNumber;
        j.number_value = n;
    }
    return j;
}
J Bool(bool b) {
    J j;
    j.type = J::Type::kBool;
    j.bool_value = b;
    return j;
}
void Put(J& j, const std::string& k, J v) { j.object_value[k] = std::move(v); }
const J& Get(const J& j, const std::string& k) {
    static const J missing;
    const auto* value = j.Find(k);
    return value ? *value : missing;
}
std::string S(const J& j, const std::string& key) {
    const auto& v = Get(j, key);
    return v.IsString() ? v.string_value : "";
}
std::int64_t I(const J& j, const std::string& key, std::int64_t fallback = 0) {
    const auto& value = Get(j, key);
    try {
        if (value.IsString()) {
            std::size_t end = 0;
            auto result = std::stoll(value.string_value, &end);
            if (end == value.string_value.size()) return result;
        }
        if (value.IsNumber() && std::isfinite(value.number_value) &&
            value.number_value > static_cast<double>(INT64_MIN) &&
            value.number_value < static_cast<double>(INT64_MAX))
            return static_cast<std::int64_t>(value.number_value);
    } catch (...) {
    }
    return fallback;
}
std::string Escape(const std::string& s) {
    std::ostringstream out;
    out << '"';
    for (unsigned char c : s) {
        if (c == '"' || c == '\\')
            out << '\\' << c;
        else if (c < 0x20)
            out << "\\u00" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c)
                << std::dec;
        else
            out << c;
    }
    out << '"';
    return out.str();
}
std::string Dump(const J& j) {
    if (j.IsNull()) return "null";
    if (j.IsString()) return Escape(j.string_value);
    if (j.IsBool()) return j.bool_value ? "true" : "false";
    if (j.IsNumber()) {
        if (!std::isfinite(j.number_value)) return "null";
        std::ostringstream out;
        out << std::setprecision(17) << j.number_value;
        return out.str();
    }
    std::string out = j.IsArray() ? "[" : "{";
    bool first = true;
    if (j.IsArray()) {
        for (const auto& v : j.array_value) {
            if (!first) out += ',';
            first = false;
            out += Dump(v);
        }
    } else {
        for (const auto& [k, v] : j.object_value) {
            if (!first) out += ',';
            first = false;
            out += Escape(k) + ':' + Dump(v);
        }
    }
    return out + (j.IsArray() ? "]" : "}");
}
bool ReadJson(const fs::path& path, J* out, std::size_t limit = kInputLimit) {
    std::error_code ec;
    if (fs::is_symlink(path, ec) || !fs::is_regular_file(path, ec)) return false;
    const auto size = fs::file_size(path, ec);
    if (ec || size > limit) return false;
    std::ifstream in(path, std::ios::binary);
    std::string contents(static_cast<std::size_t>(size), '\0');
    if (!in.read(contents.data(), static_cast<std::streamsize>(size))) return false;
    std::string error;
    return simple_json::ParseStrict(contents, out, &error) && out->IsObject();
}
bool AtomicWrite(const fs::path& path, const J& value, bool public_file) {
    std::error_code ec;
    fs::create_directories(path.parent_path(), ec);
    if (ec || fs::is_symlink(path, ec)) return false;
    auto name = path.string() + ".tmp.XXXXXX";
    std::vector<char> temp(name.begin(), name.end());
    temp.push_back('\0');
    const int fd = ::mkstemp(temp.data());
    if (fd < 0) return false;
    const auto content = Dump(value) + '\n';
    bool ok = ::fchmod(fd, public_file ? 0640 : 0600) == 0;
    std::size_t offset = 0;
    while (ok && offset < content.size()) {
        const auto count = ::write(fd, content.data() + offset, content.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            ok = false;
            break;
        }
        offset += static_cast<std::size_t>(count);
    }
    ok = ok && ::fsync(fd) == 0;
    if (::close(fd) != 0) ok = false;
    if (ok) ok = ::rename(temp.data(), path.c_str()) == 0;
    if (!ok) ::unlink(temp.data());
    if (ok) {
        const int dir = ::open(path.parent_path().c_str(), O_RDONLY | O_DIRECTORY);
        if (dir < 0) return false;
        ok = ::fsync(dir) == 0;
        ::close(dir);
    }
    return ok;
}
bool Under(const fs::path& path, const fs::path& root) {
    auto p = path.begin();
    for (auto r = root.begin(); r != root.end(); ++r, ++p)
        if (p == path.end() || *p != *r) return false;
    return true;
}
fs::path CheckedPath(const std::string& text) {
    if (text.empty() || !fs::path(text).is_absolute())
        throw std::runtime_error("absolute_path_required");
    fs::path prefix;
    for (const auto& part : fs::path(text)) {
        prefix /= part;
        std::error_code ec;
        if (fs::is_symlink(prefix, ec)) throw std::runtime_error("symlink_not_allowed");
    }
    return fs::weakly_canonical(text);
}
std::string Hash(const std::string& text) {
    // Opaque display/cursor identifier; not used for authentication or integrity security.
    std::uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : text) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    std::ostringstream out;
    out << std::hex << hash;
    return out.str();
}
bool Identifier(const std::string& value, std::size_t maximum = 100) {
    return !value.empty() && value.size() <= maximum &&
           std::all_of(value.begin(), value.end(), [](unsigned char c) {
               return std::isalnum(c) || c == '_' || c == '-' || c == '.';
           });
}
std::string CalendarDay(std::int64_t ms) {
    const std::time_t seconds = ms / 1000 + 8 * 3600;
    std::tm tm{};
    if (!gmtime_r(&seconds, &tm)) return "";
    char result[16];
    std::strftime(result, sizeof(result), "%Y%m%d", &tm);
    return result;
}
bool DayValid(const std::string& day) {
    if (day.size() != 8 ||
        !std::all_of(day.begin(), day.end(), [](unsigned char c) { return std::isdigit(c); }))
        return false;
    const int y = std::stoi(day.substr(0, 4));
    const int m = std::stoi(day.substr(4, 2));
    const int d = std::stoi(day.substr(6, 2));
    static const int lengths[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    return y >= 2000 && y <= 9999 && m >= 1 && m <= 12 && d >= 1 &&
           d <= lengths[m - 1] + (m == 2 && leap ? 1 : 0);
}
std::string Freshness(std::int64_t as_of, std::int64_t now, std::int64_t limit = 10000) {
    if (as_of <= 0) return "missing";
    if (as_of > now + 2000) return "invalid";
    return now - as_of > limit ? "stale" : "fresh";
}
bool NullablePrice(const J& value) {
    return value.IsNull() || (value.IsNumber() && std::isfinite(value.number_value) &&
                              value.number_value > 0.0 &&
                              value.number_value < kMaximumPublicPrice);
}
bool ExactInteger(const J& value, double minimum, double maximum) {
    return value.IsNumber() && std::isfinite(value.number_value) &&
           value.number_value >= minimum && value.number_value <= maximum &&
           std::trunc(value.number_value) == value.number_value;
}
J Block(J data, std::int64_t stamp, const std::string& quality, const std::string& source,
        const std::string& day) {
    J j = Obj();
    Put(j, "data", std::move(data));
    Put(j, "as_of_ms", stamp > 0 ? Num(stamp) : J{});
    Put(j, "quality", Str(quality));
    Put(j, "source", Str(source));
    Put(j, "trading_day", Str(day));
    return j;
}
void Numbers(const J& from, J& to, std::initializer_list<const char*> keys) {
    for (auto key : keys) {
        const auto& v = Get(from, key);
        Put(to, key, v.IsNumber() && std::isfinite(v.number_value) ? v : J{});
    }
}
}  // namespace

struct Publisher::Impl {
    PublisherOptions options;
    fs::path root, output, state;
    std::map<std::string, fs::path> paths;
    J descriptor, identity;
    std::string account_id, alias, instance_alias, environment;
    std::string scope_id;
    int lock_fd{-1}, output_lock_fd{-1};
    bool loaded{false};
    struct Day {
        std::map<std::string, J> orders, trades;
        std::map<std::string, std::string> aliases;
        std::vector<J> equity;
        bool dirty{false};
        bool incomplete{false};
    };
    std::map<std::string, Day> days;
    std::string inode, device, anchor;
    std::uint64_t offset{0};
    bool discarding_line{false};
    std::string wal_stream;
    std::uint64_t next_sequence{0}, first_sequence{0};
    bool has_sequence{false}, stream_boundary{true};
    std::int64_t skipped_records{0}, source_resets{0}, unverified_records{0};
    bool previous_catching_up{false};
    std::string wal_quality{"missing"};
    std::int64_t wal_as_of{0};
    explicit Impl(PublisherOptions opts) : options(std::move(opts)) {}
    ~Impl() {
        if (lock_fd >= 0) ::close(lock_fd);
        if (output_lock_fd >= 0) ::close(output_lock_fd);
    }

    std::string PublicIdentifier(const std::string& value) const {
        if (!Identifier(value) ||
            (!account_id.empty() && value.find(account_id) != std::string::npos))
            return "";
        return value;
    }
    void Ids(const J& from, J& to, std::initializer_list<const char*> keys) const {
        for (const auto* key : keys) Put(to, key, Str(PublicIdentifier(S(from, key))));
    }
    bool IdentityMatches(const J& candidate) const {
        for (const auto* key : {"environment", "broker_id", "account_id", "instance_id"})
            if (S(identity, key).empty() || S(candidate, key) != S(identity, key)) return false;
        return true;
    }
    void Initialize() {
        if (options.retention_days < 1 || options.retention_days > 90 ||
            options.strategy_stale_after_ms < 1000 || options.strategy_stale_after_ms > 3600000)
            throw std::runtime_error("invalid_retention_or_stale_interval");
        root = CheckedPath(options.source_dir);
        output = CheckedPath(options.output_dir);
        state = CheckedPath(options.state_dir);
        if (Under(output, root) || Under(root, output) || Under(state, root) ||
            Under(root, state) || Under(output, state) || Under(state, output))
            throw std::runtime_error("overlapping_directories");
        const auto identity_path = CheckedPath(options.identity_file);
        if (Under(identity_path, output) || !ReadJson(identity_path, &descriptor) ||
            I(descriptor, "schema_version") != 1 ||
            CheckedPath(S(descriptor, "recovery_root")) != root)
            throw std::runtime_error("invalid_identity_descriptor");
        identity = Get(descriptor, "identity");
        for (const auto* key : {"environment", "broker_id", "account_id", "instance_id"})
            if (!Identifier(S(identity, key))) throw std::runtime_error("incomplete_identity");
        account_id = S(identity, "account_id");
        environment = S(identity, "environment");
        alias = S(descriptor, "account_alias");
        instance_alias = S(descriptor, "instance_alias");
        for (const auto& value : {alias, instance_alias})
            if (value.empty() || value.size() > 120 ||
                value.find(account_id) != std::string::npos ||
                value.find_first_of("\r\n") != std::string::npos)
                throw std::runtime_error("invalid_public_alias");
        const std::map<std::string, std::string> defaults = {
            {"private_snapshot", "monitor/dashboard_private.json"},
            {"readiness", "monitor/readiness.json"},
            {"pipeline_health", "monitor/pipeline_health.json"},
            {"wal", "wal/events.wal"}};
        for (const auto& [key, fallback] : defaults) {
            const auto value = S(Get(descriptor, "paths"), key);
            paths[key] = CheckedPath(value.empty() ? (root / fallback).string() : value);
            if (!Under(paths[key], root))
                throw std::runtime_error("source_path_outside_identity_root");
        }
        fs::create_directories(state / "days");
        fs::create_directories(output / "days");
        lock_fd = ::open((state / "publisher.lock").c_str(), O_CREAT | O_RDWR | O_NOFOLLOW, 0600);
        if (lock_fd < 0 || ::flock(lock_fd, LOCK_EX | LOCK_NB) != 0)
            throw std::runtime_error("publisher_already_running");
        J owner;
        const auto owner_path = state / "identity.json";
        if (fs::exists(owner_path)) {
            if (!ReadJson(owner_path, &owner) || !IdentityMatches(owner))
                throw std::runtime_error("checkpoint_identity_mismatch");
        } else if (!AtomicWrite(owner_path, identity, false))
            throw std::runtime_error("identity_write_failed");
        J scope;
        if (fs::exists(state / "scope.json")) {
            if (!ReadJson(state / "scope.json", &scope))
                throw std::runtime_error("invalid_scope_marker");
            scope_id = S(scope, "data_scope_id");
            if (scope_id.size() != 32 ||
                !std::all_of(scope_id.begin(), scope_id.end(),
                             [](unsigned char c) { return std::isxdigit(c); }))
                throw std::runtime_error("invalid_scope_marker");
        } else {
            std::ifstream random("/dev/urandom", std::ios::binary);
            std::array<unsigned char, 16> bytes{};
            if (!random.read(reinterpret_cast<char*>(bytes.data()), bytes.size()))
                throw std::runtime_error("scope_random_unavailable");
            std::ostringstream encoded;
            for (auto byte : bytes)
                encoded << std::hex << std::setw(2) << std::setfill('0') << unsigned(byte);
            scope_id = encoded.str();
            scope = Obj();
            Put(scope, "data_scope_id", Str(scope_id));
            if (!AtomicWrite(state / "scope.json", scope, false))
                throw std::runtime_error("scope_write_failed");
        }
        output_lock_fd =
            ::open((output / ".publisher.lock").c_str(), O_CREAT | O_RDWR | O_NOFOLLOW, 0600);
        if (output_lock_fd < 0 || ::flock(output_lock_fd, LOCK_EX | LOCK_NB) != 0)
            throw std::runtime_error("output_already_in_use");
        J output_owner;
        if (fs::exists(output / ".publisher-owner.json")) {
            if (!ReadJson(output / ".publisher-owner.json", &output_owner) ||
                S(output_owner, "data_scope_id") != scope_id)
                throw std::runtime_error("output_scope_mismatch");
        } else {
            if (fs::exists(output / "current.json") || fs::exists(output / "days.json"))
                throw std::runtime_error("unowned_existing_public_output");
            if (!AtomicWrite(output / ".publisher-owner.json", scope, false))
                throw std::runtime_error("output_scope_write_failed");
        }
        J checkpoint;
        if (fs::exists(state / "checkpoint.json")) {
            if (!ReadJson(state / "checkpoint.json", &checkpoint) ||
                !IdentityMatches(Get(checkpoint, "identity")))
                throw std::runtime_error("invalid_checkpoint");
            inode = S(checkpoint, "inode");
            device = S(checkpoint, "device");
            offset = static_cast<std::uint64_t>(std::max<std::int64_t>(0, I(checkpoint, "offset")));
            anchor = S(checkpoint, "anchor");
            skipped_records = I(checkpoint, "skipped_records");
            source_resets = I(checkpoint, "source_resets");
            unverified_records = I(checkpoint, "unverified_records");
            discarding_line = Get(checkpoint, "discarding_line").bool_value;
            wal_stream = S(checkpoint, "wal_stream");
            next_sequence = static_cast<std::uint64_t>(
                std::max<std::int64_t>(0, I(checkpoint, "next_sequence")));
            first_sequence = static_cast<std::uint64_t>(
                std::max<std::int64_t>(0, I(checkpoint, "first_sequence")));
            has_sequence = Get(checkpoint, "has_sequence").bool_value;
            stream_boundary = Get(checkpoint, "stream_boundary").bool_value;
        }
        for (const auto& entry : fs::directory_iterator(state / "days")) {
            const auto day = entry.path().stem().string();
            if (!DayValid(day) || entry.path().extension() != ".json") continue;
            J value;
            if (!ReadJson(entry.path(), &value, 256 * 1024 * 1024) ||
                !IdentityMatches(Get(value, "identity")))
                throw std::runtime_error("invalid_daily_checkpoint");
            auto& saved = days[day];
            for (const auto& [key, item] : Get(value, "orders").object_value)
                saved.orders[key] = item;
            for (const auto& [key, item] : Get(value, "trades").object_value)
                saved.trades[key] = item;
            saved.equity = Get(value, "equity").array_value;
            saved.incomplete = Get(value, "incomplete").bool_value;
            for (const auto& [key, order] : saved.orders)
                for (const auto& a : Get(order, "_keys").array_value)
                    if (a.IsString()) saved.aliases[a.string_value] = key;
            for (const auto& [key, trade] : saved.trades)
                for (const auto& a : Get(trade, "_keys").array_value)
                    if (a.IsString() && !S(trade, "order_id").empty())
                        saved.aliases.emplace(a.string_value, S(trade, "order_id"));
            saved.dirty = true;  // Republish interrupted output generations after restart.
        }
        loaded = true;
    }

    J PrivateBlock(const J& source, bool position, std::int64_t now) {
        const auto stamp = I(source, "as_of_ms");
        const auto day = S(source, "trading_day");
        const auto& input = Get(source, "data");
        std::string quality = Freshness(stamp, now, 15000);
        if (S(source, "quality") == "failed")
            quality = "incomplete";
        else if (S(source, "quality") != "ok")
            quality = "missing";
        // A failed first query legitimately has no timestamp, trading day, or prior data.
        // Preserve that failure as incomplete instead of degrading it to ordinary missing data.
        if (S(source, "quality") != "failed" &&
            (!DayValid(day) || (!position && !input.IsObject()) ||
             (position && !input.IsArray())))
            quality = input.IsNull() ? "missing" : "invalid";
        J data = position ? Arr() : J{};
        if (!position && input.IsObject()) {
            data = Obj();
            Numbers(input, data,
                    {"balance", "available", "curr_margin", "frozen_margin", "frozen_cash",
                     "frozen_commission", "commission", "close_profit", "position_profit"});
            for (const auto& [key, value] : data.object_value)
                if (value.IsNull()) quality = "invalid";
        } else if (position && input.IsArray()) {
            data = Arr();
            for (const auto& row : input.array_value) {
                J item = Obj();
                Ids(row, item,
                    {"instrument_id", "exchange_id", "posi_direction", "hedge_flag",
                     "position_date"});
                Numbers(row, item,
                        {"position", "today_position", "yd_position", "long_frozen", "short_frozen",
                         "open_volume", "close_volume", "open_cost", "position_cost", "use_margin",
                         "position_profit", "close_profit"});
                if (S(item, "instrument_id").empty() || Get(item, "position").IsNull())
                    quality = "invalid";
                data.array_value.push_back(std::move(item));
            }
        }
        const auto source_name = S(source, "source") == "ctp"         ? "ctp"
                                 : S(source, "source") == "simulated" ? "simulated"
                                                                      : "unknown";
        return Block(std::move(data), stamp, quality, source_name, DayValid(day) ? day : "");
    }

    J Health(std::int64_t now, const std::string& day, std::int64_t writer_started) {
        J readiness, pipeline;
        const bool ready_found = ReadJson(paths.at("readiness"), &readiness);
        const bool pipeline_found = ReadJson(paths.at("pipeline_health"), &pipeline);
        const auto ready_stamp = I(readiness, "heartbeat_ts_ns") / 1000000;
        const auto pipe_stamp = I(pipeline, "generated_epoch") * 1000;
        const std::string ready_quality = !ready_found                          ? "missing"
                                          : I(readiness, "schema_version") != 2 ? "invalid"
                                          : ready_stamp < writer_started
                                              ? "stale"
                                              : Freshness(ready_stamp, now);
        const std::string pipe_quality = !pipeline_found                      ? "missing"
                                         : I(pipeline, "schema_version") != 3 ? "invalid"
                                         : pipe_stamp < writer_started        ? "stale"
                                                                       : Freshness(pipe_stamp, now);
        J r = Obj();
        Ids(readiness, r, {"mode"});
        for (const auto* key :
             {"recovery_complete", "trader_ready", "gateway_healthy", "settlement_confirmed"})
            Put(r, key, Get(readiness, key).IsBool() ? Get(readiness, key) : J{});
        Numbers(readiness, r, {"pending_exit_count", "unresolved_mapping_count", "generation"});
        J reasons = Arr();
        for (const auto& reason : Get(readiness, "reasons").array_value) {
            const auto value = PublicIdentifier(reason.string_value);
            if (!value.empty()) reasons.array_value.push_back(Str(value));
        }
        Put(r, "reasons", reasons);
        Put(r, "as_of_ms", ready_stamp > 0 ? Num(ready_stamp) : J{});
        Put(r, "quality", Str(ready_quality));
        J p = Obj();
        Ids(pipeline, p, {"overall_status", "session"});
        Numbers(pipeline, p, {"warning_count", "critical_count", "generation"});
        J stages = Arr();
        for (const std::string name :
             {"runtime", "market_data", "bar_1m", "bar_5m", "strategy", "execution"}) {
            J stage = Obj();
            Put(stage, "name", Str(name));
            Put(stage, "status", Str(PublicIdentifier(S(pipeline, name + "_status"))));
            Put(stage, "reason", Str(PublicIdentifier(S(pipeline, name + "_reason"))));
            stages.array_value.push_back(std::move(stage));
        }
        Put(p, "stages", stages);
        J products = Arr();
        for (const auto& row : Get(pipeline, "products").array_value) {
            J product = Obj();
            Ids(row, product, {"product_id", "instrument_id", "exchange_id", "status", "reason"});
            Numbers(row, product,
                    {"strategy_evaluations", "candidates", "allowed", "pending_traces",
                     "tick_age_seconds", "tick_delay_ms"});
            products.array_value.push_back(std::move(product));
        }
        Put(p, "products", products);
        Put(p, "as_of_ms", pipe_stamp > 0 ? Num(pipe_stamp) : J{});
        Put(p, "quality", Str(pipe_quality));
        Put(p, "trading_day",
            Str(DayValid(S(pipeline, "trading_day")) ? S(pipeline, "trading_day") : ""));
        J data = Obj();
        Put(data, "readiness", r);
        Put(data, "pipeline", p);
        return Block(data, std::min(ready_stamp, pipe_stamp),
                     ready_quality == "fresh" && pipe_quality == "fresh" ? "fresh" : "incomplete",
                     "readiness_pipeline_v3", day);
    }

    void BrokerInstruments(const J& positions,
                           std::map<std::string, std::string>* instruments) const {
        for (const auto& row : Get(positions, "data").array_value) {
            const auto& position = Get(row, "position");
            const auto instrument = S(row, "instrument_id");
            if (!position.IsNumber() || position.number_value == 0 || !Identifier(instrument))
                continue;
            const auto exchange = PublicIdentifier(S(row, "exchange_id"));
            auto [it, inserted] = instruments->emplace(instrument, exchange);
            if (!inserted && it->second != exchange) it->second.clear();
        }
    }

    J StrategyRisk(const J& source, std::int64_t now, const std::string& selected_day,
                   std::map<std::string, std::string>* instruments) {
        J rows = Arr();
        const auto input_quality = S(source, "quality");
        const auto source_day = S(source, "trading_day");
        const auto source_stamp = I(source, "as_of_ms");
        const auto& input = Get(source, "data");
        bool invalid = false;
        bool missing_row_stamp = false;
        std::int64_t oldest = 0;

        if ((input_quality == "ok" || input_quality == "failed") && input.IsArray()) {
            for (const auto& row : input.array_value) {
                const auto instrument = PublicIdentifier(S(row, "instrument_id"));
                const auto strategy = PublicIdentifier(S(row, "strategy_id"));
                const auto owner = PublicIdentifier(S(row, "owner_strategy_id"));
                const auto& net = Get(row, "net");
                const bool valid_net =
                    ExactInteger(net, std::numeric_limits<std::int32_t>::min(),
                                 std::numeric_limits<std::int32_t>::max()) &&
                    net.number_value != 0.0;
                static constexpr std::array<const char*, 5> kRiskPriceKeys = {
                    "avg_open", "initial_stop", "trailing_stop", "take_profit", "effective_stop"};
                const bool valid_prices = std::all_of(
                    kRiskPriceKeys.begin(), kRiskPriceKeys.end(),
                    [&](const char* key) { return NullablePrice(Get(row, key)); });
                const auto& effective_stop = Get(row, "effective_stop");
                const auto stop_kind = S(row, "stop_kind");
                const bool valid_stop =
                    (effective_stop.IsNull() && stop_kind.empty()) ||
                    (stop_kind == "initial" && effective_stop.IsNumber() &&
                     Get(row, "initial_stop").IsNumber() &&
                     effective_stop.number_value == Get(row, "initial_stop").number_value) ||
                    (stop_kind == "trailing" && effective_stop.IsNumber() &&
                     Get(row, "trailing_stop").IsNumber() &&
                     effective_stop.number_value == Get(row, "trailing_stop").number_value);
                const auto& stamp_value = Get(row, "as_of_ms");
                const bool has_stamp = !stamp_value.IsNull();
                const bool valid_stamp =
                    !has_stamp ||
                    (ExactInteger(stamp_value, 1.0, kMaximumExactJsonInteger) &&
                     stamp_value.number_value <= static_cast<double>(now + 2000));
                if (instrument.empty() || strategy.empty() || !valid_net || !valid_prices ||
                    !valid_stop || !valid_stamp) {
                    invalid = true;
                    continue;
                }
                if (!S(row, "owner_strategy_id").empty() && owner.empty()) {
                    invalid = true;
                    continue;
                }
                J item = Obj();
                Put(item, "instrument_id", Str(instrument));
                Put(item, "strategy_id", Str(strategy));
                if (!owner.empty()) Put(item, "owner_strategy_id", Str(owner));
                Put(item, "net", net);
                Numbers(row, item,
                        {"avg_open", "initial_stop", "trailing_stop", "take_profit",
                         "effective_stop"});
                if (effective_stop.IsNumber()) {
                    Put(item, "stop_kind", Str(stop_kind));
                } else {
                    Put(item, "stop_kind", J{});
                }

                if (!has_stamp) {
                    missing_row_stamp = true;
                    Put(item, "as_of_ms", J{});
                } else {
                    const auto stamp = static_cast<std::int64_t>(stamp_value.number_value);
                    Put(item, "as_of_ms", Num(stamp));
                    oldest = oldest == 0 ? stamp : std::min(oldest, stamp);
                }
                instruments->try_emplace(instrument, "");
                rows.array_value.push_back(std::move(item));
            }
        }

        std::string quality;
        if (!source.IsObject() || input_quality == "missing") {
            quality = "missing";
        } else if ((input_quality != "ok" && input_quality != "failed") || !input.IsArray() ||
                   (!DayValid(source_day) && input_quality == "ok") ||
                   (DayValid(selected_day) && DayValid(source_day) &&
                    source_day != selected_day) ||
                   invalid) {
            quality = "invalid";
        } else if (input_quality == "failed") {
            quality = "incomplete";
        } else if (missing_row_stamp && !rows.array_value.empty()) {
            quality = "incomplete";
        } else {
            quality = Freshness(oldest > 0 ? oldest : source_stamp, now,
                                options.strategy_stale_after_ms);
        }
        const auto stamp = oldest > 0 ? oldest : source_stamp;
        auto block = Block(rows, stamp, quality, "strategy_engine",
                           DayValid(source_day) ? source_day : "");
        Put(block, "stale_after_ms", Num(options.strategy_stale_after_ms));
        return block;
    }

    J MarketQuotes(const J& source, const std::map<std::string, std::string>& instruments,
                   std::int64_t now, const std::string& selected_day) {
        const auto input_quality = S(source, "quality");
        const auto source_day = S(source, "trading_day");
        const auto source_stamp = I(source, "as_of_ms");
        const auto& input = Get(source, "data");
        std::map<std::string, J> quotes;
        std::map<std::string, std::string> row_qualities;
        bool invalid = false;

        if ((input_quality == "ok" || input_quality == "failed") && input.IsArray()) {
            for (const auto& row : input.array_value) {
                const auto instrument = PublicIdentifier(S(row, "instrument_id"));
                const auto requested = instruments.find(instrument);
                if (instrument.empty() || requested == instruments.end()) continue;

                const auto exchange = PublicIdentifier(S(row, "exchange_id"));
                const bool bad_exchange = !S(row, "exchange_id").empty() && exchange.empty();
                const bool exchange_conflict = !exchange.empty() && !requested->second.empty() &&
                                               exchange != requested->second;
                const auto row_day = S(row, "trading_day");
                const bool bad_day = !DayValid(row_day) ||
                                     (DayValid(source_day) && row_day != source_day) ||
                                     (DayValid(selected_day) && row_day != selected_day);
                static constexpr std::array<const char*, 3> kMarketPriceKeys = {
                    "last_price", "bid_price_1", "ask_price_1"};
                const bool valid_prices = std::all_of(
                    kMarketPriceKeys.begin(), kMarketPriceKeys.end(),
                    [&](const char* key) { return NullablePrice(Get(row, key)); });
                const auto valid_count = [&](const char* key) {
                    const auto& value = Get(row, key);
                    return value.IsNull() ||
                           ExactInteger(value, 0.0, kMaximumExactJsonInteger);
                };
                const auto& stamp_value = Get(row, "as_of_ms");
                const bool valid_stamp =
                    ExactInteger(stamp_value, 1.0, kMaximumExactJsonInteger);
                const auto stamp = valid_stamp
                                       ? static_cast<std::int64_t>(stamp_value.number_value)
                                       : 0;
                if (bad_exchange || exchange_conflict || bad_day || !valid_prices ||
                    !valid_count("volume") || !valid_count("open_interest") || !valid_stamp ||
                    stamp > now + 2000) {
                    invalid = true;
                    continue;
                }

                J item = Obj();
                Put(item, "instrument_id", Str(instrument));
                Put(item, "exchange_id", Str(exchange.empty() ? requested->second : exchange));
                const auto product = PublicIdentifier(S(row, "product_id"));
                if (!product.empty()) Put(item, "product_id", Str(product));
                Numbers(row, item,
                        {"last_price", "bid_price_1", "ask_price_1", "volume", "open_interest"});
                const auto quality = input_quality == "failed"
                                         ? "incomplete"
                                         : Freshness(stamp, now, kRealtimeStaleAfterMs);
                Put(item, "as_of_ms", Num(stamp));
                Put(item, "quality", Str(quality));

                const auto existing = quotes.find(instrument);
                if (existing == quotes.end() || stamp > I(existing->second, "as_of_ms")) {
                    quotes[instrument] = std::move(item);
                    row_qualities[instrument] = quality;
                }
            }
        }

        J rows = Arr();
        std::size_t fresh = 0, stale = 0, missing = 0, bad = 0;
        std::int64_t oldest = 0;
        for (const auto& [instrument, exchange] : instruments) {
            auto found = quotes.find(instrument);
            if (found == quotes.end()) {
                J item = Obj();
                Put(item, "instrument_id", Str(instrument));
                Put(item, "exchange_id", Str(exchange));
                Numbers(J{}, item,
                        {"last_price", "bid_price_1", "ask_price_1", "volume", "open_interest"});
                Put(item, "as_of_ms", J{});
                Put(item, "quality", Str("missing"));
                rows.array_value.push_back(std::move(item));
                ++missing;
                continue;
            }
            const auto quality = row_qualities[instrument];
            if (quality == "fresh")
                ++fresh;
            else if (quality == "stale")
                ++stale;
            else
                ++bad;
            const auto stamp = I(found->second, "as_of_ms");
            if (stamp > 0) oldest = oldest == 0 ? stamp : std::min(oldest, stamp);
            rows.array_value.push_back(std::move(found->second));
        }

        std::string quality;
        if (!source.IsObject() || input_quality == "missing") {
            quality = "missing";
        } else if ((input_quality != "ok" && input_quality != "failed") || !input.IsArray() ||
                   (!DayValid(source_day) && input_quality == "ok") ||
                   (DayValid(selected_day) && DayValid(source_day) &&
                    source_day != selected_day) ||
                   invalid) {
            quality = "invalid";
        } else if (input_quality == "failed") {
            quality = "incomplete";
        } else if (instruments.empty()) {
            quality = Freshness(source_stamp, now, kRealtimeStaleAfterMs);
        } else if (fresh == instruments.size()) {
            quality = "fresh";
        } else if (stale == instruments.size()) {
            quality = "stale";
        } else if (missing == instruments.size()) {
            quality = "missing";
        } else if (bad > 0) {
            quality = "incomplete";
        } else {
            quality = "incomplete";
        }
        auto block = Block(rows, oldest > 0 ? oldest : source_stamp, quality,
                           "ctp_market_callback", DayValid(source_day) ? source_day : "");
        Put(block, "stale_after_ms", Num(kRealtimeStaleAfterMs));
        return block;
    }

    std::vector<std::string> OrderKeys(const J& record, const std::string& day) const {
        std::vector<std::string> keys;
        if (!S(record, "client_order_id").empty())
            keys.push_back(day + "|client|" + S(record, "client_order_id"));
        if (!S(record, "exchange_order_id").empty() && !S(record, "exchange_id").empty())
            keys.push_back(day + "|exchange|" + S(record, "exchange_id") + '|' +
                           S(record, "exchange_order_id"));
        if (!S(record, "order_ref").empty() && I(record, "front_id") > 0 &&
            I(record, "session_id") > 0)
            keys.push_back(day + "|ref|" + std::to_string(I(record, "front_id")) + '|' +
                           std::to_string(I(record, "session_id")) + '|' + S(record, "order_ref"));
        return keys;
    }
    J EventFields(const J& record, std::int64_t stamp) const {
        J row = Obj();
        Ids(record, row, {"instrument_id", "exchange_id", "strategy_id"});
        static const char* sides[] = {"buy", "sell"};
        static const char* offsets[] = {"open", "close", "close_today", "close_yesterday"};
        const auto side = I(record, "side", -1), off = I(record, "offset", -1);
        Put(row, "side", Str(side >= 0 && side < 2 ? sides[side] : "unknown"));
        Put(row, "offset", Str(off >= 0 && off < 4 ? offsets[off] : "unknown"));
        Put(row, "as_of_ms", stamp > 0 ? Num(stamp) : J{});
        Put(row, "attribution", Str(S(row, "strategy_id").empty() ? "unattributed" : "strategy"));
        return row;
    }
    std::pair<std::string, std::string> Rejection(const J& record) const {
        std::string text =
            S(record, "reason") + " " + S(record, "status_msg") + " " + S(record, "error_code");
        std::transform(text.begin(), text.end(), text.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        const auto contains = [&](std::initializer_list<const char*> tokens) {
            return std::any_of(tokens.begin(), tokens.end(), [&](const char* token) {
                return text.find(token) != std::string::npos;
            });
        };
        if (contains({"insufficient_margin", "insufficient_funds", "risk_limit_exceeded",
                      "资金不足", "保证金不足", "风险额度不足"}))
            return {"risk_limit", "风险额度或可用保证金不足"};
        if (contains({"close_only", "close-only", "opening_blocked", "open_not_allowed", "禁止开仓",
                      "只允许平仓"}))
            return {"opening_blocked", "当前交易权限禁止开仓"};
        if (contains(
                {"rate_limit", "flow_control", "too frequent", "频率限制", "报单频繁", "限流"}))
            return {"rate_limited", "委托触发频率限制"};
        if (contains({"invalid_price", "invalid_volume", "invalid_quantity", "价格无效", "数量无效",
                      "价格超出"}))
            return {"invalid_price_or_volume", "委托价格或数量未通过校验"};
        if (contains({"ctp:", "ctp_reject", "ctp reject", "ctp_order_rejected"}))
            return {"ctp_rejected", "CTP拒绝委托"};
        return {"unknown_rejection", "具体原因未识别，请在交易端核查"};
    }
    void Event(const J& record, std::int64_t now) {
        const auto kind = S(record, "kind"), type = S(record, "event_type");
        if (kind != "order" && kind != "trade") return;
        const auto day = S(record, "trading_day");
        if (S(record, "account_id") != account_id || !DayValid(day)) {
            ++skipped_records;
            return;
        }
        if (day < CalendarDay(now - (options.retention_days - 1) * kDayMs) ||
            day > CalendarDay(now + 7 * kDayMs))
            return;
        auto& history = days[day];
        std::int64_t stamp = I(record, "exchange_ts_ns") / 1000000;
        if (stamp <= 0) stamp = I(record, "ts_ns") / 1000000;
        const auto observed =
            I(record, "recv_ts_ns") > 0 ? I(record, "recv_ts_ns") / 1000000 : stamp;
        const auto keys = OrderKeys(record, day);
        std::string order_key;
        std::set<std::string> related_orders;
        for (const auto& key : keys) {
            const auto found = history.aliases.find(key);
            if (found != history.aliases.end()) {
                if (order_key.empty()) order_key = found->second;
                related_orders.insert(found->second);
            }
        }
        if (order_key.empty() && !keys.empty()) order_key = "o-" + Hash(keys.front());
        // A later callback can join an early client identity to an exchange-only identity.
        // Repoint both the order row and any already observed fills to one display id.
        for (const auto& other : related_orders) {
            if (other == order_key) continue;
            const auto found = history.orders.find(other);
            if (found != history.orders.end()) {
                auto& target = history.orders[order_key];
                J merged_keys = Arr();
                merged_keys.array_value = Get(target, "_keys").array_value;
                for (const auto& key : Get(found->second, "_keys").array_value)
                    merged_keys.array_value.push_back(key);
                if (I(found->second, "_observed") > I(target, "_observed") &&
                    !(I(target, "_status", -1) >= 3 && I(found->second, "_status", -1) < 3))
                    target = found->second;
                Put(target, "order_id", Str(order_key));
                Put(target, "_keys", merged_keys);
                history.orders.erase(found);
            }
            for (auto& [key, value] : history.aliases)
                if (value == other) value = order_key;
            for (auto& [key, trade] : history.trades)
                if (S(trade, "order_id") == other) Put(trade, "order_id", Str(order_key));
            history.dirty = true;
        }
        for (const auto& key : keys) history.aliases[key] = order_key;
        const bool fill = type == "trade_fill" || (type.empty() && kind == "trade");
        if (fill) {
            const auto trade_id = S(record, "trade_id"), exchange = S(record, "exchange_id");
            if (trade_id.empty() || exchange.empty() || stamp <= 0) {
                history.incomplete = history.dirty = true;
                ++skipped_records;
                return;
            }
            const auto key = day + '|' + exchange + '|' + trade_id;
            const auto existing = history.trades.find(key);
            J row = EventFields(record, stamp);
            Put(row, "order_id", Str(order_key));
            J private_keys = Arr();
            for (const auto& key : keys) private_keys.array_value.push_back(Str(key));
            Put(row, "_keys", private_keys);
            Put(row, "trade_id",
                Str(PublicIdentifier(trade_id).empty() ? "t-" + Hash(key) : trade_id));
            const auto volume = I(record, "last_trade_volume", 0) > 0
                                    ? I(record, "last_trade_volume")
                                    : I(record, "filled_volume");
            Put(row, "volume", Num(volume));
            Put(row, "price", Get(record, "avg_fill_price"));
            if (volume <= 0 || !Get(row, "price").IsNumber() ||
                !std::isfinite(Get(row, "price").number_value) ||
                Get(row, "price").number_value <= 0) {
                history.incomplete = history.dirty = true;
                ++skipped_records;
                return;
            }
            if (existing == history.trades.end()) {
                history.trades.emplace(key, row);
                history.dirty = true;
            } else if (I(existing->second, "volume") != volume ||
                       Dump(Get(existing->second, "price")) != Dump(Get(row, "price")) ||
                       S(existing->second, "instrument_id") != S(row, "instrument_id") ||
                       S(existing->second, "side") != S(row, "side") ||
                       S(existing->second, "offset") != S(row, "offset"))
                history.incomplete = history.dirty = true;
            return;
        }
        if (type != "order_update" && !type.empty()) return;
        if (order_key.empty()) {
            history.incomplete = history.dirty = true;
            ++skipped_records;
            return;
        }
        auto& stored = history.orders[order_key];
        const auto old_status = I(stored, "_status", -1), status = I(record, "status", -1);
        const bool terminal = old_status >= 3 && old_status <= 5;
        const auto seq = I(record, "seq");
        const bool older = observed < I(stored, "_observed") ||
                           (S(stored, "_run") == S(record, "run_id") && seq < I(stored, "_seq"));
        if (!older && !(terminal && status >= 0 && status < 3)) {
            J row = EventFields(record, stamp);
            Put(row, "order_id", Str(order_key));
            static const char* statuses[] = {"new",    "submitted", "partially_filled",
                                             "filled", "canceled",  "rejected"};
            Put(row, "status", Str(status >= 0 && status < 6 ? statuses[status] : "unknown"));
            Numbers(record, row, {"total_volume", "filled_volume", "avg_fill_price"});
            const auto rejection = Rejection(record);
            Put(row, "reject_code", Str(status == 5 ? rejection.first : ""));
            Put(row, "reject_message", Str(status == 5 ? rejection.second : ""));
            Put(row, "_status", Num(status));
            Put(row, "_observed", Num(observed));
            Put(row, "_run", Str(S(record, "run_id")));
            Put(row, "_seq", Str(std::to_string(seq)));
            Put(row, "_keys", Get(stored, "_keys").IsArray() ? Get(stored, "_keys") : Arr());
            stored = std::move(row);
            history.dirty = true;
        }
        if (!Get(stored, "_keys").IsArray()) Put(stored, "_keys", Arr());
        for (const auto& key : keys) {
            history.aliases[key] = order_key;
            auto& list = stored.object_value["_keys"].array_value;
            if (std::none_of(list.begin(), list.end(),
                             [&](const J& item) { return item.string_value == key; }))
                list.push_back(Str(key));
        }
    }

    std::string Anchor(const fs::path& file, std::uint64_t at) const {
        std::ifstream in(file, std::ios::binary);
        const auto count = std::min<std::uint64_t>(kWalLineLimit, at);
        in.seekg(static_cast<std::streamoff>(at - count));
        std::string bytes(count, '\0');
        if (!in.read(bytes.data(), static_cast<std::streamsize>(count))) return "";
        in.clear();
        in.seekg(0);
        const auto prefix_count = std::min<std::uint64_t>(256, at);
        std::string prefix(prefix_count, '\0');
        if (!in.read(prefix.data(), static_cast<std::streamsize>(prefix_count))) return "";
        return Hash(prefix + bytes);
    }
    bool Sequence(const std::string& line, const J& record) {
        if (I(record, "schema_version") != 4) {
            // Downgrading within a modern stream cannot turn corrupted input into legacy data.
            return wal_stream.empty();
        }
        std::uint64_t seq = 0, first = 0;
        if (!WalUnsignedField(line, "seq", &seq) ||
            !WalUnsignedField(line, "stream_first_sequence", &first) ||
            seq == std::numeric_limits<std::uint64_t>::max())
            return false;
        const auto stream = S(record, "stream_id");
        if (wal_stream.empty() || (stream != wal_stream && stream_boundary)) {
            wal_stream = stream;
            first_sequence = first;
            has_sequence = true;
            next_sequence = seq + 1;
            stream_boundary = false;
            return seq == first;
        }
        if (stream != wal_stream || first != first_sequence) return false;
        const bool continuous = !has_sequence || seq == next_sequence;
        // The missing/duplicate record is rejected; later contiguous observations can still
        // be displayed under incomplete coverage instead of silently claiming full history.
        if (seq >= next_sequence) next_sequence = seq + 1;
        has_sequence = true;
        stream_boundary = false;
        return continuous;
    }
    bool Tail(const fs::path& file, std::int64_t now) {
        struct stat st {};
        if (::lstat(file.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
            wal_quality = "missing";
            return false;
        }
        const auto current_inode = std::to_string(st.st_ino),
                   current_device = std::to_string(st.st_dev);
        if (!inode.empty() && (inode != current_inode || device != current_device)) {
            bool old_found = false;
            for (const auto& entry : fs::directory_iterator(file.parent_path())) {
                if (entry.path().filename().string().rfind(file.filename().string(), 0) != 0)
                    continue;
                struct stat prior {};
                if (::lstat(entry.path().c_str(), &prior) != 0 || !S_ISREG(prior.st_mode) ||
                    std::to_string(prior.st_ino) != inode || std::to_string(prior.st_dev) != device)
                    continue;
                old_found = true;
                if (!Tail(entry.path(), now)) return false;
                break;
            }
            if (!old_found) ++source_resets;
            offset = 0;
            anchor.clear();
            discarding_line = false;
            stream_boundary = true;
        }
        inode = current_inode;
        device = current_device;
        if (offset > static_cast<std::uint64_t>(st.st_size) ||
            (!anchor.empty() && Anchor(file, offset) != anchor)) {
            offset = 0;
            anchor.clear();
            discarding_line = false;
            ++source_resets;
            wal_stream.clear();
            has_sequence = false;
            next_sequence = 0;
            first_sequence = 0;
            stream_boundary = true;
        }
        std::ifstream in(file, std::ios::binary);
        in.seekg(static_cast<std::streamoff>(offset));
        std::size_t consumed = 0;
        std::string line;
        char byte = 0;
        while (consumed < kWalBatchBytes && in.get(byte)) {
            ++consumed;
            if (discarding_line) {
                ++offset;
                if (byte == '\n') discarding_line = false;
                continue;
            }
            if (byte == '\n') {
                J record;
                std::string error;
                if (simple_json::ParseStrict(line, &record, &error) && record.IsObject() &&
                    ValidateDashboardWalRecord(line, record, S(identity, "broker_id"), account_id,
                                               &error) &&
                    Sequence(line, record)) {
                    if (!error.empty()) ++unverified_records;
                    std::uint64_t seq = 0;
                    if (WalUnsignedField(line, "seq", &seq))
                        Put(record, "seq", Str(std::to_string(seq)));
                    Event(record, now);
                } else
                    ++skipped_records;
                offset += line.size() + 1;
                line.clear();
            } else {
                line.push_back(byte);
                if (line.size() > kWalLineLimit) {
                    ++skipped_records;
                    offset += line.size();
                    line.clear();
                    discarding_line = true;
                }
            }
        }
        // The cursor excludes a partial record; an overlength record is discarded with bounded
        // memory.
        anchor = Anchor(file, offset);
        wal_as_of = now;
        const bool backlog =
            consumed >= kWalBatchBytes && offset < static_cast<std::uint64_t>(st.st_size);
        wal_quality = backlog ? "catching_up"
                      : skipped_records > 0 || source_resets > 0 || unverified_records > 0
                          ? "incomplete"
                          : "fresh";
        return !backlog;
    }

    J PublicRows(const std::map<std::string, J>& input, std::size_t limit = 0) const {
        std::vector<J> values;
        for (const auto& [key, original] : input) {
            J row = Obj();
            for (const auto* field :
                 {"order_id", "trade_id", "instrument_id", "exchange_id", "strategy_id", "side",
                  "offset", "as_of_ms", "attribution", "status", "total_volume", "filled_volume",
                  "avg_fill_price", "reject_code", "reject_message", "volume", "price"}) {
                const auto* value = original.Find(field);
                if (value) Put(row, field, *value);
            }
            values.push_back(std::move(row));
        }
        std::sort(values.begin(), values.end(),
                  [](const J& a, const J& b) { return I(a, "as_of_ms") > I(b, "as_of_ms"); });
        if (limit && values.size() > limit) values.resize(limit);
        J result = Arr();
        result.array_value = std::move(values);
        return result;
    }
    J Archive(const J& data, const std::string& day, const std::string& source,
              bool incomplete) const {
        std::int64_t newest = 0;
        for (const auto& row : data.array_value) newest = std::max(newest, I(row, "as_of_ms"));
        auto result =
            Block(data, newest,
                  incomplete                                                        ? "incomplete"
                  : source != "ctp_account_samples" && wal_quality == "catching_up" ? "catching_up"
                                                                                    : "historical",
                  source, day);
        Put(result, "schema_version", Num(1));
        Put(result, "data_scope_id", Str(scope_id));
        return result;
    }
    void Save(std::int64_t now) {
        const auto earliest = CalendarDay(now - (options.retention_days - 1) * kDayMs);
        for (auto it = days.begin(); it != days.end();) {
            if (it->first >= earliest) {
                ++it;
                continue;
            }
            // Only remove explicitly named generated files beneath checked output/state roots.
            if (CheckedPath((output / "days" / it->first).string()) !=
                    output / "days" / it->first ||
                CheckedPath((state / "days").string()) != state / "days")
                throw std::runtime_error("retention_path_changed");
            for (const auto* name : {"equity.json", "orders.json", "trades.json"})
                fs::remove(output / "days" / it->first / name);
            fs::remove(output / "days" / it->first);
            fs::remove(state / "days" / (it->first + ".json"));
            it = days.erase(it);
        }
        for (auto& [day, history] : days) {
            if (!history.incomplete &&
                (skipped_records > 0 || source_resets > 0 || unverified_records > 0)) {
                history.incomplete = true;
                history.dirty = true;
            }
            if ((wal_quality == "catching_up") != previous_catching_up) history.dirty = true;
            if (!history.dirty) continue;
            if (CheckedPath((output / "days" / day).string()) != output / "days" / day ||
                CheckedPath((state / "days").string()) != state / "days")
                throw std::runtime_error("archive_path_changed");
            J saved = Obj(), orders = Obj(), trades = Obj(), equity = Arr();
            for (const auto& [key, row] : history.orders) Put(orders, key, row);
            for (const auto& [key, row] : history.trades) Put(trades, key, row);
            equity.array_value = history.equity;
            Put(saved, "identity", identity);
            Put(saved, "orders", orders);
            Put(saved, "trades", trades);
            Put(saved, "equity", equity);
            Put(saved, "incomplete", Bool(history.incomplete));
            if (!AtomicWrite(state / "days" / (day + ".json"), saved, false) ||
                !AtomicWrite(output / "days" / day / "equity.json",
                             Archive(equity, day, "ctp_account_samples", false), true) ||
                !AtomicWrite(output / "days" / day / "orders.json",
                             Archive(PublicRows(history.orders), day, "wal_order_events",
                                     history.incomplete),
                             true) ||
                !AtomicWrite(output / "days" / day / "trades.json",
                             Archive(PublicRows(history.trades), day, "wal_trade_events",
                                     history.incomplete),
                             true))
                throw std::runtime_error("daily_publish_failed");
            history.dirty = false;
        }
        J checkpoint = Obj();
        Put(checkpoint, "identity", identity);
        Put(checkpoint, "inode", Str(inode));
        Put(checkpoint, "device", Str(device));
        Put(checkpoint, "offset", Str(std::to_string(offset)));
        Put(checkpoint, "anchor", Str(anchor));
        Put(checkpoint, "skipped_records", Num(skipped_records));
        Put(checkpoint, "source_resets", Num(source_resets));
        Put(checkpoint, "unverified_records", Num(unverified_records));
        Put(checkpoint, "discarding_line", Bool(discarding_line));
        Put(checkpoint, "wal_stream", Str(wal_stream));
        Put(checkpoint, "next_sequence", Str(std::to_string(next_sequence)));
        Put(checkpoint, "first_sequence", Str(std::to_string(first_sequence)));
        Put(checkpoint, "has_sequence", Bool(has_sequence));
        Put(checkpoint, "stream_boundary", Bool(stream_boundary));
        if (!AtomicWrite(state / "checkpoint.json", checkpoint, false))
            throw std::runtime_error("checkpoint_write_failed");
        previous_catching_up = wal_quality == "catching_up";
    }

    bool Publish(std::int64_t now) {
        if (!loaded) Initialize();
        if (CheckedPath(output.string()) != output || CheckedPath(state.string()) != state)
            throw std::runtime_error("observer_directory_changed");
        // Recheck source components on every cycle, preventing a swapped symlink from escaping ACL
        // scope.
        for (const auto& [name, path] : paths)
            if (CheckedPath(path.string()) != path) throw std::runtime_error("source_path_changed");
        J private_data;
        const bool readable = ReadJson(paths.at("private_snapshot"), &private_data);
        const auto private_schema = I(private_data, "schema_version");
        const bool identity_ok = readable && (private_schema == 1 || private_schema == 2) &&
                                 IdentityMatches(Get(private_data, "identity"));
        J current = Obj();
        Put(current, "schema_version", Num(1));
        Put(current, "generated_at_ms", Num(now));
        Put(current, "data_scope_id", Str(scope_id));
        Put(current, "account_alias", Str(alias));
        Put(current, "instance_id", Str(instance_alias));
        Put(current, "environment", Str(environment));
        std::string day;
        if (identity_ok) {
            auto account = PrivateBlock(Get(private_data, "account"), false, now);
            auto positions = PrivateBlock(Get(private_data, "positions"), true, now);
            day = std::max(S(account, "trading_day"), S(positions, "trading_day"));
            Put(current, "account", account);
            Put(current, "positions", positions);
            auto health = Health(now, day, I(private_data, "writer_started_at_ms"));
            const auto& pipeline = Get(Get(health, "data"), "pipeline");
            if (S(pipeline, "quality") == "fresh") day = std::max(day, S(pipeline, "trading_day"));
            Put(health, "trading_day", Str(day));
            Put(current, "health", health);
            if (private_schema == 2) {
                std::map<std::string, std::string> instruments;
                BrokerInstruments(positions, &instruments);
                auto strategies =
                    StrategyRisk(Get(private_data, "strategy_risk"), now, day, &instruments);
                Put(current, "strategy_positions", strategies);
                Put(current, "markets",
                    MarketQuotes(Get(private_data, "market_quotes"), instruments, now, day));
            } else {
                auto strategies =
                    Block(Arr(), 0, "missing", "dashboard_private_v2", day);
                Put(strategies, "stale_after_ms", Num(options.strategy_stale_after_ms));
                Put(current, "strategy_positions", strategies);
                auto markets = Block(Arr(), 0, "missing", "dashboard_private_v2", day);
                Put(markets, "stale_after_ms", Num(kRealtimeStaleAfterMs));
                Put(current, "markets", markets);
            }
            Tail(paths.at("wal"), now);
            const auto account_day = S(account, "trading_day");
            if (S(account, "quality") == "fresh" && DayValid(account_day)) {
                auto& history = days[account_day];
                const auto stamp = I(account, "as_of_ms");
                if (history.equity.empty() ||
                    stamp / 60000 > I(history.equity.back(), "as_of_ms") / 60000) {
                    J sample = Obj();
                    Numbers(Get(account, "data"), sample,
                            {"balance", "available", "curr_margin", "commission", "close_profit",
                             "position_profit"});
                    Put(sample, "as_of_ms", Num(stamp));
                    history.equity.push_back(sample);
                    history.dirty = true;
                }
            }
            for (const auto* key : {"orders", "trades"}) {
                J rows = Arr();
                const auto it = days.find(day);
                if (it != days.end())
                    rows = PublicRows(
                        std::string(key) == "orders" ? it->second.orders : it->second.trades, 200);
                auto block = Block(
                    rows, wal_as_of, wal_quality,
                    std::string(key) == "orders" ? "wal_order_events" : "wal_trade_events", day);
                Put(block, "recent_limit", Num(200));
                Put(current, key, block);
            }
        } else {
            for (const std::string key : {"account", "positions", "health", "orders", "trades"})
                Put(current, key,
                    Block(key == "account" || key == "health" ? J{} : Arr(), 0,
                          readable ? "invalid" : "missing", "identity_unverified", ""));
            auto strategies = Block(Arr(), 0, readable ? "invalid" : "missing",
                                    "identity_unverified", "");
            Put(strategies, "stale_after_ms", Num(options.strategy_stale_after_ms));
            Put(current, "strategy_positions", strategies);
            auto markets = Block(Arr(), 0, readable ? "invalid" : "missing",
                                 "identity_unverified", "");
            Put(markets, "stale_after_ms", Num(kRealtimeStaleAfterMs));
            Put(current, "markets", markets);
        }
        Put(current, "trading_day", Str(day));
        Save(now);
        J index = Obj(), list = Arr();
        Put(index, "schema_version", Num(1));
        Put(index, "generated_at_ms", Num(now));
        Put(index, "data_scope_id", Str(scope_id));
        if (identity_ok)
            for (auto it = days.rbegin(); it != days.rend(); ++it) {
                J item = Obj();
                Put(item, "trading_day", Str(it->first));
                Put(item, "equity_points", Num(it->second.equity.size()));
                Put(item, "order_count", Num(it->second.orders.size()));
                Put(item, "trade_count", Num(it->second.trades.size()));
                list.array_value.push_back(item);
            }
        Put(index, "days", list);
        return AtomicWrite(output / "days.json", index, true) &&
               AtomicWrite(output / "current.json", current, true);
    }
};

Publisher::Publisher(PublisherOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}
Publisher::~Publisher() = default;
bool Publisher::PublishOnce(std::int64_t now_ms, std::string* error) {
    try {
        if (now_ms <= 0 || !impl_->Publish(now_ms)) {
            if (error) *error = "dashboard_publish_failed";
            return false;
        }
        return true;
    } catch (...) {
        if (error) *error = "dashboard_observer_configuration_or_io_failure";
        return false;
    }
}
}  // namespace quant_hft::dashboard
