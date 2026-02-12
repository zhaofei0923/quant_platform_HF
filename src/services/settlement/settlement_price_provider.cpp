#include "quant_hft/services/settlement_price_provider.h"

#include <dlfcn.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

namespace quant_hft {
namespace {

struct sqlite3;
struct sqlite3_stmt;

constexpr int kSqliteOk = 0;
constexpr int kSqliteRow = 100;
constexpr int kSqliteDone = 101;

using SqliteDestructor = void (*)(void*);

SqliteDestructor SqliteTransient() {
    return reinterpret_cast<SqliteDestructor>(-1);
}

std::string BuildKey(const std::string& trading_day, const std::string& instrument_id) {
    return trading_day + "|" + instrument_id;
}

struct SqliteApi {
    using OpenFn = int (*)(const char*, sqlite3**);
    using CloseFn = int (*)(sqlite3*);
    using ExecFn = int (*)(sqlite3*, const char*, int (*)(void*, int, char**, char**), void*, char**);
    using FreeFn = void (*)(void*);
    using PrepareFn = int (*)(sqlite3*, const char*, int, sqlite3_stmt**, const char**);
    using StepFn = int (*)(sqlite3_stmt*);
    using FinalizeFn = int (*)(sqlite3_stmt*);
    using BindTextFn = int (*)(sqlite3_stmt*, int, const char*, int, SqliteDestructor);
    using BindDoubleFn = int (*)(sqlite3_stmt*, int, double);
    using BindInt64Fn = int (*)(sqlite3_stmt*, int, long long);
    using ColumnTextFn = const unsigned char* (*)(sqlite3_stmt*, int);
    using ColumnDoubleFn = double (*)(sqlite3_stmt*, int);
    using ErrMsgFn = const char* (*)(sqlite3*);

    bool available{false};
    std::string load_error;
    void* handle{nullptr};

    OpenFn open{nullptr};
    CloseFn close{nullptr};
    ExecFn exec{nullptr};
    FreeFn free{nullptr};
    PrepareFn prepare{nullptr};
    StepFn step{nullptr};
    FinalizeFn finalize{nullptr};
    BindTextFn bind_text{nullptr};
    BindDoubleFn bind_double{nullptr};
    BindInt64Fn bind_int64{nullptr};
    ColumnTextFn column_text{nullptr};
    ColumnDoubleFn column_double{nullptr};
    ErrMsgFn errmsg{nullptr};

    ~SqliteApi() {
        if (handle != nullptr) {
            (void)dlclose(handle);
        }
    }
};

template <typename Fn>
bool LoadSymbol(void* handle, const char* name, Fn* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "symbol output pointer is null";
        }
        return false;
    }
    void* raw = dlsym(handle, name);
    if (raw == nullptr) {
        if (error != nullptr) {
            *error = std::string("failed to load symbol ") + name;
        }
        return false;
    }
    *out = reinterpret_cast<Fn>(raw);
    return true;
}

SqliteApi LoadSqliteApi() {
    SqliteApi api;
    const char* libs[] = {"libsqlite3.so.0", "libsqlite3.so"};
    for (const char* lib : libs) {
        api.handle = dlopen(lib, RTLD_NOW | RTLD_LOCAL);
        if (api.handle != nullptr) {
            break;
        }
    }
    if (api.handle == nullptr) {
        api.load_error = "unable to load sqlite shared library";
        return api;
    }

    std::string error;
    if (!LoadSymbol(api.handle, "sqlite3_open", &api.open, &error) ||
        !LoadSymbol(api.handle, "sqlite3_close", &api.close, &error) ||
        !LoadSymbol(api.handle, "sqlite3_exec", &api.exec, &error) ||
        !LoadSymbol(api.handle, "sqlite3_free", &api.free, &error) ||
        !LoadSymbol(api.handle, "sqlite3_prepare_v2", &api.prepare, &error) ||
        !LoadSymbol(api.handle, "sqlite3_step", &api.step, &error) ||
        !LoadSymbol(api.handle, "sqlite3_finalize", &api.finalize, &error) ||
        !LoadSymbol(api.handle, "sqlite3_bind_text", &api.bind_text, &error) ||
        !LoadSymbol(api.handle, "sqlite3_bind_double", &api.bind_double, &error) ||
        !LoadSymbol(api.handle, "sqlite3_bind_int64", &api.bind_int64, &error) ||
        !LoadSymbol(api.handle, "sqlite3_column_text", &api.column_text, &error) ||
        !LoadSymbol(api.handle, "sqlite3_column_double", &api.column_double, &error) ||
        !LoadSymbol(api.handle, "sqlite3_errmsg", &api.errmsg, &error)) {
        api.load_error = error;
        (void)dlclose(api.handle);
        api.handle = nullptr;
        return api;
    }

    api.available = true;
    return api;
}

std::unordered_map<std::string, double> ParsePriceJson(const std::string& text) {
    std::unordered_map<std::string, double> parsed;

    // Pattern A: {"rb2405": 3800.5, ...}
    const std::regex kv_pattern("\\\"([A-Za-z][A-Za-z0-9_.-]{1,31})\\\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?)");
    for (std::sregex_iterator it(text.begin(), text.end(), kv_pattern), end; it != end; ++it) {
        const auto instrument_id = (*it)[1].str();
        try {
            parsed[instrument_id] = std::stod((*it)[2].str());
        } catch (...) {
        }
    }

    // Pattern B: [{"instrument_id":"rb2405", "settlement_price":3800.5}, ...]
    const std::regex object_pattern(
        "\\{[^\\}]*\\\"instrument_id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"[^\\}]*\\\"settlement_price\\\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?)");
    for (std::sregex_iterator it(text.begin(), text.end(), object_pattern), end; it != end; ++it) {
        const auto instrument_id = (*it)[1].str();
        try {
            parsed[instrument_id] = std::stod((*it)[2].str());
        } catch (...) {
        }
    }

    return parsed;
}

}  // namespace

class ProdSettlementPriceProvider::Impl {
public:
    Impl(std::string cache_db_path, std::string api_price_json_path)
        : cache_db_path_(std::move(cache_db_path)),
          api_price_json_path_(std::move(api_price_json_path)),
          sqlite_api_(LoadSqliteApi()) {
        EnsureSqliteReady();
    }

    std::optional<std::pair<double, SettlementPriceSource>> GetSettlementPrice(
        const std::string& instrument_id,
        const std::string& trading_day) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (instrument_id.empty() || trading_day.empty()) {
            return std::nullopt;
        }

        double price = 0.0;
        std::string details;
        if (LoadManual(trading_day, instrument_id, &price, &details)) {
            return std::make_pair(price,
                                  SettlementPriceSource{SettlementPriceSource::SourceType::kManual,
                                                        details.empty() ? "manual override" : details});
        }

        if (LoadApiPrice(instrument_id, &price)) {
            StoreCache(trading_day, instrument_id, price, "API");
            return std::make_pair(
                price,
                SettlementPriceSource{SettlementPriceSource::SourceType::kApi, "api price json"});
        }

        if (LoadCache(trading_day, instrument_id, &price, &details)) {
            return std::make_pair(price,
                                  SettlementPriceSource{SettlementPriceSource::SourceType::kCache,
                                                        details.empty() ? "cache" : details});
        }

        return std::nullopt;
    }

    std::unordered_map<std::string, std::pair<double, SettlementPriceSource>> BatchGetSettlementPrices(
        const std::vector<std::string>& instrument_ids,
        const std::string& trading_day) {
        std::unordered_map<std::string, std::pair<double, SettlementPriceSource>> prices;
        for (const auto& instrument_id : instrument_ids) {
            auto price = GetSettlementPrice(instrument_id, trading_day);
            if (price.has_value()) {
                prices[instrument_id] = *price;
            }
        }
        return prices;
    }

    void SetManualOverride(const std::string& instrument_id,
                           const std::string& trading_day,
                           double price,
                           const std::string& operator_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (instrument_id.empty() || trading_day.empty()) {
            return;
        }
        const std::string key = BuildKey(trading_day, instrument_id);
        manual_cache_[key] = price;
        if (sqlite_ready_) {
            PersistManual(trading_day, instrument_id, price, operator_id);
            StoreCache(trading_day, instrument_id, price, "MANUAL");
        }
    }

private:
    bool EnsureSqliteReady() {
        if (!sqlite_api_.available) {
            return false;
        }
        if (sqlite_ready_) {
            return true;
        }

        std::filesystem::path db_path(cache_db_path_);
        std::error_code ec;
        if (db_path.has_parent_path()) {
            std::filesystem::create_directories(db_path.parent_path(), ec);
        }

        sqlite3* db = nullptr;
        if (sqlite_api_.open(cache_db_path_.c_str(), &db) != kSqliteOk || db == nullptr) {
            return false;
        }

        auto close_db = [this, &db]() {
            if (db != nullptr) {
                (void)sqlite_api_.close(db);
                db = nullptr;
            }
        };

        const char* create_cache =
            "CREATE TABLE IF NOT EXISTS settlement_price_cache ("
            "trading_day TEXT NOT NULL,"
            "instrument_id TEXT NOT NULL,"
            "price REAL NOT NULL,"
            "source TEXT NOT NULL,"
            "updated_at INTEGER NOT NULL,"
            "PRIMARY KEY (trading_day, instrument_id));";
        const char* create_manual =
            "CREATE TABLE IF NOT EXISTS manual_settlement_price_overrides ("
            "trading_day TEXT NOT NULL,"
            "instrument_id TEXT NOT NULL,"
            "price REAL NOT NULL,"
            "operator_id TEXT NOT NULL DEFAULT '',"
            "updated_at INTEGER NOT NULL,"
            "PRIMARY KEY (trading_day, instrument_id));";

        char* err = nullptr;
        if (sqlite_api_.exec(db, create_cache, nullptr, nullptr, &err) != kSqliteOk) {
            if (err != nullptr && sqlite_api_.free != nullptr) {
                sqlite_api_.free(err);
            }
            close_db();
            return false;
        }
        if (sqlite_api_.exec(db, create_manual, nullptr, nullptr, &err) != kSqliteOk) {
            if (err != nullptr && sqlite_api_.free != nullptr) {
                sqlite_api_.free(err);
            }
            close_db();
            return false;
        }

        close_db();
        sqlite_ready_ = true;
        return true;
    }

    bool OpenSqlite(sqlite3** out_db) {
        if (out_db == nullptr) {
            return false;
        }
        *out_db = nullptr;
        if (!EnsureSqliteReady()) {
            return false;
        }
        if (sqlite_api_.open(cache_db_path_.c_str(), out_db) != kSqliteOk || *out_db == nullptr) {
            return false;
        }
        return true;
    }

    bool LoadManual(const std::string& trading_day,
                    const std::string& instrument_id,
                    double* out_price,
                    std::string* out_details) {
        if (out_price == nullptr) {
            return false;
        }
        const std::string key = BuildKey(trading_day, instrument_id);
        if (const auto it = manual_cache_.find(key); it != manual_cache_.end()) {
            *out_price = it->second;
            if (out_details != nullptr) {
                *out_details = "manual in-memory";
            }
            return true;
        }
        if (!sqlite_ready_) {
            return false;
        }

        sqlite3* db = nullptr;
        if (!OpenSqlite(&db)) {
            return false;
        }
        sqlite3_stmt* stmt = nullptr;
        const char* sql =
            "SELECT price, operator_id FROM manual_settlement_price_overrides "
            "WHERE trading_day=? AND instrument_id=? LIMIT 1;";
        if (sqlite_api_.prepare(db, sql, -1, &stmt, nullptr) != kSqliteOk || stmt == nullptr) {
            (void)sqlite_api_.close(db);
            return false;
        }

        (void)sqlite_api_.bind_text(stmt, 1, trading_day.c_str(), -1, SqliteTransient());
        (void)sqlite_api_.bind_text(stmt, 2, instrument_id.c_str(), -1, SqliteTransient());
        const int step = sqlite_api_.step(stmt);
        if (step != kSqliteRow) {
            (void)sqlite_api_.finalize(stmt);
            (void)sqlite_api_.close(db);
            return false;
        }

        *out_price = sqlite_api_.column_double(stmt, 0);
        if (out_details != nullptr) {
            const auto* operator_raw = sqlite_api_.column_text(stmt, 1);
            *out_details = operator_raw != nullptr
                               ? std::string("manual override by ") +
                                     reinterpret_cast<const char*>(operator_raw)
                               : "manual override";
        }
        manual_cache_[key] = *out_price;
        (void)sqlite_api_.finalize(stmt);
        (void)sqlite_api_.close(db);
        return true;
    }

    bool LoadCache(const std::string& trading_day,
                   const std::string& instrument_id,
                   double* out_price,
                   std::string* out_details) {
        if (out_price == nullptr) {
            return false;
        }
        const std::string key = BuildKey(trading_day, instrument_id);
        if (const auto it = cache_prices_.find(key); it != cache_prices_.end()) {
            *out_price = it->second;
            if (out_details != nullptr) {
                *out_details = "cache in-memory";
            }
            return true;
        }
        if (!sqlite_ready_) {
            return false;
        }

        sqlite3* db = nullptr;
        if (!OpenSqlite(&db)) {
            return false;
        }
        sqlite3_stmt* stmt = nullptr;
        const char* sql =
            "SELECT price, source FROM settlement_price_cache "
            "WHERE trading_day=? AND instrument_id=? LIMIT 1;";
        if (sqlite_api_.prepare(db, sql, -1, &stmt, nullptr) != kSqliteOk || stmt == nullptr) {
            (void)sqlite_api_.close(db);
            return false;
        }

        (void)sqlite_api_.bind_text(stmt, 1, trading_day.c_str(), -1, SqliteTransient());
        (void)sqlite_api_.bind_text(stmt, 2, instrument_id.c_str(), -1, SqliteTransient());
        const int step = sqlite_api_.step(stmt);
        if (step != kSqliteRow) {
            (void)sqlite_api_.finalize(stmt);
            (void)sqlite_api_.close(db);
            return false;
        }

        *out_price = sqlite_api_.column_double(stmt, 0);
        cache_prices_[key] = *out_price;
        if (out_details != nullptr) {
            const auto* source_raw = sqlite_api_.column_text(stmt, 1);
            *out_details = source_raw != nullptr
                               ? std::string("cache source ") +
                                     reinterpret_cast<const char*>(source_raw)
                               : "cache";
        }

        (void)sqlite_api_.finalize(stmt);
        (void)sqlite_api_.close(db);
        return true;
    }

    bool LoadApiPrice(const std::string& instrument_id, double* out_price) {
        if (out_price == nullptr || api_price_json_path_.empty()) {
            return false;
        }
        RefreshApiJsonCache();
        const auto it = api_prices_.find(instrument_id);
        if (it == api_prices_.end()) {
            return false;
        }
        *out_price = it->second;
        return true;
    }

    void RefreshApiJsonCache() {
        std::error_code ec;
        const auto write_time = std::filesystem::last_write_time(api_price_json_path_, ec);
        if (ec) {
            return;
        }
        const auto write_key = write_time.time_since_epoch().count();
        if (write_key == api_json_stamp_) {
            return;
        }

        std::ifstream in(api_price_json_path_);
        if (!in.is_open()) {
            return;
        }
        std::ostringstream buffer;
        buffer << in.rdbuf();
        auto parsed = ParsePriceJson(buffer.str());
        if (!parsed.empty()) {
            api_prices_ = std::move(parsed);
            api_json_stamp_ = write_key;
        }
    }

    void PersistManual(const std::string& trading_day,
                       const std::string& instrument_id,
                       double price,
                       const std::string& operator_id) {
        if (!sqlite_ready_) {
            return;
        }
        sqlite3* db = nullptr;
        if (!OpenSqlite(&db)) {
            return;
        }

        sqlite3_stmt* stmt = nullptr;
        const char* sql =
            "INSERT INTO manual_settlement_price_overrides "
            "(trading_day, instrument_id, price, operator_id, updated_at) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(trading_day, instrument_id) DO UPDATE SET "
            "price=excluded.price, operator_id=excluded.operator_id, updated_at=excluded.updated_at;";

        if (sqlite_api_.prepare(db, sql, -1, &stmt, nullptr) != kSqliteOk || stmt == nullptr) {
            (void)sqlite_api_.close(db);
            return;
        }

        const auto now_sec = static_cast<long long>(
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

        (void)sqlite_api_.bind_text(stmt, 1, trading_day.c_str(), -1, SqliteTransient());
        (void)sqlite_api_.bind_text(stmt, 2, instrument_id.c_str(), -1, SqliteTransient());
        (void)sqlite_api_.bind_double(stmt, 3, price);
        (void)sqlite_api_.bind_text(stmt, 4, operator_id.c_str(), -1, SqliteTransient());
        (void)sqlite_api_.bind_int64(stmt, 5, now_sec);
        (void)sqlite_api_.step(stmt);
        (void)sqlite_api_.finalize(stmt);
        (void)sqlite_api_.close(db);
    }

    void StoreCache(const std::string& trading_day,
                    const std::string& instrument_id,
                    double price,
                    const std::string& source) {
        cache_prices_[BuildKey(trading_day, instrument_id)] = price;
        if (!sqlite_ready_) {
            return;
        }

        sqlite3* db = nullptr;
        if (!OpenSqlite(&db)) {
            return;
        }

        sqlite3_stmt* stmt = nullptr;
        const char* sql =
            "INSERT INTO settlement_price_cache "
            "(trading_day, instrument_id, price, source, updated_at) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(trading_day, instrument_id) DO UPDATE SET "
            "price=excluded.price, source=excluded.source, updated_at=excluded.updated_at;";

        if (sqlite_api_.prepare(db, sql, -1, &stmt, nullptr) != kSqliteOk || stmt == nullptr) {
            (void)sqlite_api_.close(db);
            return;
        }

        const auto now_sec = static_cast<long long>(
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

        (void)sqlite_api_.bind_text(stmt, 1, trading_day.c_str(), -1, SqliteTransient());
        (void)sqlite_api_.bind_text(stmt, 2, instrument_id.c_str(), -1, SqliteTransient());
        (void)sqlite_api_.bind_double(stmt, 3, price);
        (void)sqlite_api_.bind_text(stmt, 4, source.c_str(), -1, SqliteTransient());
        (void)sqlite_api_.bind_int64(stmt, 5, now_sec);
        (void)sqlite_api_.step(stmt);
        (void)sqlite_api_.finalize(stmt);
        (void)sqlite_api_.close(db);
    }

    std::string cache_db_path_;
    std::string api_price_json_path_;
    SqliteApi sqlite_api_;
    bool sqlite_ready_{false};
    std::mutex mutex_;

    std::unordered_map<std::string, double> manual_cache_;
    std::unordered_map<std::string, double> cache_prices_;
    std::unordered_map<std::string, double> api_prices_;
    long long api_json_stamp_{0};
};

ProdSettlementPriceProvider::ProdSettlementPriceProvider(std::string cache_db_path,
                                                         std::string api_price_json_path)
    : impl_(std::make_unique<Impl>(std::move(cache_db_path), std::move(api_price_json_path))) {}

ProdSettlementPriceProvider::~ProdSettlementPriceProvider() = default;

std::optional<std::pair<double, SettlementPriceSource>> ProdSettlementPriceProvider::GetSettlementPrice(
    const std::string& instrument_id,
    const std::string& trading_day) {
    return impl_->GetSettlementPrice(instrument_id, trading_day);
}

std::unordered_map<std::string, std::pair<double, SettlementPriceSource>>
ProdSettlementPriceProvider::BatchGetSettlementPrices(const std::vector<std::string>& instrument_ids,
                                                      const std::string& trading_day) {
    return impl_->BatchGetSettlementPrices(instrument_ids, trading_day);
}

void ProdSettlementPriceProvider::SetManualOverride(const std::string& instrument_id,
                                                    const std::string& trading_day,
                                                    double price,
                                                    const std::string& operator_id) {
    impl_->SetManualOverride(instrument_id, trading_day, price, operator_id);
}

}  // namespace quant_hft
