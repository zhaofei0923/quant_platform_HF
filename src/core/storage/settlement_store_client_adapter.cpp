#include "quant_hft/core/settlement_store_client_adapter.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <limits>
#include <sstream>
#include <thread>

namespace quant_hft {
namespace {

bool ParseBool(const std::string& raw) {
    std::string lowered = raw;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return lowered == "1" || lowered == "true" || lowered == "yes" || lowered == "on";
}

bool ParseIntComponent(const std::string& text, int* out) {
    if (out == nullptr || text.empty()) {
        return false;
    }
    try {
        const auto parsed = std::stoi(text);
        *out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

std::int64_t TimegmPortable(std::tm* utc_tm) {
    if (utc_tm == nullptr) {
        return 0;
    }
#if defined(_WIN32)
    return static_cast<std::int64_t>(_mkgmtime(utc_tm));
#else
    return static_cast<std::int64_t>(timegm(utc_tm));
#endif
}

bool ParseTimestampToEpochNanos(const std::string& raw, EpochNanos* out) {
    if (out == nullptr) {
        return false;
    }
    std::string text = raw;
    if (const auto last = text.find_last_not_of(" \t\r\n"); last == std::string::npos) {
        return false;
    } else {
        text.erase(last + 1);
    }
    if (text.size() < 19) {
        return false;
    }

    const std::string datetime = text.substr(0, 19);
    std::tm tm_utc{};
    std::istringstream stream(datetime);
    stream >> std::get_time(&tm_utc, "%Y-%m-%d %H:%M:%S");
    if (stream.fail()) {
        return false;
    }

    std::int64_t nanos = 0;
    std::size_t cursor = 19;
    if (cursor < text.size() && text[cursor] == '.') {
        ++cursor;
        const std::size_t fraction_start = cursor;
        while (cursor < text.size() &&
               std::isdigit(static_cast<unsigned char>(text[cursor])) != 0) {
            ++cursor;
        }
        std::string fraction = text.substr(fraction_start, cursor - fraction_start);
        if (fraction.size() > 9) {
            fraction.resize(9);
        }
        while (fraction.size() < 9) {
            fraction.push_back('0');
        }
        if (!fraction.empty()) {
            try {
                nanos = std::stoll(fraction);
            } catch (...) {
                return false;
            }
        }
    }

    int tz_sign = 0;
    int tz_hours = 0;
    int tz_minutes = 0;
    if (cursor < text.size() && (text[cursor] == '+' || text[cursor] == '-')) {
        tz_sign = text[cursor] == '+' ? 1 : -1;
        ++cursor;
        if (cursor + 1 >= text.size()) {
            return false;
        }
        if (!ParseIntComponent(text.substr(cursor, 2), &tz_hours)) {
            return false;
        }
        cursor += 2;
        if (cursor < text.size() && text[cursor] == ':') {
            ++cursor;
        }
        if (cursor + 1 < text.size() &&
            std::isdigit(static_cast<unsigned char>(text[cursor])) != 0 &&
            std::isdigit(static_cast<unsigned char>(text[cursor + 1])) != 0) {
            if (!ParseIntComponent(text.substr(cursor, 2), &tz_minutes)) {
                return false;
            }
        }
    }

    const std::int64_t epoch_seconds = TimegmPortable(&tm_utc);
    const std::int64_t tz_offset_seconds =
        static_cast<std::int64_t>(tz_sign) * (tz_hours * 3600 + tz_minutes * 60);
    const std::int64_t utc_seconds = epoch_seconds - tz_offset_seconds;
    if (utc_seconds > std::numeric_limits<std::int64_t>::max() / 1'000'000'000LL ||
        utc_seconds < std::numeric_limits<std::int64_t>::min() / 1'000'000'000LL) {
        return false;
    }
    *out = utc_seconds * 1'000'000'000LL + nanos;
    return true;
}

}  // namespace

SettlementStoreClientAdapter::SettlementStoreClientAdapter(
    std::shared_ptr<ITimescaleSqlClient> client,
    StorageRetryPolicy retry_policy,
    std::string trading_schema,
    std::string ops_schema)
    : client_(std::move(client)),
      retry_policy_(retry_policy),
      trading_schema_(trading_schema.empty() ? "trading_core" : std::move(trading_schema)),
      ops_schema_(ops_schema.empty() ? "ops" : std::move(ops_schema)) {}

bool SettlementStoreClientAdapter::GetRun(const std::string& trading_day,
                                          SettlementRunRecord* out,
                                          std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "output run pointer is null";
        }
        return false;
    }
    *out = SettlementRunRecord{};
    if (trading_day.empty()) {
        if (error != nullptr) {
            *error = "trading_day is empty";
        }
        return false;
    }
    if (client_ == nullptr) {
        if (error != nullptr) {
            *error = "null sql client";
        }
        return false;
    }

    std::string query_error;
    const auto rows = client_->QueryRows(TableName(ops_schema_, "settlement_runs"),
                                         "trading_day",
                                         trading_day,
                                         &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    if (rows.empty()) {
        return true;
    }

    const auto& row = rows.front();
    out->trading_day = trading_day;
    if (const auto it = row.find("status"); it != row.end()) {
        out->status = it->second;
    }
    if (const auto it = row.find("force_run"); it != row.end()) {
        out->force_run = ParseBool(it->second);
    }
    if (const auto it = row.find("error_code"); it != row.end()) {
        out->error_code = it->second;
    }
    if (const auto it = row.find("error_msg"); it != row.end()) {
        out->error_msg = it->second;
    }
    if (const auto it = row.find("evidence_path"); it != row.end()) {
        out->evidence_path = it->second;
    }
    if (const auto it = row.find("heartbeat_at"); it != row.end()) {
        (void)ParseTimestampToEpochNanos(it->second, &out->heartbeat_ts_ns);
    }
    if (const auto it = row.find("started_at"); it != row.end()) {
        (void)ParseTimestampToEpochNanos(it->second, &out->started_ts_ns);
    }
    if (const auto it = row.find("completed_at"); it != row.end()) {
        (void)ParseTimestampToEpochNanos(it->second, &out->completed_ts_ns);
    }
    return true;
}

bool SettlementStoreClientAdapter::UpsertRun(const SettlementRunRecord& run, std::string* error) {
    if (run.trading_day.empty() || run.status.empty()) {
        if (error != nullptr) {
            *error = "settlement run requires trading_day and status";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"trading_day", run.trading_day},
        {"status", run.status},
        {"force_run", run.force_run ? "1" : "0"},
        {"heartbeat_at", ToTimestamp(run.heartbeat_ts_ns)},
        {"started_at", ToTimestamp(run.started_ts_ns)},
        {"completed_at",
         ToTimestamp(run.completed_ts_ns > 0 ? run.completed_ts_ns : run.heartbeat_ts_ns)},
        {"error_code", run.error_code},
        {"error_msg", run.error_msg},
        {"evidence_path", run.evidence_path},
        {"updated_at", ToTimestamp(run.heartbeat_ts_ns > 0 ? run.heartbeat_ts_ns : NowEpochNanos())},
    };
    return UpsertWithRetry(
        TableName(ops_schema_, "settlement_runs"),
        row,
        {"trading_day"},
        {"status",
         "force_run",
         "heartbeat_at",
         "started_at",
         "completed_at",
         "error_code",
         "error_msg",
         "evidence_path",
         "updated_at"},
        error);
}

bool SettlementStoreClientAdapter::AppendSummary(const SettlementSummaryRecord& summary,
                                                 std::string* error) {
    if (summary.trading_day.empty() || summary.account_id.empty()) {
        if (error != nullptr) {
            *error = "settlement summary requires trading_day and account_id";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"trading_day", summary.trading_day},
        {"account_id", summary.account_id},
        {"pre_balance", ToString(summary.pre_balance)},
        {"deposit", ToString(summary.deposit)},
        {"withdraw", ToString(summary.withdraw)},
        {"commission", ToString(summary.commission)},
        {"close_profit", ToString(summary.close_profit)},
        {"position_profit", ToString(summary.position_profit)},
        {"balance", ToString(summary.balance)},
        {"curr_margin", ToString(summary.curr_margin)},
        {"available", ToString(summary.available)},
        {"risk_degree", ToString(summary.risk_degree)},
        {"created_at", ToTimestamp(summary.created_ts_ns)},
    };
    return InsertWithRetry(TableName(trading_schema_, "settlement_summary"), row, error);
}

bool SettlementStoreClientAdapter::AppendDetail(const SettlementDetailRecord& detail,
                                                std::string* error) {
    if (detail.trading_day.empty() || detail.instrument_id.empty() || detail.position_id <= 0) {
        if (error != nullptr) {
            *error = "settlement detail requires trading_day/instrument_id/position_id";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"trading_day", detail.trading_day},
        {"settlement_id", ToString(detail.settlement_id)},
        {"position_id", ToString(detail.position_id)},
        {"instrument_id", detail.instrument_id},
        {"volume", ToString(detail.volume)},
        {"settlement_price", ToString(detail.settlement_price)},
        {"profit", ToString(detail.profit)},
        {"created_at", ToTimestamp(detail.created_ts_ns)},
    };
    return InsertWithRetry(TableName(trading_schema_, "settlement_detail"), row, error);
}

bool SettlementStoreClientAdapter::AppendPrice(const SettlementPriceRecord& price,
                                               std::string* error) {
    if (price.trading_day.empty() || price.instrument_id.empty() || price.source.empty()) {
        if (error != nullptr) {
            *error = "settlement price requires trading_day/instrument_id/source";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"trading_day", price.trading_day},
        {"instrument_id", price.instrument_id},
        {"exchange_id", price.exchange_id},
        {"source", price.source},
        {"settlement_price", ToString(price.settlement_price)},
        {"is_final", price.is_final ? "1" : "0"},
        {"created_at", ToTimestamp(price.created_ts_ns)},
    };
    return InsertWithRetry(TableName(trading_schema_, "settlement_prices"), row, error);
}

bool SettlementStoreClientAdapter::AppendReconcileDiff(const SettlementReconcileDiffRecord& diff,
                                                       std::string* error) {
    if (diff.trading_day.empty() || diff.diff_type.empty()) {
        if (error != nullptr) {
            *error = "settlement reconcile diff requires trading_day/diff_type";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"trading_day", diff.trading_day},
        {"account_id", diff.account_id},
        {"diff_type", diff.diff_type},
        {"key_ref", diff.key_ref},
        {"local_value", ToString(diff.local_value)},
        {"ctp_value", ToString(diff.ctp_value)},
        {"delta_value", ToString(diff.delta_value)},
        {"diagnose_hint", diff.diagnose_hint},
        {"raw_payload", diff.raw_payload},
        {"created_at", ToTimestamp(diff.created_ts_ns)},
    };
    return InsertWithRetry(TableName(ops_schema_, "settlement_reconcile_diff"), row, error);
}

bool SettlementStoreClientAdapter::InsertWithRetry(
    const std::string& table,
    const std::unordered_map<std::string, std::string>& row,
    std::string* error) const {
    if (client_ == nullptr) {
        if (error != nullptr) {
            *error = "null sql client";
        }
        return false;
    }
    const int attempts = std::max(1, retry_policy_.max_attempts);
    int backoff_ms = std::max(0, retry_policy_.initial_backoff_ms);
    const int max_backoff_ms = std::max(backoff_ms, retry_policy_.max_backoff_ms);

    std::string last_error;
    for (int attempt = 1; attempt <= attempts; ++attempt) {
        std::string local_error;
        if (client_->InsertRow(table, row, &local_error)) {
            return true;
        }
        if (IsDuplicateKeyError(local_error)) {
            return true;
        }
        last_error = local_error;
        if (attempt < attempts && backoff_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(max_backoff_ms, backoff_ms * 2);
        }
    }
    if (error != nullptr) {
        *error = last_error.empty() ? "insert failed" : last_error;
    }
    return false;
}

bool SettlementStoreClientAdapter::UpsertWithRetry(
    const std::string& table,
    const std::unordered_map<std::string, std::string>& row,
    const std::vector<std::string>& conflict_keys,
    const std::vector<std::string>& update_keys,
    std::string* error) const {
    if (client_ == nullptr) {
        if (error != nullptr) {
            *error = "null sql client";
        }
        return false;
    }
    const int attempts = std::max(1, retry_policy_.max_attempts);
    int backoff_ms = std::max(0, retry_policy_.initial_backoff_ms);
    const int max_backoff_ms = std::max(backoff_ms, retry_policy_.max_backoff_ms);

    std::string last_error;
    for (int attempt = 1; attempt <= attempts; ++attempt) {
        std::string local_error;
        if (client_->UpsertRow(table, row, conflict_keys, update_keys, &local_error)) {
            return true;
        }
        if (IsUpsertUnsupportedError(local_error)) {
            return InsertWithRetry(table, row, error);
        }
        last_error = local_error;
        if (attempt < attempts && backoff_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(max_backoff_ms, backoff_ms * 2);
        }
    }
    if (error != nullptr) {
        *error = last_error.empty() ? "upsert failed" : last_error;
    }
    return false;
}

bool SettlementStoreClientAdapter::IsDuplicateKeyError(const std::string& error) const {
    std::string lowered = error;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return lowered.find("duplicate key") != std::string::npos ||
           lowered.find("already exists") != std::string::npos ||
           lowered.find("unique constraint") != std::string::npos;
}

bool SettlementStoreClientAdapter::IsUpsertUnsupportedError(const std::string& error) const {
    std::string lowered = error;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return lowered.find("upsert") != std::string::npos &&
           lowered.find("support") != std::string::npos;
}

std::string SettlementStoreClientAdapter::TableName(const std::string& schema,
                                                    const std::string& table) const {
    if (schema.empty()) {
        return table;
    }
    return schema + "." + table;
}

std::string SettlementStoreClientAdapter::ToString(std::int32_t value) {
    return std::to_string(value);
}

std::string SettlementStoreClientAdapter::ToString(std::int64_t value) {
    return std::to_string(value);
}

std::string SettlementStoreClientAdapter::ToString(double value) {
    return std::to_string(value);
}

std::string SettlementStoreClientAdapter::ToTimestamp(EpochNanos ts_ns) {
    const auto effective = ts_ns > 0 ? ts_ns : NowEpochNanos();
    std::int64_t seconds = effective / 1'000'000'000LL;
    std::int64_t nanos = effective % 1'000'000'000LL;
    if (nanos < 0) {
        nanos += 1'000'000'000LL;
        --seconds;
    }
    const auto micros = static_cast<int>(nanos / 1'000LL);
    std::time_t raw_seconds = static_cast<std::time_t>(seconds);
    std::tm utc_tm{};
#if defined(_WIN32)
    gmtime_s(&utc_tm, &raw_seconds);
#else
    gmtime_r(&raw_seconds, &utc_tm);
#endif
    std::ostringstream stream;
    stream << std::put_time(&utc_tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setw(6)
           << std::setfill('0') << micros << "+00:00";
    return stream.str();
}

}  // namespace quant_hft
