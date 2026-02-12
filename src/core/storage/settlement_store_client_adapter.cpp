#include "quant_hft/core/settlement_store_client_adapter.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <limits>
#include <sstream>
#include <thread>
#include <unordered_set>

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

int ParseIntOrDefault(const std::unordered_map<std::string, std::string>& row,
                      const std::string& key,
                      int default_value = 0) {
    const auto it = row.find(key);
    if (it == row.end() || it->second.empty()) {
        return default_value;
    }
    try {
        return std::stoi(it->second);
    } catch (...) {
        return default_value;
    }
}

std::int64_t ParseInt64OrDefault(const std::unordered_map<std::string, std::string>& row,
                                 const std::string& key,
                                 std::int64_t default_value = 0) {
    const auto it = row.find(key);
    if (it == row.end() || it->second.empty()) {
        return default_value;
    }
    try {
        return std::stoll(it->second);
    } catch (...) {
        return default_value;
    }
}

double ParseDoubleOrDefault(const std::unordered_map<std::string, std::string>& row,
                            const std::string& key,
                            double default_value = 0.0) {
    const auto it = row.find(key);
    if (it == row.end() || it->second.empty()) {
        return default_value;
    }
    try {
        return std::stod(it->second);
    } catch (...) {
        return default_value;
    }
}

std::string ParseStringOrDefault(const std::unordered_map<std::string, std::string>& row,
                                 const std::string& key,
                                 const std::string& default_value = "") {
    const auto it = row.find(key);
    if (it == row.end()) {
        return default_value;
    }
    return it->second;
}

bool MatchesTradingDay(const std::unordered_map<std::string, std::string>& row,
                       const std::string& trading_day,
                       const std::string& date_key,
                       const std::string& ts_key) {
    if (const auto it = row.find(date_key); it != row.end()) {
        if (it->second == trading_day) {
            return true;
        }
    }
    if (const auto ts_it = row.find(ts_key); ts_it != row.end()) {
        if (ts_it->second.size() >= 10 && ts_it->second.substr(0, 10) == trading_day) {
            return true;
        }
    }
    return false;
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

bool SettlementStoreClientAdapter::BeginTransaction(std::string* error) {
    (void)error;
    in_transaction_ = true;
    return true;
}

bool SettlementStoreClientAdapter::CommitTransaction(std::string* error) {
    (void)error;
    in_transaction_ = false;
    return true;
}

bool SettlementStoreClientAdapter::RollbackTransaction(std::string* error) {
    (void)error;
    in_transaction_ = false;
    return true;
}

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
        {"settlement_price", price.has_settlement_price ? ToString(price.settlement_price) : ""},
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

bool SettlementStoreClientAdapter::LoadOpenPositions(
    const std::string& account_id,
    std::vector<SettlementOpenPositionRecord>* out,
    std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "output open positions pointer is null";
        }
        return false;
    }
    out->clear();
    if (account_id.empty()) {
        if (error != nullptr) {
            *error = "account_id is empty";
        }
        return false;
    }

    std::string query_error;
    const auto rows = client_->QueryRows(
        TableName(trading_schema_, "position_detail"), "account_id", account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }

    out->reserve(rows.size());
    for (const auto& row : rows) {
        if (ParseIntOrDefault(row, "position_status", 1) != 1) {
            continue;
        }
        SettlementOpenPositionRecord position;
        position.position_id = ParseInt64OrDefault(row, "position_id");
        if (position.position_id <= 0) {
            continue;
        }
        position.account_id = ParseStringOrDefault(row, "account_id");
        position.strategy_id = ParseStringOrDefault(row, "strategy_id");
        position.instrument_id = ParseStringOrDefault(row, "instrument_id");
        position.exchange_id = ParseStringOrDefault(row, "exchange_id");
        position.open_date = ParseStringOrDefault(row, "open_date");
        position.open_price = ParseDoubleOrDefault(row, "open_price");
        position.volume = ParseIntOrDefault(row, "volume");
        position.is_today = ParseBool(ParseStringOrDefault(row, "is_today", "false"));
        position.position_date = ParseStringOrDefault(row, "position_date");
        position.close_volume = ParseIntOrDefault(row, "close_volume");
        position.position_status = ParseIntOrDefault(row, "position_status", 1);
        position.accumulated_mtm = ParseDoubleOrDefault(row, "accumulated_mtm");
        position.last_settlement_date = ParseStringOrDefault(row, "last_settlement_date");
        position.last_settlement_price = ParseDoubleOrDefault(row, "last_settlement_price");
        position.last_settlement_profit = ParseDoubleOrDefault(row, "last_settlement_profit");
        (void)ParseTimestampToEpochNanos(
            ParseStringOrDefault(row, "update_time"), &position.update_ts_ns);
        out->push_back(position);
    }
    std::sort(out->begin(), out->end(), [](const auto& lhs, const auto& rhs) {
        return lhs.position_id < rhs.position_id;
    });
    return true;
}

bool SettlementStoreClientAdapter::LoadInstruments(
    const std::vector<std::string>& instrument_ids,
    std::unordered_map<std::string, SettlementInstrumentRecord>* out,
    std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "output instrument map pointer is null";
        }
        return false;
    }
    out->clear();

    std::unordered_set<std::string> filter;
    filter.insert(instrument_ids.begin(), instrument_ids.end());

    std::string query_error;
    const auto rows = client_->QueryAllRows(TableName(trading_schema_, "instruments"), &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }

    for (const auto& row : rows) {
        const std::string instrument_id = ParseStringOrDefault(row, "instrument_id");
        if (instrument_id.empty()) {
            continue;
        }
        if (!filter.empty() && filter.find(instrument_id) == filter.end()) {
            continue;
        }
        SettlementInstrumentRecord instrument;
        instrument.instrument_id = instrument_id;
        instrument.contract_multiplier =
            std::max(1, ParseIntOrDefault(row, "contract_multiplier", 1));
        instrument.long_margin_rate = ParseDoubleOrDefault(row, "long_margin_rate");
        instrument.short_margin_rate = ParseDoubleOrDefault(row, "short_margin_rate");
        if (instrument.long_margin_rate <= 0.0) {
            instrument.long_margin_rate = ParseDoubleOrDefault(row, "margin_rate");
        }
        if (instrument.short_margin_rate <= 0.0) {
            instrument.short_margin_rate = ParseDoubleOrDefault(row, "margin_rate");
        }
        (*out)[instrument.instrument_id] = instrument;
    }
    return true;
}

bool SettlementStoreClientAdapter::UpdatePositionAfterSettlement(
    const SettlementOpenPositionRecord& position,
    std::string* error) {
    if (position.position_id <= 0 || position.open_date.empty() || position.instrument_id.empty()) {
        if (error != nullptr) {
            *error = "position requires position_id/open_date/instrument_id";
        }
        return false;
    }

    const EpochNanos now_ts = position.update_ts_ns > 0 ? position.update_ts_ns : NowEpochNanos();
    std::unordered_map<std::string, std::string> row{
        {"position_id", ToString(position.position_id)},
        {"account_id", position.account_id},
        {"strategy_id", position.strategy_id},
        {"instrument_id", position.instrument_id},
        {"exchange_id", position.exchange_id},
        {"open_date", position.open_date},
        {"open_price", ToString(position.open_price)},
        {"volume", ToString(position.volume)},
        {"is_today", position.is_today ? "1" : "0"},
        {"position_date", position.position_date},
        {"close_volume", ToString(position.close_volume)},
        {"position_status", ToString(position.position_status)},
        {"accumulated_mtm", ToString(position.accumulated_mtm)},
        {"last_settlement_date", position.last_settlement_date},
        {"last_settlement_price", ToString(position.last_settlement_price)},
        {"last_settlement_profit", ToString(position.last_settlement_profit)},
        {"update_time", ToTimestamp(now_ts)},
    };
    return UpsertWithRetry(TableName(trading_schema_, "position_detail"),
                           row,
                           {"position_id", "open_date"},
                           {"open_price",
                            "is_today",
                            "position_date",
                            "close_volume",
                            "position_status",
                            "accumulated_mtm",
                            "last_settlement_date",
                            "last_settlement_price",
                            "last_settlement_profit",
                            "update_time"},
                           error);
}

bool SettlementStoreClientAdapter::RolloverPositionDetail(const std::string& account_id,
                                                          std::string* error) {
    std::vector<SettlementOpenPositionRecord> positions;
    if (!LoadOpenPositions(account_id, &positions, error)) {
        return false;
    }
    for (auto& position : positions) {
        if (!position.is_today) {
            continue;
        }
        position.is_today = false;
        position.update_ts_ns = NowEpochNanos();
        if (!UpdatePositionAfterSettlement(position, error)) {
            return false;
        }
    }
    return true;
}

bool SettlementStoreClientAdapter::RolloverPositionSummary(const std::string& account_id,
                                                           std::string* error) {
    std::string query_error;
    const auto rows = client_->QueryRows(
        TableName(trading_schema_, "position_summary"), "account_id", account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }

    for (const auto& row : rows) {
        const int long_today = ParseIntOrDefault(row, "long_today_volume");
        const int short_today = ParseIntOrDefault(row, "short_today_volume");
        const int long_yd = ParseIntOrDefault(row, "long_yd_volume");
        const int short_yd = ParseIntOrDefault(row, "short_yd_volume");
        const int long_volume = ParseIntOrDefault(row, "long_volume");
        const int short_volume = ParseIntOrDefault(row, "short_volume");

        std::unordered_map<std::string, std::string> update{
            {"account_id", ParseStringOrDefault(row, "account_id")},
            {"strategy_id", ParseStringOrDefault(row, "strategy_id")},
            {"instrument_id", ParseStringOrDefault(row, "instrument_id")},
            {"exchange_id", ParseStringOrDefault(row, "exchange_id")},
            {"long_volume", ToString(long_volume)},
            {"short_volume", ToString(short_volume)},
            {"net_volume", ToString(long_volume - short_volume)},
            {"long_today_volume", "0"},
            {"short_today_volume", "0"},
            {"long_yd_volume", ToString(long_yd + long_today)},
            {"short_yd_volume", ToString(short_yd + short_today)},
            {"avg_long_price", ParseStringOrDefault(row, "avg_long_price")},
            {"avg_short_price", ParseStringOrDefault(row, "avg_short_price")},
            {"position_profit", ParseStringOrDefault(row, "position_profit")},
            {"margin", ParseStringOrDefault(row, "margin")},
            {"update_time", ToTimestamp(NowEpochNanos())},
        };
        if (!UpsertWithRetry(TableName(trading_schema_, "position_summary"),
                             update,
                             {"account_id", "strategy_id", "instrument_id"},
                             {"long_volume",
                              "short_volume",
                              "net_volume",
                              "long_today_volume",
                              "short_today_volume",
                              "long_yd_volume",
                              "short_yd_volume",
                              "avg_long_price",
                              "avg_short_price",
                              "position_profit",
                              "margin",
                              "update_time"},
                             error)) {
            return false;
        }
    }
    return true;
}

bool SettlementStoreClientAdapter::LoadAccountFunds(const std::string& account_id,
                                                    const std::string& trading_day,
                                                    SettlementAccountFundsRecord* out,
                                                    std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "output account funds pointer is null";
        }
        return false;
    }
    *out = SettlementAccountFundsRecord{};
    out->account_id = account_id;
    out->trading_day = trading_day;
    if (account_id.empty() || trading_day.empty()) {
        if (error != nullptr) {
            *error = "account_id and trading_day are required";
        }
        return false;
    }

    std::string query_error;
    const auto rows = client_->QueryRows(
        TableName(trading_schema_, "account_funds"), "account_id", account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    for (const auto& row : rows) {
        if (ParseStringOrDefault(row, "trading_day") != trading_day) {
            continue;
        }
        out->exists = true;
        out->pre_balance = ParseDoubleOrDefault(row, "pre_balance");
        out->deposit = ParseDoubleOrDefault(row, "deposit");
        out->withdraw = ParseDoubleOrDefault(row, "withdraw");
        out->frozen_commission = ParseDoubleOrDefault(row, "frozen_commission");
        out->frozen_margin = ParseDoubleOrDefault(row, "frozen_margin");
        out->available = ParseDoubleOrDefault(row, "available");
        out->curr_margin = ParseDoubleOrDefault(row, "curr_margin");
        out->commission = ParseDoubleOrDefault(row, "commission");
        out->close_profit = ParseDoubleOrDefault(row, "close_profit");
        out->position_profit = ParseDoubleOrDefault(row, "position_profit");
        out->balance = ParseDoubleOrDefault(row, "balance");
        out->risk_degree = ParseDoubleOrDefault(row, "risk_degree");
        out->pre_settlement_balance = ParseDoubleOrDefault(row, "pre_settlement_balance");
        out->floating_profit = ParseDoubleOrDefault(row, "floating_profit");
        (void)ParseTimestampToEpochNanos(ParseStringOrDefault(row, "update_time"), &out->update_ts_ns);
        return true;
    }
    return true;
}

bool SettlementStoreClientAdapter::SumDeposit(const std::string& account_id,
                                              const std::string& trading_day,
                                              double* out,
                                              std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "sum output pointer is null";
        }
        return false;
    }
    *out = 0.0;
    std::string query_error;
    const auto rows = client_->QueryRows(
        TableName(trading_schema_, "fund_transfer"), "account_id", account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    for (const auto& row : rows) {
        if (!MatchesTradingDay(row, trading_day, "request_time", "request_time")) {
            continue;
        }
        if (ParseStringOrDefault(row, "direction") != "0") {
            continue;
        }
        if (ParseIntOrDefault(row, "status", 0) == 2) {
            continue;
        }
        *out += ParseDoubleOrDefault(row, "amount");
    }
    return true;
}

bool SettlementStoreClientAdapter::SumWithdraw(const std::string& account_id,
                                               const std::string& trading_day,
                                               double* out,
                                               std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "sum output pointer is null";
        }
        return false;
    }
    *out = 0.0;
    std::string query_error;
    const auto rows = client_->QueryRows(
        TableName(trading_schema_, "fund_transfer"), "account_id", account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    for (const auto& row : rows) {
        if (!MatchesTradingDay(row, trading_day, "request_time", "request_time")) {
            continue;
        }
        if (ParseStringOrDefault(row, "direction") != "1") {
            continue;
        }
        if (ParseIntOrDefault(row, "status", 0) == 2) {
            continue;
        }
        *out += ParseDoubleOrDefault(row, "amount");
    }
    return true;
}

bool SettlementStoreClientAdapter::SumCommission(const std::string& account_id,
                                                 const std::string& trading_day,
                                                 double* out,
                                                 std::string* error) const {
    return SumTradeField(account_id, trading_day, "commission", out, error);
}

bool SettlementStoreClientAdapter::SumCloseProfit(const std::string& account_id,
                                                  const std::string& trading_day,
                                                  double* out,
                                                  std::string* error) const {
    return SumTradeField(account_id, trading_day, "profit", out, error);
}

bool SettlementStoreClientAdapter::UpsertAccountFunds(const SettlementAccountFundsRecord& funds,
                                                      std::string* error) {
    if (funds.account_id.empty() || funds.trading_day.empty()) {
        if (error != nullptr) {
            *error = "account funds requires account_id and trading_day";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"account_id", funds.account_id},
        {"trading_day", funds.trading_day},
        {"currency", "CNY"},
        {"pre_balance", ToString(funds.pre_balance)},
        {"deposit", ToString(funds.deposit)},
        {"withdraw", ToString(funds.withdraw)},
        {"frozen_commission", ToString(funds.frozen_commission)},
        {"frozen_margin", ToString(funds.frozen_margin)},
        {"available", ToString(funds.available)},
        {"curr_margin", ToString(funds.curr_margin)},
        {"commission", ToString(funds.commission)},
        {"close_profit", ToString(funds.close_profit)},
        {"position_profit", ToString(funds.position_profit)},
        {"balance", ToString(funds.balance)},
        {"risk_degree", ToString(funds.risk_degree)},
        {"pre_settlement_balance", ToString(funds.pre_settlement_balance)},
        {"floating_profit", ToString(funds.floating_profit)},
        {"update_time", ToTimestamp(funds.update_ts_ns)},
    };
    return UpsertWithRetry(TableName(trading_schema_, "account_funds"),
                           row,
                           {"account_id", "trading_day"},
                           {"currency",
                            "pre_balance",
                            "deposit",
                            "withdraw",
                            "frozen_commission",
                            "frozen_margin",
                            "available",
                            "curr_margin",
                            "commission",
                            "close_profit",
                            "position_profit",
                            "balance",
                            "risk_degree",
                            "pre_settlement_balance",
                            "floating_profit",
                            "update_time"},
                           error);
}

bool SettlementStoreClientAdapter::LoadPositionSummary(
    const std::string& account_id,
    std::vector<SettlementPositionSummaryRecord>* out,
    std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "output position summary pointer is null";
        }
        return false;
    }
    out->clear();
    std::string query_error;
    const auto rows = client_->QueryRows(
        TableName(trading_schema_, "position_summary"), "account_id", account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    for (const auto& row : rows) {
        SettlementPositionSummaryRecord item;
        item.account_id = ParseStringOrDefault(row, "account_id");
        item.strategy_id = ParseStringOrDefault(row, "strategy_id");
        item.instrument_id = ParseStringOrDefault(row, "instrument_id");
        item.exchange_id = ParseStringOrDefault(row, "exchange_id");
        item.long_volume = ParseIntOrDefault(row, "long_volume");
        item.short_volume = ParseIntOrDefault(row, "short_volume");
        item.long_today_volume = ParseIntOrDefault(row, "long_today_volume");
        item.short_today_volume = ParseIntOrDefault(row, "short_today_volume");
        item.long_yd_volume = ParseIntOrDefault(row, "long_yd_volume");
        item.short_yd_volume = ParseIntOrDefault(row, "short_yd_volume");
        out->push_back(item);
    }
    return true;
}

bool SettlementStoreClientAdapter::LoadOrderKeysByDay(const std::string& account_id,
                                                      const std::string& trading_day,
                                                      std::vector<SettlementOrderKey>* out,
                                                      std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "output order key list pointer is null";
        }
        return false;
    }
    out->clear();
    std::string query_error;
    const auto rows =
        client_->QueryRows(TableName(trading_schema_, "orders"), "account_id", account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }

    std::unordered_set<std::string> dedupe;
    for (const auto& row : rows) {
        if (!MatchesTradingDay(row, trading_day, "insert_time", "insert_time")) {
            continue;
        }
        SettlementOrderKey key;
        key.order_ref = ParseStringOrDefault(row, "order_ref");
        key.front_id = ParseIntOrDefault(row, "front_id");
        key.session_id = ParseIntOrDefault(row, "session_id");
        const std::string dedupe_key =
            key.order_ref + "|" + std::to_string(key.front_id) + "|" + std::to_string(key.session_id);
        if (dedupe.insert(dedupe_key).second) {
            out->push_back(key);
        }
    }
    return true;
}

bool SettlementStoreClientAdapter::LoadTradeIdsByDay(const std::string& account_id,
                                                     const std::string& trading_day,
                                                     std::vector<std::string>* out,
                                                     std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "output trade id list pointer is null";
        }
        return false;
    }
    out->clear();
    std::string query_error;
    const auto rows =
        client_->QueryRows(TableName(trading_schema_, "trades"), "account_id", account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }

    std::unordered_set<std::string> dedupe;
    for (const auto& row : rows) {
        if (!MatchesTradingDay(row, trading_day, "trade_time", "trade_time")) {
            continue;
        }
        const std::string trade_id = ParseStringOrDefault(row, "trade_id");
        if (!trade_id.empty() && dedupe.insert(trade_id).second) {
            out->push_back(trade_id);
        }
    }
    return true;
}

bool SettlementStoreClientAdapter::UpsertSystemConfig(const std::string& key,
                                                      const std::string& value,
                                                      std::string* error) {
    if (key.empty()) {
        if (error != nullptr) {
            *error = "system config key is empty";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"config_key", key},
        {"config_value", value},
        {"description", ""},
        {"update_time", ToTimestamp(NowEpochNanos())},
    };
    return UpsertWithRetry(TableName(ops_schema_, "system_config"),
                           row,
                           {"config_key"},
                           {"config_value", "update_time"},
                           error);
}

bool SettlementStoreClientAdapter::SumTradeField(const std::string& account_id,
                                                 const std::string& trading_day,
                                                 const std::string& field_name,
                                                 double* out,
                                                 std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "sum output pointer is null";
        }
        return false;
    }
    *out = 0.0;
    std::string query_error;
    const auto rows =
        client_->QueryRows(TableName(trading_schema_, "trades"), "account_id", account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    for (const auto& row : rows) {
        if (!MatchesTradingDay(row, trading_day, "trade_time", "trade_time")) {
            continue;
        }
        *out += ParseDoubleOrDefault(row, field_name);
    }
    return true;
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
