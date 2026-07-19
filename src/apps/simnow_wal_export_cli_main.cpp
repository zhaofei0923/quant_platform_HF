#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/backtest/product_fee_config_loader.h"
#include "quant_hft/contracts/types.h"
#include "quant_hft/core/storage_client_factory.h"
#include "quant_hft/core/storage_connection_config.h"
#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/trading_ledger_store_client_adapter.h"

namespace {

using quant_hft::OrderEvent;

struct WalRecord {
    std::string raw_line;
    std::int64_t seq{0};
    int schema_version{0};
    std::string kind;
    std::string event_type;
    std::string run_id;
    std::string trading_day;
    OrderEvent event;
};

struct ExportSpec {
    std::string wal_file;
    std::string trading_day;
    std::string run_id_filter;
    std::string export_root;
    std::string reconcile_root;
    std::string orders_csv;
    std::string fills_csv;
    std::string events_jsonl;
    std::string summary_json;
    std::string summary_md;
    std::string reconcile_json;
    std::string reconcile_md;
    std::string fee_config;
    bool project_db{false};
    bool query_db{false};
    bool strict_reconcile{false};
    bool strict_input{false};
};

struct ExportStats {
    std::int64_t lines_total{0};
    std::int64_t parse_errors{0};
    std::int64_t ignored_lines{0};
    std::int64_t exported_order_events{0};
    std::int64_t exported_trade_fills{0};
    std::int64_t raw_trade_fill_records{0};
    std::int64_t duplicate_trade_fills{0};
    std::int64_t unresolved_trade_fills{0};
    std::int64_t projected_order_events{0};
    std::int64_t projected_trade_fills{0};
    std::int64_t db_projection_failures{0};
    std::int64_t db_order_events{-1};
    std::int64_t db_trade_fills{-1};
    std::int64_t max_seq{0};
    std::int64_t unpriced_fills{0};
    double total_commission{0.0};
    bool wal_missing{false};
    bool db_queried{false};
    bool fee_config_loaded{false};
    bool order_identity_match{false};
    bool trade_identity_match{false};
    std::int64_t unresolved_order_identities{0};
    std::int64_t db_unresolved_order_identities{0};
    std::int64_t db_unresolved_trade_identities{0};
    std::int64_t nonterminal_order_identities{0};
    std::int64_t db_nonterminal_order_identities{0};
    std::string db_error;
    std::string fee_config_error;
    std::map<std::string, std::int64_t> orders_by_instrument;
    std::map<std::string, std::int64_t> fills_by_instrument;
    std::map<std::string, double> commission_by_instrument;
    std::map<std::string, std::pair<std::int64_t, std::string>> wal_latest_order_status;
    std::set<std::string> wal_trade_identity_set;
    std::set<std::string> db_order_terminal_identity_set;
    std::set<std::string> db_trade_identity_set;
};

std::string GetEnvOrDefault(const char* key, const std::string& fallback) {
    const char* value = std::getenv(key);
    if (value == nullptr || std::string(value).empty()) {
        return fallback;
    }
    return std::string(value);
}

std::string DefaultPathFromRoot(const std::string& suffix) {
    const std::string root = GetEnvOrDefault("QUANT_ROOT", "");
    if (root.empty()) {
        return suffix;
    }
    return (std::filesystem::path(root) / suffix).string();
}

std::string NormalizeTradingDay(std::string value) {
    value.erase(std::remove(value.begin(), value.end(), '-'), value.end());
    value.erase(std::remove_if(value.begin(), value.end(),
                               [](unsigned char ch) { return std::isspace(ch) != 0; }),
                value.end());
    return value;
}

std::string TrimCopy(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
                    return std::isspace(ch) == 0;
                }));
    value.erase(std::find_if(value.rbegin(), value.rend(),
                             [](unsigned char ch) { return std::isspace(ch) == 0; })
                    .base(),
                value.end());
    return value;
}

std::string TradingDayForDb(const std::string& trading_day) {
    const std::string day = NormalizeTradingDay(trading_day);
    if (day.size() != 8) {
        return trading_day;
    }
    return day.substr(0, 4) + "-" + day.substr(4, 2) + "-" + day.substr(6, 2);
}

std::string FormatLocalTradingDay(std::int64_t ts_ns) {
    if (ts_ns <= 0) {
        return "";
    }
    std::time_t seconds = static_cast<std::time_t>(ts_ns / 1'000'000'000LL + 8 * 60 * 60);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &seconds);
#else
    gmtime_r(&seconds, &tm);
#endif
    char buffer[9] = {0};
    if (std::strftime(buffer, sizeof(buffer), "%Y%m%d", &tm) == 0) {
        return "";
    }
    return std::string(buffer);
}

std::string ExtractTradingDayFromRunId(const std::string& run_id) {
    for (std::size_t i = 0; i + 8 <= run_id.size(); ++i) {
        bool all_digits = true;
        for (std::size_t j = 0; j < 8; ++j) {
            if (std::isdigit(static_cast<unsigned char>(run_id[i + j])) == 0) {
                all_digits = false;
                break;
            }
        }
        if (all_digits && run_id[i] == '2' && run_id[i + 1] == '0') {
            return run_id.substr(i, 8);
        }
    }
    return "";
}

bool ExtractRawValue(const std::string& line, const std::string& key, std::string* raw_value) {
    const std::string marker = "\"" + key + "\":";
    const auto marker_pos = line.find(marker);
    if (marker_pos == std::string::npos) {
        return false;
    }
    std::size_t pos = marker_pos + marker.size();
    while (pos < line.size() && std::isspace(static_cast<unsigned char>(line[pos])) != 0) {
        ++pos;
    }
    if (pos >= line.size()) {
        return false;
    }

    if (line[pos] == '"') {
        ++pos;
        std::string value;
        value.push_back('"');
        bool escaped = false;
        while (pos < line.size()) {
            const char ch = line[pos++];
            value.push_back(ch);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                *raw_value = value;
                return true;
            }
        }
        return false;
    }

    std::size_t end = pos;
    while (end < line.size() && line[end] != ',' && line[end] != '}') {
        ++end;
    }
    if (end <= pos) {
        return false;
    }
    *raw_value = line.substr(pos, end - pos);
    return true;
}

std::string UnescapeJsonString(const std::string& raw) {
    if (raw.size() < 2 || raw.front() != '"' || raw.back() != '"') {
        return raw;
    }
    std::string out;
    out.reserve(raw.size());
    bool escaped = false;
    for (std::size_t i = 1; i + 1 < raw.size(); ++i) {
        const char ch = raw[i];
        if (escaped) {
            switch (ch) {
                case 'n':
                    out.push_back('\n');
                    break;
                case 'r':
                    out.push_back('\r');
                    break;
                case 't':
                    out.push_back('\t');
                    break;
                case '"':
                case '\\':
                    out.push_back(ch);
                    break;
                default:
                    out.push_back(ch);
                    break;
            }
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
            continue;
        }
        out.push_back(ch);
    }
    return out;
}

bool ParseStringField(const std::string& line, const std::string& key, std::string* value) {
    std::string raw;
    if (!ExtractRawValue(line, key, &raw) || raw.empty() || raw.front() != '"') {
        return false;
    }
    *value = UnescapeJsonString(raw);
    return true;
}

bool ParseInt64Field(const std::string& line, const std::string& key, std::int64_t* value) {
    std::string raw;
    if (!ExtractRawValue(line, key, &raw)) {
        return false;
    }
    try {
        *value = std::stoll(raw);
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseIntField(const std::string& line, const std::string& key, int* value) {
    std::int64_t parsed = 0;
    if (!ParseInt64Field(line, key, &parsed)) {
        return false;
    }
    *value = static_cast<int>(parsed);
    return true;
}

bool ParseDoubleField(const std::string& line, const std::string& key, double* value) {
    std::string raw;
    if (!ExtractRawValue(line, key, &raw)) {
        return false;
    }
    try {
        *value = std::stod(raw);
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseBoolText(const std::string& value, bool fallback) {
    std::string lowered = value;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    if (lowered == "1" || lowered == "true" || lowered == "yes" || lowered == "on") {
        return true;
    }
    if (lowered == "0" || lowered == "false" || lowered == "no" || lowered == "off") {
        return false;
    }
    return fallback;
}

bool ParseStatus(int raw, quant_hft::OrderStatus* status) {
    if (raw < static_cast<int>(quant_hft::OrderStatus::kNew) ||
        raw > static_cast<int>(quant_hft::OrderStatus::kRejected)) {
        return false;
    }
    *status = static_cast<quant_hft::OrderStatus>(raw);
    return true;
}

bool ParseWalRecord(const std::string& line, WalRecord* record) {
    if (record == nullptr) {
        return false;
    }
    WalRecord parsed;
    parsed.raw_line = line;
    if (!ParseStringField(line, "kind", &parsed.kind)) {
        return false;
    }
    if (parsed.kind != "order" && parsed.kind != "trade") {
        return false;
    }

    (void)ParseStringField(line, "event_type", &parsed.event_type);
    (void)ParseStringField(line, "run_id", &parsed.run_id);
    (void)ParseInt64Field(line, "seq", &parsed.seq);
    (void)ParseIntField(line, "schema_version", &parsed.schema_version);

    OrderEvent event;
    (void)ParseStringField(line, "account_id", &event.account_id);
    (void)ParseStringField(line, "strategy_id", &event.strategy_id);
    (void)ParseStringField(line, "client_order_id", &event.client_order_id);
    (void)ParseStringField(line, "exchange_order_id", &event.exchange_order_id);
    (void)ParseStringField(line, "instrument_id", &event.instrument_id);
    (void)ParseStringField(line, "exchange_id", &event.exchange_id);
    (void)ParseStringField(line, "trade_id", &event.trade_id);
    (void)ParseStringField(line, "event_source", &event.event_source);
    (void)ParseStringField(line, "reason", &event.reason);
    (void)ParseStringField(line, "status_msg", &event.status_msg);
    (void)ParseStringField(line, "order_submit_status", &event.order_submit_status);
    (void)ParseStringField(line, "order_ref", &event.order_ref);
    (void)ParseStringField(line, "trace_id", &event.trace_id);
    (void)ParseStringField(line, "execution_algo_id", &event.execution_algo_id);
    (void)ParseStringField(line, "venue", &event.venue);
    (void)ParseStringField(line, "route_id", &event.route_id);
    (void)ParseInt64Field(line, "exchange_ts_ns", &event.exchange_ts_ns);
    (void)ParseInt64Field(line, "recv_ts_ns", &event.recv_ts_ns);
    (void)ParseInt64Field(line, "ts_ns", &event.ts_ns);
    (void)ParseIntField(line, "total_volume", &event.total_volume);
    (void)ParseIntField(line, "filled_volume", &event.filled_volume);
    (void)ParseIntField(line, "last_trade_volume", &event.last_trade_volume);
    (void)ParseDoubleField(line, "avg_fill_price", &event.avg_fill_price);
    (void)ParseIntField(line, "front_id", &event.front_id);
    (void)ParseIntField(line, "session_id", &event.session_id);
    (void)ParseIntField(line, "slice_index", &event.slice_index);
    (void)ParseIntField(line, "slice_total", &event.slice_total);
    (void)ParseDoubleField(line, "slippage_bps", &event.slippage_bps);
    (void)ParseDoubleField(line, "impact_cost", &event.impact_cost);

    int raw_side = 0;
    if (ParseIntField(line, "side", &raw_side) && raw_side >= 0 && raw_side <= 1) {
        event.side = static_cast<quant_hft::Side>(raw_side);
    }
    int raw_offset = 0;
    if (ParseIntField(line, "offset", &raw_offset) && raw_offset >= 0 && raw_offset <= 3) {
        event.offset = static_cast<quant_hft::OffsetFlag>(raw_offset);
    }
    int raw_status = 0;
    if (ParseIntField(line, "status", &raw_status)) {
        (void)ParseStatus(raw_status, &event.status);
    }

    parsed.event = std::move(event);
    (void)ParseStringField(line, "trading_day", &parsed.trading_day);
    parsed.trading_day = NormalizeTradingDay(parsed.trading_day);
    if (parsed.trading_day.empty()) {
        parsed.trading_day = ExtractTradingDayFromRunId(parsed.run_id);
    }
    if (parsed.trading_day.empty()) {
        const std::int64_t effective_ts =
            parsed.event.recv_ts_ns > 0
                ? parsed.event.recv_ts_ns
                : (parsed.event.ts_ns > 0 ? parsed.event.ts_ns : parsed.event.exchange_ts_ns);
        parsed.trading_day = FormatLocalTradingDay(effective_ts);
    }
    parsed.event.trading_day = parsed.trading_day;
    parsed.event.raw_trade_id = parsed.event.trade_id;
    *record = std::move(parsed);
    return true;
}

bool IsOrderUpdate(const WalRecord& record) {
    return record.event_type == "order_update" ||
           (record.event_type.empty() && (record.kind == "order" || record.kind == "trade"));
}

bool IsTradeFill(const WalRecord& record) {
    return record.event_type == "trade_fill" ||
           (record.event_type.empty() && record.kind == "trade");
}

std::string CanonicalTradeKey(const WalRecord& record) {
    const std::string trade_id = TrimCopy(record.event.trade_id);
    if (trade_id.empty()) {
        return "";
    }
    return TrimCopy(record.event.account_id) + "|" + NormalizeTradingDay(record.trading_day) + "|" +
           TrimCopy(record.event.exchange_id) + "|" + trade_id;
}

bool IsTerminalStatus(quant_hft::OrderStatus status) {
    return status == quant_hft::OrderStatus::kFilled ||
           status == quant_hft::OrderStatus::kCanceled ||
           status == quant_hft::OrderStatus::kRejected;
}

bool IsTerminalStatusText(const std::string& status) {
    try {
        const int raw = std::stoi(status);
        if (raw < static_cast<int>(quant_hft::OrderStatus::kNew) ||
            raw > static_cast<int>(quant_hft::OrderStatus::kRejected)) {
            return false;
        }
        return IsTerminalStatus(static_cast<quant_hft::OrderStatus>(raw));
    } catch (...) {
        return false;
    }
}

std::string StableOrderKey(const std::string& account_id, const std::string& trading_day,
                           const std::string& client_order_id, const std::string& exchange_order_id,
                           const std::string& exchange_id, const std::string& order_ref,
                           const std::string& front_id, const std::string& session_id) {
    const std::string account = TrimCopy(account_id);
    const std::string day = NormalizeTradingDay(trading_day);
    if (!client_order_id.empty()) {
        return account + "|" + day + "|client|" + TrimCopy(client_order_id);
    }
    if (!exchange_order_id.empty() && !exchange_id.empty()) {
        return account + "|" + day + "|broker|" + TrimCopy(exchange_id) + "|" +
               TrimCopy(exchange_order_id);
    }
    if (!order_ref.empty()) {
        return account + "|" + day + "|session|" + TrimCopy(front_id) + "|" + TrimCopy(session_id) +
               "|" + TrimCopy(order_ref);
    }
    return {};
}

std::string StableOrderKey(const WalRecord& record) {
    return StableOrderKey(record.event.account_id, record.trading_day, record.event.client_order_id,
                          record.event.exchange_order_id, record.event.exchange_id,
                          record.event.order_ref, std::to_string(record.event.front_id),
                          std::to_string(record.event.session_id));
}

std::string RowValue(const std::unordered_map<std::string, std::string>& row,
                     const std::string& key) {
    const auto it = row.find(key);
    return it == row.end() ? "" : it->second;
}

bool RowMatchesTradingDay(const std::unordered_map<std::string, std::string>& row,
                          const std::string& trading_day) {
    const auto it = row.find("trade_date");
    return it == row.end() || trading_day.empty() || it->second == TradingDayForDb(trading_day) ||
           NormalizeTradingDay(it->second) == NormalizeTradingDay(trading_day);
}

std::int64_t ParseRowInt64(const std::unordered_map<std::string, std::string>& row,
                           const std::string& key) {
    try {
        return std::stoll(RowValue(row, key));
    } catch (...) {
        return 0;
    }
}

void BuildWalTerminalIdentitySet(ExportStats* stats, std::set<std::string>* terminal_identities) {
    if (stats == nullptr || terminal_identities == nullptr) {
        return;
    }
    terminal_identities->clear();
    stats->nonterminal_order_identities = 0;
    for (const auto& [stable_key, latest] : stats->wal_latest_order_status) {
        if (!IsTerminalStatusText(latest.second)) {
            ++stats->nonterminal_order_identities;
            continue;
        }
        terminal_identities->insert(stable_key + "|status|" + latest.second);
    }
}

bool LoadDbIdentitySets(const std::shared_ptr<quant_hft::ITimescaleSqlClient>& client,
                        const std::string& schema, const std::string& trading_day,
                        ExportStats* stats, std::string* error) {
    if (client == nullptr || stats == nullptr) {
        if (error != nullptr) {
            *error = "database client or stats is null";
        }
        return false;
    }

    std::string query_error;
    const auto order_rows = client->QueryAllRows(schema + ".order_events", &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    std::map<std::string, std::pair<std::int64_t, std::string>> latest_orders;
    stats->db_order_events = 0;
    for (const auto& row : order_rows) {
        if (!RowMatchesTradingDay(row, trading_day)) {
            continue;
        }
        ++stats->db_order_events;
        const std::string stable_key = StableOrderKey(
            RowValue(row, "account_id"), trading_day, RowValue(row, "client_order_id"),
            RowValue(row, "exchange_order_id"), RowValue(row, "exchange_id"),
            RowValue(row, "order_ref"), RowValue(row, "front_id"), RowValue(row, "session_id"));
        if (stable_key.empty()) {
            ++stats->db_unresolved_order_identities;
            continue;
        }
        const std::int64_t ts_ns = ParseRowInt64(row, "ts_ns");
        const std::string status = RowValue(row, "status");
        auto& latest = latest_orders[stable_key];
        if (latest.first <= ts_ns) {
            latest = {ts_ns, status};
        }
    }
    stats->db_order_terminal_identity_set.clear();
    stats->db_nonterminal_order_identities = 0;
    for (const auto& [stable_key, latest] : latest_orders) {
        if (!IsTerminalStatusText(latest.second)) {
            ++stats->db_nonterminal_order_identities;
            continue;
        }
        stats->db_order_terminal_identity_set.insert(stable_key + "|status|" + latest.second);
    }

    query_error.clear();
    const auto trade_rows = client->QueryAllRows(schema + ".trade_events", &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    stats->db_trade_fills = 0;
    stats->db_trade_identity_set.clear();
    for (const auto& row : trade_rows) {
        if (!RowMatchesTradingDay(row, trading_day)) {
            continue;
        }
        ++stats->db_trade_fills;
        const std::string canonical_key = TrimCopy(RowValue(row, "idempotency_key"));
        if (canonical_key.empty()) {
            ++stats->db_unresolved_trade_identities;
            continue;
        }
        stats->db_trade_identity_set.insert(canonical_key);
    }
    return true;
}

std::string CsvEscape(const std::string& value) {
    const bool needs_quotes =
        value.find(',') != std::string::npos || value.find('"') != std::string::npos ||
        value.find('\n') != std::string::npos || value.find('\r') != std::string::npos;
    if (!needs_quotes) {
        return value;
    }
    std::string escaped;
    escaped.reserve(value.size() + 8);
    escaped.push_back('"');
    for (char ch : value) {
        if (ch == '"') {
            escaped += "\"\"";
        } else {
            escaped.push_back(ch);
        }
    }
    escaped.push_back('"');
    return escaped;
}

std::string ToText(std::int64_t value) { return std::to_string(value); }

std::string ToText(double value) {
    std::ostringstream out;
    out << std::setprecision(12) << value;
    return out.str();
}

void WriteOrderCsvHeader(std::ostream& out) {
    out << "seq,event_type,run_id,trading_day,account_id,strategy_id,client_order_id,"
           "exchange_order_id,instrument_id,exchange_id,side,offset,status,total_volume,"
           "filled_volume,last_trade_volume,avg_fill_price,event_source,trade_id,order_ref,"
           "front_id,session_id,status_msg,reason,trace_id,exchange_ts_ns,recv_ts_ns,ts_ns\n";
}

void WriteOrderCsvRow(std::ostream& out, const WalRecord& record) {
    const auto& event = record.event;
    out << record.seq << ',' << CsvEscape(record.event_type) << ',' << CsvEscape(record.run_id)
        << ',' << CsvEscape(record.trading_day) << ',' << CsvEscape(event.account_id) << ','
        << CsvEscape(event.strategy_id) << ',' << CsvEscape(event.client_order_id) << ','
        << CsvEscape(event.exchange_order_id) << ',' << CsvEscape(event.instrument_id) << ','
        << CsvEscape(event.exchange_id) << ',' << static_cast<int>(event.side) << ','
        << static_cast<int>(event.offset) << ',' << static_cast<int>(event.status) << ','
        << event.total_volume << ',' << event.filled_volume << ',' << event.last_trade_volume << ','
        << ToText(event.avg_fill_price) << ',' << CsvEscape(event.event_source) << ','
        << CsvEscape(event.trade_id) << ',' << CsvEscape(event.order_ref) << ',' << event.front_id
        << ',' << event.session_id << ',' << CsvEscape(event.status_msg) << ','
        << CsvEscape(event.reason) << ',' << CsvEscape(event.trace_id) << ','
        << event.exchange_ts_ns << ',' << event.recv_ts_ns << ',' << event.ts_ns << '\n';
}

void WriteFillCsvHeader(std::ostream& out) {
    out << "seq,event_type,run_id,trading_day,account_id,strategy_id,client_order_id,"
           "exchange_order_id,instrument_id,exchange_id,side,offset,trade_id,filled_volume,"
           "last_trade_volume,avg_fill_price,commission,event_source,trace_id,exchange_ts_ns,"
           "recv_ts_ns,ts_ns\n";
}

void WriteFillCsvRow(std::ostream& out, const WalRecord& record, double commission) {
    const auto& event = record.event;
    out << record.seq << ',' << CsvEscape(record.event_type) << ',' << CsvEscape(record.run_id)
        << ',' << CsvEscape(record.trading_day) << ',' << CsvEscape(event.account_id) << ','
        << CsvEscape(event.strategy_id) << ',' << CsvEscape(event.client_order_id) << ','
        << CsvEscape(event.exchange_order_id) << ',' << CsvEscape(event.instrument_id) << ','
        << CsvEscape(event.exchange_id) << ',' << static_cast<int>(event.side) << ','
        << static_cast<int>(event.offset) << ',' << CsvEscape(event.trade_id) << ','
        << event.filled_volume << ',' << event.last_trade_volume << ','
        << ToText(event.avg_fill_price) << ',' << ToText(commission) << ','
        << CsvEscape(event.event_source) << ',' << CsvEscape(event.trace_id) << ','
        << event.exchange_ts_ns << ',' << event.recv_ts_ns << ',' << event.ts_ns << '\n';
}

std::string JsonCountMap(const std::map<std::string, std::int64_t>& counts) {
    std::ostringstream out;
    out << "{";
    bool first = true;
    for (const auto& [key, value] : counts) {
        if (!first) {
            out << ",";
        }
        first = false;
        out << "\"" << quant_hft::apps::JsonEscape(key) << "\":" << value;
    }
    out << "}";
    return out.str();
}

std::string JsonDoubleMap(const std::map<std::string, double>& values) {
    std::ostringstream out;
    out << "{";
    bool first = true;
    for (const auto& [key, value] : values) {
        if (!first) {
            out << ",";
        }
        first = false;
        out << "\"" << quant_hft::apps::JsonEscape(key) << "\":" << ToText(value);
    }
    out << "}";
    return out.str();
}

std::string RenderSummaryJson(const ExportSpec& spec, const ExportStats& stats) {
    std::ostringstream out;
    out << "{\n"
        << "  \"trading_day\": \"" << quant_hft::apps::JsonEscape(spec.trading_day) << "\",\n"
        << "  \"wal_file\": \"" << quant_hft::apps::JsonEscape(spec.wal_file) << "\",\n"
        << "  \"wal_missing\": " << (stats.wal_missing ? "true" : "false") << ",\n"
        << "  \"lines_total\": " << stats.lines_total << ",\n"
        << "  \"parse_errors\": " << stats.parse_errors << ",\n"
        << "  \"ignored_lines\": " << stats.ignored_lines << ",\n"
        << "  \"order_events\": " << stats.exported_order_events << ",\n"
        << "  \"trade_fills\": " << stats.exported_trade_fills << ",\n"
        << "  \"raw_trade_fill_records\": " << stats.raw_trade_fill_records << ",\n"
        << "  \"duplicate_trade_fills\": " << stats.duplicate_trade_fills << ",\n"
        << "  \"unresolved_trade_fills\": " << stats.unresolved_trade_fills << ",\n"
        << "  \"unresolved_order_identities\": " << stats.unresolved_order_identities << ",\n"
        << "  \"nonterminal_order_identities\": " << stats.nonterminal_order_identities << ",\n"
        << "  \"total_commission\": " << ToText(stats.total_commission) << ",\n"
        << "  \"unpriced_fills\": " << stats.unpriced_fills << ",\n"
        << "  \"fee_config\": \"" << quant_hft::apps::JsonEscape(spec.fee_config) << "\",\n"
        << "  \"fee_config_loaded\": " << (stats.fee_config_loaded ? "true" : "false") << ",\n"
        << "  \"project_db\": " << (spec.project_db ? "true" : "false") << ",\n"
        << "  \"projected_order_events\": " << stats.projected_order_events << ",\n"
        << "  \"projected_trade_fills\": " << stats.projected_trade_fills << ",\n"
        << "  \"db_projection_failures\": " << stats.db_projection_failures << ",\n"
        << "  \"orders_by_instrument\": " << JsonCountMap(stats.orders_by_instrument) << ",\n"
        << "  \"fills_by_instrument\": " << JsonCountMap(stats.fills_by_instrument) << ",\n"
        << "  \"commission_by_instrument\": " << JsonDoubleMap(stats.commission_by_instrument)
        << ",\n"
        << "  \"outputs\": {\n"
        << "    \"orders_csv\": \"" << quant_hft::apps::JsonEscape(spec.orders_csv) << "\",\n"
        << "    \"trade_fills_csv\": \"" << quant_hft::apps::JsonEscape(spec.fills_csv) << "\",\n"
        << "    \"events_jsonl\": \"" << quant_hft::apps::JsonEscape(spec.events_jsonl) << "\"\n"
        << "  }\n"
        << "}\n";
    return out.str();
}

std::string RenderReconcileJson(const ExportSpec& spec, const ExportStats& stats) {
    const bool db_available = stats.db_queried && stats.db_error.empty() &&
                              stats.db_order_events >= 0 && stats.db_trade_fills >= 0;
    const bool order_match = db_available && stats.order_identity_match;
    const bool fill_match = db_available && stats.trade_identity_match;
    const bool incomplete =
        !db_available || stats.unresolved_trade_fills > 0 ||
        stats.unresolved_order_identities > 0 || stats.db_unresolved_order_identities > 0 ||
        stats.db_unresolved_trade_identities > 0 || stats.nonterminal_order_identities > 0 ||
        stats.db_nonterminal_order_identities > 0;
    const bool ok = !incomplete && !stats.wal_missing && stats.parse_errors == 0 &&
                    stats.db_projection_failures == 0 && order_match && fill_match;
    const char* status = ok ? "ok" : (incomplete ? "incomplete" : "failed");

    std::ostringstream out;
    out << "{\n"
        << "  \"trading_day\": \"" << quant_hft::apps::JsonEscape(spec.trading_day) << "\",\n"
        << "  \"status\": \"" << status << "\",\n"
        << "  \"complete\": " << (!incomplete ? "true" : "false") << ",\n"
        << "  \"db_queried\": " << (stats.db_queried ? "true" : "false") << ",\n"
        << "  \"db_error\": \"" << quant_hft::apps::JsonEscape(stats.db_error) << "\",\n"
        << "  \"wal\": {\"order_events\": " << stats.exported_order_events
        << ", \"trade_fills\": " << stats.exported_trade_fills
        << ", \"raw_trade_fill_records\": " << stats.raw_trade_fill_records
        << ", \"duplicate_trade_fills\": " << stats.duplicate_trade_fills
        << ", \"unresolved_trade_fills\": " << stats.unresolved_trade_fills
        << ", \"unresolved_order_identities\": " << stats.unresolved_order_identities
        << ", \"nonterminal_order_identities\": " << stats.nonterminal_order_identities << "},\n"
        << "  \"db\": {\"order_events\": " << stats.db_order_events
        << ", \"trade_fills\": " << stats.db_trade_fills
        << ", \"unresolved_order_identities\": " << stats.db_unresolved_order_identities
        << ", \"unresolved_trade_identities\": " << stats.db_unresolved_trade_identities
        << ", \"nonterminal_order_identities\": " << stats.db_nonterminal_order_identities << "},\n"
        << "  \"match\": {\"order_events\": " << (order_match ? "true" : "false")
        << ", \"trade_fills\": " << (fill_match ? "true" : "false")
        << ", \"comparison\": \"stable_identity_and_terminal_status\"},\n"
        << "  \"projected\": {\"order_events\": " << stats.projected_order_events
        << ", \"trade_fills\": " << stats.projected_trade_fills
        << ", \"failures\": " << stats.db_projection_failures << "}\n"
        << "}\n";
    return out.str();
}

std::string RenderSummaryMd(const ExportSpec& spec, const ExportStats& stats) {
    std::ostringstream out;
    out << "# SimNow WAL Export " << spec.trading_day << "\n\n"
        << "- WAL: " << spec.wal_file << "\n"
        << "- Orders CSV: " << spec.orders_csv << "\n"
        << "- Trade fills CSV: " << spec.fills_csv << "\n"
        << "- Events JSONL: " << spec.events_jsonl << "\n"
        << "- Lines: " << stats.lines_total << "\n"
        << "- Order events: " << stats.exported_order_events << "\n"
        << "- Trade fills: " << stats.exported_trade_fills << "\n"
        << "- Raw trade fill records: " << stats.raw_trade_fill_records << "\n"
        << "- Duplicate trade fills: " << stats.duplicate_trade_fills << "\n"
        << "- Unresolved trade fills: " << stats.unresolved_trade_fills << "\n"
        << "- Unresolved order identities: " << stats.unresolved_order_identities << "\n"
        << "- Non-terminal order identities: " << stats.nonterminal_order_identities << "\n"
        << "- Total commission: " << ToText(stats.total_commission) << "\n"
        << "- Unpriced fills: " << stats.unpriced_fills << "\n"
        << "- Fee config: " << (stats.fee_config_loaded ? spec.fee_config : "disabled") << "\n"
        << "- Parse errors: " << stats.parse_errors << "\n"
        << "- DB projection: " << (spec.project_db ? "enabled" : "disabled") << "\n"
        << "- DB projection failures: " << stats.db_projection_failures << "\n";
    return out.str();
}

std::string RenderReconcileMd(const ExportSpec& spec, const ExportStats& stats) {
    std::ostringstream out;
    out << "# SimNow Reconcile " << spec.trading_day << "\n\n"
        << "| Source | Order events | Trade fills |\n"
        << "| --- | ---: | ---: |\n"
        << "| WAL | " << stats.exported_order_events << " | " << stats.exported_trade_fills
        << " |\n"
        << "| DB | " << stats.db_order_events << " | " << stats.db_trade_fills << " |\n\n";
    out << "- DB queried: " << (stats.db_queried ? "yes" : "no") << "\n"
        << "- Canonical duplicate fills skipped: " << stats.duplicate_trade_fills << "\n"
        << "- Unresolved fills (missing trade_id): " << stats.unresolved_trade_fills << "\n"
        << "- Order identity sets match: " << (stats.order_identity_match ? "yes" : "no") << "\n"
        << "- Trade identity sets match: " << (stats.trade_identity_match ? "yes" : "no") << "\n";
    if (!stats.db_error.empty()) {
        out << "DB query/projection note: " << stats.db_error << "\n";
    }
    return out.str();
}

ExportSpec ParseSpec(const quant_hft::apps::ArgMap& args) {
    ExportSpec spec;
    spec.trading_day =
        NormalizeTradingDay(quant_hft::apps::GetArgAny(args, {"trading-day", "trading_day"}));
    spec.run_id_filter = quant_hft::apps::GetArgAny(args, {"run-id", "run_id"});
    spec.wal_file = quant_hft::apps::GetArgAny(
        args, {"wal-file", "wal_file"},
        GetEnvOrDefault("SIMNOW_WAL_FILE",
                        DefaultPathFromRoot("runtime/trading/wal/simnow/events.wal")));
    spec.export_root = quant_hft::apps::GetArgAny(
        args, {"export-root", "export_root"},
        GetEnvOrDefault("SIMNOW_EXPORT_ROOT",
                        DefaultPathFromRoot("runtime/trading/exports/simnow")));
    spec.reconcile_root = quant_hft::apps::GetArgAny(
        args, {"reconcile-root", "reconcile_root"},
        GetEnvOrDefault("SIMNOW_RECONCILE_ROOT",
                        DefaultPathFromRoot("runtime/trading/reconcile/simnow")));

    const std::filesystem::path export_day_dir =
        std::filesystem::path(spec.export_root) / spec.trading_day;
    const std::filesystem::path reconcile_day_dir =
        std::filesystem::path(spec.reconcile_root) / spec.trading_day;
    spec.orders_csv = quant_hft::apps::GetArgAny(args, {"orders-csv", "orders_csv"},
                                                 (export_day_dir / "orders.csv").string());
    spec.fills_csv = quant_hft::apps::GetArgAny(args, {"trade-fills-csv", "trade_fills_csv"},
                                                (export_day_dir / "trade_fills.csv").string());
    spec.events_jsonl = quant_hft::apps::GetArgAny(args, {"events-jsonl", "events_jsonl"},
                                                   (export_day_dir / "events.jsonl").string());
    spec.summary_json = quant_hft::apps::GetArgAny(args, {"summary-json", "summary_json"},
                                                   (export_day_dir / "summary.json").string());
    spec.summary_md = quant_hft::apps::GetArgAny(args, {"summary-md", "summary_md"},
                                                 (export_day_dir / "summary.md").string());
    spec.reconcile_json =
        quant_hft::apps::GetArgAny(args, {"reconcile-json", "reconcile_json"},
                                   (reconcile_day_dir / "reconcile.json").string());
    spec.reconcile_md = quant_hft::apps::GetArgAny(args, {"reconcile-md", "reconcile_md"},
                                                   (reconcile_day_dir / "reconcile.md").string());
    spec.fee_config = quant_hft::apps::GetArgAny(
        args, {"fee-config", "fee_config"},
        GetEnvOrDefault("SIMNOW_FEE_CONFIG",
                        DefaultPathFromRoot("configs/strategies/instrument_info.json")));
    spec.project_db =
        ParseBoolText(quant_hft::apps::GetArgAny(args, {"project-db", "project_db"}), false);
    spec.query_db =
        ParseBoolText(quant_hft::apps::GetArgAny(args, {"query-db", "query_db"}), spec.project_db);
    spec.strict_reconcile = ParseBoolText(
        quant_hft::apps::GetArgAny(args, {"strict-reconcile", "strict_reconcile"}), false);
    spec.strict_input =
        ParseBoolText(quant_hft::apps::GetArgAny(args, {"strict-input", "strict_input"}), false);
    return spec;
}

bool EnsureParentDir(const std::string& path, std::string* error) {
    try {
        const std::filesystem::path file_path(path);
        if (!file_path.parent_path().empty()) {
            std::filesystem::create_directories(file_path.parent_path());
        }
        return true;
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = ex.what();
        }
        return false;
    }
}

bool MatchesSpec(const ExportSpec& spec, const WalRecord& record) {
    if (!spec.run_id_filter.empty() && record.run_id != spec.run_id_filter) {
        return false;
    }
    if (!spec.trading_day.empty() && !record.trading_day.empty() &&
        record.trading_day != spec.trading_day) {
        return false;
    }
    return true;
}

int RunExport(const ExportSpec& spec) {
    if (spec.trading_day.empty()) {
        std::cerr << "simnow_wal_export_cli: --trading-day is required\n";
        return 2;
    }

    std::string error;
    for (const std::string& path :
         {spec.orders_csv, spec.fills_csv, spec.events_jsonl, spec.summary_json, spec.summary_md,
          spec.reconcile_json, spec.reconcile_md}) {
        if (!EnsureParentDir(path, &error)) {
            std::cerr << "simnow_wal_export_cli: " << error << '\n';
            return 2;
        }
    }

    ExportStats stats;
    std::set<std::string> canonical_trade_keys;

    // Load per-product fee config so trade fills can be priced. A missing or invalid
    // config is non-fatal: commission defaults to 0 and unpriced fills are counted.
    quant_hft::ProductFeeBook fee_book;
    if (!spec.fee_config.empty()) {
        std::string fee_error;
        if (quant_hft::LoadProductFeeConfig(spec.fee_config, &fee_book, &fee_error)) {
            stats.fee_config_loaded = true;
        } else {
            stats.fee_config_error = fee_error;
            std::cerr << "simnow_wal_export_cli: fee config not loaded (" << fee_error
                      << "); commissions default to 0\n";
        }
    }

    std::ofstream orders_out(spec.orders_csv, std::ios::out | std::ios::trunc);
    std::ofstream fills_out(spec.fills_csv, std::ios::out | std::ios::trunc);
    std::ofstream events_out(spec.events_jsonl, std::ios::out | std::ios::trunc);
    if (!orders_out.is_open() || !fills_out.is_open() || !events_out.is_open()) {
        std::cerr << "simnow_wal_export_cli: failed to open export outputs\n";
        return 2;
    }
    WriteOrderCsvHeader(orders_out);
    WriteFillCsvHeader(fills_out);

    std::shared_ptr<quant_hft::ITimescaleSqlClient> db_client;
    std::unique_ptr<quant_hft::TradingLedgerStoreClientAdapter> ledger_store;
    std::string schema;
    if (spec.project_db || spec.query_db) {
        const auto storage_config = quant_hft::StorageConnectionConfig::FromEnvironment();
        schema = storage_config.timescale.trading_schema.empty()
                     ? "trading_core"
                     : storage_config.timescale.trading_schema;
        db_client = quant_hft::StorageClientFactory::CreateTimescaleClient(storage_config, &error);
        if (db_client == nullptr) {
            stats.db_error = error.empty() ? "timescale client creation failed" : error;
        } else {
            std::string ping_error;
            if (!db_client->Ping(&ping_error)) {
                stats.db_error = ping_error;
            } else if (spec.project_db) {
                quant_hft::StorageRetryPolicy retry_policy;
                retry_policy.max_attempts = 3;
                retry_policy.initial_backoff_ms = 1;
                retry_policy.max_backoff_ms = 8;
                ledger_store = std::make_unique<quant_hft::TradingLedgerStoreClientAdapter>(
                    db_client, retry_policy, schema);
            }
        }
    }

    std::ifstream wal_in(spec.wal_file);
    if (!wal_in.is_open()) {
        stats.wal_missing = true;
    } else {
        std::string line;
        while (std::getline(wal_in, line)) {
            ++stats.lines_total;
            WalRecord record;
            if (!ParseWalRecord(line, &record)) {
                ++stats.ignored_lines;
                continue;
            }
            if (!MatchesSpec(spec, record)) {
                ++stats.ignored_lines;
                continue;
            }
            stats.max_seq = std::max(stats.max_seq, record.seq);
            events_out << record.raw_line << '\n';

            if (IsOrderUpdate(record)) {
                const std::string stable_order_key = StableOrderKey(record);
                if (stable_order_key.empty()) {
                    ++stats.unresolved_order_identities;
                } else {
                    auto& latest = stats.wal_latest_order_status[stable_order_key];
                    const std::int64_t order_sequence =
                        record.seq > 0 ? record.seq : record.event.ts_ns;
                    if (latest.first <= order_sequence) {
                        latest = {order_sequence,
                                  std::to_string(static_cast<int>(record.event.status))};
                    }
                }
                WriteOrderCsvRow(orders_out, record);
                ++stats.exported_order_events;
                ++stats.orders_by_instrument[record.event.instrument_id];
                if (ledger_store != nullptr) {
                    std::string append_error;
                    if (ledger_store->AppendOrderEvent(record.event, &append_error)) {
                        ++stats.projected_order_events;
                    } else {
                        ++stats.db_projection_failures;
                        if (stats.db_error.empty()) {
                            stats.db_error = append_error;
                        }
                    }
                }
            }
            if (IsTradeFill(record)) {
                ++stats.raw_trade_fill_records;
                const std::string trade_key = CanonicalTradeKey(record);
                if (trade_key.empty()) {
                    ++stats.unresolved_trade_fills;
                    continue;
                }
                if (!canonical_trade_keys.insert(trade_key).second) {
                    ++stats.duplicate_trade_fills;
                    continue;
                }
                stats.wal_trade_identity_set.insert(trade_key);
                const auto& event = record.event;
                // Each trade_fill record represents a single exchange trade, so price the
                // incremental last_trade_volume (fall back to filled_volume when absent).
                std::int32_t fill_volume =
                    event.last_trade_volume > 0 ? event.last_trade_volume : event.filled_volume;
                double commission = 0.0;
                const quant_hft::ProductFeeEntry* fee_entry = fee_book.Find(event.instrument_id);
                if (fee_entry != nullptr) {
                    commission = quant_hft::ProductFeeBook::ComputeCommission(
                        *fee_entry, event.offset, fill_volume, event.avg_fill_price);
                } else if (fill_volume > 0) {
                    ++stats.unpriced_fills;
                }
                stats.total_commission += commission;
                stats.commission_by_instrument[event.instrument_id] += commission;
                WriteFillCsvRow(fills_out, record, commission);
                ++stats.exported_trade_fills;
                ++stats.fills_by_instrument[event.instrument_id];
                if (ledger_store != nullptr) {
                    std::string append_error;
                    if (ledger_store->AppendTradeEvent(record.event, &append_error)) {
                        ++stats.projected_trade_fills;
                    } else {
                        ++stats.db_projection_failures;
                        if (stats.db_error.empty()) {
                            stats.db_error = append_error;
                        }
                    }
                }
            }
        }
    }

    if (ledger_store != nullptr && stats.max_seq > 0) {
        std::string offset_error;
        const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
        if (!ledger_store->UpsertReplayOffset(spec.wal_file, stats.max_seq, now_ns,
                                              &offset_error)) {
            ++stats.db_projection_failures;
            if (stats.db_error.empty()) {
                stats.db_error = offset_error;
            }
        }
    }

    if (spec.query_db && db_client != nullptr && stats.db_error.empty()) {
        stats.db_queried = true;
        std::string query_error;
        if (!LoadDbIdentitySets(db_client, schema, spec.trading_day, &stats, &query_error)) {
            stats.db_error = query_error;
        }
    }

    std::set<std::string> wal_order_terminal_identity_set;
    BuildWalTerminalIdentitySet(&stats, &wal_order_terminal_identity_set);
    stats.order_identity_match =
        stats.db_queried && stats.db_error.empty() && stats.unresolved_order_identities == 0 &&
        stats.db_unresolved_order_identities == 0 &&
        wal_order_terminal_identity_set == stats.db_order_terminal_identity_set;
    stats.trade_identity_match = stats.db_queried && stats.db_error.empty() &&
                                 stats.unresolved_trade_fills == 0 &&
                                 stats.db_unresolved_trade_identities == 0 &&
                                 stats.wal_trade_identity_set == stats.db_trade_identity_set;

    if (!quant_hft::apps::WriteTextFile(spec.summary_json, RenderSummaryJson(spec, stats),
                                        &error) ||
        !quant_hft::apps::WriteTextFile(spec.summary_md, RenderSummaryMd(spec, stats), &error) ||
        !quant_hft::apps::WriteTextFile(spec.reconcile_json, RenderReconcileJson(spec, stats),
                                        &error) ||
        !quant_hft::apps::WriteTextFile(spec.reconcile_md, RenderReconcileMd(spec, stats),
                                        &error)) {
        std::cerr << "simnow_wal_export_cli: " << error << '\n';
        return 2;
    }

    std::cout << "simnow_wal_export_cli: orders=" << stats.exported_order_events
              << " trade_fills=" << stats.exported_trade_fills
              << " projected_orders=" << stats.projected_order_events
              << " projected_fills=" << stats.projected_trade_fills
              << " outputs=" << std::filesystem::path(spec.orders_csv).parent_path().string()
              << '\n';

    const bool db_available = stats.db_queried && stats.db_order_events >= 0 &&
                              stats.db_trade_fills >= 0 && stats.db_error.empty();
    const bool db_mismatch =
        db_available && (!stats.order_identity_match || !stats.trade_identity_match);
    const bool reconcile_incomplete =
        !db_available || stats.unresolved_trade_fills > 0 ||
        stats.unresolved_order_identities > 0 || stats.db_unresolved_order_identities > 0 ||
        stats.db_unresolved_trade_identities > 0 || stats.nonterminal_order_identities > 0 ||
        stats.db_nonterminal_order_identities > 0;
    if ((stats.wal_missing && spec.strict_input) || stats.parse_errors > 0 ||
        stats.db_projection_failures > 0 ||
        (spec.strict_reconcile && (db_mismatch || reconcile_incomplete))) {
        return 2;
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const auto args = quant_hft::apps::ParseArgs(argc, argv);
    if (quant_hft::apps::HasArg(args, "help") || quant_hft::apps::HasArg(args, "h")) {
        std::cout << "Usage: simnow_wal_export_cli --trading-day YYYYMMDD [--wal-file path] "
                     "[--export-root dir] [--reconcile-root dir] [--project-db 0|1] "
                     "[--query-db 0|1] [--strict-reconcile 0|1]\n";
        return 0;
    }
    return RunExport(ParseSpec(args));
}
