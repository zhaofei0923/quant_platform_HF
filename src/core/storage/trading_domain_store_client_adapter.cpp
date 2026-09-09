#include "quant_hft/core/trading_domain_store_client_adapter.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <limits>
#include <sstream>
#include <thread>
#include <unordered_set>

#include "quant_hft/model/independent_strategy_books.h"

namespace quant_hft {

namespace {

std::string ToDirectionCode(Side side) { return side == Side::kSell ? "1" : "0"; }

std::string ToOffsetCode(OffsetFlag offset) {
    switch (offset) {
        case OffsetFlag::kOpen:
            return "0";
        case OffsetFlag::kClose:
            return "1";
        case OffsetFlag::kCloseToday:
            return "2";
        case OffsetFlag::kCloseYesterday:
            return "3";
    }
    return "0";
}

std::string ToOrderTypeCode(OrderType type) { return type == OrderType::kMarket ? "2" : "1"; }

std::string ToDate(EpochNanos ts_ns) {
    const auto effective = ts_ns > 0 ? ts_ns : NowEpochNanos();
    const std::time_t seconds = static_cast<std::time_t>(effective / 1'000'000'000LL);
    std::tm utc_tm{};
#if defined(_WIN32)
    gmtime_s(&utc_tm, &seconds);
#else
    gmtime_r(&seconds, &utc_tm);
#endif
    char buffer[11] = {0};
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", &utc_tm) == 0) {
        return "1970-01-01";
    }
    return std::string(buffer);
}

std::int32_t ParseIntOrDefault(const std::unordered_map<std::string, std::string>& row,
                               const std::string& key, std::int32_t default_value = 0) {
    const auto it = row.find(key);
    if (it == row.end() || it->second.empty()) {
        return default_value;
    }
    try {
        return static_cast<std::int32_t>(std::stoi(it->second));
    } catch (...) {
        return default_value;
    }
}

double ParseDoubleOrDefault(const std::unordered_map<std::string, std::string>& row,
                            const std::string& key, double default_value = 0.0) {
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

}  // namespace

TradingDomainStoreClientAdapter::TradingDomainStoreClientAdapter(
    std::shared_ptr<ITimescaleSqlClient> client, StorageRetryPolicy retry_policy,
    std::string schema)
    : client_(std::move(client)),
      retry_policy_(retry_policy),
      schema_(schema.empty() ? "trading_core" : std::move(schema)) {}

bool TradingDomainStoreClientAdapter::UpsertOrder(const Order& order, std::string* error) {
    if (client_ == nullptr) {
        if (error != nullptr) {
            *error = "null sql client";
        }
        return false;
    }
    if (order.order_id.empty() || order.account_id.empty() || order.strategy_id.empty() ||
        order.symbol.empty()) {
        if (error != nullptr) {
            *error = "order requires order_id/account_id/strategy_id/symbol";
        }
        return false;
    }
    const auto insert_time = ToTimestamp(order.created_at_ns);
    const auto update_time =
        ToTimestamp(order.updated_at_ns > 0 ? order.updated_at_ns : order.created_at_ns);
    std::unordered_map<std::string, std::string> row{
        {"order_ref", order.order_id},
        {"account_id", order.account_id},
        {"strategy_id", order.strategy_id},
        {"component_id", order.component_id},
        {"hedge_flag", std::to_string(static_cast<int>(order.hedge_flag))},
        {"instrument_id", order.symbol},
        {"exchange_id", order.exchange},
        {"order_type", ToOrderTypeCode(order.order_type)},
        {"direction", ToDirectionCode(order.side)},
        {"offset_flag", ToOffsetCode(order.offset)},
        {"price_type", "1"},
        {"limit_price", ToString(order.price)},
        {"volume_original", ToString(order.quantity)},
        {"volume_traded", ToString(order.filled_quantity)},
        {"volume_canceled", ToString(order.status == OrderStatus::kCanceled
                                         ? std::max(0, order.quantity - order.filled_quantity)
                                         : 0)},
        {"order_status", ToString(static_cast<std::int32_t>(order.status))},
        {"insert_time", insert_time},
        {"update_time", update_time},
        {"status_msg", order.message},
    };
    std::string transaction_error;
    const bool ok = client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (!tx.LockTransactionKey("domain-order:" + std::to_string(order.account_id.size()) +
                                           ":" + order.account_id + order.order_id,
                                       e))
                return false;
            const auto rows = tx.QueryRows(TableName("orders"), "order_ref", order.order_id, e);
            if (!e->empty()) return false;
            for (const auto& existing : rows) {
                const auto account = existing.find("account_id");
                if (account == existing.end() || account->second != order.account_id) continue;
                if (existing.at("strategy_id") != order.strategy_id ||
                    existing.at("instrument_id") != order.symbol ||
                    ParseIntOrDefault(existing, "hedge_flag", 0) != static_cast<int>(order.hedge_flag)) {
                    *e = "order reference identity conflict";
                    return false;
                }
                // Recovery may replay older lifecycle events against a newer durable row.
                const auto previous =
                    static_cast<OrderStatus>(ParseIntOrDefault(existing, "order_status"));
                const bool terminal = previous == OrderStatus::kFilled ||
                                      previous == OrderStatus::kCanceled ||
                                      previous == OrderStatus::kRejected;
                if (ParseIntOrDefault(existing, "volume_traded") > order.filled_quantity ||
                    (terminal && previous != order.status))
                    return true;
                row["insert_time"] = existing.at("insert_time");
                break;
            }
            return tx.UpsertRow(TableName("orders"), row,
                                {"account_id", "order_ref", "insert_time"},
                                {"exchange_id", "volume_original", "volume_traded",
                                 "volume_canceled", "order_status", "update_time", "status_msg"},
                                e);
        },
        &transaction_error);
    if (error != nullptr) *error = transaction_error;
    return ok;
}

bool TradingDomainStoreClientAdapter::AppendTrade(const Trade& trade, std::string* error) {
    if (client_ == nullptr) {
        if (error != nullptr) {
            *error = "null sql client";
        }
        return false;
    }
    if (trade.trade_id.empty() || trade.order_id.empty() || trade.account_id.empty() ||
        trade.strategy_id.empty() || trade.symbol.empty()) {
        if (error != nullptr) {
            *error = "trade requires trade_id/order_id/account_id/strategy_id/symbol";
        }
        return false;
    }
    std::string binding_error;
    const auto bindings = client_->QueryRows(TableName("account_brokers"), "account_id",
                                             trade.account_id, &binding_error);
    const auto runtimes = client_->QueryRows(TableName("runtime_namespace_bindings"), "account_id",
                                             trade.account_id, &binding_error);
    if (!binding_error.empty() || !bindings.empty() || !runtimes.empty()) {
        if (error != nullptr)
            *error = binding_error.empty()
                         ? "atomic account requires durable ApplyTrade; raw AppendTrade is disabled"
                         : binding_error;
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"trade_id", trade.trade_id},
        {"order_id", trade.order_id},
        {"order_ref", trade.order_id},
        {"account_id", trade.account_id},
        {"strategy_id", trade.strategy_id},
        {"instrument_id", trade.symbol},
        {"exchange_id", trade.exchange},
        {"direction", ToDirectionCode(trade.side)},
        {"offset_flag", ToOffsetCode(trade.offset)},
        {"price", ToString(trade.price)},
        {"volume", ToString(trade.quantity)},
        {"trade_time", ToTimestamp(trade.trade_ts_ns)},
        {"ctp_trade_time", ToTimestamp(trade.trade_ts_ns)},
        {"commission", ToString(trade.commission)},
        {"profit", ToString(trade.profit)},
    };
    std::string query_error;
    const auto existing_rows =
        client_->QueryRows(TableName("trades"), "trade_id", trade.trade_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    if (!existing_rows.empty()) {
        return true;
    }
    return InsertWithRetry("trades", row, error);
}

bool TradingDomainStoreClientAdapter::UpsertPosition(const Position& position, std::string* error) {
    if (position.account_id.empty() || position.strategy_id.empty() || position.symbol.empty()) {
        if (error != nullptr) {
            *error = "position requires account_id/strategy_id/symbol";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"account_id", position.account_id},
        {"strategy_id", position.strategy_id},
        {"instrument_id", position.symbol},
        {"exchange_id", position.exchange},
        {"long_volume", ToString(position.long_qty)},
        {"short_volume", ToString(position.short_qty)},
        {"net_volume", ToString(position.long_qty - position.short_qty)},
        {"long_today_volume", ToString(position.long_today_qty)},
        {"short_today_volume", ToString(position.short_today_qty)},
        {"long_yd_volume", ToString(position.long_yd_qty)},
        {"short_yd_volume", ToString(position.short_yd_qty)},
        {"avg_long_price", ToString(position.avg_long_price)},
        {"avg_short_price", ToString(position.avg_short_price)},
        {"position_profit", ToString(position.position_profit)},
        {"margin", ToString(position.margin)},
        {"update_time", ToTimestamp(position.update_time_ns)},
        {"hedge_flag", std::to_string(static_cast<int>(position.hedge_flag))},
        {"trading_day", position.trading_day},
        {"version", std::to_string(position.version)},
    };
    if (client_ == nullptr) return false;
    return client_->UpsertRow(
        TableName("position_summary"), row,
        {"account_id", "strategy_id", "instrument_id", "exchange_id", "hedge_flag"}, {}, error);
}

bool TradingDomainStoreClientAdapter::UpsertAccount(const Account& account, std::string* error) {
    if (account.account_id.empty()) {
        if (error != nullptr) {
            *error = "account requires account_id";
        }
        return false;
    }
    const auto update_date = ToDate(account.update_time_ns);
    std::unordered_map<std::string, std::string> row{
        {"account_id", account.account_id},
        {"trading_day", update_date},
        {"balance", ToString(account.balance)},
        {"available", ToString(account.available)},
        {"curr_margin", ToString(account.margin)},
        {"commission", ToString(account.commission)},
        {"position_profit", ToString(account.position_profit)},
        {"close_profit", ToString(account.close_profit)},
        {"risk_degree", ToString(account.risk_degree)},
        {"update_time", ToTimestamp(account.update_time_ns)},
    };
    return InsertWithRetry("account_funds", row, error);
}

bool TradingDomainStoreClientAdapter::AppendRiskEvent(const RiskEventRecord& risk_event,
                                                      std::string* error) {
    if (risk_event.account_id.empty()) {
        if (error != nullptr) {
            *error = "risk event requires account_id";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"account_id", risk_event.account_id},
        {"strategy_id", risk_event.strategy_id},
        {"rule_id", risk_event.rule_id},
        {"event_type", ToString(risk_event.event_type)},
        {"event_level", ToString(risk_event.event_level)},
        {"severity", ToString(risk_event.event_level)},
        {"instrument_id", risk_event.instrument_id},
        {"order_ref", risk_event.order_ref},
        {"event_desc", risk_event.event_desc},
        {"tags", risk_event.tags_json.empty() ? "{}" : risk_event.tags_json},
        {"details", risk_event.details_json.empty() ? "{}" : risk_event.details_json},
        {"event_time", ToTimestamp(risk_event.event_ts_ns)},
    };
    return InsertWithRetry("risk_events", row, error);
}

bool TradingDomainStoreClientAdapter::MarkProcessedOrderEvent(
    const ProcessedOrderEventRecord& event, std::string* error) {
    if (client_ == nullptr) {
        if (error != nullptr) {
            *error = "null sql client";
        }
        return false;
    }
    if (event.event_key.empty()) {
        if (error != nullptr) {
            *error = "event_key is required";
        }
        return false;
    }

    const std::unordered_map<std::string, std::string> row{
        {"event_key", event.event_key},
        {"order_ref", event.order_ref},
        {"front_id", ToString(event.front_id)},
        {"session_id", ToString(event.session_id)},
        {"event_type", ToString(event.event_type)},
        {"trade_id", event.trade_id},
        {"event_source", event.event_source},
        {"processed_at", ToTimestamp(event.processed_ts_ns)},
    };

    const int attempts = std::max(1, retry_policy_.max_attempts);
    int backoff_ms = std::max(0, retry_policy_.initial_backoff_ms);
    const int max_backoff_ms = std::max(backoff_ms, retry_policy_.max_backoff_ms);
    std::string last_error;
    for (int attempt = 1; attempt <= attempts; ++attempt) {
        std::string local_error;
        if (client_->InsertRow("ops.processed_order_events", row, &local_error)) {
            return true;
        }
        std::string lowered = local_error;
        std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                       [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
        if (lowered.find("duplicate key") != std::string::npos ||
            lowered.find("already exists") != std::string::npos) {
            return true;
        }
        last_error = local_error;
        if (attempt < attempts && backoff_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(max_backoff_ms, backoff_ms * 2);
        }
    }
    if (error != nullptr) {
        *error = last_error.empty() ? "mark processed order event failed" : last_error;
    }
    return false;
}

bool TradingDomainStoreClientAdapter::ExistsProcessedOrderEvent(const std::string& event_key,
                                                                bool* exists,
                                                                std::string* error) const {
    if (exists == nullptr) {
        if (error != nullptr) {
            *error = "exists output pointer is null";
        }
        return false;
    }
    *exists = false;
    if (client_ == nullptr) {
        if (error != nullptr) {
            *error = "null sql client";
        }
        return false;
    }
    if (event_key.empty()) {
        return true;
    }
    std::string query_error;
    const auto rows =
        client_->QueryRows("ops.processed_order_events", "event_key", event_key, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    *exists = !rows.empty();
    return true;
}

bool TradingDomainStoreClientAdapter::InsertPositionDetailFromTrade(const Trade&,
                                                                    std::string* error) {
    if (error != nullptr)
        *error = "standalone position mutations are disabled; use atomic ApplyTrade";
    return false;
}

bool TradingDomainStoreClientAdapter::ClosePositionDetailFifo(const Trade&, std::string* error) {
    if (error != nullptr)
        *error = "standalone position mutations are disabled; use atomic ApplyTrade";
    return false;
}

bool TradingDomainStoreClientAdapter::LoadPositionSummary(const std::string& account_id,
                                                          const std::string& strategy_id,
                                                          std::vector<Position>* out,
                                                          std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "out pointer is null";
        }
        return false;
    }
    out->clear();
    if (client_ == nullptr) {
        if (error != nullptr) {
            *error = "null sql client";
        }
        return false;
    }

    std::string query_error;
    const auto rows =
        client_->QueryRows(TableName("position_summary"), "account_id", account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }

    for (const auto& row : rows) {
        const auto row_strategy = row.find("strategy_id") == row.end() ? "" : row.at("strategy_id");
        if (!strategy_id.empty() && row_strategy != strategy_id) {
            continue;
        }
        Position position;
        position.account_id = account_id;
        position.strategy_id = row_strategy;
        position.symbol = row.find("instrument_id") == row.end() ? "" : row.at("instrument_id");
        position.exchange = row.find("exchange_id") == row.end() ? "" : row.at("exchange_id");
        position.long_qty = ParseIntOrDefault(row, "long_volume", 0);
        position.short_qty = ParseIntOrDefault(row, "short_volume", 0);
        position.long_today_qty = ParseIntOrDefault(row, "long_today_volume", 0);
        position.short_today_qty = ParseIntOrDefault(row, "short_today_volume", 0);
        position.long_yd_qty = ParseIntOrDefault(row, "long_yd_volume", 0);
        position.short_yd_qty = ParseIntOrDefault(row, "short_yd_volume", 0);
        position.avg_long_price = ParseDoubleOrDefault(row, "avg_long_price", 0.0);
        position.avg_short_price = ParseDoubleOrDefault(row, "avg_short_price", 0.0);
        position.position_profit = ParseDoubleOrDefault(row, "position_profit", 0.0);
        position.margin = ParseDoubleOrDefault(row, "margin", 0.0);
        position.hedge_flag = static_cast<HedgeFlag>(ParseIntOrDefault(row, "hedge_flag", 0));
        position.trading_day = row.count("trading_day") ? row.at("trading_day") : "";
        position.version = row.count("version") ? std::stoull(row.at("version")) : 0;
        out->push_back(std::move(position));
    }
    return true;
}

bool TradingDomainStoreClientAdapter::UpdateOrderCancelRetry(const std::string& client_order_id,
                                                             std::int32_t cancel_retry_count,
                                                             EpochNanos last_cancel_ts_ns,
                                                             std::string* error) {
    if (client_order_id.empty()) {
        if (error != nullptr) {
            *error = "client_order_id is empty";
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
    const auto existing_rows =
        client_->QueryRows(TableName("orders"), "order_ref", client_order_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    if (existing_rows.empty()) {
        if (error != nullptr) {
            *error = "order not found";
        }
        return false;
    }
    const auto& existing = existing_rows.front();
    auto row = existing;
    row["cancel_retry_count"] = ToString(cancel_retry_count);
    row["last_cancel_time"] = ToTimestamp(last_cancel_ts_ns);
    row["update_time"] = ToTimestamp(last_cancel_ts_ns);
    if (row.find("insert_time") == row.end() || row["insert_time"].empty()) {
        row["insert_time"] = ToTimestamp(last_cancel_ts_ns);
    }
    std::string upsert_error;
    if (!client_->UpsertRow(TableName("orders"), row, {"order_ref", "insert_time"},
                            {"cancel_retry_count", "last_cancel_time", "update_time"},
                            &upsert_error)) {
        if (error != nullptr) {
            *error = upsert_error;
        }
        return false;
    }
    return true;
}

bool TradingDomainStoreClientAdapter::InsertWithRetry(
    const std::string& table, const std::unordered_map<std::string, std::string>& row,
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
        if (client_->InsertRow(TableName(table), row, &local_error)) {
            return true;
        }
        std::string lowered = local_error;
        std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                       [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
        if (lowered.find("duplicate key") != std::string::npos ||
            lowered.find("already exists") != std::string::npos) {
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

std::string TradingDomainStoreClientAdapter::ToString(std::int32_t value) {
    return std::to_string(value);
}

std::string TradingDomainStoreClientAdapter::ToString(std::int64_t value) {
    return std::to_string(value);
}

std::string TradingDomainStoreClientAdapter::ToString(double value) {
    std::ostringstream text;
    text << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
    return text.str();
}

std::string TradingDomainStoreClientAdapter::ToTimestamp(EpochNanos ts_ns) {
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

std::string TradingDomainStoreClientAdapter::TableName(const std::string& table) const {
    if (schema_.empty()) {
        return table;
    }
    return schema_ + "." + table;
}

namespace {
using DomainRow = std::unordered_map<std::string, std::string>;

std::string Field(const DomainRow& row, const std::string& key) {
    const auto it = row.find(key);
    return it == row.end() ? "" : it->second;
}

std::string IdentityOf(const Trade& trade) {
    return TradeIdentity{2,
                         trade.account_id,
                         trade.trading_day,
                         trade.exchange,
                         trade.raw_trade_id.empty() ? trade.trade_id : trade.raw_trade_id,
                         trade.side,
                         trade.broker_id}
        .Key();
}

std::string Fingerprint(const Trade& trade) {
    // Do not include arrival timestamps, query source, local order refs or cumulative fills.
    std::ostringstream out;
    out << std::setprecision(std::numeric_limits<double>::max_digits10);
    for (const auto& text : {trade.strategy_id, trade.symbol}) out << text.size() << ':' << text;
    if (!trade.component_id.empty())
        out << ":component:" << trade.component_id.size() << ':' << trade.component_id;
    out << ':' << static_cast<int>(trade.side) << ':' << static_cast<int>(trade.offset) << ':'
        << static_cast<int>(trade.hedge_flag) << ':' << trade.price << ':' << trade.quantity;
    return out.str();
}

bool AckReceipt(ITimescaleSqlClient& tx, const std::string& schema, const WalReceipt& receipt,
                std::string* error, const std::string& identity_key = "") {
    const auto table = [&schema](const char* name) {
        return schema.empty() ? std::string(name) : schema + "." + name;
    };
    if (!receipt.durable || receipt.stream_id.empty() ||
        receipt.sequence < receipt.first_sequence) {
        *error = "durable receipt with stream identity required";
        return false;
    }
    if (!tx.LockTransactionKey("domain-wal:" + receipt.stream_id, error)) return false;
    auto checkpoints =
        tx.QueryRows(table("domain_watermarks"), "stream_id", receipt.stream_id, error);
    if (!error->empty()) return false;
    std::uint64_t next = receipt.first_sequence;
    if (!checkpoints.empty()) {
        if (Field(checkpoints.front(), "first_sequence") !=
            std::to_string(receipt.first_sequence)) {
            *error = "WAL stream first sequence conflict";
            return false;
        }
        next = std::stoull(Field(checkpoints.front(), "next_sequence"));
    }
    const auto receipt_key = receipt.stream_id + ":" + std::to_string(receipt.sequence);
    auto prior = tx.QueryRows(table("domain_receipts"), "receipt_key", receipt_key, error);
    if (!error->empty()) return false;
    if (!prior.empty() && (Field(prior.front(), "checksum") != std::to_string(receipt.checksum) ||
                           Field(prior.front(), "identity_key") != identity_key)) {
        *error = "WAL receipt checksum or trade identity conflict";
        return false;
    }
    if (prior.empty() && !tx.InsertRow(table("domain_receipts"),
                                       {{"receipt_key", receipt_key},
                                        {"stream_id", receipt.stream_id},
                                        {"sequence", std::to_string(receipt.sequence)},
                                        {"identity_key", identity_key},
                                        {"checksum", std::to_string(receipt.checksum)}},
                                       error))
        return false;
    const auto receipts =
        tx.QueryRows(table("domain_receipts"), "stream_id", receipt.stream_id, error);
    if (!error->empty()) return false;
    std::unordered_set<std::uint64_t> completed;
    for (const auto& row : receipts) completed.insert(std::stoull(Field(row, "sequence")));
    while (completed.count(next) != 0) {
        if (next == std::numeric_limits<std::uint64_t>::max()) {
            *error = "WAL sequence exhausted";
            return false;
        }
        ++next;
    }
    return tx.UpsertRow(table("domain_watermarks"),
                        {{"stream_id", receipt.stream_id},
                         {"first_sequence", std::to_string(receipt.first_sequence)},
                         {"next_sequence", std::to_string(next)}},
                        {"stream_id"}, {"next_sequence"}, error);
}

int StrategyReservationRemaining(const DomainRow& row) {
    const int bound = Field(row, "terminal") == "true"
                          ? ParseIntOrDefault(row, "reported_filled", 0)
                          : ParseIntOrDefault(row, "quantity", 0);
    return std::max(0, bound - ParseIntOrDefault(row, "booked_qty", 0));
}
Position ReadPhysical(const DomainRow& row) {
    Position p;
    p.account_id = Field(row, "account_id");
    p.strategy_id = "__physical__";
    p.symbol = Field(row, "instrument_id");
    p.exchange = Field(row, "exchange_id");
    p.hedge_flag = static_cast<HedgeFlag>(ParseIntOrDefault(row, "hedge_flag", 0));
    p.trading_day = Field(row, "trading_day");
    p.long_qty = ParseIntOrDefault(row, "long_qty", 0);
    p.short_qty = ParseIntOrDefault(row, "short_qty", 0);
    p.long_today_qty = ParseIntOrDefault(row, "long_today_qty", 0);
    p.short_today_qty = ParseIntOrDefault(row, "short_today_qty", 0);
    p.long_yd_qty = ParseIntOrDefault(row, "long_yd_qty", 0);
    p.short_yd_qty = ParseIntOrDefault(row, "short_yd_qty", 0);
    p.version = Field(row, "version").empty() ? 0 : std::stoull(Field(row, "version"));
    return p;
}
bool SavePhysical(ITimescaleSqlClient& tx, const std::string& table, const Position& p,
                  std::string* error) {
    const DomainRow row{{"account_id", p.account_id},
                        {"instrument_id", p.symbol},
                        {"exchange_id", p.exchange},
                        {"hedge_flag", std::to_string(static_cast<int>(p.hedge_flag))},
                        {"trading_day", p.trading_day},
                        {"version", std::to_string(p.version)},
                        {"long_qty", std::to_string(p.long_qty)},
                        {"short_qty", std::to_string(p.short_qty)},
                        {"long_today_qty", std::to_string(p.long_today_qty)},
                        {"short_today_qty", std::to_string(p.short_today_qty)},
                        {"long_yd_qty", std::to_string(p.long_yd_qty)},
                        {"short_yd_qty", std::to_string(p.short_yd_qty)}};
    return tx.UpsertRow(table, row, {"account_id", "instrument_id", "exchange_id", "hedge_flag"},
                        {}, error);
}
std::string PhysicalKey(const Position& p) {
    return std::to_string(p.exchange.size()) + ":" + p.exchange + ":" +
           std::to_string(p.symbol.size()) + ":" + p.symbol + ":" +
           std::to_string(static_cast<int>(p.hedge_flag));
}
bool SameBook(const DomainRow& lot, const Trade& trade) {
    return Field(lot, "strategy_id") == trade.strategy_id &&
           Field(lot, "instrument_id") == trade.symbol &&
           Field(lot, "exchange_id") == trade.exchange &&
           Field(lot, "hedge_flag") == std::to_string(static_cast<int>(trade.hedge_flag));
}
}  // namespace

bool TradingDomainStoreClientAdapter::BindRuntimeIdentity(const std::string& environment,
                                                          const std::string& broker,
                                                          const std::string& account,
                                                          const std::string& instance,
                                                          std::string* error) {
    std::string local;
    if (error == nullptr) error = &local;
    error->clear();
    if (client_ == nullptr || environment.empty() || broker.empty() || account.empty() ||
        instance.empty()) {
        *error = "complete runtime identity required";
        return false;
    }
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (!tx.LockTransactionKey("domain-account:" + account, e)) return false;
            const auto rows =
                tx.QueryRows(TableName("runtime_namespace_bindings"), "account_id", account, e);
            if (!e->empty()) return false;
            if (!rows.empty()) {
                if (Field(rows.front(), "environment") != environment ||
                    Field(rows.front(), "broker_id") != broker) {
                    *e = "account namespace is already bound to another environment/broker";
                    return false;
                }
                return true;
            }
            return tx.InsertRow(TableName("runtime_namespace_bindings"),
                                {{"account_id", account},
                                 {"environment", environment},
                                 {"broker_id", broker},
                                 {"initial_instance", instance}},
                                e);
        },
        error);
}

bool TradingDomainStoreClientAdapter::AdvanceTradingDay(const std::string& account,
                                                        const std::string& broker,
                                                        const std::string& day,
                                                        std::string* error) {
    std::string local;
    if (error == nullptr) error = &local;
    error->clear();
    if (client_ == nullptr || account.empty() || broker.empty() || day.size() != 8) {
        *error = "account/broker/trading day required";
        return false;
    }
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (!tx.LockTransactionKey("domain-account:" + account, e)) return false;
            auto binding = tx.QueryRows(TableName("account_brokers"), "account_id", account, e);
            if (!e->empty()) return false;
            if (!binding.empty() && Field(binding.front(), "broker_id") != broker) {
                *e = "account broker conflict";
                return false;
            }
            auto alias = std::shared_ptr<ITimescaleSqlClient>(&tx, [](ITimescaleSqlClient*) {});
            StorageRetryPolicy once;
            once.max_attempts = 1;
            TradingDomainStoreClientAdapter store(alias, once, schema_);
            std::vector<Position> positions;
            if (!store.LoadPositionSummary(account, "", &positions, e)) return false;
            if (positions.empty()) return true;
            if (binding.empty()) {
                *e = "unversioned account needs migration";
                return false;
            }
            for (const auto& p : positions) {
                if (p.trading_day.empty() || p.trading_day > day) {
                    *e = "cannot infer or reverse a position trading day";
                    return false;
                }
            }
            std::uint64_t sequence = Field(binding.front(), "commit_sequence").empty()
                                         ? 0
                                         : std::stoull(Field(binding.front(), "commit_sequence"));
            bool changed = false;
            for (auto position : positions) {
                if (position.trading_day == day) continue;
                changed = true;
                position.long_yd_qty += position.long_today_qty;
                position.short_yd_qty += position.short_today_qty;
                position.long_today_qty = position.short_today_qty = 0;
                position.trading_day = day;
                ++position.version;
                ++sequence;
                position.update_time_ns = NowEpochNanos();
                if (!store.UpsertPosition(position, e)) return false;
                const auto key = "rollover:" + account + ":" + day + ":" + std::to_string(sequence);
                const DomainRow row{
                    {"outbox_id", key},
                    {"identity_key", key},
                    {"event_kind", "position_rollover"},
                    {"account_id", account},
                    {"strategy_id", position.strategy_id},
                    {"instrument_id", position.symbol},
                    {"position_version", std::to_string(position.version)},
                    {"commit_sequence", std::to_string(sequence)},
                    {"broker_id", broker},
                    {"trading_day", day},
                    {"raw_trade_id", ""},
                    {"order_ref", ""},
                    {"exchange_order_id", ""},
                    {"exchange_id", position.exchange},
                    {"side", "0"},
                    {"offset_flag", "0"},
                    {"hedge_flag", std::to_string(static_cast<int>(position.hedge_flag))},
                    {"price", "0"},
                    {"quantity", "0"},
                    {"trade_ts_ns", std::to_string(position.update_time_ns)},
                    {"commission", "0"},
                    {"profit", "0"},
                    {"valuation_complete", "false"},
                    {"close_today", "0"},
                    {"close_yesterday", "0"},
                    {"close_rule_source", "trading_day_rollover"},
                    {"close_rule_version", "v1"},
                    {"valuation_source", ""},
                    {"long_qty", std::to_string(position.long_qty)},
                    {"short_qty", std::to_string(position.short_qty)},
                    {"long_today_qty", "0"},
                    {"short_today_qty", "0"},
                    {"long_yd_qty", std::to_string(position.long_yd_qty)},
                    {"short_yd_qty", std::to_string(position.short_yd_qty)},
                    {"avg_long_price", ToString(position.avg_long_price)},
                    {"avg_short_price", ToString(position.avg_short_price)},
                    {"position_profit", ToString(position.position_profit)},
                    {"margin", ToString(position.margin)},
                    {"acknowledged", "false"}};
                if (!tx.InsertRow(TableName("trade_outbox"), row, e)) return false;
            }
            const auto physical_rows =
                tx.QueryRows(TableName("broker_position_summary"), "account_id", account, e);
            if (!e->empty()) return false;
            for (const auto& row : physical_rows) {
                auto p = ReadPhysical(row);
                if (p.trading_day > day) {
                    *e = "cannot reverse physical trading day";
                    return false;
                }
                if (p.trading_day == day) continue;
                p.long_yd_qty += p.long_today_qty;
                p.long_today_qty = 0;
                p.short_yd_qty += p.short_today_qty;
                p.short_today_qty = 0;
                p.trading_day = day;
                ++p.version;
                if (!SavePhysical(tx, TableName("broker_position_summary"), p, e)) return false;
            }
            if (!changed) return true;
            auto lots = tx.QueryRows(TableName("position_detail"), "account_id", account, e);
            if (!e->empty()) return false;
            for (auto lot : lots) {
                if (Field(lot, "lot_id").empty()) {
                    *e = "unversioned position detail";
                    return false;
                }
                if (Field(lot, "open_day") >= day) continue;
                lot.erase("position_id");
                for (auto it = lot.begin(); it != lot.end();) {
                    if (it->second.empty())
                        it = lot.erase(it);
                    else
                        ++it;
                }
                lot["is_today"] = "false";
                lot["position_date"] =
                    day.substr(0, 4) + "-" + day.substr(4, 2) + "-" + day.substr(6, 2);
                if (!tx.UpsertRow(TableName("position_detail"), lot, {"lot_id", "open_date"},
                                  {"is_today", "position_date"}, e))
                    return false;
            }
            return tx.UpsertRow(TableName("account_brokers"),
                                {{"account_id", account},
                                 {"broker_id", broker},
                                 {"commit_sequence", std::to_string(sequence)}},
                                {"account_id"}, {"commit_sequence"}, e);
        },
        error);
}

bool TradingDomainStoreClientAdapter::TransferStrategyCapital(
    const std::string& account, const std::string& transfer_id, const std::string& from,
    const std::string& to, double amount, const std::string& reason, std::string* error) {
    std::string local;
    if (!error) error = &local;
    error->clear();
    if (!client_ || account.empty() || transfer_id.empty() || from.empty() || to.empty() ||
        from == to || !std::isfinite(amount) || amount <= 0 || reason.empty()) {
        *error = "explicit valid capital transfer required";
        return false;
    }
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (!tx.LockTransactionKey("domain-account:" + account, e)) return false;
            const auto prior = tx.QueryRows(TableName("strategy_capital_transfers"), "transfer_id",
                                            transfer_id, e);
            if (!e->empty()) return false;
            if (!prior.empty()) {
                const auto& p = prior.front();
                if (Field(p, "account_id") != account || Field(p, "from_strategy") != from ||
                    Field(p, "to_strategy") != to || Field(p, "reason") != reason ||
                    ParseDoubleOrDefault(p, "amount", 0) != amount) {
                    *e = "capital transfer identity conflict";
                    return false;
                }
                return true;
            }
            const auto rows = tx.QueryRows(TableName("strategy_capital"), "account_id", account, e);
            if (!e->empty()) return false;
            DomainRow debit, credit;
            for (const auto& row : rows) {
                if (Field(row, "strategy_id") == from) debit = row;
                if (Field(row, "strategy_id") == to) credit = row;
            }
            if (debit.empty() || credit.empty()) {
                *e = "capital transfer owner is unallocated";
                return false;
            }
            const auto positions =
                tx.QueryRows(TableName("position_summary"), "account_id", account, e);
            if (!e->empty()) return false;
            for (const auto& p : positions)
                if ((Field(p, "strategy_id") == from || Field(p, "strategy_id") == to) &&
                    (ParseIntOrDefault(p, "long_volume", 0) != 0 ||
                     ParseIntOrDefault(p, "short_volume", 0) != 0)) {
                    *e = "capital transfer requires affected instances naturally flat";
                    return false;
                }
            const auto orders = tx.QueryRows(TableName("orders"), "account_id", account, e);
            if (!e->empty()) return false;
            for (const auto& o : orders)
                if (Field(o, "strategy_id") == from || Field(o, "strategy_id") == to) {
                    const auto status =
                        static_cast<OrderStatus>(ParseIntOrDefault(o, "order_status", 0));
                    if (status != OrderStatus::kCanceled && status != OrderStatus::kRejected &&
                        status != OrderStatus::kFilled) {
                        *e = "capital transfer requires no active orders";
                        return false;
                    }
                }
            for (const auto* reservation_table :
                 {"strategy_close_reservations", "strategy_open_reservations"}) {
                const auto reservations =
                    tx.QueryRows(TableName(reservation_table), "account_id", account, e);
                if (!e->empty()) return false;
                for (const auto& r : reservations)
                    if ((Field(r, "strategy_id") == from || Field(r, "strategy_id") == to) &&
                        StrategyReservationRemaining(r) > 0) {
                        *e = "capital transfer awaits committed fills";
                        return false;
                    }
            }
            const double from_adjustment = ParseDoubleOrDefault(debit, "capital_adjustment", 0);
            const double available = ParseDoubleOrDefault(debit, "initial_capital", 0) +
                                     from_adjustment +
                                     ParseDoubleOrDefault(debit, "realized_pnl", 0) -
                                     ParseDoubleOrDefault(debit, "commission", 0);
            if (amount > available) {
                *e = "capital transfer exceeds source equity";
                return false;
            }
            debit["capital_adjustment"] = ToString(from_adjustment - amount);
            credit["capital_adjustment"] =
                ToString(ParseDoubleOrDefault(credit, "capital_adjustment", 0) + amount);
            if (!tx.UpsertRow(TableName("strategy_capital"), debit, {"account_id", "strategy_id"},
                              {"capital_adjustment"}, e) ||
                !tx.UpsertRow(TableName("strategy_capital"), credit, {"account_id", "strategy_id"},
                              {"capital_adjustment"}, e))
                return false;
            return tx.InsertRow(TableName("strategy_capital_transfers"),
                                {{"transfer_id", transfer_id},
                                 {"account_id", account},
                                 {"from_strategy", from},
                                 {"to_strategy", to},
                                 {"amount", ToString(amount)},
                                 {"reason", reason},
                                 {"recorded_ts_ns", std::to_string(NowEpochNanos())}},
                                e);
        },
        error);
}
bool TradingDomainStoreClientAdapter::AppendCapitalReconciliation(
    const CapitalReconciliationSnapshot& s, std::string* error) {
    std::string local;
    if (!error) error = &local;
    error->clear();
    const double values[] = {s.broker_equity,       s.broker_realized,    s.broker_unrealized,
                             s.broker_commission,   s.strategy_allocated, s.strategy_realized,
                             s.strategy_unrealized, s.strategy_commission};
    if (!client_ || s.account_id.empty() || s.observed_ts_ns <= 0 ||
        !std::all_of(std::begin(values), std::end(values),
                     [](double v) { return std::isfinite(v); })) {
        *error = "complete finite capital reconciliation required";
        return false;
    }
    const double strategy_equity =
        s.strategy_allocated + s.strategy_realized + s.strategy_unrealized - s.strategy_commission;
    const double realized_difference = s.broker_realized - s.strategy_realized;
    const double floating_difference = s.broker_unrealized - s.strategy_unrealized;
    const double fee_difference = s.broker_commission - s.strategy_commission;
    const double cash_settlement_bridge = s.broker_equity - s.broker_realized -
                                          s.broker_unrealized + s.broker_commission -
                                          s.strategy_allocated;
    return InsertWithRetry("strategy_capital_reconciliations",
                           {{"account_id", s.account_id},
                            {"observed_ts_ns", std::to_string(s.observed_ts_ns)},
                            {"trading_day", s.trading_day},
                            {"broker_equity", ToString(s.broker_equity)},
                            {"strategy_equity", ToString(strategy_equity)},
                            {"strategy_allocated", ToString(s.strategy_allocated)},
                            {"strategy_realized", ToString(s.strategy_realized)},
                            {"strategy_unrealized", ToString(s.strategy_unrealized)},
                            {"strategy_commission", ToString(s.strategy_commission)},
                            {"realized_basis_difference", ToString(realized_difference)},
                            {"floating_basis_difference", ToString(floating_difference)},
                            {"fee_basis_difference", ToString(fee_difference)},
                            {"cash_and_settlement_bridge", ToString(cash_settlement_bridge)},
                            {"total_difference", ToString(s.broker_equity - strategy_equity)},
                            {"basis", "broker_reported_vs_strategy_opening_lots_v1"},
                            {"automatic_allocation", "false"}},
                           error);
}

bool TradingDomainStoreClientAdapter::ReserveStrategyOpen(
    const StrategyOpenReservationRequest& request, std::string* error) {
    std::string local;
    if (!error) error = &local;
    error->clear();
    const auto& i = request.intent;
    if (!client_ || i.offset != OffsetFlag::kOpen || i.client_order_id.empty() ||
        i.account_id.empty() || i.strategy_id.empty() || i.volume <= 0 || !std::isfinite(i.price) ||
        i.price <= 0) {
        *error = "valid attributed open intent required";
        return false;
    }
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (!tx.LockTransactionKey("domain-account:" + i.account_id, e)) return false;
            const auto reservations = tx.QueryRows(TableName("strategy_open_reservations"),
                                                   "account_id", i.account_id, e);
            if (!e->empty()) return false;
            double frozen = 0, account_frozen = 0;
            for (const auto& r : reservations) {
                if (Field(r, "order_ref") == i.client_order_id) {
                    if (Field(r, "strategy_id") != i.strategy_id ||
                        Field(r, "instrument_id") != i.instrument_id ||
                        ParseIntOrDefault(r, "quantity", 0) != i.volume ||
                        ParseDoubleOrDefault(r, "price", 0) != i.price ||
                        ParseIntOrDefault(r, "side", -1) != static_cast<int>(i.side) ||
                        ParseIntOrDefault(r, "hedge_flag", 0) != static_cast<int>(i.hedge_flag)) {
                        *e = "strategy open reservation identity conflict";
                        return false;
                    }
                    return true;
                }
                const double funds =
                    StrategyReservationRemaining(r) * ParseDoubleOrDefault(r, "unit_funds", 0);
                account_frozen += funds;
                if (Field(r, "strategy_id") == i.strategy_id) frozen += funds;
            }
            if (request.recovery_existing_only) {
                *e = "recovered open order has no durable strategy reservation";
                return false;
            }
            if (!std::isfinite(request.new_margin_and_fee) || request.new_margin_and_fee <= 0 ||
                !std::isfinite(request.max_margin_to_equity_ratio) ||
                request.max_margin_to_equity_ratio <= 0 || request.max_margin_to_equity_ratio > 1) {
                *e = "invalid strategy fund request";
                return false;
            }
            auto alias = std::shared_ptr<ITimescaleSqlClient>(&tx, [](ITimescaleSqlClient*) {});
            TradingDomainStoreClientAdapter store(alias, {}, schema_);
            StrategyCapitalSnapshot capital;
            std::vector<Position> own;
            if (!store.LoadStrategyCapital(i.account_id, i.strategy_id, &capital, e) ||
                !store.LoadPositionSummary(i.account_id, i.strategy_id, &own, e))
                return false;
            std::unordered_map<std::string, double> marks, multipliers;
            for (const auto& item : request.instruments) {
                marks[item.first] = item.second.mark;
                multipliers[item.first] = item.second.multiplier;
            }
            double equity = 0, unrealized = 0, margin = 0;
            if (!MarkStrategyCapital(capital, own, marks, multipliers, &equity, &unrealized, e) ||
                !StrategyGrossMargin(own, request.instruments, &margin, e))
                return false;
            if (!std::isfinite(frozen) || frozen < 0 || equity <= 0 ||
                margin + frozen + request.new_margin_and_fee >
                    equity * request.max_margin_to_equity_ratio) {
                *e = "strategy margin budget exhausted including pending and unbooked fills";
                return false;
            }
            if (request.account_equity != 0 || request.max_account_margin_to_equity_ratio != 0) {
                if (!std::isfinite(request.account_equity) || request.account_equity <= 0 ||
                    !std::isfinite(request.max_account_margin_to_equity_ratio) ||
                    request.max_account_margin_to_equity_ratio <= 0 ||
                    request.max_account_margin_to_equity_ratio > 1 ||
                    !std::isfinite(account_frozen) || account_frozen < 0) {
                    *e = "valid broker equity and gross account ratio required";
                    return false;
                }
                std::vector<Position> account_positions;
                double gross_account_margin = 0;
                if (!store.LoadPositionSummary(i.account_id, "", &account_positions, e) ||
                    !StrategyGrossMargin(account_positions, request.instruments,
                                         &gross_account_margin, e)) return false;
                if (gross_account_margin + account_frozen + request.new_margin_and_fee >
                    request.account_equity * request.max_account_margin_to_equity_ratio) {
                    *e = "gross account margin budget exhausted including all strategy reservations";
                    return false;
                }
            }
            return tx.InsertRow(TableName("strategy_open_reservations"),
                                {{"account_id", i.account_id},
                                 {"strategy_id", i.strategy_id},
                                 {"order_ref", i.client_order_id},
                                 {"instrument_id", i.instrument_id},
                                 {"side", std::to_string(static_cast<int>(i.side))},
                                 {"hedge_flag", std::to_string(static_cast<int>(i.hedge_flag))},
                                 {"price", ToString(i.price)},
                                 {"quantity", std::to_string(i.volume)},
                                 {"unit_funds", ToString(request.new_margin_and_fee / i.volume)},
                                 {"reported_filled", "0"},
                                 {"booked_qty", "0"},
                                 {"terminal", "false"}},
                                e);
        },
        error);
}

bool TradingDomainStoreClientAdapter::ReserveStrategyClose(const OrderIntent& intent,
                                                           std::string* error) {
    std::string local;
    if (!error) error = &local;
    error->clear();
    if (!client_ || intent.offset == OffsetFlag::kOpen || intent.client_order_id.empty() ||
        intent.account_id.empty() || intent.strategy_id.empty() || intent.volume <= 0) {
        *error = "valid attributed close intent required";
        return false;
    }
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (!tx.LockTransactionKey("domain-account:" + intent.account_id, e)) return false;
            auto alias = std::shared_ptr<ITimescaleSqlClient>(&tx, [](ITimescaleSqlClient*) {});
            TradingDomainStoreClientAdapter store(alias, {}, schema_);
            std::vector<Position> own;
            if (!store.LoadPositionSummary(intent.account_id, intent.strategy_id, &own, e))
                return false;
            int already_booked = 0;
            const auto history =
                tx.QueryRows(TableName("trade_outbox"), "account_id", intent.account_id, e);
            if (!e->empty()) return false;
            for (const auto& row : history)
                if (Field(row, "order_ref") == intent.client_order_id &&
                    Field(row, "event_kind") != "position_rollover") {
                    if (Field(row, "strategy_id") != intent.strategy_id) {
                        *e = "recovered order owner mismatch";
                        return false;
                    }
                    already_booked += ParseIntOrDefault(row, "quantity", 0);
                }
            if (already_booked > intent.volume) {
                *e = "recovered order volume below committed fills";
                return false;
            }
            int quantity = 0;
            for (const auto& p : own)
                if (p.symbol == intent.instrument_id && p.hedge_flag == intent.hedge_flag)
                    quantity += intent.side == Side::kSell ? p.long_qty : p.short_qty;
            const auto reservations = tx.QueryRows(TableName("strategy_close_reservations"),
                                                   "account_id", intent.account_id, e);
            if (!e->empty()) return false;
            for (const auto& row : reservations) {
                if (Field(row, "order_ref") == intent.client_order_id) {
                    if (Field(row, "strategy_id") == intent.strategy_id &&
                        Field(row, "instrument_id") == intent.instrument_id &&
                        ParseIntOrDefault(row, "quantity", 0) == intent.volume &&
                        ParseIntOrDefault(row, "hedge_flag", -1) ==
                            static_cast<int>(intent.hedge_flag) &&
                        ParseIntOrDefault(row, "side", -1) == static_cast<int>(intent.side))
                        return true;
                    *e = "strategy close reservation identity conflict";
                    return false;
                }
                if (Field(row, "strategy_id") == intent.strategy_id &&
                    Field(row, "instrument_id") == intent.instrument_id &&
                    ParseIntOrDefault(row, "side", -1) == static_cast<int>(intent.side) &&
                    ParseIntOrDefault(row, "hedge_flag", -1) == static_cast<int>(intent.hedge_flag))
                    quantity -= StrategyReservationRemaining(row);
            }
            if (intent.volume - already_booked > quantity) {
                *e = "strategy close exceeds unfrozen owned position";
                return false;
            }
            return tx.InsertRow(
                TableName("strategy_close_reservations"),
                {{"account_id", intent.account_id},
                 {"strategy_id", intent.strategy_id},
                 {"order_ref", intent.client_order_id},
                 {"instrument_id", intent.instrument_id},
                 {"side", std::to_string(static_cast<int>(intent.side))},
                 {"hedge_flag", std::to_string(static_cast<int>(intent.hedge_flag))},
                 {"quantity", std::to_string(intent.volume)},
                 {"reported_filled", std::to_string(already_booked)},
                 {"booked_qty", std::to_string(already_booked)},
                 {"terminal", "false"}},
                e);
        },
        error);
}
bool TradingDomainStoreClientAdapter::ObserveStrategyOrderEvent(const OrderEvent& event,
                                                                std::string* error) {
    std::string local;
    if (!error) error = &local;
    error->clear();
    if (!client_ || event.account_id.empty()) {
        *error = "account identity required";
        return false;
    }
    // Rejected cancellation leaves the original order live. Its action status
    // cannot release the original order's cash or inventory reservation.
    if (event.event_source == "OnRspOrderAction" || event.event_source == "OnErrRtnOrderAction")
        return true;
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (!tx.LockTransactionKey("domain-account:" + event.account_id, e)) return false;
            for (const auto* reservation_table :
                 {"strategy_close_reservations", "strategy_open_reservations"}) {
                const auto rows =
                    tx.QueryRows(TableName(reservation_table), "account_id", event.account_id, e);
                if (!e->empty()) return false;
                for (auto row : rows)
                    if (Field(row, "order_ref") == event.client_order_id) {
                        if (!event.strategy_id.empty() &&
                            Field(row, "strategy_id") != event.strategy_id) {
                            *e = "strategy close reservation owner conflict";
                            return false;
                        }
                        const int filled = std::max(ParseIntOrDefault(row, "reported_filled", 0),
                                                    event.filled_volume);
                        if (filled > ParseIntOrDefault(row, "quantity", 0)) {
                            *e = "reservation cumulative overfill";
                            return false;
                        }
                        row["reported_filled"] = std::to_string(filled);
                        if (event.status == OrderStatus::kCanceled ||
                            event.status == OrderStatus::kRejected ||
                            event.status == OrderStatus::kFilled)
                            row["terminal"] = "true";
                        if (!tx.UpsertRow(TableName(reservation_table), row,
                                          {"account_id", "order_ref"},
                                          {"reported_filled", "terminal"}, e))
                            return false;
                    }
            }
            return true;
        },
        error);
}
bool TradingDomainStoreClientAdapter::LoadStrategyCloseReserved(const std::string& account,
                                                                const std::string& strategy,
                                                                const std::string& instrument,
                                                                Side side, std::int32_t* out,
                                                                std::string* error) const {
    std::string local;
    if (!error) error = &local;
    error->clear();
    if (!client_ || !out) {
        *error = "client and output required";
        return false;
    }
    const auto rows =
        client_->QueryRows(TableName("strategy_close_reservations"), "account_id", account, error);
    if (!error->empty()) return false;
    *out = 0;
    for (const auto& row : rows)
        if (Field(row, "strategy_id") == strategy && Field(row, "instrument_id") == instrument &&
            ParseIntOrDefault(row, "side", -1) == static_cast<int>(side))
            *out += StrategyReservationRemaining(row);
    return true;
}

bool TradingDomainStoreClientAdapter::LoadStrategyCloseReserved(
    const std::string& account, const std::string& strategy, const std::string& instrument,
    Side side, HedgeFlag hedge_flag, std::int32_t* out, std::string* error) const {
    std::string local;
    if (!error) error = &local;
    error->clear();
    if (!client_ || !out) {
        *error = "client and output required";
        return false;
    }
    const auto rows =
        client_->QueryRows(TableName("strategy_close_reservations"), "account_id", account, error);
    if (!error->empty()) return false;
    *out = 0;
    for (const auto& row : rows)
        if (Field(row, "strategy_id") == strategy && Field(row, "instrument_id") == instrument &&
            ParseIntOrDefault(row, "side", -1) == static_cast<int>(side) &&
            ParseIntOrDefault(row, "hedge_flag", -1) == static_cast<int>(hedge_flag))
            *out += StrategyReservationRemaining(row);
    return true;
}

bool TradingDomainStoreClientAdapter::ConfigureIndependentStrategyBooks(
    const std::string& account, const std::unordered_map<std::string, double>& allocations,
    std::string* error) {
    std::string local;
    if (!error) error = &local;
    error->clear();
    if (!client_ || account.empty() || allocations.empty()) {
        *error = "account and allocations required";
        return false;
    }
    for (const auto& a : allocations)
        if (a.first.empty() || a.first.rfind("__", 0) == 0 || !std::isfinite(a.second) ||
            a.second <= 0) {
            *error = "nonempty owner and positive finite allocation required";
            return false;
        }
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (!tx.LockTransactionKey("domain-account:" + account, e)) return false;
            const auto current =
                tx.QueryRows(TableName("strategy_capital"), "account_id", account, e);
            if (!e->empty()) return false;
            if (!current.empty()) {
                if (current.size() != allocations.size()) {
                    *e = "capital allocation changes require explicit migration";
                    return false;
                }
                for (const auto& row : current) {
                    const auto a = allocations.find(Field(row, "strategy_id"));
                    if (a == allocations.end() ||
                        a->second != ParseDoubleOrDefault(row, "initial_capital", 0)) {
                        *e = "capital allocation changes require explicit migration";
                        return false;
                    }
                }
                return true;
            }
            auto alias = std::shared_ptr<ITimescaleSqlClient>(&tx, [](ITimescaleSqlClient*) {});
            TradingDomainStoreClientAdapter store(alias, {}, schema_);
            std::vector<Position> positions;
            if (!store.LoadPositionSummary(account, "", &positions, e)) return false;
            std::unordered_map<std::string, Position> physical;
            for (const auto& p : positions) {
                if (!allocations.count(p.strategy_id) || p.version == 0 || p.trading_day.empty()) {
                    *e = "unallocated or unversioned existing position blocks independent books";
                    return false;
                }
                auto& total = physical[PhysicalKey(p)];
                if (!total.trading_day.empty() && total.trading_day != p.trading_day) {
                    *e = "strategy books require aligned trading day before migration";
                    return false;
                }
                total.account_id = account;
                total.strategy_id = "__physical__";
                total.symbol = p.symbol;
                total.exchange = p.exchange;
                total.hedge_flag = p.hedge_flag;
                total.trading_day = p.trading_day;
                total.version += p.version;
                total.long_qty += p.long_qty;
                total.short_qty += p.short_qty;
                total.long_today_qty += p.long_today_qty;
                total.short_today_qty += p.short_today_qty;
                total.long_yd_qty += p.long_yd_qty;
                total.short_yd_qty += p.short_yd_qty;
            }
            std::vector<TradeOutboxRecord> history;
            if (!store.LoadTradeHistory(account, "", &history, e)) return false;
            std::unordered_map<std::string, double> pnl, fees;
            for (const auto& r : history) {
                if (!allocations.count(r.strategy_id) || !r.trade.valuation_complete) {
                    *e = "unallocated or unvalued history requires explicit capital migration";
                    return false;
                }
                pnl[r.strategy_id] += r.trade.profit;
                fees[r.strategy_id] += r.trade.commission;
            }
            for (const auto& a : allocations)
                if (!tx.InsertRow(TableName("strategy_capital"),
                                  {{"account_id", account},
                                   {"strategy_id", a.first},
                                   {"initial_capital", ToString(a.second)},
                                   {"realized_pnl", ToString(pnl[a.first])},
                                   {"commission", ToString(fees[a.first])}},
                                  e))
                    return false;
            for (const auto& p : physical)
                if (!SavePhysical(tx, TableName("broker_position_summary"), p.second, e))
                    return false;
            return true;
        },
        error);
}
bool TradingDomainStoreClientAdapter::LoadBrokerPositionSummary(const std::string& account,
                                                                std::vector<Position>* out,
                                                                std::string* error) const {
    std::string local;
    if (!error) error = &local;
    error->clear();
    if (!client_ || !out) {
        *error = "client and output required";
        return false;
    }
    const auto capital =
        client_->QueryRows(TableName("strategy_capital"), "account_id", account, error);
    if (!error->empty()) return false;
    if (capital.empty()) return LoadPositionSummary(account, "", out, error);
    const auto rows =
        client_->QueryRows(TableName("broker_position_summary"), "account_id", account, error);
    if (!error->empty()) return false;
    out->clear();
    for (const auto& row : rows) out->push_back(ReadPhysical(row));
    return true;
}
bool TradingDomainStoreClientAdapter::LoadStrategyBook(const std::string& account,
                                                       const std::string& strategy,
                                                       StrategyCapitalSnapshot* capital,
                                                       std::vector<Position>* positions,
                                                       std::string* error) const {
    std::string local;
    if (!error) error = &local;
    error->clear();
    if (!client_) {
        *error = "SQL client required";
        return false;
    }
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (!tx.LockTransactionKey("domain-account:" + account, e)) return false;
            auto alias = std::shared_ptr<ITimescaleSqlClient>(&tx, [](ITimescaleSqlClient*) {});
            TradingDomainStoreClientAdapter store(alias, {}, schema_);
            return store.LoadStrategyCapital(account, strategy, capital, e) &&
                   store.LoadPositionSummary(account, strategy, positions, e);
        },
        error);
}

bool TradingDomainStoreClientAdapter::LoadStrategyCapital(const std::string& account,
                                                          const std::string& strategy,
                                                          StrategyCapitalSnapshot* out,
                                                          std::string* error) const {
    std::string local;
    if (!error) error = &local;
    error->clear();
    if (!client_ || !out) {
        *error = "client and output required";
        return false;
    }
    const auto rows =
        client_->QueryRows(TableName("strategy_capital"), "account_id", account, error);
    if (!error->empty()) return false;
    for (const auto& row : rows)
        if (Field(row, "strategy_id") == strategy) {
            *out = {};
            out->account_id = account;
            out->strategy_id = strategy;
            out->initial_capital = ParseDoubleOrDefault(row, "initial_capital", 0);
            out->realized_pnl = ParseDoubleOrDefault(row, "realized_pnl", 0);
            out->commission = ParseDoubleOrDefault(row, "commission", 0);
            out->capital_adjustment = ParseDoubleOrDefault(row, "capital_adjustment", 0);
            const auto reservations = client_->QueryRows(TableName("strategy_open_reservations"),
                                                         "account_id", account, error);
            if (!error->empty()) return false;
            for (const auto& reservation : reservations)
                if (Field(reservation, "strategy_id") == strategy)
                    out->reserved_open_funds += StrategyReservationRemaining(reservation) *
                                                ParseDoubleOrDefault(reservation, "unit_funds", 0);
            if (!std::isfinite(out->reserved_open_funds) || out->reserved_open_funds < 0) {
                *error = "invalid strategy reserved funds";
                return false;
            }

            out->equity_before_marks = out->initial_capital + out->capital_adjustment +
                                       out->realized_pnl - out->commission;
            return true;
        }
    *error = "strategy has no capital allocation";
    return false;
}

bool TradingDomainStoreClientAdapter::ApplyTrade(const TradeApplyRequest& request,
                                                 TradeApplyResult* result, std::string* error) {
    std::string local_error;
    if (error == nullptr) error = &local_error;
    error->clear();
    if (result == nullptr) {
        *error = "null trade apply result";
        return false;
    }
    *result = {};
    Trade trade = request.trade;
    if (request.allow_ephemeral && trade.trading_day.empty()) {
        trade.trading_day = ToDate(trade.trade_ts_ns);
        trade.trading_day.erase(
            std::remove(trade.trading_day.begin(), trade.trading_day.end(), '-'),
            trade.trading_day.end());
    }
    const auto key = IdentityOf(trade);
    result->identity_key = key;
    if (client_ == nullptr || key.empty() || trade.trading_day.size() != 8 ||
        !std::all_of(trade.trading_day.begin(), trade.trading_day.end(),
                     [](unsigned char c) { return std::isdigit(c); }) ||
        trade.strategy_id.empty() || trade.symbol.empty() || trade.quantity <= 0 ||
        !std::isfinite(trade.price) || trade.price <= 0 ||
        (!request.allow_ephemeral && (trade.broker_id.empty() || !request.receipt.durable ||
                                      request.receipt.stream_id.empty()))) {
        result->error = *error = "trade identity, quantity, price and durable WAL receipt required";
        return false;
    }
    const auto fingerprint = Fingerprint(trade);
    TradeApplyResult committed;
    committed.identity_key = key;
    const bool ok = client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* tx_error) {
            if (request.receipt.durable &&
                !tx.LockTransactionKey("domain-wal:" + request.receipt.stream_id, tx_error))
                return false;
            if (!tx.LockTransactionKey("domain-account:" + trade.account_id, tx_error))
                return false;
            const auto ack = [&]() {
                return !request.receipt.durable ||
                       AckReceipt(tx, schema_, request.receipt, tx_error, key);
            };
            const auto conflict = [&](const std::string& reason) {
                committed.status = TradeApplyStatus::kConflict;
                committed.error = reason;
                return tx.InsertRow(TableName("trade_conflicts"),
                                    {{"identity_key", key},
                                     {"account_id", trade.account_id},
                                     {"fingerprint", fingerprint},
                                     {"reason", reason}},
                                    tx_error);
            };
            auto bindings = tx.QueryRows(TableName("account_brokers"), "account_id",
                                         trade.account_id, tx_error);
            if (!tx_error->empty()) return false;
            if (!bindings.empty() && Field(bindings.front(), "broker_id") != trade.broker_id)
                return conflict("domain account already bound to another broker");
            if (bindings.empty()) {
                for (const auto* table : {"trades", "position_summary", "position_detail"}) {
                    const auto legacy =
                        tx.QueryRows(TableName(table), "account_id", trade.account_id, tx_error);
                    if (!tx_error->empty()) return false;
                    if (!legacy.empty())
                        return conflict(
                            "unversioned existing account book requires explicit "
                            "migration/baseline reconciliation");
                }
                if (!tx.InsertRow(
                        TableName("account_brokers"),
                        {{"account_id", trade.account_id}, {"broker_id", trade.broker_id}},
                        tx_error))
                    return false;
            }
            auto prior =
                tx.QueryRows(TableName("trade_applications"), "identity_key", key, tx_error);
            if (!tx_error->empty()) return false;
            if (!prior.empty()) {
                if (Field(prior.front(), "fingerprint") != fingerprint ||
                    (!trade.exchange_order_id.empty() &&
                     !Field(prior.front(), "exchange_order_id").empty() &&
                     Field(prior.front(), "exchange_order_id") != trade.exchange_order_id))
                    return conflict("same trade identity has a conflicting immutable payload");
                committed.status = Field(prior.front(), "disposition") == "covered"
                                       ? TradeApplyStatus::kCoveredByBaseline
                                       : TradeApplyStatus::kDuplicate;
                if (committed.status == TradeApplyStatus::kDuplicate) {
                    const auto projection =
                        tx.QueryRows(TableName("trade_outbox"), "outbox_id", key, tx_error);
                    if (!tx_error->empty()) return false;
                    if (projection.size() != 1) {
                        *tx_error = "applied trade is missing its committed projection";
                        return false;
                    }
                    committed.outbox_id = key;
                    committed.close_allocation = {
                        ParseIntOrDefault(projection.front(), "close_today", 0),
                        ParseIntOrDefault(projection.front(), "close_yesterday", 0)};
                    committed.independent_books =
                        Field(projection.front(), "independent_books") == "true";
                    committed.broker_close_allocation =
                        committed.independent_books
                            ? CloseAllocation{ParseIntOrDefault(projection.front(),
                                                                "broker_close_today", 0),
                                              ParseIntOrDefault(projection.front(),
                                                                "broker_close_yesterday", 0)}
                            : committed.close_allocation;
                    committed.commit_sequence =
                        std::stoull(Field(projection.front(), "commit_sequence"));
                    committed.close_rule_source = Field(projection.front(), "close_rule_source");
                    committed.close_rule_version = Field(projection.front(), "close_rule_version");
                }
                return ack();
            }
            const auto baselines = tx.QueryRows(TableName("position_baselines"), "account_id",
                                                trade.account_id, tx_error);
            if (!tx_error->empty()) return false;
            if (request.historical && !baselines.empty())
                return conflict("historical fill is absent from the baseline coverage manifest");

            auto alias = std::shared_ptr<ITimescaleSqlClient>(&tx, [](ITimescaleSqlClient*) {});
            StorageRetryPolicy once;
            once.max_attempts = 1;
            TradingDomainStoreClientAdapter store(alias, once, schema_);
            const auto capital_rows = tx.QueryRows(TableName("strategy_capital"), "account_id",
                                                   trade.account_id, tx_error);
            if (!tx_error->empty()) return false;
            committed.independent_books = !capital_rows.empty();
            DomainRow owner_capital;
            for (const auto& row : capital_rows)
                if (Field(row, "strategy_id") == trade.strategy_id) owner_capital = row;
            if (committed.independent_books && owner_capital.empty())
                return conflict("unallocated trade owner blocks independent account booking");
            Position physical;
            if (committed.independent_books) {
                std::vector<Position> physical_rows;
                if (!store.LoadBrokerPositionSummary(trade.account_id, &physical_rows, tx_error))
                    return false;
                for (const auto& p : physical_rows)
                    if (p.symbol == trade.symbol && p.exchange == trade.exchange &&
                        p.hedge_flag == trade.hedge_flag)
                        physical = p;
                if (!physical.trading_day.empty() && trade.trading_day < physical.trading_day)
                    return conflict("trade precedes physical account trading day");
                if (physical.trading_day < trade.trading_day) {
                    physical.long_yd_qty += physical.long_today_qty;
                    physical.long_today_qty = 0;
                    physical.short_yd_qty += physical.short_today_qty;
                    physical.short_today_qty = 0;
                    physical.trading_day = trade.trading_day;
                }
            }
            std::vector<Position> summaries;
            if (!store.LoadPositionSummary(trade.account_id, trade.strategy_id, &summaries,
                                           tx_error))
                return false;
            Position position;
            position.account_id = trade.account_id;
            position.strategy_id = trade.strategy_id;
            position.symbol = trade.symbol;
            position.exchange = trade.exchange;
            position.hedge_flag = trade.hedge_flag;
            for (const auto& existing : summaries)
                if (existing.symbol == trade.symbol && existing.exchange == trade.exchange &&
                    existing.hedge_flag == trade.hedge_flag)
                    position = existing;
            if (!position.trading_day.empty() && trade.trading_day < position.trading_day)
                return conflict(
                    "trade precedes the current position trading day; reconciliation required");
            if (!position.trading_day.empty() && position.trading_day < trade.trading_day) {
                position.long_yd_qty += position.long_today_qty;
                position.short_yd_qty += position.short_today_qty;
                position.long_today_qty = position.short_today_qty = 0;
            }
            position.trading_day = trade.trading_day;
            const auto& policy = request.accounting_policy;
            const auto valid_rate = [](const TradeFeeRate& rate) {
                return std::isfinite(rate.by_money) && rate.by_money >= 0 &&
                       std::isfinite(rate.by_volume) && rate.by_volume >= 0;
            };
            const bool fee_verified =
                policy.fee_model == TradeFeeModel::kExplicitCommission
                    ? std::isfinite(policy.commission) && policy.commission >= 0
                    : (policy.fee_model == TradeFeeModel::kMoneyPlusVolumeV1 &&
                       valid_rate(policy.open_fee) && valid_rate(policy.close_fee) &&
                       valid_rate(policy.close_today_fee) &&
                       policy.fee_date_basis == "close_allocation_v1" &&
                       !policy.fee_allocation_source.empty() &&
                       !policy.fee_allocation_version.empty());
            const bool valuation_verified =
                policy.valuation_inputs_verified && std::isfinite(policy.contract_multiplier) &&
                policy.contract_multiplier > 0 && fee_verified && !policy.valuation_source.empty();
            if ((request.require_verified_accounting || committed.independent_books) &&
                !valuation_verified) {
                *tx_error =
                    "new trade requires verified accounting policy evidence; WAL remains pending";
                return false;
            }
            trade.valuation_complete = false;
            if (valuation_verified) {
                trade.commission = policy.commission;
                trade.profit = 0;
                trade.valuation_complete = true;
            }
            const auto lot_row = [&](const std::string& id, int quantity) {
                const auto date = trade.trading_day.substr(0, 4) + "-" +
                                  trade.trading_day.substr(4, 2) + "-" +
                                  trade.trading_day.substr(6, 2);
                return DomainRow{
                    {"lot_id", id},
                    {"account_id", trade.account_id},
                    {"strategy_id", trade.strategy_id},
                    {"instrument_id", trade.symbol},
                    {"exchange_id", trade.exchange},
                    {"hedge_flag", std::to_string(static_cast<int>(trade.hedge_flag))},
                    {"side", std::to_string(static_cast<int>(trade.side))},
                    {"open_day", trade.trading_day},
                    {"remaining_qty", std::to_string(quantity)},
                    {"open_price", ToString(trade.price)},
                    {"open_date", date},
                    {"position_date", date},
                    {"is_today", "true"},
                    {"volume", std::to_string(trade.side == Side::kBuy ? quantity : -quantity)},
                    {"close_volume", "0"},
                    {"position_status", "1"},
                    {"open_trade_id", id},
                    {"open_order_ref", trade.order_id},
                    {"update_time", ToTimestamp(trade.trade_ts_ns)}};
            };
            if (trade.offset == OffsetFlag::kOpen) {
                auto& qty = trade.side == Side::kBuy ? position.long_qty : position.short_qty;
                auto& today =
                    trade.side == Side::kBuy ? position.long_today_qty : position.short_today_qty;
                auto& average =
                    trade.side == Side::kBuy ? position.avg_long_price : position.avg_short_price;
                if (qty > std::numeric_limits<std::int32_t>::max() - trade.quantity) {
                    *tx_error = "position quantity overflow";
                    return false;
                }
                average = (average * qty + trade.price * trade.quantity) / (qty + trade.quantity);
                qty += trade.quantity;
                today += trade.quantity;
                if (!tx.InsertRow(TableName("position_detail"), lot_row(key, trade.quantity),
                                  tx_error))
                    return false;
            } else {
                auto& qty = trade.side == Side::kSell ? position.long_qty : position.short_qty;
                auto& today =
                    trade.side == Side::kSell ? position.long_today_qty : position.short_today_qty;
                auto& yesterday =
                    trade.side == Side::kSell ? position.long_yd_qty : position.short_yd_qty;
                int td = 0;
                int yd = 0;
                if (committed.independent_books) {
                    CloseAllocation economic;
                    if (!PlanIndependentClose(position, physical, trade,
                                              policy.generic_close_priority, &economic,
                                              &committed.broker_close_allocation, tx_error))
                        return false;
                    td = economic.today;
                    yd = economic.yesterday;
                    if (trade.offset == OffsetFlag::kClose && trade.exchange != "SHFE" &&
                        trade.exchange != "INE") {
                        if (policy.close_rule_source.empty() || policy.close_rule_version.empty())
                            return conflict(
                                "physical generic close requires verified policy evidence");
                        committed.close_rule_source = policy.close_rule_source;
                        committed.close_rule_version = policy.close_rule_version;
                    }
                } else if (trade.offset == OffsetFlag::kCloseToday)
                    td = trade.quantity;
                else if (trade.offset == OffsetFlag::kCloseYesterday)
                    yd = trade.quantity;
                else if (trade.exchange == "SHFE" || trade.exchange == "INE")
                    yd = trade.quantity;
                else {
                    if (!request.allow_ephemeral &&
                        (policy.generic_close_priority == GenericClosePriority::kUnspecified ||
                         policy.close_rule_source.empty() || policy.close_rule_version.empty()))
                        return conflict(
                            "generic close needs a verified exchange/broker allocation policy");
                    if (policy.generic_close_priority == GenericClosePriority::kYesterdayFirst) {
                        yd = std::min(yesterday, trade.quantity);
                        td = trade.quantity - yd;
                    } else {
                        td = std::min(today, trade.quantity);
                        yd = trade.quantity - td;
                    }
                    committed.close_rule_source =
                        request.allow_ephemeral && policy.close_rule_source.empty()
                            ? "research_legacy_today_first"
                            : policy.close_rule_source;
                    committed.close_rule_version = policy.close_rule_version;
                }
                if (committed.close_rule_source.empty()) {
                    committed.close_rule_source = "explicit_offset";
                    committed.close_rule_version = "ctp-offset-v1";
                }
                if (td > today || yd > yesterday || td + yd != trade.quantity ||
                    trade.quantity > qty)
                    return conflict(
                        "close allocation exceeds the specified today/yesterday position");
                auto lots = tx.QueryRows(TableName("position_detail"), "account_id",
                                         trade.account_id, tx_error);
                if (!tx_error->empty()) return false;
                std::sort(lots.begin(), lots.end(), [](const DomainRow& a, const DomainRow& b) {
                    if (Field(a, "open_day") != Field(b, "open_day"))
                        return Field(a, "open_day") < Field(b, "open_day");
                    return Field(a, "lot_id") < Field(b, "lot_id");
                });
                int remaining_td = td, remaining_yd = yd;
                std::vector<DomainRow> changes;
                for (auto lot : lots) {
                    if (!SameBook(lot, trade) ||
                        Field(lot, "side") == std::to_string(static_cast<int>(trade.side)))
                        continue;
                    const bool is_today = Field(lot, "open_day") == trade.trading_day;
                    if (!is_today && Field(lot, "open_day") >= trade.trading_day) continue;
                    auto& remaining = is_today ? remaining_td : remaining_yd;
                    const int available = ParseIntOrDefault(lot, "remaining_qty", 0);
                    const int take = std::min(available, remaining);
                    if (take <= 0) continue;
                    if (valuation_verified) {
                        const double open_price = ParseDoubleOrDefault(lot, "open_price", 0);
                        if (!std::isfinite(open_price) || open_price <= 0) {
                            trade.valuation_complete = false;
                        } else {
                            trade.profit += (trade.side == Side::kSell ? trade.price - open_price
                                                                       : open_price - trade.price) *
                                            take * policy.contract_multiplier;
                        }
                    }
                    lot["remaining_qty"] = std::to_string(available - take);
                    lot["close_volume"] = std::to_string(
                        std::abs(ParseIntOrDefault(lot, "volume", 0)) - available + take);
                    lot["position_status"] = available == take ? "2" : "1";
                    lot.erase("position_id");
                    // QueryRows represents SQL NULL as an empty string. Omit nullable
                    // read-only columns when constructing INSERT ... ON CONFLICT.
                    for (auto it = lot.begin(); it != lot.end();) {
                        if (it->second.empty())
                            it = lot.erase(it);
                        else
                            ++it;
                    }
                    changes.push_back(std::move(lot));
                    remaining -= take;
                }
                if (remaining_td != 0 || remaining_yd != 0)
                    return conflict(
                        "position detail cannot satisfy close allocation; baseline/reconciliation "
                        "required");
                for (const auto& lot : changes)
                    if (!tx.UpsertRow(TableName("position_detail"), lot, {"lot_id", "open_date"},
                                      {"remaining_qty", "close_volume", "position_status"},
                                      tx_error))
                        return false;
                qty -= trade.quantity;
                today -= td;
                yesterday -= yd;
                if (qty == 0)
                    (trade.side == Side::kSell ? position.avg_long_price
                                               : position.avg_short_price) = 0;
                else if (committed.independent_books) {
                    const auto remaining_lots = tx.QueryRows(
                        TableName("position_detail"), "account_id", trade.account_id, tx_error);
                    if (!tx_error->empty()) return false;
                    double cost = 0;
                    int count = 0;
                    for (const auto& lot : remaining_lots) {
                        if (!SameBook(lot, trade) ||
                            Field(lot, "side") == std::to_string(static_cast<int>(trade.side)))
                            continue;
                        const int n = ParseIntOrDefault(lot, "remaining_qty", 0);
                        cost += n * ParseDoubleOrDefault(lot, "open_price", 0);
                        count += n;
                    }
                    if (count != qty) return conflict("economic FIFO residual quantity mismatch");
                    (trade.side == Side::kSell ? position.avg_long_price
                                               : position.avg_short_price) = cost / count;
                }
                committed.close_allocation = {td, yd};
            }
            if (!committed.independent_books)
                committed.broker_close_allocation = committed.close_allocation;
            if (valuation_verified && policy.fee_model == TradeFeeModel::kMoneyPlusVolumeV1) {
                const auto cost = [&](const TradeFeeRate& rate, int quantity) {
                    return static_cast<long double>(trade.price) * policy.contract_multiplier *
                               quantity * rate.by_money +
                           static_cast<long double>(quantity) * rate.by_volume;
                };
                const long double commission =
                    trade.offset == OffsetFlag::kOpen
                        ? cost(policy.open_fee, trade.quantity)
                        : cost(policy.close_today_fee, committed.broker_close_allocation.today) +
                              cost(policy.close_fee, committed.broker_close_allocation.yesterday);
                trade.commission = static_cast<double>(commission);
                if (!std::isfinite(trade.commission) || trade.commission < 0)
                    trade.valuation_complete = false;
            }
            if (!std::isfinite(trade.profit)) trade.valuation_complete = false;
            if ((request.require_verified_accounting || committed.independent_books) &&
                !trade.valuation_complete) {
                *tx_error = "verified trade valuation is incomplete; WAL remains pending";
                return false;
            }
            if (committed.independent_books) {
                if (!ApplyPhysicalFill(&physical, trade, committed.broker_close_allocation,
                                       tx_error) ||
                    !SavePhysical(tx, TableName("broker_position_summary"), physical, tx_error))
                    return false;
                {
                    const auto reservation_table = trade.offset == OffsetFlag::kOpen
                                                       ? "strategy_open_reservations"
                                                       : "strategy_close_reservations";
                    const auto reservations = tx.QueryRows(
                        TableName(reservation_table), "account_id", trade.account_id, tx_error);
                    if (!tx_error->empty()) return false;
                    for (auto row : reservations)
                        if (Field(row, "order_ref") == trade.order_id) {
                            if (Field(row, "strategy_id") != trade.strategy_id ||
                                Field(row, "instrument_id") != trade.symbol ||
                                ParseIntOrDefault(row, "side", -1) != static_cast<int>(trade.side) ||
                                ParseIntOrDefault(row, "hedge_flag", 0) != static_cast<int>(trade.hedge_flag)) {
                                *tx_error = "fill reservation owner, instrument, side or hedge mismatch";
                                return false;
                            }
                            const int booked =
                                ParseIntOrDefault(row, "booked_qty", 0) + trade.quantity;
                            if (booked > ParseIntOrDefault(row, "quantity", 0)) {
                                *tx_error = "strategy reservation overfill";
                                return false;
                            }
                            row["booked_qty"] = std::to_string(booked);
                            if (!tx.UpsertRow(TableName(reservation_table), row,
                                              {"account_id", "order_ref"}, {"booked_qty"},
                                              tx_error))
                                return false;
                        }
                }
                const double realized =
                    ParseDoubleOrDefault(owner_capital, "realized_pnl", 0) + trade.profit;
                const double fees =
                    ParseDoubleOrDefault(owner_capital, "commission", 0) + trade.commission;
                if (!std::isfinite(realized) || !std::isfinite(fees)) {
                    *tx_error = "nonfinite strategy capital";
                    return false;
                }
                owner_capital["realized_pnl"] = ToString(realized);
                owner_capital["commission"] = ToString(fees);
                if (!tx.UpsertRow(TableName("strategy_capital"), owner_capital,
                                  {"account_id", "strategy_id"}, {"realized_pnl", "commission"},
                                  tx_error))
                    return false;
            }
            ++position.version;
            position.update_time_ns = trade.trade_ts_ns;
            if (!store.UpsertPosition(position, tx_error)) return false;
            const std::uint64_t commit_sequence =
                bindings.empty()
                    ? 1
                    : (Field(bindings.front(), "commit_sequence").empty()
                           ? 1
                           : std::stoull(Field(bindings.front(), "commit_sequence")) + 1);
            if (!tx.UpsertRow(TableName("account_brokers"),
                              {{"account_id", trade.account_id},
                               {"broker_id", trade.broker_id},
                               {"commit_sequence", std::to_string(commit_sequence)}},
                              {"account_id"}, {"commit_sequence"}, tx_error))
                return false;
            // Numeric SQL order_id is optional: local refs are not database surrogate IDs.
            const DomainRow fill{{"trade_id", key},
                                 {"order_ref", trade.order_id},
                                 {"account_id", trade.account_id},
                                 {"strategy_id", trade.strategy_id},
                                 {"component_id", trade.component_id},
                                 {"instrument_id", trade.symbol},
                                 {"exchange_id", trade.exchange},
                                 {"direction", ToDirectionCode(trade.side)},
                                 {"offset_flag", ToOffsetCode(trade.offset)},
                                 {"price", ToString(trade.price)},
                                 {"volume", ToString(trade.quantity)},
                                 {"trade_time", ToTimestamp(trade.trade_ts_ns)},
                                 {"commission", ToString(trade.commission)},
                                 {"profit", ToString(trade.profit)}};
            if (!tx.InsertRow(TableName("trades"), fill, tx_error)) return false;
            if (!tx.InsertRow(TableName("trade_applications"),
                              {{"identity_key", key},
                               {"account_id", trade.account_id},
                               {"fingerprint", fingerprint},
                               {"exchange_order_id", trade.exchange_order_id},
                               {"disposition", "applied"}},
                              tx_error))
                return false;
            if (!tx.InsertRow(
                    TableName("trade_outbox"),
                    {{"outbox_id", key},
                     {"identity_key", key},
                     {"account_id", trade.account_id},
                     {"strategy_id", trade.strategy_id},
                     {"component_id", trade.component_id},
                     {"commit_sequence", std::to_string(commit_sequence)},
                     {"instrument_id", trade.symbol},
                     {"position_version", std::to_string(position.version)},
                     {"broker_id", trade.broker_id},
                     {"trading_day", trade.trading_day},
                     {"raw_trade_id",
                      trade.raw_trade_id.empty() ? trade.trade_id : trade.raw_trade_id},
                     {"order_ref", trade.order_id},
                     {"exchange_order_id", trade.exchange_order_id},
                     {"exchange_id", trade.exchange},
                     {"side", std::to_string(static_cast<int>(trade.side))},
                     {"offset_flag", std::to_string(static_cast<int>(trade.offset))},
                     {"hedge_flag", std::to_string(static_cast<int>(trade.hedge_flag))},
                     {"price", ToString(trade.price)},
                     {"quantity", std::to_string(trade.quantity)},
                     {"trade_ts_ns", std::to_string(trade.trade_ts_ns)},
                     {"commission", ToString(trade.commission)},
                     {"profit", ToString(trade.profit)},
                     {"valuation_complete", trade.valuation_complete ? "true" : "false"},
                     {"independent_books", committed.independent_books ? "true" : "false"},
                     {"broker_close_today",
                      std::to_string(committed.broker_close_allocation.today)},
                     {"broker_close_yesterday",
                      std::to_string(committed.broker_close_allocation.yesterday)},
                     {"close_today", std::to_string(committed.close_allocation.today)},
                     {"close_yesterday", std::to_string(committed.close_allocation.yesterday)},
                     {"close_rule_source", committed.close_rule_source},
                     {"close_rule_version", committed.close_rule_version},
                     {"valuation_source", valuation_verified ? policy.valuation_source : ""},
                     {"fee_model", valuation_verified
                                       ? (policy.fee_model == TradeFeeModel::kMoneyPlusVolumeV1
                                              ? "money_plus_volume_v1"
                                              : "explicit_commission")
                                       : ""},
                     {"fee_date_basis", valuation_verified ? policy.fee_date_basis : ""},
                     {"fee_allocation_source",
                      valuation_verified ? policy.fee_allocation_source : ""},
                     {"fee_allocation_version",
                      valuation_verified ? policy.fee_allocation_version : ""},
                     {"long_qty", std::to_string(position.long_qty)},
                     {"short_qty", std::to_string(position.short_qty)},
                     {"long_today_qty", std::to_string(position.long_today_qty)},
                     {"short_today_qty", std::to_string(position.short_today_qty)},
                     {"long_yd_qty", std::to_string(position.long_yd_qty)},
                     {"short_yd_qty", std::to_string(position.short_yd_qty)},
                     {"avg_long_price", ToString(position.avg_long_price)},
                     {"avg_short_price", ToString(position.avg_short_price)},
                     {"position_profit", ToString(position.position_profit)},
                     {"margin", ToString(position.margin)},
                     {"acknowledged", "false"}},
                    tx_error))
                return false;
            if (!ack()) return false;
            committed.status = TradeApplyStatus::kApplied;
            committed.position = position;
            committed.outbox_id = key;
            committed.commit_sequence = commit_sequence;
            return true;
        },
        error);
    if (!ok) {
        result->error = *error;
        return false;
    }
    *result = std::move(committed);
    if (result->status == TradeApplyStatus::kConflict) *error = result->error;
    return true;
}

bool TradingDomainStoreClientAdapter::InstallPositionBaseline(const PositionBaseline& baseline,
                                                              std::string* error) {
    std::string local;
    if (error == nullptr) error = &local;
    error->clear();
    if (client_ == nullptr || !baseline.complete || baseline.baseline_id.empty() ||
        baseline.account_id.empty() || baseline.broker_id.empty() ||
        baseline.trading_day.size() != 8) {
        *error = "complete baseline with account/broker/day identity required";
        return false;
    }
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (!tx.LockTransactionKey("domain-account:" + baseline.account_id, e)) return false;
            // A snapshot cannot overwrite an existing booked ledger. Reconcile it instead.
            for (const auto* table : {"position_baselines", "trade_applications",
                                      "position_summary", "position_detail", "strategy_capital"}) {
                auto rows = tx.QueryRows(TableName(table), "account_id", baseline.account_id, e);
                if (!e->empty()) return false;
                if (!rows.empty()) {
                    *e = "baseline requires a new empty account book";
                    return false;
                }
            }
            const auto bindings =
                tx.QueryRows(TableName("account_brokers"), "account_id", baseline.account_id, e);
            if (!e->empty()) return false;
            if (!bindings.empty() && Field(bindings.front(), "broker_id") != baseline.broker_id) {
                *e = "baseline account broker binding conflict";
                return false;
            }
            if (bindings.empty() &&
                !tx.InsertRow(
                    TableName("account_brokers"),
                    {{"account_id", baseline.account_id}, {"broker_id", baseline.broker_id}}, e))
                return false;
            auto alias = std::shared_ptr<ITimescaleSqlClient>(&tx, [](ITimescaleSqlClient*) {});
            StorageRetryPolicy once;
            once.max_attempts = 1;
            TradingDomainStoreClientAdapter store(alias, once, schema_);
            std::unordered_set<std::string> books;
            for (auto position : baseline.positions) {
                if (position.account_id != baseline.account_id || position.strategy_id.empty() ||
                    position.symbol.empty() || position.exchange.empty() ||
                    position.long_today_qty < 0 || position.long_yd_qty < 0 ||
                    position.short_today_qty < 0 || position.short_yd_qty < 0 ||
                    position.long_qty != position.long_today_qty + position.long_yd_qty ||
                    position.short_qty != position.short_today_qty + position.short_yd_qty) {
                    *e = "invalid baseline position quantities or identity";
                    return false;
                }
                const auto book = position.strategy_id + ":" + position.exchange + ":" +
                                  position.symbol + ":" +
                                  std::to_string(static_cast<int>(position.hedge_flag));
                if (!books.insert(book).second) {
                    *e = "duplicate baseline position book";
                    return false;
                }
                position.trading_day = baseline.trading_day;
                position.version = 1;
                if (!store.UpsertPosition(position, e)) return false;
                // Baseline lots retain explicit today/yesterday buckets. Their synthetic identity
                // is never treated as a historical fill identity.
                for (int side = 0; side != 2; ++side) {
                    for (int bucket = 0; bucket != 2; ++bucket) {
                        const int quantity =
                            side == 0
                                ? (bucket == 0 ? position.long_today_qty : position.long_yd_qty)
                                : (bucket == 0 ? position.short_today_qty : position.short_yd_qty);
                        if (quantity == 0) continue;
                        const auto id = "baseline:" + baseline.baseline_id + ":" + book + ":" +
                                        std::to_string(side) + ":" + std::to_string(bucket);
                        // Yesterday is a category, not a guessed exchange calendar date.
                        const auto open_day = bucket == 0 ? baseline.trading_day : "00000000";
                        const auto date = baseline.trading_day.substr(0, 4) + "-" +
                                          baseline.trading_day.substr(4, 2) + "-" +
                                          baseline.trading_day.substr(6, 2);
                        const DomainRow lot{
                            {"lot_id", id},
                            {"account_id", position.account_id},
                            {"strategy_id", position.strategy_id},
                            {"instrument_id", position.symbol},
                            {"exchange_id", position.exchange},
                            {"hedge_flag", std::to_string(static_cast<int>(position.hedge_flag))},
                            {"side", std::to_string(side)},
                            {"open_day", open_day},
                            {"remaining_qty", std::to_string(quantity)},
                            {"open_price", ToString(side == 0 ? position.avg_long_price
                                                              : position.avg_short_price)},
                            {"open_date", date},
                            {"position_date", date},
                            {"is_today", bucket == 0 ? "true" : "false"},
                            {"volume", std::to_string(side == 0 ? quantity : -quantity)},
                            {"close_volume", "0"},
                            {"position_status", "1"},
                            {"open_trade_id", id}};
                        if (!tx.InsertRow(TableName("position_detail"), lot, e)) return false;
                    }
                }
            }
            std::unordered_set<std::string> covered;
            for (const auto& trade : baseline.covered_trades) {
                const auto id = IdentityOf(trade);
                if (id.empty() || trade.account_id != baseline.account_id ||
                    trade.broker_id != baseline.broker_id || trade.quantity <= 0 ||
                    !std::isfinite(trade.price) || !covered.insert(id).second) {
                    *e = "invalid or duplicate covered trade identity";
                    return false;
                }
                if (!tx.InsertRow(TableName("trade_applications"),
                                  {{"identity_key", id},
                                   {"account_id", trade.account_id},
                                   {"fingerprint", Fingerprint(trade)},
                                   {"exchange_order_id", trade.exchange_order_id},
                                   {"disposition", "covered"}},
                                  e))
                    return false;
            }
            return tx.InsertRow(TableName("position_baselines"),
                                {{"baseline_id", baseline.baseline_id},
                                 {"account_id", baseline.account_id},
                                 {"trading_day", baseline.trading_day},
                                 {"broker_id", baseline.broker_id}},
                                e);
        },
        error);
}

bool TradingDomainStoreClientAdapter::AcknowledgeReceipt(const WalReceipt& receipt,
                                                         std::string* error) {
    std::string local;
    if (error == nullptr) error = &local;
    error->clear();
    if (client_ == nullptr) {
        *error = "null SQL client";
        return false;
    }
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            return AckReceipt(tx, schema_, receipt, e);
        },
        error);
}

bool TradingDomainStoreClientAdapter::LoadWatermark(const std::string& stream, DomainWatermark* out,
                                                    std::string* error) const {
    if (client_ == nullptr || out == nullptr) return false;
    std::string local;
    auto rows = client_->QueryRows(TableName("domain_watermarks"), "stream_id", stream, &local);
    if (!local.empty()) {
        if (error != nullptr) *error = local;
        return false;
    }
    *out = {stream, 0, false};
    if (!rows.empty()) {
        out->initialized = true;
        out->next_sequence = std::stoull(Field(rows.front(), "next_sequence"));
    }
    return true;
}

bool TradingDomainStoreClientAdapter::LoadPendingOutbox(const std::string& account,
                                                        std::vector<TradeOutboxRecord>* out,
                                                        std::string* error) const {
    return LoadPendingOutboxForConsumer("position", account, out, error);
}

bool TradingDomainStoreClientAdapter::LoadTradeHistory(const std::string& account,
                                                       const std::string& trading_day,
                                                       std::vector<TradeOutboxRecord>* out,
                                                       std::string* error) const {
    if (!LoadPendingOutboxForConsumer("", account, out, error)) return false;
    out->erase(
        std::remove_if(out->begin(), out->end(),
                       [&](const TradeOutboxRecord& r) {
                           return r.event_kind != "trade" ||
                                  (!trading_day.empty() && r.trade.trading_day != trading_day);
                       }),
        out->end());
    return true;
}

bool TradingDomainStoreClientAdapter::LoadPendingOutboxForConsumer(
    const std::string& consumer, const std::string& account, std::vector<TradeOutboxRecord>* out,
    std::string* error) const {
    if (client_ == nullptr || out == nullptr) return false;
    std::string local;
    const auto rows = account.empty() ? client_->QueryAllRows(TableName("trade_outbox"), &local)
                                      : client_->QueryRows(TableName("trade_outbox"), "account_id",
                                                           account, &local);
    if (!local.empty()) {
        if (error != nullptr) *error = local;
        return false;
    }
    const auto acks =
        client_->QueryRows(TableName("trade_outbox_acks"), "consumer", consumer, &local);
    if (!local.empty()) {
        if (error != nullptr) *error = local;
        return false;
    }
    std::unordered_set<std::string> acknowledged;
    if (!consumer.empty())
        for (const auto& row : acks) acknowledged.insert(Field(row, "outbox_id"));
    out->clear();
    for (const auto& row : rows) {
        if (acknowledged.count(Field(row, "outbox_id"))) continue;
        TradeOutboxRecord record;
        record.event_kind = Field(row, "event_kind").empty() ? "trade" : Field(row, "event_kind");
        record.outbox_id = Field(row, "outbox_id");
        record.identity_key = Field(row, "identity_key");
        record.account_id = Field(row, "account_id");
        record.strategy_id = Field(row, "strategy_id");
        record.instrument_id = Field(row, "instrument_id");
        record.position_version = std::stoull(Field(row, "position_version"));
        record.commit_sequence = std::stoull(Field(row, "commit_sequence"));
        record.close_allocation = {ParseIntOrDefault(row, "close_today", 0),
                                   ParseIntOrDefault(row, "close_yesterday", 0)};
        record.independent_books = Field(row, "independent_books") == "true";
        record.broker_close_allocation =
            record.independent_books
                ? CloseAllocation{ParseIntOrDefault(row, "broker_close_today", 0),
                                  ParseIntOrDefault(row, "broker_close_yesterday", 0)}
                : record.close_allocation;
        record.close_rule_source = Field(row, "close_rule_source");
        record.close_rule_version = Field(row, "close_rule_version");
        record.valuation_source = Field(row, "valuation_source");
        record.fee_model = Field(row, "fee_model");
        record.fee_date_basis = Field(row, "fee_date_basis");
        record.fee_allocation_source = Field(row, "fee_allocation_source");
        record.fee_allocation_version = Field(row, "fee_allocation_version");
        auto& trade = record.trade;
        trade.trade_id = record.identity_key;
        trade.account_id = record.account_id;
        trade.strategy_id = record.strategy_id;
        trade.component_id = Field(row, "component_id");
        trade.symbol = record.instrument_id;
        trade.broker_id = Field(row, "broker_id");
        trade.trading_day = Field(row, "trading_day");
        trade.raw_trade_id = Field(row, "raw_trade_id");
        trade.order_id = Field(row, "order_ref");
        trade.exchange_order_id = Field(row, "exchange_order_id");
        trade.exchange = Field(row, "exchange_id");
        trade.side = static_cast<Side>(ParseIntOrDefault(row, "side", 0));
        trade.offset = static_cast<OffsetFlag>(ParseIntOrDefault(row, "offset_flag", 0));
        trade.hedge_flag = static_cast<HedgeFlag>(ParseIntOrDefault(row, "hedge_flag", 0));
        trade.quantity = ParseIntOrDefault(row, "quantity", 0);
        trade.price = ParseDoubleOrDefault(row, "price", 0);
        trade.commission = ParseDoubleOrDefault(row, "commission", 0);
        trade.profit = ParseDoubleOrDefault(row, "profit", 0);
        trade.valuation_complete =
            Field(row, "valuation_complete") == "true" || Field(row, "valuation_complete") == "t";
        trade.trade_ts_ns = std::stoll(Field(row, "trade_ts_ns"));
        auto& position = record.position;
        position.account_id = trade.account_id;
        position.strategy_id = trade.strategy_id;
        position.symbol = trade.symbol;
        position.exchange = trade.exchange;
        position.hedge_flag = trade.hedge_flag;
        position.trading_day = trade.trading_day;
        position.version = record.position_version;
        position.update_time_ns = trade.trade_ts_ns;
        position.long_qty = ParseIntOrDefault(row, "long_qty", 0);
        position.short_qty = ParseIntOrDefault(row, "short_qty", 0);
        position.long_today_qty = ParseIntOrDefault(row, "long_today_qty", 0);
        position.short_today_qty = ParseIntOrDefault(row, "short_today_qty", 0);
        position.long_yd_qty = ParseIntOrDefault(row, "long_yd_qty", 0);
        position.short_yd_qty = ParseIntOrDefault(row, "short_yd_qty", 0);
        position.avg_long_price = ParseDoubleOrDefault(row, "avg_long_price", 0);
        position.avg_short_price = ParseDoubleOrDefault(row, "avg_short_price", 0);
        position.position_profit = ParseDoubleOrDefault(row, "position_profit", 0);
        position.margin = ParseDoubleOrDefault(row, "margin", 0);
        out->push_back(std::move(record));
    }
    std::sort(out->begin(), out->end(), [](const TradeOutboxRecord& a, const TradeOutboxRecord& b) {
        if (a.commit_sequence != b.commit_sequence) return a.commit_sequence < b.commit_sequence;
        return a.outbox_id < b.outbox_id;
    });
    return true;
}

bool TradingDomainStoreClientAdapter::AcknowledgeOutbox(const std::string& id, std::string* error) {
    return AcknowledgeOutboxForConsumer("position", id, error);
}

bool TradingDomainStoreClientAdapter::AcknowledgeOutboxForConsumer(const std::string& consumer,
                                                                   const std::string& id,
                                                                   std::string* error) {
    if (client_ == nullptr) return false;
    std::string local;
    if (error == nullptr) error = &local;
    error->clear();
    return client_->RunInTransaction(
        [&](ITimescaleSqlClient& tx, std::string* e) {
            if (consumer.empty() || !tx.LockTransactionKey("outbox:" + consumer + ":" + id, e))
                return false;
            auto rows = tx.QueryRows(TableName("trade_outbox"), "outbox_id", id, e);
            if (!e->empty() || rows.size() != 1) return false;
            return tx.UpsertRow(TableName("trade_outbox_acks"),
                                {{"consumer", consumer}, {"outbox_id", id}},
                                {"consumer", "outbox_id"}, {}, e);
        },
        error);
}

}  // namespace quant_hft
