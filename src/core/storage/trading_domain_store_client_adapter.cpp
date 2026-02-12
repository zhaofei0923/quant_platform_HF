#include "quant_hft/core/trading_domain_store_client_adapter.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <thread>

namespace quant_hft {

namespace {

std::string ToDirectionCode(Side side) {
    return side == Side::kSell ? "1" : "0";
}

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

std::string ToOrderTypeCode(OrderType type) {
    return type == OrderType::kMarket ? "2" : "1";
}

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
                               const std::string& key,
                               std::int32_t default_value = 0) {
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

}  // namespace

TradingDomainStoreClientAdapter::TradingDomainStoreClientAdapter(
    std::shared_ptr<ITimescaleSqlClient> client,
    StorageRetryPolicy retry_policy,
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
    const auto update_time = ToTimestamp(order.updated_at_ns > 0 ? order.updated_at_ns : order.created_at_ns);
    std::unordered_map<std::string, std::string> row{
        {"order_ref", order.order_id},
        {"account_id", order.account_id},
        {"strategy_id", order.strategy_id},
        {"instrument_id", order.symbol},
        {"exchange_id", order.exchange},
        {"order_type", ToOrderTypeCode(order.order_type)},
        {"direction", ToDirectionCode(order.side)},
        {"offset_flag", ToOffsetCode(order.offset)},
        {"price_type", "1"},
        {"limit_price", ToString(order.price)},
        {"volume_original", ToString(order.quantity)},
        {"volume_traded", ToString(order.filled_quantity)},
        {"volume_canceled",
         ToString(std::max(0, order.quantity - order.filled_quantity))},
        {"order_status", ToString(static_cast<std::int32_t>(order.status))},
        {"insert_time", insert_time},
        {"update_time", update_time},
        {"status_msg", order.message},
    };
    std::string query_error;
    const auto existing_rows = client_->QueryRows(TableName("orders"), "order_ref", order.order_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    if (!existing_rows.empty()) {
        return true;
    }
    return InsertWithRetry("orders", row, error);
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
    const auto existing_rows = client_->QueryRows(TableName("trades"), "trade_id", trade.trade_id, &query_error);
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
    };
    return InsertWithRetry("position_summary", row, error);
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
    const ProcessedOrderEventRecord& event,
    std::string* error) {
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
        std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
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

bool TradingDomainStoreClientAdapter::InsertPositionDetailFromTrade(const Trade& trade,
                                                                    std::string* error) {
    if (trade.account_id.empty() || trade.strategy_id.empty() || trade.symbol.empty() ||
        trade.trade_id.empty()) {
        if (error != nullptr) {
            *error = "trade requires account_id/strategy_id/symbol/trade_id";
        }
        return false;
    }
    if (trade.offset != OffsetFlag::kOpen) {
        if (error != nullptr) {
            *error = "InsertPositionDetailFromTrade only accepts open trades";
        }
        return false;
    }

    const std::int32_t signed_volume =
        trade.side == Side::kBuy ? trade.quantity : -trade.quantity;
    const auto open_date = ToDate(trade.trade_ts_ns);
    const std::unordered_map<std::string, std::string> row{
        {"account_id", trade.account_id},
        {"strategy_id", trade.strategy_id},
        {"instrument_id", trade.symbol},
        {"exchange_id", trade.exchange},
        {"open_date", open_date},
        {"open_price", ToString(trade.price)},
        {"volume", ToString(signed_volume)},
        {"is_today", "true"},
        {"position_date", open_date},
        {"open_order_ref", trade.order_id},
        {"open_trade_id", trade.trade_id},
        {"close_volume", "0"},
        {"position_status", "1"},
        {"update_time", ToTimestamp(trade.trade_ts_ns)},
    };
    return InsertWithRetry("position_detail", row, error);
}

bool TradingDomainStoreClientAdapter::ClosePositionDetailFifo(const Trade& trade,
                                                              std::string* error) {
    if (trade.account_id.empty() || trade.strategy_id.empty() || trade.symbol.empty()) {
        if (error != nullptr) {
            *error = "trade requires account_id/strategy_id/symbol";
        }
        return false;
    }
    if (trade.offset == OffsetFlag::kOpen) {
        if (error != nullptr) {
            *error = "ClosePositionDetailFifo only accepts close trades";
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
    auto rows = client_->QueryRows(TableName("position_detail"), "account_id", trade.account_id, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    std::sort(rows.begin(), rows.end(), [](const auto& lhs, const auto& rhs) {
        const auto lhs_date = lhs.find("open_date") == lhs.end() ? "" : lhs.at("open_date");
        const auto rhs_date = rhs.find("open_date") == rhs.end() ? "" : rhs.at("open_date");
        if (lhs_date != rhs_date) {
            return lhs_date < rhs_date;
        }
        return ParseIntOrDefault(lhs, "position_id", 0) < ParseIntOrDefault(rhs, "position_id", 0);
    });

    std::int32_t remaining = std::max(0, trade.quantity);
    for (const auto& row : rows) {
        if (remaining <= 0) {
            break;
        }
        const auto strategy_it = row.find("strategy_id");
        const auto symbol_it = row.find("instrument_id");
        if (strategy_it == row.end() || symbol_it == row.end()) {
            continue;
        }
        if (strategy_it->second != trade.strategy_id || symbol_it->second != trade.symbol) {
            continue;
        }
        const auto position_status = ParseIntOrDefault(row, "position_status", 0);
        if (position_status != 1) {
            continue;
        }

        const auto signed_volume = ParseIntOrDefault(row, "volume", 0);
        const auto abs_volume = std::abs(signed_volume);
        const auto close_volume = ParseIntOrDefault(row, "close_volume", 0);
        const auto closable = std::max(0, abs_volume - close_volume);
        if (closable <= 0) {
            continue;
        }
        const auto take = std::min(remaining, closable);
        const auto updated_close_volume = close_volume + take;

        const auto position_id =
            row.find("position_id") == row.end() ? "" : row.at("position_id");
        const auto open_date = row.find("open_date") == row.end() ? ToDate(trade.trade_ts_ns)
                                                                    : row.at("open_date");
        const auto position_date =
            row.find("position_date") == row.end() ? open_date : row.at("position_date");
        const auto open_price =
            row.find("open_price") == row.end() ? ToString(trade.price) : row.at("open_price");
        const auto volume = row.find("volume") == row.end()
                                ? ToString(trade.side == Side::kBuy ? trade.quantity : -trade.quantity)
                                : row.at("volume");
        const auto open_trade_id =
            row.find("open_trade_id") == row.end() ? "" : row.at("open_trade_id");

        std::unordered_map<std::string, std::string> upsert_row{
            {"open_date", open_date},
            {"account_id", trade.account_id},
            {"strategy_id", trade.strategy_id},
            {"instrument_id", trade.symbol},
            {"exchange_id", trade.exchange},
            {"open_price", open_price},
            {"volume", volume},
            {"is_today", row.find("is_today") == row.end() ? "false" : row.at("is_today")},
            {"position_date", position_date},
            {"open_order_ref", row.find("open_order_ref") == row.end() ? "" : row.at("open_order_ref")},
            {"open_trade_id", open_trade_id},
            {"close_volume", ToString(updated_close_volume)},
            {"close_price", ToString(trade.price)},
            {"close_profit", ToString(trade.profit)},
            {"position_status", updated_close_volume >= abs_volume ? "2" : "1"},
            {"update_time", ToTimestamp(trade.trade_ts_ns)},
        };
        if (!position_id.empty()) {
            upsert_row["position_id"] = position_id;
        }
        std::vector<std::string> conflict_keys;
        if (!position_id.empty()) {
            conflict_keys = {"position_id", "open_date"};
        } else if (!open_trade_id.empty()) {
            conflict_keys = {"account_id", "strategy_id", "open_trade_id"};
        } else {
            conflict_keys = {"account_id", "strategy_id", "instrument_id", "open_date"};
        }
        std::string upsert_error;
        if (!client_->UpsertRow(TableName("position_detail"),
                                upsert_row,
                                conflict_keys,
                                {"close_volume", "close_price", "close_profit", "position_status", "update_time"},
                                &upsert_error)) {
            if (error != nullptr) {
                *error = upsert_error;
            }
            return false;
        }
        remaining -= take;
    }
    return true;
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
    if (!client_->UpsertRow(TableName("orders"),
                            row,
                            {"order_ref", "insert_time"},
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
        if (client_->InsertRow(TableName(table), row, &local_error)) {
            return true;
        }
        std::string lowered = local_error;
        std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
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
    return std::to_string(value);
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

}  // namespace quant_hft
