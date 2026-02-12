#include "quant_hft/core/trading_domain_store_client_adapter.h"

#include <algorithm>
#include <cctype>
#include <chrono>
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
        {"event_type", ToString(risk_event.event_type)},
        {"event_level", ToString(risk_event.event_level)},
        {"instrument_id", risk_event.instrument_id},
        {"order_ref", risk_event.order_ref},
        {"event_desc", risk_event.event_desc},
        {"event_time", ToTimestamp(risk_event.event_ts_ns)},
    };
    return InsertWithRetry("risk_events", row, error);
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
