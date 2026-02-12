#include "quant_hft/core/trading_ledger_store_client_adapter.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <ctime>
#include <sstream>
#include <thread>

namespace quant_hft {

TradingLedgerStoreClientAdapter::TradingLedgerStoreClientAdapter(
    std::shared_ptr<ITimescaleSqlClient> client,
    StorageRetryPolicy retry_policy,
    std::string schema)
    : client_(std::move(client)),
      retry_policy_(retry_policy),
      schema_(schema.empty() ? "trading_core" : std::move(schema)) {}

bool TradingLedgerStoreClientAdapter::AppendOrderEvent(const OrderEvent& event,
                                                       std::string* error) {
    if (event.client_order_id.empty()) {
        if (error != nullptr) {
            *error = "empty client_order_id";
        }
        return false;
    }
    const std::int64_t effective_ts =
        event.recv_ts_ns > 0 ? event.recv_ts_ns : (event.ts_ns > 0 ? event.ts_ns : NowEpochNanos());
    std::unordered_map<std::string, std::string> row{
        {"trade_date", BuildTradeDate(effective_ts)},
        {"idempotency_key", BuildIdempotencyKey(event)},
        {"account_id", event.account_id},
        {"client_order_id", event.client_order_id},
        {"exchange_order_id", event.exchange_order_id},
        {"instrument_id", event.instrument_id},
        {"exchange_id", event.exchange_id},
        {"status", std::to_string(static_cast<int>(event.status))},
        {"total_volume", ToString(event.total_volume)},
        {"filled_volume", ToString(event.filled_volume)},
        {"avg_fill_price", ToString(event.avg_fill_price)},
        {"reason", event.reason},
        {"status_msg", event.status_msg},
        {"order_submit_status", event.order_submit_status},
        {"order_ref", event.order_ref},
        {"front_id", ToString(event.front_id)},
        {"session_id", ToString(event.session_id)},
        {"trade_id", event.trade_id},
        {"event_source", event.event_source},
        {"exchange_ts_ns", ToString(event.exchange_ts_ns)},
        {"recv_ts_ns", ToString(event.recv_ts_ns)},
        {"ts_ns", ToString(event.ts_ns)},
        {"trace_id", event.trace_id},
        {"execution_algo_id", event.execution_algo_id},
        {"slice_index", ToString(event.slice_index)},
        {"slice_total", ToString(event.slice_total)},
        {"throttle_applied", event.throttle_applied ? "1" : "0"},
        {"venue", event.venue},
        {"route_id", event.route_id},
        {"slippage_bps", ToString(event.slippage_bps)},
        {"impact_cost", ToString(event.impact_cost)},
    };
    return InsertWithRetry("order_events", row, error);
}

bool TradingLedgerStoreClientAdapter::AppendTradeEvent(const OrderEvent& event,
                                                       std::string* error) {
    if (event.client_order_id.empty()) {
        if (error != nullptr) {
            *error = "empty client_order_id";
        }
        return false;
    }
    const std::int64_t effective_ts =
        event.recv_ts_ns > 0 ? event.recv_ts_ns : (event.ts_ns > 0 ? event.ts_ns : NowEpochNanos());
    std::unordered_map<std::string, std::string> row{
        {"trade_date", BuildTradeDate(effective_ts)},
        {"idempotency_key", BuildIdempotencyKey(event)},
        {"account_id", event.account_id},
        {"client_order_id", event.client_order_id},
        {"exchange_order_id", event.exchange_order_id},
        {"instrument_id", event.instrument_id},
        {"exchange_id", event.exchange_id},
        {"trade_id", event.trade_id},
        {"filled_volume", ToString(event.filled_volume)},
        {"avg_fill_price", ToString(event.avg_fill_price)},
        {"exchange_ts_ns", ToString(event.exchange_ts_ns)},
        {"recv_ts_ns", ToString(event.recv_ts_ns)},
        {"ts_ns", ToString(event.ts_ns)},
        {"trace_id", event.trace_id},
        {"event_source", event.event_source},
    };
    return InsertWithRetry("trade_events", row, error);
}

bool TradingLedgerStoreClientAdapter::AppendAccountSnapshot(
    const TradingAccountSnapshot& snapshot,
    std::string* error) {
    if (snapshot.account_id.empty()) {
        if (error != nullptr) {
            *error = "empty account_id";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"account_id", snapshot.account_id},
        {"investor_id", snapshot.investor_id},
        {"trading_day", snapshot.trading_day},
        {"balance", ToString(snapshot.balance)},
        {"available", ToString(snapshot.available)},
        {"curr_margin", ToString(snapshot.curr_margin)},
        {"frozen_margin", ToString(snapshot.frozen_margin)},
        {"frozen_cash", ToString(snapshot.frozen_cash)},
        {"frozen_commission", ToString(snapshot.frozen_commission)},
        {"commission", ToString(snapshot.commission)},
        {"close_profit", ToString(snapshot.close_profit)},
        {"position_profit", ToString(snapshot.position_profit)},
        {"ts_ns", ToString(snapshot.ts_ns)},
        {"recv_ts_ns", ToString(snapshot.ts_ns)},
        {"source", snapshot.source},
    };
    return InsertWithRetry("account_snapshots", row, error);
}

bool TradingLedgerStoreClientAdapter::AppendPositionSnapshot(
    const InvestorPositionSnapshot& snapshot,
    std::string* error) {
    if (snapshot.account_id.empty() || snapshot.instrument_id.empty()) {
        if (error != nullptr) {
            *error = "empty account_id or instrument_id";
        }
        return false;
    }
    std::unordered_map<std::string, std::string> row{
        {"account_id", snapshot.account_id},
        {"investor_id", snapshot.investor_id},
        {"instrument_id", snapshot.instrument_id},
        {"exchange_id", snapshot.exchange_id},
        {"posi_direction", snapshot.posi_direction},
        {"hedge_flag", snapshot.hedge_flag},
        {"position_date", snapshot.position_date},
        {"position", ToString(snapshot.position)},
        {"today_position", ToString(snapshot.today_position)},
        {"yd_position", ToString(snapshot.yd_position)},
        {"long_frozen", ToString(snapshot.long_frozen)},
        {"short_frozen", ToString(snapshot.short_frozen)},
        {"open_volume", ToString(snapshot.open_volume)},
        {"close_volume", ToString(snapshot.close_volume)},
        {"position_cost", ToString(snapshot.position_cost)},
        {"open_cost", ToString(snapshot.open_cost)},
        {"position_profit", ToString(snapshot.position_profit)},
        {"close_profit", ToString(snapshot.close_profit)},
        {"margin_rate_by_money", ToString(snapshot.margin_rate_by_money)},
        {"margin_rate_by_volume", ToString(snapshot.margin_rate_by_volume)},
        {"use_margin", ToString(snapshot.use_margin)},
        {"ts_ns", ToString(snapshot.ts_ns)},
        {"recv_ts_ns", ToString(snapshot.ts_ns)},
        {"source", snapshot.source},
    };
    return InsertWithRetry("position_snapshots", row, error);
}

bool TradingLedgerStoreClientAdapter::UpsertReplayOffset(const std::string& stream_name,
                                                         std::int64_t last_seq,
                                                         std::int64_t updated_ts_ns,
                                                         std::string* error) {
    if (client_ == nullptr) {
        if (error != nullptr) {
            *error = "null sql client";
        }
        return false;
    }
    if (stream_name.empty()) {
        if (error != nullptr) {
            *error = "empty stream_name";
        }
        return false;
    }

    std::unordered_map<std::string, std::string> row{
        {"stream_name", stream_name},
        {"last_seq", ToString(last_seq)},
        {"updated_ts_ns", ToString(updated_ts_ns)},
    };
    std::string insert_error;
    if (client_->InsertRow(TableName("replay_offsets"), row, &insert_error)) {
        return true;
    }
    if (!IsDuplicateKeyError(insert_error)) {
        if (error != nullptr) {
            *error = insert_error;
        }
        return false;
    }

    std::string query_error;
    const auto rows =
        client_->QueryRows(TableName("replay_offsets"), "stream_name", stream_name, &query_error);
    if (!query_error.empty()) {
        if (error != nullptr) {
            *error = query_error;
        }
        return false;
    }
    if (rows.empty()) {
        if (error != nullptr) {
            *error = "replay offset duplicate but query returned no rows";
        }
        return false;
    }
    const auto existing_it = rows.front().find("last_seq");
    if (existing_it == rows.front().end()) {
        if (error != nullptr) {
            *error = "replay offset row missing last_seq";
        }
        return false;
    }
    try {
        const auto existing_seq = std::stoll(existing_it->second);
        if (existing_seq >= last_seq) {
            return true;
        }
    } catch (...) {
        if (error != nullptr) {
            *error = "invalid replay offset last_seq";
        }
        return false;
    }

    if (error != nullptr) {
        *error =
            "replay offset exists with lower last_seq and cannot be updated by insert-only client";
    }
    return false;
}

bool TradingLedgerStoreClientAdapter::InsertWithRetry(
    const std::string& table,
    const std::unordered_map<std::string, std::string>& row,
    std::string* error) const {
    if (client_ == nullptr) {
        if (error != nullptr) {
            *error = "null sql client";
        }
        return false;
    }

    int attempts = std::max(1, retry_policy_.max_attempts);
    int backoff_ms = std::max(0, retry_policy_.initial_backoff_ms);
    const int max_backoff_ms = std::max(backoff_ms, retry_policy_.max_backoff_ms);

    std::string last_error;
    for (int attempt = 1; attempt <= attempts; ++attempt) {
        std::string local_error;
        if (client_->InsertRow(TableName(table), row, &local_error)) {
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

bool TradingLedgerStoreClientAdapter::IsDuplicateKeyError(const std::string& error) const {
    std::string lowered = error;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return lowered.find("duplicate key") != std::string::npos ||
           lowered.find("already exists") != std::string::npos;
}

std::string TradingLedgerStoreClientAdapter::TableName(const std::string& table) const {
    if (schema_.empty()) {
        return table;
    }
    return schema_ + "." + table;
}

std::string TradingLedgerStoreClientAdapter::ToString(std::int32_t value) {
    return std::to_string(value);
}

std::string TradingLedgerStoreClientAdapter::ToString(std::int64_t value) {
    return std::to_string(value);
}

std::string TradingLedgerStoreClientAdapter::ToString(double value) {
    return std::to_string(value);
}

std::string TradingLedgerStoreClientAdapter::BuildTradeDate(std::int64_t ts_ns) {
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / 1'000'000'000LL);
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

std::string TradingLedgerStoreClientAdapter::BuildIdempotencyKey(const OrderEvent& event) {
    std::ostringstream key;
    key << event.client_order_id << "|" << event.event_source << "|" << event.ts_ns << "|"
        << event.filled_volume << "|" << event.trade_id;
    return key.str();
}

}  // namespace quant_hft
