#include "quant_hft/core/redis_realtime_store_client_adapter.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace quant_hft {

RedisRealtimeStoreClientAdapter::RedisRealtimeStoreClientAdapter(
    std::shared_ptr<IRedisHashClient> client,
    StorageRetryPolicy retry_policy)
    : client_(std::move(client)), retry_policy_(retry_policy) {}

void RedisRealtimeStoreClientAdapter::UpsertMarketSnapshot(
    const MarketSnapshot& snapshot) {
    if (snapshot.instrument_id.empty()) {
        return;
    }

    const auto key = RedisKeyBuilder::MarketTickLatest(snapshot.instrument_id);
    std::unordered_map<std::string, std::string> fields{
        {"instrument_id", snapshot.instrument_id},
        {"last_price", ToString(snapshot.last_price)},
        {"bid_price_1", ToString(snapshot.bid_price_1)},
        {"ask_price_1", ToString(snapshot.ask_price_1)},
        {"bid_volume_1", ToString(snapshot.bid_volume_1)},
        {"ask_volume_1", ToString(snapshot.ask_volume_1)},
        {"volume", ToString(snapshot.volume)},
        {"exchange_ts_ns", ToString(snapshot.exchange_ts_ns)},
        {"recv_ts_ns", ToString(snapshot.recv_ts_ns)},
    };
    (void)WriteWithRetry(key, fields);
}

void RedisRealtimeStoreClientAdapter::UpsertOrderEvent(const OrderEvent& event) {
    if (event.client_order_id.empty()) {
        return;
    }

    const auto key = RedisKeyBuilder::OrderInfo(event.client_order_id);
    std::unordered_map<std::string, std::string> fields{
        {"account_id", event.account_id},
        {"client_order_id", event.client_order_id},
        {"exchange_order_id", event.exchange_order_id},
        {"instrument_id", event.instrument_id},
        {"exchange_id", event.exchange_id},
        {"status", OrderStatusToString(event.status)},
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
    (void)WriteWithRetry(key, fields);
}

void RedisRealtimeStoreClientAdapter::UpsertPositionSnapshot(
    const PositionSnapshot& position) {
    if (position.account_id.empty() || position.instrument_id.empty()) {
        return;
    }

    const auto key = RedisKeyBuilder::Position(position.account_id,
                                               position.instrument_id,
                                               position.direction);
    std::unordered_map<std::string, std::string> fields{
        {"account_id", position.account_id},
        {"instrument_id", position.instrument_id},
        {"direction", PositionDirectionToString(position.direction)},
        {"volume", ToString(position.volume)},
        {"avg_price", ToString(position.avg_price)},
        {"unrealized_pnl", ToString(position.unrealized_pnl)},
        {"margin", ToString(position.margin)},
        {"ts_ns", ToString(position.ts_ns)},
    };
    (void)WriteWithRetry(key, fields);
}

void RedisRealtimeStoreClientAdapter::UpsertStateSnapshot7D(
    const StateSnapshot7D& snapshot) {
    if (snapshot.instrument_id.empty()) {
        return;
    }

    const auto key = RedisKeyBuilder::StateSnapshot7DLatest(snapshot.instrument_id);
    std::unordered_map<std::string, std::string> fields{
        {"instrument_id", snapshot.instrument_id},
        {"trend_score", ToString(snapshot.trend.score)},
        {"trend_confidence", ToString(snapshot.trend.confidence)},
        {"volatility_score", ToString(snapshot.volatility.score)},
        {"volatility_confidence", ToString(snapshot.volatility.confidence)},
        {"liquidity_score", ToString(snapshot.liquidity.score)},
        {"liquidity_confidence", ToString(snapshot.liquidity.confidence)},
        {"sentiment_score", ToString(snapshot.sentiment.score)},
        {"sentiment_confidence", ToString(snapshot.sentiment.confidence)},
        {"seasonality_score", ToString(snapshot.seasonality.score)},
        {"seasonality_confidence", ToString(snapshot.seasonality.confidence)},
        {"pattern_score", ToString(snapshot.pattern.score)},
        {"pattern_confidence", ToString(snapshot.pattern.confidence)},
        {"event_drive_score", ToString(snapshot.event_drive.score)},
        {"event_drive_confidence", ToString(snapshot.event_drive.confidence)},
        {"ts_ns", ToString(snapshot.ts_ns)},
    };
    (void)WriteWithRetry(key, fields);
}

bool RedisRealtimeStoreClientAdapter::GetMarketSnapshot(const std::string& instrument_id,
                                                        MarketSnapshot* out) const {
    if (out == nullptr || instrument_id.empty()) {
        return false;
    }

    std::unordered_map<std::string, std::string> row;
    if (!ReadHash(RedisKeyBuilder::MarketTickLatest(instrument_id), &row)) {
        return false;
    }

    MarketSnapshot snapshot;
    snapshot.instrument_id = GetOrEmpty(row, "instrument_id");
    if (!ParseDouble(row, "last_price", &snapshot.last_price)) {
        return false;
    }
    (void)ParseDouble(row, "bid_price_1", &snapshot.bid_price_1);
    (void)ParseDouble(row, "ask_price_1", &snapshot.ask_price_1);
    (void)ParseInt64(row, "bid_volume_1", &snapshot.bid_volume_1);
    (void)ParseInt64(row, "ask_volume_1", &snapshot.ask_volume_1);
    (void)ParseInt64(row, "volume", &snapshot.volume);
    (void)ParseInt64(row, "exchange_ts_ns", &snapshot.exchange_ts_ns);
    (void)ParseInt64(row, "recv_ts_ns", &snapshot.recv_ts_ns);
    *out = snapshot;
    return true;
}

bool RedisRealtimeStoreClientAdapter::GetOrderEvent(const std::string& client_order_id,
                                                    OrderEvent* out) const {
    if (out == nullptr || client_order_id.empty()) {
        return false;
    }

    std::unordered_map<std::string, std::string> row;
    if (!ReadHash(RedisKeyBuilder::OrderInfo(client_order_id), &row)) {
        return false;
    }

    OrderEvent event;
    event.account_id = GetOrEmpty(row, "account_id");
    event.client_order_id = GetOrEmpty(row, "client_order_id");
    event.exchange_order_id = GetOrEmpty(row, "exchange_order_id");
    event.instrument_id = GetOrEmpty(row, "instrument_id");
    event.exchange_id = GetOrEmpty(row, "exchange_id");
    if (!ParseOrderStatus(GetOrEmpty(row, "status"), &event.status)) {
        return false;
    }
    if (!ParseInt32(row, "total_volume", &event.total_volume)) {
        return false;
    }
    if (!ParseInt32(row, "filled_volume", &event.filled_volume)) {
        return false;
    }
    if (!ParseDouble(row, "avg_fill_price", &event.avg_fill_price)) {
        return false;
    }
    event.reason = GetOrEmpty(row, "reason");
    event.status_msg = GetOrEmpty(row, "status_msg");
    event.order_submit_status = GetOrEmpty(row, "order_submit_status");
    event.order_ref = GetOrEmpty(row, "order_ref");
    (void)ParseInt32(row, "front_id", &event.front_id);
    (void)ParseInt32(row, "session_id", &event.session_id);
    event.trade_id = GetOrEmpty(row, "trade_id");
    event.event_source = GetOrEmpty(row, "event_source");
    if (!ParseInt64(row, "ts_ns", &event.ts_ns)) {
        return false;
    }
    event.trace_id = GetOrEmpty(row, "trace_id");
    event.execution_algo_id = GetOrEmpty(row, "execution_algo_id");
    if (!ParseInt32(row, "slice_index", &event.slice_index)) {
        event.slice_index = 0;
    }
    if (!ParseInt32(row, "slice_total", &event.slice_total)) {
        event.slice_total = 0;
    }
    std::int32_t throttle_applied = 0;
    if (ParseInt32(row, "throttle_applied", &throttle_applied)) {
        event.throttle_applied = throttle_applied > 0;
    } else {
        const auto raw = GetOrEmpty(row, "throttle_applied");
        if (raw == "true" || raw == "TRUE" || raw == "yes" || raw == "YES") {
            event.throttle_applied = true;
        }
    }
    event.venue = GetOrEmpty(row, "venue");
    event.route_id = GetOrEmpty(row, "route_id");
    if (!ParseDouble(row, "slippage_bps", &event.slippage_bps)) {
        event.slippage_bps = 0.0;
    }
    if (!ParseDouble(row, "impact_cost", &event.impact_cost)) {
        event.impact_cost = 0.0;
    }
    *out = event;
    return true;
}

bool RedisRealtimeStoreClientAdapter::GetPositionSnapshot(
    const std::string& account_id,
    const std::string& instrument_id,
    PositionDirection direction,
    PositionSnapshot* out) const {
    if (out == nullptr || account_id.empty() || instrument_id.empty()) {
        return false;
    }

    std::unordered_map<std::string, std::string> row;
    if (!ReadHash(RedisKeyBuilder::Position(account_id, instrument_id, direction), &row)) {
        return false;
    }

    PositionSnapshot position;
    position.account_id = GetOrEmpty(row, "account_id");
    position.instrument_id = GetOrEmpty(row, "instrument_id");
    if (!ParsePositionDirection(GetOrEmpty(row, "direction"), &position.direction)) {
        return false;
    }
    if (!ParseInt32(row, "volume", &position.volume)) {
        return false;
    }
    if (!ParseDouble(row, "avg_price", &position.avg_price)) {
        return false;
    }
    if (!ParseDouble(row, "unrealized_pnl", &position.unrealized_pnl)) {
        return false;
    }
    if (!ParseDouble(row, "margin", &position.margin)) {
        return false;
    }
    if (!ParseInt64(row, "ts_ns", &position.ts_ns)) {
        return false;
    }
    *out = position;
    return true;
}

bool RedisRealtimeStoreClientAdapter::GetStateSnapshot7D(const std::string& instrument_id,
                                                         StateSnapshot7D* out) const {
    if (out == nullptr || instrument_id.empty()) {
        return false;
    }

    std::unordered_map<std::string, std::string> row;
    if (!ReadHash(RedisKeyBuilder::StateSnapshot7DLatest(instrument_id), &row)) {
        return false;
    }

    auto parse_dimension = [&](const char* prefix, StateDimension* dim) -> bool {
        if (dim == nullptr) {
            return false;
        }
        double score = 0.0;
        double confidence = 0.0;
        const std::string score_key = std::string(prefix) + "_score";
        const std::string confidence_key = std::string(prefix) + "_confidence";
        if (!ParseDouble(row, score_key, &score) || !ParseDouble(row, confidence_key, &confidence)) {
            return false;
        }
        dim->score = score;
        dim->confidence = confidence;
        return true;
    };

    StateSnapshot7D snapshot;
    snapshot.instrument_id = GetOrEmpty(row, "instrument_id");
    if (snapshot.instrument_id.empty()) {
        snapshot.instrument_id = instrument_id;
    }
    if (!parse_dimension("trend", &snapshot.trend)) {
        return false;
    }
    if (!parse_dimension("volatility", &snapshot.volatility)) {
        return false;
    }
    if (!parse_dimension("liquidity", &snapshot.liquidity)) {
        return false;
    }
    if (!parse_dimension("sentiment", &snapshot.sentiment)) {
        return false;
    }
    if (!parse_dimension("seasonality", &snapshot.seasonality)) {
        return false;
    }
    if (!parse_dimension("pattern", &snapshot.pattern)) {
        return false;
    }
    if (!parse_dimension("event_drive", &snapshot.event_drive)) {
        return false;
    }
    if (!ParseInt64(row, "ts_ns", &snapshot.ts_ns)) {
        return false;
    }
    *out = snapshot;
    return true;
}

bool RedisRealtimeStoreClientAdapter::WriteWithRetry(
    const std::string& key,
    const std::unordered_map<std::string, std::string>& fields) const {
    if (client_ == nullptr || key.empty()) {
        return false;
    }

    int attempts = std::max(1, retry_policy_.max_attempts);
    int backoff_ms = std::max(0, retry_policy_.initial_backoff_ms);
    const int max_backoff_ms = std::max(backoff_ms, retry_policy_.max_backoff_ms);

    for (int attempt = 1; attempt <= attempts; ++attempt) {
        std::string error;
        if (client_->HSet(key, fields, &error)) {
            return true;
        }
        if (attempt < attempts && backoff_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(max_backoff_ms, backoff_ms * 2);
        }
    }
    return false;
}

bool RedisRealtimeStoreClientAdapter::ReadHash(
    const std::string& key,
    std::unordered_map<std::string, std::string>* out) const {
    if (client_ == nullptr || out == nullptr || key.empty()) {
        return false;
    }
    std::string error;
    return client_->HGetAll(key, out, &error);
}

std::string RedisRealtimeStoreClientAdapter::OrderStatusToString(OrderStatus status) {
    switch (status) {
        case OrderStatus::kNew:
            return "NEW";
        case OrderStatus::kAccepted:
            return "ACCEPTED";
        case OrderStatus::kPartiallyFilled:
            return "PARTIALLY_FILLED";
        case OrderStatus::kFilled:
            return "FILLED";
        case OrderStatus::kCanceled:
            return "CANCELED";
        case OrderStatus::kRejected:
            return "REJECTED";
    }
    return "REJECTED";
}

bool RedisRealtimeStoreClientAdapter::ParseOrderStatus(const std::string& text,
                                                       OrderStatus* out) {
    if (out == nullptr) {
        return false;
    }
    if (text == "NEW") {
        *out = OrderStatus::kNew;
        return true;
    }
    if (text == "ACCEPTED") {
        *out = OrderStatus::kAccepted;
        return true;
    }
    if (text == "PARTIALLY_FILLED") {
        *out = OrderStatus::kPartiallyFilled;
        return true;
    }
    if (text == "FILLED") {
        *out = OrderStatus::kFilled;
        return true;
    }
    if (text == "CANCELED") {
        *out = OrderStatus::kCanceled;
        return true;
    }
    if (text == "REJECTED") {
        *out = OrderStatus::kRejected;
        return true;
    }
    return false;
}

std::string RedisRealtimeStoreClientAdapter::PositionDirectionToString(
    PositionDirection direction) {
    return direction == PositionDirection::kShort ? "SHORT" : "LONG";
}

bool RedisRealtimeStoreClientAdapter::ParsePositionDirection(
    const std::string& text,
    PositionDirection* out) {
    if (out == nullptr) {
        return false;
    }
    if (text == "SHORT") {
        *out = PositionDirection::kShort;
        return true;
    }
    if (text == "LONG") {
        *out = PositionDirection::kLong;
        return true;
    }
    return false;
}

std::string RedisRealtimeStoreClientAdapter::ToString(std::int32_t value) {
    return std::to_string(value);
}

std::string RedisRealtimeStoreClientAdapter::ToString(std::int64_t value) {
    return std::to_string(value);
}

std::string RedisRealtimeStoreClientAdapter::ToString(double value) {
    return std::to_string(value);
}

bool RedisRealtimeStoreClientAdapter::ParseInt32(
    const std::unordered_map<std::string, std::string>& row,
    const std::string& key,
    std::int32_t* out) {
    if (out == nullptr) {
        return false;
    }
    const auto it = row.find(key);
    if (it == row.end()) {
        return false;
    }
    try {
        *out = static_cast<std::int32_t>(std::stoi(it->second));
        return true;
    } catch (...) {
        return false;
    }
}

bool RedisRealtimeStoreClientAdapter::ParseInt64(
    const std::unordered_map<std::string, std::string>& row,
    const std::string& key,
    std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    const auto it = row.find(key);
    if (it == row.end()) {
        return false;
    }
    try {
        *out = static_cast<std::int64_t>(std::stoll(it->second));
        return true;
    } catch (...) {
        return false;
    }
}

bool RedisRealtimeStoreClientAdapter::ParseDouble(
    const std::unordered_map<std::string, std::string>& row,
    const std::string& key,
    double* out) {
    if (out == nullptr) {
        return false;
    }
    const auto it = row.find(key);
    if (it == row.end()) {
        return false;
    }
    try {
        *out = std::stod(it->second);
        return true;
    } catch (...) {
        return false;
    }
}

std::string RedisRealtimeStoreClientAdapter::GetOrEmpty(
    const std::unordered_map<std::string, std::string>& row,
    const std::string& key) {
    const auto it = row.find(key);
    if (it == row.end()) {
        return "";
    }
    return it->second;
}

}  // namespace quant_hft
