#include "quant_hft/core/redis_realtime_store.h"

namespace quant_hft {

namespace {

const char* DirectionToSuffix(PositionDirection direction) {
    return direction == PositionDirection::kShort ? "SHORT" : "LONG";
}

}  // namespace

std::string RedisKeyBuilder::OrderInfo(const std::string& order_id) {
    return "trade:order:" + order_id + ":info";
}

std::string RedisKeyBuilder::MarketTickLatest(const std::string& instrument_id) {
    return "market:tick:" + instrument_id + ":latest";
}

std::string RedisKeyBuilder::Position(const std::string& account_id,
                                      const std::string& instrument_id,
                                      PositionDirection direction) {
    return "trade:position:" + account_id + ":" + instrument_id + ":" +
           DirectionToSuffix(direction);
}

void RedisRealtimeStore::UpsertMarketSnapshot(const MarketSnapshot& snapshot) {
    if (snapshot.instrument_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    market_snapshots_[RedisKeyBuilder::MarketTickLatest(snapshot.instrument_id)] = snapshot;
}

void RedisRealtimeStore::UpsertOrderEvent(const OrderEvent& event) {
    if (event.client_order_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    order_events_[RedisKeyBuilder::OrderInfo(event.client_order_id)] = event;
}

void RedisRealtimeStore::UpsertPositionSnapshot(const PositionSnapshot& position) {
    if (position.account_id.empty() || position.instrument_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    position_snapshots_[RedisKeyBuilder::Position(
        position.account_id, position.instrument_id, position.direction)] = position;
}

bool RedisRealtimeStore::GetMarketSnapshot(const std::string& instrument_id,
                                           MarketSnapshot* out) const {
    if (out == nullptr || instrument_id.empty()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto key = RedisKeyBuilder::MarketTickLatest(instrument_id);
    const auto it = market_snapshots_.find(key);
    if (it == market_snapshots_.end()) {
        return false;
    }
    *out = it->second;
    return true;
}

bool RedisRealtimeStore::GetOrderEvent(const std::string& client_order_id,
                                       OrderEvent* out) const {
    if (out == nullptr || client_order_id.empty()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto key = RedisKeyBuilder::OrderInfo(client_order_id);
    const auto it = order_events_.find(key);
    if (it == order_events_.end()) {
        return false;
    }
    *out = it->second;
    return true;
}

bool RedisRealtimeStore::GetPositionSnapshot(const std::string& account_id,
                                             const std::string& instrument_id,
                                             PositionDirection direction,
                                             PositionSnapshot* out) const {
    if (out == nullptr || account_id.empty() || instrument_id.empty()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto key = RedisKeyBuilder::Position(account_id, instrument_id, direction);
    const auto it = position_snapshots_.find(key);
    if (it == position_snapshots_.end()) {
        return false;
    }
    *out = it->second;
    return true;
}

}  // namespace quant_hft
