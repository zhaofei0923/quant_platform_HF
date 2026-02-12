#include "quant_hft/services/position_manager.h"

#include <algorithm>
#include <cmath>
#include <utility>

namespace quant_hft {

namespace {

bool IsCloseOffset(OffsetFlag offset) {
    return offset == OffsetFlag::kClose || offset == OffsetFlag::kCloseToday ||
           offset == OffsetFlag::kCloseYesterday;
}

std::string PositionMapKey(const Position& position) {
    return position.account_id + "|" + position.strategy_id + "|" + position.symbol;
}

}  // namespace

PositionManager::PositionManager(std::shared_ptr<ITradingDomainStore> domain_store,
                                 std::shared_ptr<IRedisHashClient> redis_client)
    : domain_store_(std::move(domain_store)),
      redis_client_(std::move(redis_client)) {}

bool PositionManager::UpdatePosition(const Trade& trade, std::string* error) {
    if (trade.account_id.empty() || trade.symbol.empty() || trade.strategy_id.empty()) {
        if (error != nullptr) {
            *error = "trade requires account_id/strategy_id/symbol";
        }
        return false;
    }
    if (domain_store_ == nullptr) {
        if (error != nullptr) {
            *error = "null trading domain store";
        }
        return false;
    }
    if (redis_client_ == nullptr) {
        if (error != nullptr) {
            *error = "null redis client";
        }
        return false;
    }

    std::vector<Position> existing_positions;
    std::string store_error;
    if (!domain_store_->LoadPositionSummary(trade.account_id,
                                            trade.strategy_id,
                                            &existing_positions,
                                            &store_error)) {
        if (error != nullptr) {
            *error = store_error;
        }
        return false;
    }

    Position before;
    before.account_id = trade.account_id;
    before.strategy_id = trade.strategy_id;
    before.symbol = trade.symbol;
    before.exchange = trade.exchange;

    bool found = false;
    for (const auto& position : existing_positions) {
        if (position.symbol == trade.symbol) {
            before = position;
            found = true;
            break;
        }
    }
    (void)found;

    Position after = before;
    after.update_time_ns = trade.trade_ts_ns > 0 ? trade.trade_ts_ns : NowEpochNanos();

    const auto qty = std::max(0, trade.quantity);
    if (!IsCloseOffset(trade.offset)) {
        if (trade.side == Side::kBuy) {
            const auto notional_before =
                after.avg_long_price * static_cast<double>(after.long_qty);
            after.long_qty += qty;
            after.long_today_qty += qty;
            if (after.long_qty > 0) {
                after.avg_long_price =
                    (notional_before + trade.price * static_cast<double>(qty)) /
                    static_cast<double>(after.long_qty);
            }
        } else {
            const auto notional_before =
                after.avg_short_price * static_cast<double>(after.short_qty);
            after.short_qty += qty;
            after.short_today_qty += qty;
            if (after.short_qty > 0) {
                after.avg_short_price =
                    (notional_before + trade.price * static_cast<double>(qty)) /
                    static_cast<double>(after.short_qty);
            }
        }
        if (!domain_store_->InsertPositionDetailFromTrade(trade, &store_error)) {
            if (error != nullptr) {
                *error = store_error;
            }
            return false;
        }
    } else {
        if (trade.side == Side::kSell) {
            const auto close_qty = std::min(after.long_qty, qty);
            after.long_qty = std::max(0, after.long_qty - close_qty);
            auto consume = close_qty;
            const auto td = std::min(after.long_today_qty, consume);
            after.long_today_qty -= td;
            consume -= td;
            after.long_yd_qty = std::max(0, after.long_yd_qty - consume);
        } else {
            const auto close_qty = std::min(after.short_qty, qty);
            after.short_qty = std::max(0, after.short_qty - close_qty);
            auto consume = close_qty;
            const auto td = std::min(after.short_today_qty, consume);
            after.short_today_qty -= td;
            consume -= td;
            after.short_yd_qty = std::max(0, after.short_yd_qty - consume);
        }
        if (!domain_store_->ClosePositionDetailFifo(trade, &store_error)) {
            if (error != nullptr) {
                *error = store_error;
            }
            return false;
        }
    }

    after.margin = 0.0;
    after.position_profit = 0.0;
    if (!domain_store_->UpsertPosition(after, &store_error)) {
        if (error != nullptr) {
            *error = store_error;
        }
        return false;
    }
    if (!SyncPositionToRedis(before, after, error)) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    latest_positions_[PositionMapKey(after)] = after;
    return true;
}

std::vector<Position> PositionManager::GetCurrentPositions(const std::string& account_id) const {
    std::vector<Position> out;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [key, position] : latest_positions_) {
        (void)key;
        if (position.account_id == account_id) {
            out.push_back(position);
        }
    }
    return out;
}

bool PositionManager::ReconcilePositions(const std::string& account_id,
                                         const std::string& strategy_id,
                                         const std::string& trading_day,
                                         std::string* error) {
    (void)trading_day;
    if (domain_store_ == nullptr || redis_client_ == nullptr) {
        if (error != nullptr) {
            *error = "position manager dependencies are null";
        }
        return false;
    }
    std::vector<Position> positions;
    std::string store_error;
    if (!domain_store_->LoadPositionSummary(account_id, strategy_id, &positions, &store_error)) {
        if (error != nullptr) {
            *error = store_error;
        }
        return false;
    }
    for (const auto& position : positions) {
        const std::unordered_map<std::string, std::string> fields{
            {"long_volume", std::to_string(position.long_qty)},
            {"short_volume", std::to_string(position.short_qty)},
            {"long_today", std::to_string(position.long_today_qty)},
            {"short_today", std::to_string(position.short_today_qty)},
            {"long_yd", std::to_string(position.long_yd_qty)},
            {"short_yd", std::to_string(position.short_yd_qty)},
        };
        std::string redis_error;
        if (!redis_client_->HSet(PositionRedisKey(position.account_id, position.symbol),
                                 fields,
                                 &redis_error)) {
            if (error != nullptr) {
                *error = redis_error;
            }
            return false;
        }
    }
    return true;
}

std::string PositionManager::PositionRedisKey(const std::string& account_id,
                                              const std::string& instrument_id) {
    return "position:" + account_id + ":" + instrument_id;
}

bool PositionManager::SyncPositionToRedis(const Position& before,
                                          const Position& after,
                                          std::string* error) {
    const auto key = PositionRedisKey(after.account_id, after.symbol);
    struct DeltaField {
        const char* name;
        std::int64_t delta;
    };
    const std::vector<DeltaField> deltas{
        {"long_volume", static_cast<std::int64_t>(after.long_qty - before.long_qty)},
        {"short_volume", static_cast<std::int64_t>(after.short_qty - before.short_qty)},
        {"long_today", static_cast<std::int64_t>(after.long_today_qty - before.long_today_qty)},
        {"short_today", static_cast<std::int64_t>(after.short_today_qty - before.short_today_qty)},
        {"long_yd", static_cast<std::int64_t>(after.long_yd_qty - before.long_yd_qty)},
        {"short_yd", static_cast<std::int64_t>(after.short_yd_qty - before.short_yd_qty)},
    };

    for (const auto& delta : deltas) {
        if (delta.delta == 0) {
            continue;
        }
        std::string redis_error;
        if (!redis_client_->HIncrBy(key, delta.name, delta.delta, &redis_error)) {
            if (error != nullptr) {
                *error = redis_error;
            }
            return false;
        }
    }
    return true;
}

}  // namespace quant_hft

