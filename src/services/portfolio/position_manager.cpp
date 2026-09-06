#include "quant_hft/services/position_manager.h"

#include <utility>

namespace quant_hft {
namespace {
std::string PositionMapKey(const Position& p) {
    return p.account_id + "|" + p.strategy_id + "|" + p.exchange + "|" + p.symbol + "|" +
           std::to_string(static_cast<int>(p.hedge_flag));
}
}  // namespace

PositionManager::PositionManager(std::shared_ptr<ITradingDomainStore> domain_store,
                                 std::shared_ptr<IRedisHashClient> redis_client)
    : domain_store_(std::move(domain_store)), redis_client_(std::move(redis_client)) {}

bool PositionManager::UpdatePosition(const Trade& trade, std::string* error) {
    if (domain_store_ == nullptr) {
        if (error != nullptr) *error = "null trading domain store";
        return false;
    }
    TradeApplyRequest request;
    request.trade = trade;
    request.allow_ephemeral = true;
    TradeApplyResult result;
    if (!domain_store_->ApplyTrade(request, &result, error) ||
        result.status == TradeApplyStatus::kConflict || result.status == TradeApplyStatus::kFailed)
        return false;
    return DrainOutbox(trade.account_id, error);
}

std::vector<Position> PositionManager::GetCurrentPositions(const std::string& account_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Position> out;
    for (const auto& [key, position] : latest_positions_) {
        (void)key;
        if (position.account_id == account_id) out.push_back(position);
    }
    return out;
}

bool PositionManager::ReconcilePositions(const std::string& account_id,
                                         const std::string& strategy_id,
                                         const std::string& trading_day, std::string* error) {
    (void)strategy_id;
    (void)trading_day;
    std::lock_guard<std::mutex> lock(mutex_);
    return RefreshAccountProjection(account_id, error);
}

bool PositionManager::RefreshAccountProjection(const std::string& account_id, std::string* error) {
    if (domain_store_ == nullptr || redis_client_ == nullptr) {
        if (error != nullptr) *error = "position manager dependencies are null";
        return false;
    }
    std::vector<Position> positions;
    if (!domain_store_->LoadPositionSummary(account_id, "", &positions, error)) return false;
    std::unordered_map<std::string, Position> aggregates;
    for (const auto& position : positions) {
        latest_positions_[PositionMapKey(position)] = position;
        auto& total = aggregates[position.symbol];
        total.account_id = account_id;
        total.symbol = position.symbol;
        total.long_qty += position.long_qty;
        total.short_qty += position.short_qty;
        total.long_today_qty += position.long_today_qty;
        total.short_today_qty += position.short_today_qty;
        total.long_yd_qty += position.long_yd_qty;
        total.short_yd_qty += position.short_yd_qty;
        total.version += position.version;
    }
    for (const auto& [symbol, position] : aggregates) {
        const std::unordered_map<std::string, std::string> fields{
            {"long_volume", std::to_string(position.long_qty)},
            {"short_volume", std::to_string(position.short_qty)},
            {"long_today", std::to_string(position.long_today_qty)},
            {"short_today", std::to_string(position.short_today_qty)},
            {"long_yd", std::to_string(position.long_yd_qty)},
            {"short_yd", std::to_string(position.short_yd_qty)},
            {"version", std::to_string(position.version)}};
        if (!redis_client_->HSetVersioned(PositionRedisKey(account_id, symbol), fields,
                                          position.version, error))
            return false;
    }
    return true;
}

bool PositionManager::DrainOutbox(const std::string& account_id, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (domain_store_ == nullptr) return false;
    std::vector<TradeOutboxRecord> pending;
    if (!domain_store_->LoadPendingOutbox(account_id, &pending, error)) return false;
    // Project the current snapshot, never an old delta. A retry after Redis success
    // and acknowledgement failure cannot count the fill twice.
    if (!RefreshAccountProjection(account_id, error)) return false;
    for (const auto& event : pending)
        if (!domain_store_->AcknowledgeOutbox(event.outbox_id, error)) return false;
    return true;
}

std::string PositionManager::PositionRedisKey(const std::string& account_id,
                                              const std::string& instrument_id) {
    return "position:" + account_id + ":" + instrument_id;
}
}  // namespace quant_hft
