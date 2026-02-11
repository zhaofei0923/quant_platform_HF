#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "quant_hft/interfaces/realtime_cache.h"

namespace quant_hft {

class RedisKeyBuilder {
public:
    static std::string OrderInfo(const std::string& order_id);
    static std::string MarketTickLatest(const std::string& instrument_id);
    static std::string StateSnapshot7DLatest(const std::string& instrument_id);
    static std::string Position(const std::string& account_id,
                                const std::string& instrument_id,
                                PositionDirection direction);
};

class RedisRealtimeStore : public IRealtimeCache {
public:
    void UpsertMarketSnapshot(const MarketSnapshot& snapshot) override;
    void UpsertOrderEvent(const OrderEvent& event) override;
    void UpsertPositionSnapshot(const PositionSnapshot& position) override;
    void UpsertStateSnapshot7D(const StateSnapshot7D& snapshot) override;

    bool GetMarketSnapshot(const std::string& instrument_id,
                           MarketSnapshot* out) const override;
    bool GetOrderEvent(const std::string& client_order_id,
                       OrderEvent* out) const override;
    bool GetPositionSnapshot(const std::string& account_id,
                             const std::string& instrument_id,
                             PositionDirection direction,
                             PositionSnapshot* out) const override;
    bool GetStateSnapshot7D(const std::string& instrument_id,
                            StateSnapshot7D* out) const override;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, MarketSnapshot> market_snapshots_;
    std::unordered_map<std::string, OrderEvent> order_events_;
    std::unordered_map<std::string, PositionSnapshot> position_snapshots_;
    std::unordered_map<std::string, StateSnapshot7D> state_snapshots7d_;
};

}  // namespace quant_hft
