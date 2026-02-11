#pragma once

#include <cstdint>
#include <string>

namespace quant_hft {

using EpochNanos = std::int64_t;

inline EpochNanos NowEpochNanos();

enum class Side {
    kBuy,
    kSell,
};

enum class OffsetFlag {
    kOpen,
    kClose,
    kCloseToday,
    kCloseYesterday,
};

enum class OrderType {
    kLimit,
    kMarket,
};

enum class OrderStatus {
    kNew,
    kAccepted,
    kPartiallyFilled,
    kFilled,
    kCanceled,
    kRejected,
};

enum class PositionDirection {
    kLong,
    kShort,
};

enum class RiskAction {
    kAllow,
    kReject,
    kReview,
};

struct StateDimension {
    double score{0.0};
    double confidence{0.0};
};

struct MarketSnapshot {
    std::string instrument_id;
    double last_price{0.0};
    double bid_price_1{0.0};
    double ask_price_1{0.0};
    std::int64_t bid_volume_1{0};
    std::int64_t ask_volume_1{0};
    std::int64_t volume{0};
    EpochNanos exchange_ts_ns{0};
    EpochNanos recv_ts_ns{0};
};

struct StateSnapshot7D {
    std::string instrument_id;
    StateDimension trend;
    StateDimension volatility;
    StateDimension liquidity;
    StateDimension sentiment;
    StateDimension seasonality;
    StateDimension pattern;
    StateDimension event_drive;
    EpochNanos ts_ns{0};
};

struct SignalIntent {
    std::string strategy_id;
    std::string instrument_id;
    Side side{Side::kBuy};
    OffsetFlag offset{OffsetFlag::kOpen};
    std::int32_t volume{0};
    double limit_price{0.0};
    EpochNanos ts_ns{0};
    std::string trace_id;
};

struct OrderIntent {
    std::string account_id;
    std::string client_order_id;
    std::string instrument_id;
    Side side{Side::kBuy};
    OffsetFlag offset{OffsetFlag::kOpen};
    OrderType type{OrderType::kLimit};
    std::int32_t volume{0};
    double price{0.0};
    EpochNanos ts_ns{0};
    std::string trace_id;
};

struct RiskDecision {
    RiskAction action{RiskAction::kReview};
    std::string rule_id;
    std::string rule_group{"default"};
    std::string rule_version{"v1"};
    std::string policy_id;
    std::string policy_scope;
    double observed_value{0.0};
    double threshold_value{0.0};
    std::string decision_tags;
    std::string reason;
    EpochNanos decision_ts_ns{0};
};

struct OrderEvent {
    std::string account_id;
    std::string client_order_id;
    std::string exchange_order_id;
    std::string instrument_id;
    OrderStatus status{OrderStatus::kNew};
    std::int32_t total_volume{0};
    std::int32_t filled_volume{0};
    double avg_fill_price{0.0};
    std::string reason;
    EpochNanos ts_ns{0};
    std::string trace_id;
    std::string execution_algo_id;
    std::int32_t slice_index{0};
    std::int32_t slice_total{0};
    bool throttle_applied{false};
    std::string venue;
    std::string route_id;
    double slippage_bps{0.0};
    double impact_cost{0.0};
};

struct PositionSnapshot {
    std::string account_id;
    std::string instrument_id;
    PositionDirection direction{PositionDirection::kLong};
    std::int32_t volume{0};
    double avg_price{0.0};
    double unrealized_pnl{0.0};
    double margin{0.0};
    EpochNanos ts_ns{0};
};

struct HealthEvent {
    std::string component;
    std::string level;
    std::string message;
    EpochNanos ts_ns{0};
};

}  // namespace quant_hft

#include <chrono>

namespace quant_hft {

inline EpochNanos NowEpochNanos() {
    const auto now = std::chrono::time_point_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now());
    return now.time_since_epoch().count();
}

}  // namespace quant_hft
