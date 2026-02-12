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

enum class HedgeFlag {
    kSpeculation,
    kHedge,
    kArbitrage,
};

enum class OrderType {
    kLimit,
    kMarket,
};

enum class TimeCondition {
    kGFD,
    kIOC,
    kGTC,
};

enum class VolumeCondition {
    kAV,
    kMV,
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

struct Exchange {
    std::string id;
    std::string name;
};

struct Instrument {
    std::string symbol;
    std::string exchange_id;
    std::string product_id;
    std::int32_t contract_multiplier{0};
    double price_tick{0.0};
    double margin_rate{0.0};
    double commission_rate{0.0};
    std::string commission_type;
    double close_today_commission_rate{0.0};
};

struct Tick {
    std::string symbol;
    std::string exchange;
    EpochNanos ts_ns{0};
    EpochNanos exchange_ts_ns{0};
    double last_price{0.0};
    std::int32_t last_volume{0};
    double ask_price1{0.0};
    std::int32_t ask_volume1{0};
    double bid_price1{0.0};
    std::int32_t bid_volume1{0};
    std::int64_t volume{0};
    double turnover{0.0};
    std::int64_t open_interest{0};
};

struct Bar {
    std::string symbol;
    std::string exchange;
    std::string timeframe;
    EpochNanos ts_ns{0};
    double open{0.0};
    double high{0.0};
    double low{0.0};
    double close{0.0};
    std::int64_t volume{0};
    double turnover{0.0};
    std::int64_t open_interest{0};
};

struct Order {
    std::string order_id;
    std::string account_id;
    std::string strategy_id;
    std::string symbol;
    std::string exchange;
    Side side{Side::kBuy};
    OffsetFlag offset{OffsetFlag::kOpen};
    OrderType order_type{OrderType::kLimit};
    double price{0.0};
    std::int32_t quantity{0};
    std::int32_t filled_quantity{0};
    double avg_fill_price{0.0};
    OrderStatus status{OrderStatus::kNew};
    EpochNanos created_at_ns{0};
    EpochNanos updated_at_ns{0};
    double commission{0.0};
    std::string message;
};

struct Trade {
    std::string trade_id;
    std::string order_id;
    std::string account_id;
    std::string strategy_id;
    std::string symbol;
    std::string exchange;
    Side side{Side::kBuy};
    OffsetFlag offset{OffsetFlag::kOpen};
    double price{0.0};
    std::int32_t quantity{0};
    EpochNanos trade_ts_ns{0};
    double commission{0.0};
    double profit{0.0};
};

struct Position {
    std::string symbol;
    std::string exchange;
    std::string strategy_id;
    std::string account_id;
    std::int32_t long_qty{0};
    std::int32_t short_qty{0};
    std::int32_t long_today_qty{0};
    std::int32_t short_today_qty{0};
    std::int32_t long_yd_qty{0};
    std::int32_t short_yd_qty{0};
    double avg_long_price{0.0};
    double avg_short_price{0.0};
    double position_profit{0.0};
    double margin{0.0};
    EpochNanos update_time_ns{0};
};

struct Account {
    std::string account_id;
    double balance{0.0};
    double available{0.0};
    double margin{0.0};
    double commission{0.0};
    double position_profit{0.0};
    double close_profit{0.0};
    double risk_degree{0.0};
    EpochNanos update_time_ns{0};
};

struct MarketSnapshot {
    std::string instrument_id;
    std::string exchange_id;
    std::string trading_day;
    std::string action_day;
    std::string update_time;
    std::int32_t update_millisec{0};
    double last_price{0.0};
    double bid_price_1{0.0};
    double ask_price_1{0.0};
    std::int64_t bid_volume_1{0};
    std::int64_t ask_volume_1{0};
    std::int64_t volume{0};
    double settlement_price{0.0};
    double average_price_raw{0.0};
    double average_price_norm{0.0};
    bool is_valid_settlement{false};
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
    std::string strategy_id;
    std::string instrument_id;
    Side side{Side::kBuy};
    OffsetFlag offset{OffsetFlag::kOpen};
    HedgeFlag hedge_flag{HedgeFlag::kSpeculation};
    OrderType type{OrderType::kLimit};
    TimeCondition time_condition{TimeCondition::kGFD};
    VolumeCondition volume_condition{VolumeCondition::kAV};
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
    std::string exchange_id;
    Side side{Side::kBuy};
    OffsetFlag offset{OffsetFlag::kOpen};
    OrderStatus status{OrderStatus::kNew};
    std::int32_t total_volume{0};
    std::int32_t filled_volume{0};
    double avg_fill_price{0.0};
    std::string reason;
    std::string status_msg;
    std::string order_submit_status;
    std::string order_ref;
    std::int32_t front_id{0};
    std::int32_t session_id{0};
    std::string trade_id;
    std::string event_source;
    EpochNanos exchange_ts_ns{0};
    EpochNanos recv_ts_ns{0};
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

struct TradingAccountSnapshot {
    std::string account_id;
    std::string investor_id;
    double balance{0.0};
    double available{0.0};
    double curr_margin{0.0};
    double frozen_margin{0.0};
    double frozen_cash{0.0};
    double frozen_commission{0.0};
    double commission{0.0};
    double close_profit{0.0};
    double position_profit{0.0};
    std::string trading_day;
    EpochNanos ts_ns{0};
    std::string source;
};

struct InvestorPositionSnapshot {
    std::string account_id;
    std::string investor_id;
    std::string instrument_id;
    std::string exchange_id;
    std::string posi_direction;
    std::string hedge_flag;
    std::string position_date;
    std::int32_t position{0};
    std::int32_t today_position{0};
    std::int32_t yd_position{0};
    std::int32_t long_frozen{0};
    std::int32_t short_frozen{0};
    std::int32_t open_volume{0};
    std::int32_t close_volume{0};
    double position_cost{0.0};
    double open_cost{0.0};
    double position_profit{0.0};
    double close_profit{0.0};
    double margin_rate_by_money{0.0};
    double margin_rate_by_volume{0.0};
    double use_margin{0.0};
    EpochNanos ts_ns{0};
    std::string source;
};

struct BrokerTradingParamsSnapshot {
    std::string account_id;
    std::string investor_id;
    std::string margin_price_type;
    std::string algorithm;
    EpochNanos ts_ns{0};
    std::string source;
};

struct InstrumentMetaSnapshot {
    std::string instrument_id;
    std::string exchange_id;
    std::string product_id;
    std::int32_t volume_multiple{0};
    double price_tick{0.0};
    bool max_margin_side_algorithm{false};
    EpochNanos ts_ns{0};
    std::string source;
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
