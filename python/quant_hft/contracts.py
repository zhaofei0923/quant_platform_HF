from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OffsetFlag(str, Enum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    CLOSE_TODAY = "CLOSE_TODAY"
    CLOSE_YESTERDAY = "CLOSE_YESTERDAY"


@dataclass(frozen=True)
class SignalIntent:
    strategy_id: str
    instrument_id: str
    side: Side
    offset: OffsetFlag
    volume: int
    limit_price: float
    ts_ns: int
    trace_id: str


@dataclass(frozen=True)
class MarketSnapshot:
    instrument_id: str
    last_price: float
    bid_price_1: float
    ask_price_1: float
    bid_volume_1: int
    ask_volume_1: int
    volume: int
    exchange_ts_ns: int
    recv_ts_ns: int
    exchange_id: str = ""
    trading_day: str = ""
    action_day: str = ""
    update_time: str = ""
    update_millisec: int = 0
    settlement_price: float = 0.0
    average_price_raw: float = 0.0
    average_price_norm: float = 0.0
    is_valid_settlement: bool = False


@dataclass(frozen=True)
class OrderEvent:
    account_id: str
    client_order_id: str
    instrument_id: str
    status: str
    total_volume: int
    filled_volume: int
    avg_fill_price: float
    reason: str
    ts_ns: int
    trace_id: str
    exchange_order_id: str = ""
    exchange_id: str = ""
    status_msg: str = ""
    order_submit_status: str = ""
    order_ref: str = ""
    front_id: int = 0
    session_id: int = 0
    trade_id: str = ""
    event_source: str = ""
    exchange_ts_ns: int = 0
    recv_ts_ns: int = 0
    execution_algo_id: str = ""
    slice_index: int = 0
    slice_total: int = 0
    throttle_applied: bool = False
    venue: str = ""
    route_id: str = ""
    slippage_bps: float = 0.0
    impact_cost: float = 0.0


@dataclass(frozen=True)
class RiskDecision:
    action: str
    rule_id: str
    rule_group: str = "default"
    rule_version: str = "v1"
    policy_id: str = ""
    policy_scope: str = ""
    observed_value: float = 0.0
    threshold_value: float = 0.0
    decision_tags: str = ""
    reason: str = ""
    decision_ts_ns: int = 0


@dataclass(frozen=True)
class StateSnapshot7D:
    instrument_id: str
    trend: dict[str, float]
    volatility: dict[str, float]
    liquidity: dict[str, float]
    sentiment: dict[str, float]
    seasonality: dict[str, float]
    pattern: dict[str, float]
    event_drive: dict[str, float]
    ts_ns: int


@dataclass(frozen=True)
class TradingAccountSnapshot:
    account_id: str
    investor_id: str
    balance: float = 0.0
    available: float = 0.0
    curr_margin: float = 0.0
    frozen_margin: float = 0.0
    frozen_cash: float = 0.0
    frozen_commission: float = 0.0
    commission: float = 0.0
    close_profit: float = 0.0
    position_profit: float = 0.0
    trading_day: str = ""
    ts_ns: int = 0
    source: str = ""


@dataclass(frozen=True)
class InvestorPositionSnapshot:
    account_id: str
    investor_id: str
    instrument_id: str
    exchange_id: str = ""
    posi_direction: str = ""
    hedge_flag: str = ""
    position_date: str = ""
    position: int = 0
    today_position: int = 0
    yd_position: int = 0
    long_frozen: int = 0
    short_frozen: int = 0
    open_volume: int = 0
    close_volume: int = 0
    position_cost: float = 0.0
    open_cost: float = 0.0
    position_profit: float = 0.0
    close_profit: float = 0.0
    margin_rate_by_money: float = 0.0
    margin_rate_by_volume: float = 0.0
    use_margin: float = 0.0
    ts_ns: int = 0
    source: str = ""


@dataclass(frozen=True)
class BrokerTradingParamsSnapshot:
    account_id: str
    investor_id: str
    margin_price_type: str = ""
    algorithm: str = ""
    ts_ns: int = 0
    source: str = ""


@dataclass(frozen=True)
class InstrumentMetaSnapshot:
    instrument_id: str
    exchange_id: str = ""
    product_id: str = ""
    volume_multiple: int = 0
    price_tick: float = 0.0
    max_margin_side_algorithm: bool = False
    ts_ns: int = 0
    source: str = ""


@dataclass(frozen=True)
class ResearchRunContract:
    dataset_version: str = "local"
    factor_set_version: str = "default"
    experiment_id: str = ""
    attribution: dict[str, float] | None = None
    risk_decomposition: dict[str, float] | None = None
