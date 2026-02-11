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
    execution_algo_id: str = ""
    slice_index: int = 0
    slice_total: int = 0
    throttle_applied: bool = False


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
