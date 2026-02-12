from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime as DateTime
from datetime import timezone as DateTimeZone
from decimal import Decimal
from enum import Enum


def _utc_now() -> DateTime:
    return DateTime.now(DateTimeZone.utc)


@dataclass(frozen=True)
class Exchange:
    id: str
    name: str = ""


@dataclass(frozen=True)
class Instrument:
    symbol: str
    exchange: Exchange
    product_id: str
    contract_multiplier: int
    price_tick: Decimal
    margin_rate: Decimal
    commission_rate: Decimal
    commission_type: str
    close_today_commission_rate: Decimal | None = None


@dataclass(frozen=True)
class Tick:
    symbol: str
    exchange: str
    datetime: DateTime
    exchange_timestamp: DateTime | None
    last_price: Decimal
    last_volume: int
    ask_price1: Decimal
    ask_volume1: int
    bid_price1: Decimal
    bid_volume1: int
    volume: int
    turnover: Decimal
    open_interest: int
    ask_prices: list[Decimal] = field(default_factory=list)
    ask_volumes: list[int] = field(default_factory=list)
    bid_prices: list[Decimal] = field(default_factory=list)
    bid_volumes: list[int] = field(default_factory=list)


@dataclass(frozen=True)
class Bar:
    symbol: str
    exchange: str
    timeframe: str
    datetime: DateTime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    turnover: Decimal
    open_interest: int


class Direction(str, Enum):
    BUY = "buy"
    SELL = "sell"


class Offset(str, Enum):
    OPEN = "open"
    CLOSE = "close"
    CLOSE_TODAY = "close_today"
    CLOSE_YESTERDAY = "close_yesterday"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    FAK = "fak"
    FOK = "fok"


class OrderStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIAL_FILLED = "partial_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


@dataclass
class Order:
    order_id: str
    strategy_id: str
    symbol: str
    exchange: str
    direction: Direction
    offset: Offset
    order_type: OrderType
    price: Decimal | None
    quantity: int
    filled_quantity: int = 0
    avg_fill_price: Decimal = Decimal("0")
    status: OrderStatus = OrderStatus.PENDING
    created_at: DateTime | None = None
    updated_at: DateTime | None = None
    commission: Decimal = Decimal("0")
    message: str | None = None
    account_id: str = ""


@dataclass(frozen=True)
class Trade:
    trade_id: str
    order_id: str
    strategy_id: str
    symbol: str
    exchange: str
    direction: Direction
    offset: Offset
    price: Decimal
    quantity: int
    trade_time: DateTime
    commission: Decimal
    profit: Decimal = Decimal("0")
    account_id: str = ""


@dataclass
class Position:
    symbol: str
    exchange: str
    strategy_id: str
    account_id: str
    long_qty: int = 0
    short_qty: int = 0
    long_today_qty: int = 0
    short_today_qty: int = 0
    long_yd_qty: int = 0
    short_yd_qty: int = 0
    avg_long_price: Decimal = Decimal("0")
    avg_short_price: Decimal = Decimal("0")
    position_profit: Decimal = Decimal("0")
    margin: Decimal = Decimal("0")
    update_time: DateTime = field(default_factory=_utc_now)


@dataclass
class Account:
    account_id: str
    balance: Decimal = Decimal("0")
    available: Decimal = Decimal("0")
    margin: Decimal = Decimal("0")
    commission: Decimal = Decimal("0")
    position_profit: Decimal = Decimal("0")
    close_profit: Decimal = Decimal("0")
    risk_degree: float = 0.0
    update_time: DateTime = field(default_factory=_utc_now)
