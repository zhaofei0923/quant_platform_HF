"""quant_hft Python runtime package."""

from .contracts import (
    Account,
    Bar,
    Order,
    OrderEvent,
    OrderIntent,
    Position,
    SignalIntent,
    StateSnapshot7D,
    Tick,
    Trade,
)

try:
    from .data_feed import BacktestDataFeed, LiveDataFeed, Timestamp
except ModuleNotFoundError:  # pragma: no cover
    BacktestDataFeed = None  # type: ignore[assignment]
    LiveDataFeed = None  # type: ignore[assignment]
    Timestamp = None  # type: ignore[assignment]

try:
    from .strategy import Strategy
except (ImportError, AttributeError):  # pragma: no cover
    from .strategy import StrategyBase as Strategy

__all__ = [
    "Account",
    "Bar",
    "Order",
    "OrderEvent",
    "OrderIntent",
    "Position",
    "SignalIntent",
    "StateSnapshot7D",
    "Tick",
    "Trade",
    "BacktestDataFeed",
    "LiveDataFeed",
    "Timestamp",
    "Strategy",
]
