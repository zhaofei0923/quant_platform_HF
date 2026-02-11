from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D

BACKTEST_CTX_REQUIRED_KEYS = ("run_id", "mode", "clock_ns", "metrics", "artifacts")


class StrategyTemplate(str, Enum):
    TREND = "trend"
    ARBITRAGE = "arbitrage"
    MARKET_MAKING = "market_making"


def ensure_backtest_ctx(
    ctx: dict[str, Any],
    *,
    run_id: str,
    mode: str,
    clock_ns: int,
) -> None:
    """Populate required backtest context keys with deterministic defaults."""
    ctx.setdefault("run_id", run_id)
    ctx.setdefault("mode", mode)
    ctx.setdefault("clock_ns", clock_ns)
    ctx.setdefault("metrics", {})
    ctx.setdefault("artifacts", {})


def validate_signal_intents(intents: list[SignalIntent]) -> None:
    """Validate strategy outputs against SignalIntent contract constraints."""
    for index, intent in enumerate(intents):
        if not intent.strategy_id:
            raise ValueError(f"intent[{index}] strategy_id is required")
        if not intent.instrument_id:
            raise ValueError(f"intent[{index}] instrument_id is required")
        if not isinstance(intent.side, Side):
            raise ValueError(f"intent[{index}] side must be Side enum")
        if not isinstance(intent.offset, OffsetFlag):
            raise ValueError(f"intent[{index}] offset must be OffsetFlag enum")
        if intent.volume <= 0:
            raise ValueError(f"intent[{index}] volume must be positive")
        if intent.trace_id == "":
            raise ValueError(f"intent[{index}] trace_id is required")


class StrategyBase(ABC):
    """Base strategy interface.

    Intentionally excludes on_tick for phase-1 latency boundary enforcement.
    """

    strategy_id: str

    def __init__(
        self, strategy_id: str, template: StrategyTemplate = StrategyTemplate.TREND
    ) -> None:
        self.strategy_id = strategy_id
        self.template = template

    @abstractmethod
    def on_bar(self, ctx: dict[str, Any], bar_batch: list[dict[str, Any]]) -> list[SignalIntent]:
        raise NotImplementedError

    @abstractmethod
    def on_state(self, ctx: dict[str, Any], state_snapshot: StateSnapshot7D) -> list[SignalIntent]:
        raise NotImplementedError

    @abstractmethod
    def on_order_event(self, ctx: dict[str, Any], order_event: OrderEvent) -> None:
        raise NotImplementedError
