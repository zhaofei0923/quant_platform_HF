from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from quant_hft.contracts import OrderEvent, SignalIntent, StateSnapshot7D


class StrategyBase(ABC):
    """Base strategy interface.

    Intentionally excludes on_tick for phase-1 latency boundary enforcement.
    """

    strategy_id: str

    def __init__(self, strategy_id: str) -> None:
        self.strategy_id = strategy_id

    @abstractmethod
    def on_bar(self, ctx: dict[str, Any], bar_batch: list[dict[str, Any]]) -> list[SignalIntent]:
        raise NotImplementedError

    @abstractmethod
    def on_state(self, ctx: dict[str, Any], state_snapshot: StateSnapshot7D) -> list[SignalIntent]:
        raise NotImplementedError

    @abstractmethod
    def on_order_event(self, ctx: dict[str, Any], order_event: OrderEvent) -> None:
        raise NotImplementedError
