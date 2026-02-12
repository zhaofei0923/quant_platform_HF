from __future__ import annotations

from abc import ABC, abstractmethod

from quant_hft.runtime.unified.models import Bar, Order, Tick, Trade


class Strategy(ABC):
    def __init__(self, strategy_id: str) -> None:
        self.strategy_id = strategy_id
        self.data: object | None = None
        self.broker: object | None = None
        self.clock: object | None = None

    @abstractmethod
    def initialize(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def on_tick(self, tick: Tick) -> None:
        raise NotImplementedError

    @abstractmethod
    def on_bar(self, bar: Bar) -> None:
        raise NotImplementedError

    def on_order(self, order: Order) -> None:  # pragma: no cover - optional hook
        del order

    def on_trade(self, trade: Trade) -> None:  # pragma: no cover - optional hook
        del trade
