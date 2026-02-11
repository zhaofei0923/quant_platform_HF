from __future__ import annotations

from dataclasses import dataclass, field

from quant_hft.contracts import OrderEvent, SignalIntent, StateSnapshot7D
from quant_hft.strategy.base import StrategyBase


@dataclass
class StrategyRuntime:
    strategies: list[StrategyBase] = field(default_factory=list)

    def add_strategy(self, strategy: StrategyBase) -> None:
        self.strategies.append(strategy)

    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        intents: list[SignalIntent] = []
        for strategy in self.strategies:
            intents.extend(strategy.on_bar(ctx, bar_batch))
        return intents

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        intents: list[SignalIntent] = []
        for strategy in self.strategies:
            intents.extend(strategy.on_state(ctx, state_snapshot))
        return intents

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        for strategy in self.strategies:
            strategy.on_order_event(ctx, order_event)
