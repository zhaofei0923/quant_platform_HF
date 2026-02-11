from __future__ import annotations

from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.strategy.base import StrategyBase


class DemoStrategy(StrategyBase):
    def __init__(self, strategy_id: str) -> None:
        super().__init__(strategy_id)
        self._counter = 0

    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        return []

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        self._counter += 1
        trend = state_snapshot.trend.get("score", 0.0)
        side = Side.BUY if trend >= 0 else Side.SELL
        trace_id = (
            f"{self.strategy_id}-{state_snapshot.instrument_id}"
            f"-{state_snapshot.ts_ns}-{self._counter}"
        )
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id=state_snapshot.instrument_id,
                side=side,
                offset=OffsetFlag.OPEN,
                volume=1,
                limit_price=4500.0,
                ts_ns=state_snapshot.ts_ns,
                trace_id=trace_id,
            )
        ]

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        events = ctx.setdefault("order_events", [])
        if isinstance(events, list):
            events.append(order_event)
