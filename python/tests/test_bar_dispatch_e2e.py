from __future__ import annotations

from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.runtime.redis_hash import InMemoryRedisHashClient
from quant_hft.runtime.redis_schema import strategy_bar_key, strategy_intent_key
from quant_hft.runtime.strategy_runner import StrategyRunner
from quant_hft.strategy.base import StrategyBase


class _BarEchoStrategy(StrategyBase):
    def __init__(self, strategy_id: str) -> None:
        super().__init__(strategy_id)

    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        if not bar_batch:
            return []
        bar = bar_batch[0]
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id=str(bar["instrument_id"]),
                side=Side.BUY,
                offset=OffsetFlag.OPEN,
                volume=1,
                limit_price=float(bar["close"]),
                ts_ns=int(bar["ts_ns"]),
                trace_id=f"{self.strategy_id}-e2e-{bar['ts_ns']}",
            )
        ]

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        return []

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        _ = ctx
        _ = order_event


def test_core_engine_bar_payload_can_drive_python_strategy_runner() -> None:
    runtime = StrategyRuntime()
    runtime.add_strategy(_BarEchoStrategy("demo"))

    redis = InMemoryRedisHashClient()

    redis.hset(
        strategy_bar_key("demo", "SHFE.ag2406"),
        {
            "instrument_id": "SHFE.ag2406",
            "exchange": "SHFE",
            "timeframe": "1m",
            "ts_ns": "901",
            "open": "5200.0",
            "high": "5208.0",
            "low": "5199.0",
            "close": "5203.5",
            "volume": "18",
            "turnover": "0.0",
            "open_interest": "0",
        },
    )

    runner = StrategyRunner(
        runtime=runtime,
        redis_client=redis,
        strategy_id="demo",
        instruments=["SHFE.ag2406"],
        poll_interval_ms=50,
    )

    emitted = runner.run_once()
    assert emitted == 1

    payload = redis.hgetall(strategy_intent_key("demo"))
    assert payload["count"] == "1"
    parts = payload["intent_0"].split("|")
    assert parts[0] == "SHFE.ag2406"
    assert parts[1] == "BUY"
    assert parts[2] == "OPEN"
    assert parts[4] == "5203.5"
    assert parts[6] == "demo-e2e-901"
