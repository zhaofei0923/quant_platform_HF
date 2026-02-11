from __future__ import annotations

from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.runtime.redis_hash import InMemoryRedisHashClient
from quant_hft.runtime.redis_schema import order_event_key, state_snapshot_key, strategy_intent_key
from quant_hft.runtime.strategy_runner import StrategyRunner
from quant_hft.strategy.base import StrategyBase


class RunnerStrategy(StrategyBase):
    def __init__(self, strategy_id: str) -> None:
        super().__init__(strategy_id)
        self.events: list[OrderEvent] = []

    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        return []

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id=state_snapshot.instrument_id,
                side=Side.BUY,
                offset=OffsetFlag.OPEN,
                volume=2,
                limit_price=4500.0,
                ts_ns=state_snapshot.ts_ns,
                trace_id=f"{self.strategy_id}-trace-{state_snapshot.ts_ns}",
            )
        ]

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        self.events.append(order_event)


def _seed_state(redis: InMemoryRedisHashClient, instrument_id: str, ts_ns: int) -> None:
    redis.hset(
        state_snapshot_key(instrument_id),
        {
            "instrument_id": instrument_id,
            "trend_score": "0.1",
            "trend_confidence": "0.9",
            "volatility_score": "0.2",
            "volatility_confidence": "0.8",
            "liquidity_score": "0.3",
            "liquidity_confidence": "0.7",
            "sentiment_score": "0.4",
            "sentiment_confidence": "0.6",
            "seasonality_score": "0.0",
            "seasonality_confidence": "0.2",
            "pattern_score": "0.1",
            "pattern_confidence": "0.3",
            "event_drive_score": "0.0",
            "event_drive_confidence": "0.2",
            "ts_ns": str(ts_ns),
        },
    )


def test_strategy_runner_emits_intent_batch_and_reads_order_event() -> None:
    runtime = StrategyRuntime()
    strategy = RunnerStrategy("demo")
    runtime.add_strategy(strategy)

    redis = InMemoryRedisHashClient()
    _seed_state(redis, "SHFE.ag2406", 101)

    runner = StrategyRunner(
        runtime=runtime,
        redis_client=redis,
        strategy_id="demo",
        instruments=["SHFE.ag2406"],
        poll_interval_ms=200,
    )

    emitted = runner.run_once()
    assert emitted == 1

    intent_fields = redis.hgetall(strategy_intent_key("demo"))
    assert intent_fields["seq"] == "1"
    assert intent_fields["count"] == "1"
    intent_payload = intent_fields["intent_0"]
    parts = intent_payload.split("|")
    assert len(parts) == 7
    assert parts[0] == "SHFE.ag2406"
    assert parts[1] == "BUY"
    assert parts[2] == "OPEN"
    trace_id = parts[6]

    redis.hset(
        order_event_key(trace_id),
        {
            "account_id": "sim-account",
            "client_order_id": trace_id,
            "instrument_id": "SHFE.ag2406",
            "status": "ACCEPTED",
            "total_volume": "2",
            "filled_volume": "0",
            "avg_fill_price": "0.0",
            "reason": "",
            "ts_ns": "102",
            "trace_id": trace_id,
        },
    )

    emitted_second = runner.run_once()
    assert emitted_second == 0
    assert len(strategy.events) == 1
    assert strategy.events[0].status == "ACCEPTED"
