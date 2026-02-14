from __future__ import annotations

from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.runtime.redis_hash import InMemoryRedisHashClient
from quant_hft.runtime.redis_schema import (
    order_event_key,
    state_snapshot_key,
    strategy_bar_key,
    strategy_intent_key,
)
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


class BarOnlyStrategy(StrategyBase):
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
                trace_id=f"{self.strategy_id}-bar-{bar['ts_ns']}",
            )
        ]

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        return []

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        _ = ctx
        _ = order_event


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
    assert strategy.events[0].execution_algo_id == ""
    assert strategy.events[0].slice_index == 0
    assert strategy.events[0].slice_total == 0
    assert strategy.events[0].throttle_applied is False
    assert strategy.events[0].venue == ""
    assert strategy.events[0].route_id == ""
    assert strategy.events[0].slippage_bps == 0.0
    assert strategy.events[0].impact_cost == 0.0


def test_strategy_runner_reads_optional_execution_metadata_from_order_event() -> None:
    runtime = StrategyRuntime()
    strategy = RunnerStrategy("demo")
    runtime.add_strategy(strategy)

    redis = InMemoryRedisHashClient()
    _seed_state(redis, "SHFE.ag2406", 201)

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
    trace_id = intent_fields["intent_0"].split("|")[6]

    redis.hset(
        order_event_key(trace_id),
        {
            "account_id": "sim-account",
            "client_order_id": trace_id,
            "instrument_id": "SHFE.ag2406",
            "status": "ACCEPTED",
            "total_volume": "2",
            "filled_volume": "1",
            "avg_fill_price": "4500.5",
            "reason": "",
            "ts_ns": "202",
            "trace_id": trace_id,
            "execution_algo_id": "twap",
            "slice_index": "2",
            "slice_total": "5",
            "throttle_applied": "1",
            "venue": "SIM",
            "route_id": "route-sim-1",
            "slippage_bps": "1.25",
            "impact_cost": "8.5",
        },
    )

    emitted_second = runner.run_once()
    assert emitted_second == 0
    assert len(strategy.events) == 1
    assert strategy.events[0].execution_algo_id == "twap"
    assert strategy.events[0].slice_index == 2
    assert strategy.events[0].slice_total == 5
    assert strategy.events[0].throttle_applied is True
    assert strategy.events[0].venue == "SIM"
    assert strategy.events[0].route_id == "route-sim-1"
    assert strategy.events[0].slippage_bps == 1.25
    assert strategy.events[0].impact_cost == 8.5


def test_strategy_runner_consumes_bar_and_emits_intent() -> None:
    runtime = StrategyRuntime()
    strategy = BarOnlyStrategy("demo")
    runtime.add_strategy(strategy)

    redis = InMemoryRedisHashClient()
    redis.hset(
        strategy_bar_key("demo", "SHFE.ag2406"),
        {
            "instrument_id": "SHFE.ag2406",
            "exchange": "SHFE",
            "timeframe": "1m",
            "ts_ns": "301",
            "open": "4500.0",
            "high": "4502.0",
            "low": "4498.0",
            "close": "4501.0",
            "volume": "7",
            "turnover": "0.0",
            "open_interest": "0",
        },
    )

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
    assert intent_fields["count"] == "1"
    parts = intent_fields["intent_0"].split("|")
    assert parts[0] == "SHFE.ag2406"
    assert parts[4] == "4501.0"


def test_strategy_runner_dispatches_same_instrument_bar_to_multiple_strategies() -> None:
    redis = InMemoryRedisHashClient()

    runtime_s1 = StrategyRuntime()
    runtime_s1.add_strategy(BarOnlyStrategy("s1"))
    runtime_s2 = StrategyRuntime()
    runtime_s2.add_strategy(BarOnlyStrategy("s2"))

    redis.hset(
        strategy_bar_key("s1", "SHFE.ag2406"),
        {
            "instrument_id": "SHFE.ag2406",
            "exchange": "SHFE",
            "timeframe": "1m",
            "ts_ns": "401",
            "open": "5000.0",
            "high": "5005.0",
            "low": "4998.0",
            "close": "5001.0",
            "volume": "11",
            "turnover": "0.0",
            "open_interest": "0",
        },
    )
    redis.hset(
        strategy_bar_key("s2", "SHFE.ag2406"),
        {
            "instrument_id": "SHFE.ag2406",
            "exchange": "SHFE",
            "timeframe": "1m",
            "ts_ns": "402",
            "open": "5001.0",
            "high": "5006.0",
            "low": "4999.0",
            "close": "5002.0",
            "volume": "12",
            "turnover": "0.0",
            "open_interest": "0",
        },
    )

    runner_s1 = StrategyRunner(
        runtime=runtime_s1,
        redis_client=redis,
        strategy_id="s1",
        instruments=["SHFE.ag2406"],
        poll_interval_ms=100,
    )
    runner_s2 = StrategyRunner(
        runtime=runtime_s2,
        redis_client=redis,
        strategy_id="s2",
        instruments=["SHFE.ag2406"],
        poll_interval_ms=100,
    )

    assert runner_s1.run_once() == 1
    assert runner_s2.run_once() == 1

    payload_s1 = redis.hgetall(strategy_intent_key("s1"))
    payload_s2 = redis.hgetall(strategy_intent_key("s2"))
    assert payload_s1["count"] == "1"
    assert payload_s2["count"] == "1"
    assert payload_s1["intent_0"].split("|")[6].startswith("s1-bar-")
    assert payload_s2["intent_0"].split("|")[6].startswith("s2-bar-")
