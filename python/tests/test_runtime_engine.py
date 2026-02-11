from __future__ import annotations

from pathlib import Path

import pytest
from quant_hft.backtest.replay import BacktestRunSpec, run_backtest_spec
from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.strategy.base import BACKTEST_CTX_REQUIRED_KEYS, StrategyBase


class DemoStrategy(StrategyBase):
    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id="SHFE.ag2406",
                side=Side.BUY,
                offset=OffsetFlag.OPEN,
                volume=1,
                limit_price=4500.0,
                ts_ns=1,
                trace_id="t1",
            )
        ]

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        return []

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        ctx["events"] = int(ctx.get("events", 0)) + 1


def test_runtime_collects_intents() -> None:
    runtime = StrategyRuntime()
    runtime.add_strategy(DemoStrategy("demo"))

    intents = runtime.on_bar({}, [{"instrument_id": "SHFE.ag2406"}])
    assert len(intents) == 1
    assert intents[0].strategy_id == "demo"


def test_runtime_forwards_order_event() -> None:
    runtime = StrategyRuntime()
    runtime.add_strategy(DemoStrategy("demo"))

    ctx: dict[str, object] = {}
    runtime.on_order_event(
        ctx,
        OrderEvent(
            account_id="a1",
            client_order_id="c1",
            instrument_id="SHFE.ag2406",
            status="FILLED",
            total_volume=1,
            filled_volume=1,
            avg_fill_price=4500.0,
            reason="",
            ts_ns=1,
            trace_id="t1",
        ),
    )
    assert ctx["events"] == 1


def test_state_dispatch() -> None:
    runtime = StrategyRuntime()
    runtime.add_strategy(DemoStrategy("demo"))

    intents = runtime.on_state(
        {},
        StateSnapshot7D(
            instrument_id="SHFE.ag2406",
            trend={"score": 0.1, "confidence": 0.8},
            volatility={"score": 0.2, "confidence": 0.8},
            liquidity={"score": 0.3, "confidence": 0.8},
            sentiment={"score": 0.4, "confidence": 0.8},
            seasonality={"score": 0.0, "confidence": 0.2},
            pattern={"score": 0.1, "confidence": 0.3},
            event_drive={"score": 0.0, "confidence": 0.2},
            ts_ns=1,
        ),
    )
    assert intents == []


class ContextProbeStrategy(StrategyBase):
    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        ctx["observed_keys"] = sorted(ctx.keys())
        return []

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        return []

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        return None


class InvalidIntentStrategy(StrategyBase):
    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        return [
            SignalIntent(
                strategy_id="",
                instrument_id="",
                side=Side.BUY,
                offset=OffsetFlag.OPEN,
                volume=0,
                limit_price=1.0,
                ts_ns=1,
                trace_id="",
            )
        ]

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        return []

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        return None


def test_backtest_runtime_context_contains_required_keys(tmp_path: Path) -> None:
    csv_path = tmp_path / "ctx_probe.csv"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,4100.0,1,4099.0,10,4101.0,12,4100.0,0.0,100",
                "20230103,rb2305,09:01:01,0,4101.0,2,4100.0,10,4102.0,12,4100.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )
    runtime = StrategyRuntime()
    runtime.add_strategy(ContextProbeStrategy("ctx"))
    ctx: dict[str, object] = {}
    run_backtest_spec(
        BacktestRunSpec(
            csv_path=str(csv_path),
            max_ticks=100,
            deterministic_fills=False,
            run_id="ctx-run",
        ),
        runtime,
        ctx=ctx,
    )

    observed = set(ctx["observed_keys"])  # type: ignore[index]
    for key in BACKTEST_CTX_REQUIRED_KEYS:
        assert key in observed


def test_runtime_rejects_invalid_signal_intent_contract() -> None:
    runtime = StrategyRuntime()
    runtime.add_strategy(InvalidIntentStrategy("bad"))
    with pytest.raises(ValueError):
        runtime.on_bar({}, [{"instrument_id": "SHFE.ag2406"}])
