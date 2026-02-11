from __future__ import annotations

from pathlib import Path

import pytest
from quant_hft.backtest.replay import (
    BacktestRunSpec,
    build_backtest_spec_from_template,
    load_backtest_run_spec,
    replay_csv_minute_bars,
    run_backtest_spec,
)
from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.strategy.base import BACKTEST_CTX_REQUIRED_KEYS, StrategyBase


class EmitPerBarStrategy(StrategyBase):
    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        bar = bar_batch[0]
        price = float(bar["close"])
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id=str(bar["instrument_id"]),
                side=Side.BUY,
                offset=OffsetFlag.OPEN,
                volume=1,
                limit_price=price,
                ts_ns=int(bar["ts_ns"]),
                trace_id=f"{self.strategy_id}-{bar['minute']}",
            )
        ]

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        return []

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        return None


def test_replay_emits_minute_bars_and_intents(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,4100.0,1,4099.0,10,4101.0,12,4100.0,0.0,100",
                "20230103,rb2305,09:00:30,0,4102.0,2,4101.0,11,4103.0,13,4101.0,0.0,100",
                "20230103,rb2305,09:01:02,0,4103.0,5,4102.0,10,4104.0,15,4102.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )

    runtime = StrategyRuntime()
    runtime.add_strategy(EmitPerBarStrategy("s1"))

    report = replay_csv_minute_bars(csv_path, runtime, ctx={})
    assert report.ticks_read == 3
    assert report.bars_emitted == 2
    assert report.intents_emitted == 2
    assert report.first_instrument == "rb2305"
    assert report.last_instrument == "rb2305"


def test_replay_smoke_on_real_backtest_file_if_present() -> None:
    data_path = Path("backtest_data/rb.csv")
    if not data_path.exists():
        pytest.skip("backtest_data/rb.csv not found")

    runtime = StrategyRuntime()
    report = replay_csv_minute_bars(data_path, runtime, ctx={}, max_ticks=2000)
    assert report.ticks_read == 2000
    assert report.bars_emitted > 0
    assert report.first_instrument.lower().startswith("rb")


def test_run_backtest_spec_is_reproducible_and_populates_ctx(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample_spec.csv"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,4100.0,1,4099.0,10,4101.0,12,4100.0,0.0,100",
                "20230103,rb2305,09:01:02,0,4103.0,5,4102.0,10,4104.0,15,4102.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )
    spec = BacktestRunSpec(
        csv_path=str(csv_path),
        max_ticks=100,
        deterministic_fills=True,
        run_id="spec-run-1",
    )

    runtime1 = StrategyRuntime()
    runtime1.add_strategy(EmitPerBarStrategy("s1"))
    ctx1: dict[str, object] = {}
    result1 = run_backtest_spec(spec, runtime1, ctx=ctx1)

    runtime2 = StrategyRuntime()
    runtime2.add_strategy(EmitPerBarStrategy("s1"))
    ctx2: dict[str, object] = {}
    result2 = run_backtest_spec(spec, runtime2, ctx=ctx2)

    assert result1.input_signature == result2.input_signature
    assert result1.to_dict() == result2.to_dict()
    assert result1.run_id == "spec-run-1"
    assert result1.deterministic is not None
    assert result1.deterministic.performance.total_pnl == pytest.approx(3.0)
    assert result1.deterministic.performance.order_status_counts["FILLED"] == 2
    deterministic_payload = result1.to_dict()["deterministic"]
    assert isinstance(deterministic_payload, dict)
    assert "performance" in deterministic_payload
    assert deterministic_payload["performance"]["total_pnl"] == pytest.approx(3.0)
    for key in BACKTEST_CTX_REQUIRED_KEYS:
        assert key in ctx1
        assert key in ctx2


def test_load_backtest_run_spec_from_json(tmp_path: Path) -> None:
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(
        '{"csv_path":"data.csv","max_ticks":1000,"deterministic_fills":false,"run_id":"spec-1"}',
        encoding="utf-8",
    )
    spec = load_backtest_run_spec(spec_path)
    assert spec.csv_path == "data.csv"
    assert spec.max_ticks == 1000
    assert spec.deterministic_fills is False
    assert spec.run_id == "spec-1"


def test_build_backtest_spec_from_template(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    spec = build_backtest_spec_from_template(
        "deterministic_regression",
        csv_path="backtest_data/rb.csv",
        run_id="reg-001",
        wal_dir=wal_dir,
    )
    assert spec.csv_path == "backtest_data/rb.csv"
    assert spec.max_ticks == 20000
    assert spec.deterministic_fills is True
    assert spec.run_id == "reg-001"
    assert spec.wal_path is not None
    assert spec.wal_path.endswith("reg-001.wal")


def test_build_backtest_spec_from_unknown_template_raises() -> None:
    with pytest.raises(ValueError):
        build_backtest_spec_from_template("unknown-template", csv_path="backtest_data/rb.csv")
