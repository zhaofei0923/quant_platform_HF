from __future__ import annotations

import itertools
import json
from pathlib import Path

import pytest
from quant_hft.backtest.replay import (
    BacktestRunSpec,
    build_backtest_spec_from_template,
    iter_parquet_ticks,
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


class StateOnlyStrategy(StrategyBase):
    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        return []

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        observed = ctx.setdefault("state_observed", [])
        assert isinstance(observed, list)
        observed.append(state_snapshot)
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id=state_snapshot.instrument_id,
                side=Side.BUY,
                offset=OffsetFlag.OPEN,
                volume=1,
                limit_price=state_snapshot.trend["score"] + 4000.0,
                ts_ns=state_snapshot.ts_ns,
                trace_id=f"{self.strategy_id}-{state_snapshot.ts_ns}",
            )
        ]

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        return None


class OneShotBuyStrategy(StrategyBase):
    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        if ctx.get("one_shot_done"):
            return []
        bar = bar_batch[0]
        ctx["one_shot_done"] = True
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id=str(bar["instrument_id"]),
                side=Side.BUY,
                offset=OffsetFlag.OPEN,
                volume=1,
                limit_price=float(bar["close"]),
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
    assert report.instrument_count == 1
    assert report.instrument_universe == ("rb2305",)
    assert report.first_ts_ns > 0
    assert report.last_ts_ns >= report.first_ts_ns


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
    assert result1.data_signature == result2.data_signature
    assert result1.to_dict() == result2.to_dict()
    assert result1.run_id == "spec-run-1"
    assert result1.deterministic is not None
    assert result1.deterministic.performance.total_pnl == pytest.approx(3.0)
    assert result1.deterministic.performance.order_status_counts["FILLED"] == 2
    deterministic_payload = result1.to_dict()["deterministic"]
    assert isinstance(deterministic_payload, dict)
    assert "performance" in deterministic_payload
    assert deterministic_payload["performance"]["total_pnl"] == pytest.approx(3.0)
    replay_payload = result1.to_dict()["replay"]
    assert replay_payload["instrument_count"] == 1
    assert replay_payload["instrument_universe"] == ["rb2305"]
    assert replay_payload["first_ts_ns"] > 0
    assert replay_payload["last_ts_ns"] >= replay_payload["first_ts_ns"]
    for key in BACKTEST_CTX_REQUIRED_KEYS:
        assert key in ctx1
        assert key in ctx2


def test_run_backtest_spec_can_emit_state_snapshot_contract(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample_state.csv"
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
    runtime = StrategyRuntime()
    runtime.add_strategy(StateOnlyStrategy("state-demo"))
    ctx: dict[str, object] = {}

    result = run_backtest_spec(
        BacktestRunSpec(
            csv_path=str(csv_path),
            max_ticks=100,
            deterministic_fills=False,
            emit_state_snapshots=True,
            run_id="state-run",
        ),
        runtime,
        ctx=ctx,
    )

    assert result.replay.bars_emitted == 2
    assert result.replay.intents_emitted == 2
    observed = ctx.get("state_observed")
    assert isinstance(observed, list)
    assert len(observed) == 2
    snapshot = observed[0]
    assert isinstance(snapshot, StateSnapshot7D)
    assert snapshot.instrument_id == "rb2305"
    assert "score" in snapshot.trend


def test_replay_tracks_multi_instrument_overview(tmp_path: Path) -> None:
    csv_path = tmp_path / "multi.csv"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,4100.0,1,4099.0,10,4101.0,12,4100.0,0.0,100",
                "20230103,ag2406,09:00:30,0,5100.0,1,5099.0,10,5101.0,12,5100.0,0.0,100",
                "20230103,rb2305,09:01:01,0,4102.0,2,4101.0,10,4103.0,12,4101.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )
    runtime = StrategyRuntime()
    report = replay_csv_minute_bars(csv_path, runtime, ctx={})
    assert report.instrument_count == 2
    assert report.instrument_universe == ("ag2406", "rb2305")
    assert report.first_ts_ns > 0
    assert report.last_ts_ns >= report.first_ts_ns


def test_run_backtest_spec_data_signature_changes_when_csv_changes(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample_sig.csv"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,4100.0,1,4099.0,10,4101.0,12,4100.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )
    spec = BacktestRunSpec(csv_path=str(csv_path), max_ticks=10, deterministic_fills=False)
    runtime = StrategyRuntime()

    result1 = run_backtest_spec(spec, runtime, ctx={})
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,4200.0,1,4199.0,10,4201.0,12,4200.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )
    result2 = run_backtest_spec(spec, runtime, ctx={})

    assert result1.input_signature == result2.input_signature
    assert result1.data_signature != result2.data_signature


def test_iter_parquet_ticks_preserves_time_order_and_max_ticks(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    dataset_root = tmp_path / "parquet"
    part1 = dataset_root / "source=c" / "trading_day=20230103" / "instrument_id=c2305"
    part2 = dataset_root / "source=c" / "trading_day=20230103" / "instrument_id=c2309"
    part1.mkdir(parents=True, exist_ok=True)
    part2.mkdir(parents=True, exist_ok=True)

    rows1 = [
        {
            "TradingDay": "20230103",
            "InstrumentID": "c2305",
            "UpdateTime": "09:00:02",
            "UpdateMillisec": "0",
            "LastPrice": "100.0",
            "Volume": "1",
            "BidPrice1": "99.0",
            "BidVolume1": "1",
            "AskPrice1": "101.0",
            "AskVolume1": "1",
            "AveragePrice": "100.0",
            "Turnover": "0",
            "OpenInterest": "0",
        },
        {
            "TradingDay": "20230103",
            "InstrumentID": "c2305",
            "UpdateTime": "09:00:04",
            "UpdateMillisec": "0",
            "LastPrice": "100.2",
            "Volume": "2",
            "BidPrice1": "99.2",
            "BidVolume1": "1",
            "AskPrice1": "101.2",
            "AskVolume1": "1",
            "AveragePrice": "100.1",
            "Turnover": "0",
            "OpenInterest": "0",
        },
    ]
    rows2 = [
        {
            "TradingDay": "20230103",
            "InstrumentID": "c2309",
            "UpdateTime": "09:00:01",
            "UpdateMillisec": "0",
            "LastPrice": "200.0",
            "Volume": "1",
            "BidPrice1": "199.0",
            "BidVolume1": "1",
            "AskPrice1": "201.0",
            "AskVolume1": "1",
            "AveragePrice": "200.0",
            "Turnover": "0",
            "OpenInterest": "0",
        },
        {
            "TradingDay": "20230103",
            "InstrumentID": "c2309",
            "UpdateTime": "09:00:03",
            "UpdateMillisec": "0",
            "LastPrice": "200.2",
            "Volume": "2",
            "BidPrice1": "199.2",
            "BidVolume1": "1",
            "AskPrice1": "201.2",
            "AskVolume1": "1",
            "AveragePrice": "200.1",
            "Turnover": "0",
            "OpenInterest": "0",
        },
    ]

    pq.write_table(pa.Table.from_pylist(rows1), str(part1 / "part-0000.parquet"))
    pq.write_table(pa.Table.from_pylist(rows2), str(part2 / "part-0000.parquet"))

    ticks = list(itertools.islice(iter_parquet_ticks(dataset_root, max_ticks=3), 3))
    assert [tick.update_time for tick in ticks] == ["09:00:01", "09:00:02", "09:00:03"]


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
    assert spec.emit_state_snapshots is False


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


def test_core_sim_strict_rollover_records_event_and_slippage(tmp_path: Path) -> None:
    csv_path = tmp_path / "rollover_strict.csv"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,100.0,1,99.0,10,101.0,10,100.0,0.0,100",
                "20230103,rb2305,09:01:01,0,101.0,2,100.0,10,102.0,10,101.0,0.0,100",
                "20230103,rb2310,09:02:01,0,110.0,3,109.0,10,111.0,10,110.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )

    runtime = StrategyRuntime()
    runtime.add_strategy(OneShotBuyStrategy("rollover-strict"))
    result = run_backtest_spec(
        BacktestRunSpec(
            csv_path=str(csv_path),
            engine_mode="core_sim",
            rollover_mode="strict",
            rollover_price_mode="bbo",
            rollover_slippage_bps=10.0,
            deterministic_fills=True,
            max_ticks=100,
            run_id="core-sim-strict",
        ),
        runtime,
        ctx={},
    )

    assert result.deterministic is not None
    events = result.deterministic.rollover_events
    assert len(events) == 1
    assert events[0]["from_instrument"] == "rb2305"
    assert events[0]["to_instrument"] == "rb2310"
    assert events[0]["mode"] == "strict"
    assert events[0]["price_mode"] == "bbo"
    assert [item["action"] for item in result.deterministic.rollover_actions] == ["close", "open"]
    assert result.deterministic.rollover_slippage_cost > 0.0


def test_core_sim_carry_rollover_has_zero_slippage(tmp_path: Path) -> None:
    csv_path = tmp_path / "rollover_carry.csv"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,100.0,1,99.0,10,101.0,10,100.0,0.0,100",
                "20230103,rb2305,09:01:01,0,101.0,2,100.0,10,102.0,10,101.0,0.0,100",
                "20230103,rb2310,09:02:01,0,110.0,3,109.0,10,111.0,10,110.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )

    runtime = StrategyRuntime()
    runtime.add_strategy(OneShotBuyStrategy("rollover-carry"))
    result = run_backtest_spec(
        BacktestRunSpec(
            csv_path=str(csv_path),
            engine_mode="core_sim",
            rollover_mode="carry",
            rollover_price_mode="last",
            rollover_slippage_bps=50.0,
            deterministic_fills=True,
            max_ticks=100,
            run_id="core-sim-carry",
        ),
        runtime,
        ctx={},
    )

    assert result.deterministic is not None
    events = result.deterministic.rollover_events
    assert len(events) == 1
    assert events[0]["mode"] == "carry"
    assert events[0]["price_mode"] == "last"
    assert [item["action"] for item in result.deterministic.rollover_actions] == ["carry"]
    assert result.deterministic.rollover_slippage_cost == pytest.approx(0.0)


def test_core_sim_rollover_actions_match_wal_records(tmp_path: Path) -> None:
    csv_path = tmp_path / "rollover_wal.csv"
    wal_path = tmp_path / "rollover.wal"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,100.0,1,99.0,10,101.0,10,100.0,0.0,100",
                "20230103,rb2305,09:01:01,0,101.0,2,100.0,10,102.0,10,101.0,0.0,100",
                "20230103,rb2310,09:02:01,0,110.0,3,109.0,10,111.0,10,110.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )

    runtime = StrategyRuntime()
    runtime.add_strategy(OneShotBuyStrategy("rollover-wal"))
    result = run_backtest_spec(
        BacktestRunSpec(
            csv_path=str(csv_path),
            engine_mode="core_sim",
            rollover_mode="strict",
            rollover_price_mode="bbo",
            rollover_slippage_bps=5.0,
            deterministic_fills=True,
            max_ticks=100,
            wal_path=str(wal_path),
            run_id="core-sim-wal",
        ),
        runtime,
        ctx={},
    )

    assert result.deterministic is not None
    wal_records = [
        json.loads(line)
        for line in wal_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    rollover_lines = [item for item in wal_records if item.get("kind") == "rollover"]
    assert len(rollover_lines) == len(result.deterministic.rollover_actions)
    assert [item["action"] for item in rollover_lines] == [
        item["action"] for item in result.deterministic.rollover_actions
    ]
    assert result.deterministic.wal_records == len(wal_records)
