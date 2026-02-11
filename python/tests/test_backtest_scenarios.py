from __future__ import annotations

import json
from pathlib import Path

import pytest
from quant_hft.backtest.replay import replay_csv_with_deterministic_fills
from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.strategy.base import StrategyBase


class EmitOnEveryBarStrategy(StrategyBase):
    def __init__(self, strategy_id: str, side: Side) -> None:
        super().__init__(strategy_id)
        self._side = side

    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        bar = bar_batch[0]
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id=str(bar["instrument_id"]),
                side=self._side,
                offset=OffsetFlag.OPEN if self._side == Side.BUY else OffsetFlag.CLOSE,
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


class AlternatingOpenCloseStrategy(StrategyBase):
    def __init__(self, strategy_id: str) -> None:
        super().__init__(strategy_id)
        self._index = 0

    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        bar = bar_batch[0]
        self._index += 1
        side = Side.BUY if self._index % 2 == 1 else Side.SELL
        offset = OffsetFlag.OPEN if side == Side.BUY else OffsetFlag.CLOSE
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id=str(bar["instrument_id"]),
                side=side,
                offset=offset,
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


def test_deterministic_replay_counts_multi_instrument_bars(tmp_path: Path) -> None:
    csv_path = tmp_path / "multi.csv"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,4100.0,1,4099.0,10,4101.0,12,4100.0,0.0,100",
                "20230103,rb2305,09:00:30,0,4102.0,2,4101.0,11,4103.0,13,4101.0,0.0,100",
                "20230103,hc2305,09:00:33,0,3920.0,3,3919.0,15,3921.0,17,3920.0,0.0,80",
                "20230103,rb2305,09:01:02,0,4103.0,5,4102.0,10,4104.0,15,4102.0,0.0,100",
                "20230103,hc2305,09:01:40,0,3925.0,6,3924.0,14,3926.0,18,3924.0,0.0,80",
            ]
        ),
        encoding="utf-8",
    )

    runtime = StrategyRuntime()
    runtime.add_strategy(EmitOnEveryBarStrategy("s1", Side.BUY))
    report = replay_csv_with_deterministic_fills(csv_path, runtime, ctx={})

    assert report.replay.ticks_read == 5
    assert report.replay.bars_emitted == 4
    assert report.intents_processed == 4
    assert report.order_events_emitted == 8
    assert report.instrument_bars == {"rb2305": 2, "hc2305": 2}
    assert report.invariant_violations == ()


def test_deterministic_replay_writes_wal_records_consistently(tmp_path: Path) -> None:
    csv_path = tmp_path / "single.csv"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,4100.0,1,4099.0,10,4101.0,12,4100.0,0.0,100",
                "20230103,rb2305,09:01:01,0,4105.0,3,4104.0,10,4106.0,12,4102.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )
    wal_path = tmp_path / "deterministic.wal"

    runtime = StrategyRuntime()
    runtime.add_strategy(EmitOnEveryBarStrategy("s1", Side.BUY))
    report = replay_csv_with_deterministic_fills(csv_path, runtime, ctx={}, wal_path=wal_path)

    lines = wal_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == report.wal_records
    assert report.wal_records == report.order_events_emitted
    assert report.order_events_emitted == report.intents_processed * 2

    records = [json.loads(line) for line in lines]
    assert {record["status"] for record in records} == {1, 3}
    assert all(record["kind"] in {"order", "trade"} for record in records)
    assert all(record["client_order_id"].startswith("bt-") for record in records)


def test_deterministic_replay_tracks_pnl_invariants(tmp_path: Path) -> None:
    csv_path = tmp_path / "pnl.csv"
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,100.0,1,99.0,10,101.0,10,100.0,0.0,100",
                "20230103,rb2305,09:01:01,0,105.0,2,104.0,10,106.0,10,102.0,0.0,100",
                "20230103,rb2305,09:02:01,0,102.0,3,101.0,10,103.0,10,102.0,0.0,100",
                "20230103,rb2305,09:03:01,0,101.0,4,100.0,10,102.0,10,102.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )

    runtime = StrategyRuntime()
    runtime.add_strategy(AlternatingOpenCloseStrategy("s-pnl"))
    report = replay_csv_with_deterministic_fills(csv_path, runtime, ctx={})

    pnl = report.instrument_pnl["rb2305"]
    assert pnl.net_position == 0
    assert pnl.realized_pnl == pytest.approx(4.0)
    assert pnl.unrealized_pnl == pytest.approx(0.0)
    assert report.total_realized_pnl == pytest.approx(4.0)
    assert report.total_unrealized_pnl == pytest.approx(0.0)
    assert report.invariant_violations == ()
    assert report.performance.total_pnl == pytest.approx(4.0)
    assert report.performance.max_drawdown == pytest.approx(1.0)
    assert report.performance.order_status_counts["ACCEPTED"] == report.intents_processed
    assert report.performance.order_status_counts["FILLED"] == report.intents_processed
