from __future__ import annotations

from pathlib import Path

import pytest
from quant_hft.backtest.replay import replay_csv_minute_bars
from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.strategy.base import StrategyBase


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
