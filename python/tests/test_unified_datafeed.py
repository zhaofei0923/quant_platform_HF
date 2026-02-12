from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from quant_hft.runtime.unified.datafeed import BacktestDataFeed, LiveDataFeed
from quant_hft.runtime.unified.history_reader import HistoryDataReader
from quant_hft.runtime.unified.models import Bar, Tick


class _FakeHistoryReader(HistoryDataReader):
    def __init__(self) -> None:
        base = datetime(2026, 1, 1, 9, 0, 0)
        self._ticks = [
            Tick(
                symbol="ag2406",
                exchange="SHFE",
                datetime=base,
                exchange_timestamp=base,
                last_price=Decimal("100"),
                last_volume=1,
                ask_price1=Decimal("101"),
                ask_volume1=1,
                bid_price1=Decimal("99"),
                bid_volume1=1,
                volume=1,
                turnover=Decimal("100"),
                open_interest=1,
            ),
            Tick(
                symbol="ag2406",
                exchange="SHFE",
                datetime=base + timedelta(seconds=1),
                exchange_timestamp=base + timedelta(seconds=1),
                last_price=Decimal("101"),
                last_volume=1,
                ask_price1=Decimal("102"),
                ask_volume1=1,
                bid_price1=Decimal("100"),
                bid_volume1=1,
                volume=2,
                turnover=Decimal("201"),
                open_interest=2,
            ),
        ]

    def read_ticks(self, symbol: str, start: datetime, end: datetime):  # type: ignore[override]
        return iter(
            [
                tick
                for tick in self._ticks
                if tick.symbol == symbol and start <= tick.datetime <= end
            ]
        )

    def read_bars(self, symbol: str, timeframe: str, start: datetime, end: datetime):  # type: ignore[override]
        del timeframe
        bars = [
            Bar(
                symbol=symbol,
                exchange="SHFE",
                timeframe="1min",
                datetime=tick.datetime,
                open=tick.last_price,
                high=tick.last_price,
                low=tick.last_price,
                close=tick.last_price,
                volume=tick.volume,
                turnover=tick.turnover,
                open_interest=tick.open_interest,
            )
            for tick in self._ticks
            if start <= tick.datetime <= end
        ]
        return iter(bars)


@dataclass
class _FakeMdAdapter:
    connected: bool = False
    subscribed: list[str] | None = None
    callback: Any | None = None

    def connect(self, config: dict[str, object]) -> bool:
        del config
        self.connected = True
        return True

    def disconnect(self) -> None:
        self.connected = False

    def subscribe(self, symbols: list[str]) -> bool:
        self.subscribed = list(symbols)
        return True

    def unsubscribe(self, symbols: list[str]) -> bool:
        del symbols
        self.subscribed = []
        return True

    def on_tick(self, callback: Any) -> None:
        self.callback = callback

    def emit_tick(self, symbol: str) -> None:
        if self.callback is None:
            return
        self.callback(
            {
                "instrument_id": symbol,
                "last_price": 101.0,
                "bid_price_1": 100.0,
                "ask_price_1": 102.0,
                "bid_volume_1": 1,
                "ask_volume_1": 1,
                "volume": 3,
                "ts_ns": int(datetime.now(timezone.utc).timestamp() * 1_000_000_000),
            }
        )


def test_backtest_datafeed_replays_ticks_in_order() -> None:
    history = _FakeHistoryReader()
    feed = BacktestDataFeed(history)
    received: list[Tick] = []

    feed.subscribe(["ag2406"], on_tick=lambda tick: received.append(tick))
    start = datetime(2026, 1, 1, 8, 59, 0)
    end = datetime(2026, 1, 1, 9, 2, 0)
    feed.run(start=start, end=end)

    assert len(received) == 2
    assert received[0].datetime <= received[1].datetime
    assert feed.current_tick("ag2406") == received[-1]


def test_live_datafeed_accepts_tick_callbacks() -> None:
    md = _FakeMdAdapter()
    history = _FakeHistoryReader()
    feed = LiveDataFeed(md, history, connect_config={"market_front_address": "tcp://sim-md"})
    received: list[Tick] = []
    feed.subscribe(["ag2406"], on_tick=lambda tick: received.append(tick))

    run_thread = threading.Thread(target=lambda: feed.run(run_seconds=1), daemon=True)
    run_thread.start()
    time.sleep(0.05)
    md.emit_tick("ag2406")
    run_thread.join(timeout=2.0)
    feed.stop()

    assert received
    assert received[0].symbol == "ag2406"
    assert received[0].datetime.tzinfo == timezone.utc
    assert feed.current_tick("ag2406") is not None
