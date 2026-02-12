from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal

from quant_hft.runtime.unified.history_reader import HistoryDataReader
from quant_hft.runtime.unified.models import Bar, Tick


def _to_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return default
    return default


class DataFeed(ABC):
    @abstractmethod
    def subscribe(
        self,
        symbols: list[str],
        on_tick: Callable[[Tick], None],
        on_bar: Callable[[Bar], None] | None = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_history_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str,
    ) -> list[Bar]:
        raise NotImplementedError

    @abstractmethod
    def get_history_ticks(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int | None = None,
    ) -> list[Tick]:
        raise NotImplementedError

    @abstractmethod
    def current_tick(self, symbol: str) -> Tick | None:
        raise NotImplementedError

    @abstractmethod
    def run(self, **kwargs: object) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError


class BacktestDataFeed(DataFeed):
    def __init__(self, history_reader: HistoryDataReader) -> None:
        self._history_reader = history_reader
        self._symbols: list[str] = []
        self._on_tick: Callable[[Tick], None] | None = None
        self._on_bar: Callable[[Bar], None] | None = None
        self._current_ticks: dict[str, Tick] = {}
        self._running = False

    def subscribe(
        self,
        symbols: list[str],
        on_tick: Callable[[Tick], None],
        on_bar: Callable[[Bar], None] | None = None,
    ) -> None:
        self._symbols = list(dict.fromkeys(symbols))
        self._on_tick = on_tick
        self._on_bar = on_bar

    def get_history_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str,
    ) -> list[Bar]:
        return list(self._history_reader.read_bars(symbol, timeframe, start, end))

    def get_history_ticks(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int | None = None,
    ) -> list[Tick]:
        ticks = list(self._history_reader.read_ticks(symbol, start, end))
        if limit is None or limit <= 0:
            return ticks
        return ticks[-limit:]

    def current_tick(self, symbol: str) -> Tick | None:
        return self._current_ticks.get(symbol)

    def run(self, **kwargs: object) -> None:
        if self._on_tick is None:
            return
        start = kwargs.get("start")
        end = kwargs.get("end")
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            raise ValueError("backtest datafeed requires datetime start/end")
        self._running = True
        stream: list[Tick] = []
        for symbol in self._symbols:
            stream.extend(self.get_history_ticks(symbol, start, end))
        stream.sort(key=lambda item: item.datetime)
        for tick in stream:
            if not self._running:
                break
            self._current_ticks[tick.symbol] = tick
            self._on_tick(tick)

    def stop(self) -> None:
        self._running = False


class LiveDataFeed(DataFeed):
    def __init__(
        self,
        md_adapter: object,
        history_reader: HistoryDataReader,
        connect_config: dict[str, object] | None = None,
    ) -> None:
        self._md_adapter = md_adapter
        self._history_reader = history_reader
        self._connect_config = connect_config or {}
        self._symbols: list[str] = []
        self._on_tick: Callable[[Tick], None] | None = None
        self._on_bar: Callable[[Bar], None] | None = None
        self._current_ticks: dict[str, Tick] = {}
        self._running = False
        self._connected = False
        self._lock = threading.Lock()
        callback_reg = getattr(self._md_adapter, "on_tick", None)
        if callback_reg is not None:
            callback_reg(self._on_tick_payload)

    def subscribe(
        self,
        symbols: list[str],
        on_tick: Callable[[Tick], None],
        on_bar: Callable[[Bar], None] | None = None,
    ) -> None:
        self._symbols = list(dict.fromkeys(symbols))
        self._on_tick = on_tick
        self._on_bar = on_bar
        if self._connected:
            subscribe = getattr(self._md_adapter, "subscribe", None)
            if subscribe is not None:
                subscribe(self._symbols)

    def get_history_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str,
    ) -> list[Bar]:
        return list(self._history_reader.read_bars(symbol, timeframe, start, end))

    def get_history_ticks(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int | None = None,
    ) -> list[Tick]:
        ticks = list(self._history_reader.read_ticks(symbol, start, end))
        if limit is None or limit <= 0:
            return ticks
        return ticks[-limit:]

    def current_tick(self, symbol: str) -> Tick | None:
        with self._lock:
            return self._current_ticks.get(symbol)

    def run(self, **kwargs: object) -> None:
        run_seconds = kwargs.get("run_seconds")
        runtime_sec = int(run_seconds) if isinstance(run_seconds, int) and run_seconds > 0 else 0
        if not self._connected:
            connect = getattr(self._md_adapter, "connect", None)
            if connect is not None and not connect(dict(self._connect_config)):
                raise RuntimeError("live datafeed connect failed")
            self._connected = True
        if self._symbols:
            subscribe = getattr(self._md_adapter, "subscribe", None)
            if subscribe is not None and not subscribe(self._symbols):
                raise RuntimeError("live datafeed subscribe failed")
        self._running = True
        started = time.monotonic()
        while self._running:
            if runtime_sec > 0 and (time.monotonic() - started) >= runtime_sec:
                break
            time.sleep(0.05)

    def stop(self) -> None:
        self._running = False
        if self._connected:
            unsubscribe = getattr(self._md_adapter, "unsubscribe", None)
            if unsubscribe is not None and self._symbols:
                unsubscribe(self._symbols)
            disconnect = getattr(self._md_adapter, "disconnect", None)
            if disconnect is not None:
                disconnect()
        self._connected = False

    def _on_tick_payload(self, payload: dict[str, object]) -> None:
        symbol = str(payload.get("instrument_id", "")).strip()
        if not symbol:
            return
        ts_ns = _to_int(payload.get("ts_ns", time.time_ns()), time.time_ns())
        dt = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
        tick = Tick(
            symbol=symbol,
            exchange=str(payload.get("exchange_id", "")),
            datetime=dt,
            exchange_timestamp=None,
            last_price=Decimal(str(payload.get("last_price", 0.0))),
            last_volume=_to_int(payload.get("last_volume", 0), 0),
            ask_price1=Decimal(str(payload.get("ask_price_1", payload.get("ask_price1", 0.0)))),
            ask_volume1=_to_int(payload.get("ask_volume_1", payload.get("ask_volume1", 0)), 0),
            bid_price1=Decimal(str(payload.get("bid_price_1", payload.get("bid_price1", 0.0)))),
            bid_volume1=_to_int(payload.get("bid_volume_1", payload.get("bid_volume1", 0)), 0),
            volume=_to_int(payload.get("volume", 0), 0),
            turnover=Decimal(str(payload.get("turnover", 0.0))),
            open_interest=_to_int(payload.get("open_interest", 0), 0),
        )
        with self._lock:
            self._current_ticks[symbol] = tick
        if self._on_tick is not None:
            self._on_tick(tick)
