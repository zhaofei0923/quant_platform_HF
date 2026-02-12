from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus
from urllib.request import urlopen

from quant_hft.runtime.unified.models import Bar, Tick


def _to_datetime(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    if value is None:
        return fallback
    text = str(value).strip()
    if not text:
        return fallback
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return fallback


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _row_to_tick(symbol: str, row: dict[str, Any]) -> Tick:
    now = datetime.utcnow()
    dt = _to_datetime(
        row.get("datetime", row.get("time", row.get("ts", row.get("event_time")))),
        now,
    )
    exchange_ts = row.get("exchange_timestamp", row.get("exchange_time"))
    return Tick(
        symbol=str(row.get("symbol", symbol)),
        exchange=str(row.get("exchange", row.get("exchange_id", ""))),
        datetime=dt,
        exchange_timestamp=_to_datetime(exchange_ts, dt) if exchange_ts else None,
        last_price=_to_decimal(row.get("last_price", row.get("close", 0))),
        last_volume=int(row.get("last_volume", row.get("volume", 0) or 0)),
        ask_price1=_to_decimal(row.get("ask_price1", row.get("ask_price_1", 0))),
        ask_volume1=int(row.get("ask_volume1", row.get("ask_volume_1", 0) or 0)),
        bid_price1=_to_decimal(row.get("bid_price1", row.get("bid_price_1", 0))),
        bid_volume1=int(row.get("bid_volume1", row.get("bid_volume_1", 0) or 0)),
        volume=int(row.get("volume", 0) or 0),
        turnover=_to_decimal(row.get("turnover", 0)),
        open_interest=int(row.get("open_interest", row.get("openInterest", 0) or 0)),
    )


def _row_to_bar(symbol: str, timeframe: str, row: dict[str, Any]) -> Bar:
    now = datetime.utcnow()
    dt = _to_datetime(
        row.get("datetime", row.get("time", row.get("ts", row.get("event_time")))),
        now,
    )
    return Bar(
        symbol=str(row.get("symbol", symbol)),
        exchange=str(row.get("exchange", row.get("exchange_id", ""))),
        timeframe=str(row.get("timeframe", timeframe)),
        datetime=dt,
        open=_to_decimal(row.get("open", 0)),
        high=_to_decimal(row.get("high", 0)),
        low=_to_decimal(row.get("low", 0)),
        close=_to_decimal(row.get("close", row.get("last_price", 0))),
        volume=int(row.get("volume", 0) or 0),
        turnover=_to_decimal(row.get("turnover", 0)),
        open_interest=int(row.get("open_interest", row.get("openInterest", 0) or 0)),
    )


class HistoryDataReader(ABC):
    @abstractmethod
    def read_ticks(self, symbol: str, start: datetime, end: datetime) -> Iterator[Tick]:
        raise NotImplementedError

    @abstractmethod
    def read_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> Iterator[Bar]:
        raise NotImplementedError


class ParquetHistoryReader(HistoryDataReader):
    def __init__(self, data_path: str) -> None:
        self._data_path = Path(data_path)
        self._duckdb = None
        try:
            import duckdb  # type: ignore
        except Exception:  # pragma: no cover - optional dependency
            self._duckdb = None
        else:
            self._duckdb = duckdb.connect()

    def read_ticks(self, symbol: str, start: datetime, end: datetime) -> Iterator[Tick]:
        file_path = self._data_path / f"{symbol}_tick.parquet"
        if not file_path.exists() or self._duckdb is None:
            return iter(())
        rows = self._duckdb.execute(
            "SELECT * FROM read_parquet(?)",
            [str(file_path)],
        ).fetchdf()
        ticks: list[Tick] = []
        for row in rows.to_dict(orient="records"):
            tick = _row_to_tick(symbol, row)
            if start <= tick.datetime <= end:
                ticks.append(tick)
        ticks.sort(key=lambda item: item.datetime)
        return iter(ticks)

    def read_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> Iterator[Bar]:
        file_path = self._data_path / f"{symbol}_{timeframe}.parquet"
        if not file_path.exists() or self._duckdb is None:
            return iter(())
        rows = self._duckdb.execute(
            "SELECT * FROM read_parquet(?)",
            [str(file_path)],
        ).fetchdf()
        bars: list[Bar] = []
        for row in rows.to_dict(orient="records"):
            bar = _row_to_bar(symbol, timeframe, row)
            if start <= bar.datetime <= end:
                bars.append(bar)
        bars.sort(key=lambda item: item.datetime)
        return iter(bars)


class ClickHouseHistoryReader(HistoryDataReader):
    def __init__(self, dsn: str, database: str = "quant_hft", timeout_sec: float = 3.0) -> None:
        self._dsn = dsn.rstrip("/")
        self._database = database
        self._timeout_sec = max(timeout_sec, 0.1)

    def read_ticks(self, symbol: str, start: datetime, end: datetime) -> Iterator[Tick]:
        sql = (
            "SELECT symbol, exchange_id AS exchange, event_time AS datetime, "
            "last_price, bid_price_1 AS bid_price1, ask_price_1 AS ask_price1, "
            "bid_volume_1 AS bid_volume1, ask_volume_1 AS ask_volume1, volume, turnover, "
            "open_interest FROM market_ticks "
            f"WHERE symbol = '{symbol}' "
            f"AND event_time BETWEEN toDateTime64('{start.isoformat()}', 3, 'UTC') "
            f"AND toDateTime64('{end.isoformat()}', 3, 'UTC') "
            "ORDER BY event_time"
        )
        rows = self._query_rows(sql)
        return iter(_row_to_tick(symbol, row) for row in rows)

    def read_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> Iterator[Bar]:
        table = "market_bars"
        sql = (
            "SELECT symbol, exchange_id AS exchange, timeframe, event_time AS datetime, "
            "open, high, low, close, volume, turnover, open_interest "
            f"FROM {table} "
            f"WHERE symbol = '{symbol}' AND timeframe = '{timeframe}' "
            f"AND event_time BETWEEN toDateTime64('{start.isoformat()}', 3, 'UTC') "
            f"AND toDateTime64('{end.isoformat()}', 3, 'UTC') "
            "ORDER BY event_time"
        )
        rows = self._query_rows(sql)
        return iter(_row_to_bar(symbol, timeframe, row) for row in rows)

    def _query_rows(self, sql: str) -> list[dict[str, Any]]:
        query = quote_plus(f"{sql} FORMAT JSONEachRow")
        url = f"{self._dsn}/?database={self._database}&query={query}"
        try:
            with urlopen(url, timeout=self._timeout_sec) as resp:
                payload = resp.read().decode("utf-8")
        except Exception:
            return []
        rows: list[dict[str, Any]] = []
        for line in payload.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                rows.append(parsed)
        return rows
