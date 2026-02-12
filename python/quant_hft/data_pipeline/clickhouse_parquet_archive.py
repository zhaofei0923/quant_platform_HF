from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import request


@dataclass(frozen=True)
class ClickHouseParquetArchiveConfig:
    clickhouse_dsn: str
    output_dir: Path | str
    table: str = "quant_hft.market_ticks"
    start_trading_day: str = ""
    end_trading_day: str = ""
    limit: int = 0
    timeout_seconds: int = 30
    compression: str = "zstd"
    allow_jsonl_fallback: bool = False


@dataclass(frozen=True)
class ClickHouseParquetArchiveReport:
    row_count: int
    output_files: tuple[Path, ...]
    trading_days: tuple[str, ...]
    writer_backend: str
    query: str


def _safe_int(value: object, default: int = 0) -> int:
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


def _trading_day_from_row(row: dict[str, Any]) -> str:
    trading_day = str(row.get("trading_day", "")).strip()
    if trading_day:
        return trading_day
    recv_ts_ns = _safe_int(row.get("recv_ts_ns", 0), 0)
    if recv_ts_ns <= 0:
        return datetime.now(timezone.utc).strftime("%Y%m%d")
    dt = datetime.fromtimestamp(recv_ts_ns / 1_000_000_000, tz=timezone.utc)
    return dt.strftime("%Y%m%d")


def build_market_ticks_query(config: ClickHouseParquetArchiveConfig) -> str:
    where_clauses: list[str] = []
    if config.start_trading_day:
        where_clauses.append(f"trading_day >= '{config.start_trading_day}'")
    if config.end_trading_day:
        where_clauses.append(f"trading_day <= '{config.end_trading_day}'")

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    limit_sql = ""
    if config.limit > 0:
        limit_sql = f" LIMIT {config.limit}"

    return (
        "SELECT instrument_id, exchange_id, trading_day, action_day, update_time, "
        "update_millisec, last_price, bid_price_1, ask_price_1, bid_volume_1, "
        "ask_volume_1, volume, exchange_ts_ns, recv_ts_ns, published_ts_ns "
        f"FROM {config.table}"
        f"{where_sql} "
        "ORDER BY trading_day, recv_ts_ns"
        f"{limit_sql} "
        "FORMAT JSONEachRow"
    )


def fetch_rows_from_clickhouse(
    clickhouse_dsn: str,
    query: str,
    timeout_seconds: int,
) -> list[dict[str, Any]]:
    if not clickhouse_dsn:
        raise ValueError("clickhouse_dsn is required")

    req = request.Request(
        clickhouse_dsn,
        data=query.encode("utf-8"),
        headers={"Content-Type": "text/plain; charset=utf-8"},
        method="POST",
    )
    with request.urlopen(req, timeout=max(1, timeout_seconds)) as resp:
        payload = resp.read().decode("utf-8")

    rows: list[dict[str, Any]] = []
    for line in payload.splitlines():
        raw = line.strip()
        if not raw:
            continue
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def _write_parquet_with_pyarrow(
    rows: list[dict[str, Any]],
    destination: Path,
    compression: str,
) -> str:
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    destination.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, destination, compression=compression)
    return "pyarrow"


def _write_jsonl_fallback(rows: list[dict[str, Any]], destination: Path) -> str:
    fallback_path = destination.with_suffix(".jsonl")
    fallback_path.parent.mkdir(parents=True, exist_ok=True)
    with fallback_path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=True) + "\n")
    return "jsonl_fallback"


def archive_ticks_to_parquet(
    config: ClickHouseParquetArchiveConfig,
    *,
    fetch_rows: Callable[[str], list[dict[str, Any]]] | None = None,
) -> ClickHouseParquetArchiveReport:
    query = build_market_ticks_query(config)

    def _default_fetch(query: str) -> list[dict[str, Any]]:
        return fetch_rows_from_clickhouse(config.clickhouse_dsn, query, config.timeout_seconds)

    fetch_fn = fetch_rows or _default_fetch

    rows = fetch_fn(query)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        day = _trading_day_from_row(row)
        grouped[day].append(row)

    output_root = Path(config.output_dir)
    writer_backend = "pyarrow"
    output_files: list[Path] = []
    for day in sorted(grouped.keys()):
        day_rows = grouped[day]
        if not day_rows:
            continue
        destination = output_root / f"trading_day={day}" / "market_ticks.parquet"
        try:
            writer_backend = _write_parquet_with_pyarrow(day_rows, destination, config.compression)
            output_files.append(destination)
        except ModuleNotFoundError:
            if not config.allow_jsonl_fallback:
                raise RuntimeError(
                    "pyarrow is required for parquet export; "
                    "set allow_jsonl_fallback=true for local fallback"
                ) from None
            writer_backend = _write_jsonl_fallback(day_rows, destination)
            output_files.append(destination.with_suffix(".jsonl"))

    return ClickHouseParquetArchiveReport(
        row_count=len(rows),
        output_files=tuple(output_files),
        trading_days=tuple(sorted(grouped.keys())),
        writer_backend=writer_backend,
        query=query,
    )


__all__ = [
    "ClickHouseParquetArchiveConfig",
    "ClickHouseParquetArchiveReport",
    "archive_ticks_to_parquet",
    "build_market_ticks_query",
    "fetch_rows_from_clickhouse",
]
