from __future__ import annotations

import csv
import importlib
import io
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from shutil import copyfile
from typing import Any, Protocol, cast

from quant_hft.contracts import OrderEvent


@dataclass(frozen=True)
class MarketSnapshotRecord:
    instrument_id: str
    ts_ns: int
    last_price: float
    bid_price_1: float
    ask_price_1: float
    volume: int


class _CursorLike(Protocol):
    description: Sequence[Sequence[object] | tuple[object, ...]] | None

    def fetchall(self) -> Sequence[Sequence[object] | tuple[object, ...]]: ...


class _SqlConnectionLike(Protocol):
    def execute(self, sql: str, parameters: Sequence[object] | None = None) -> _CursorLike: ...

    def executemany(self, sql: str, parameters: Iterable[Sequence[object]]) -> Any: ...

    def commit(self) -> Any: ...

    def close(self) -> Any: ...


def _is_valid_identifier(name: str) -> bool:
    if not name:
        return False
    if not (name[0].isalpha() or name[0] == "_"):
        return False
    return all(ch.isalnum() or ch == "_" for ch in name)


def _load_duckdb_connect() -> Any | None:
    try:
        module = importlib.import_module("duckdb")
    except ModuleNotFoundError:
        return None
    connect = getattr(module, "connect", None)
    if not callable(connect):
        return None
    return connect


def _load_minio_client_ctor() -> Any | None:
    try:
        module = importlib.import_module("minio")
    except ModuleNotFoundError:
        return None
    ctor = getattr(module, "Minio", None)
    if not callable(ctor):
        return None
    return ctor


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, str)):
        return int(value)
    raise TypeError(f"cannot coerce {type(value)!r} to int")


def _coerce_float(value: object) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float, str)):
        return float(value)
    raise TypeError(f"cannot coerce {type(value)!r} to float")


class DuckDbAnalyticsStore:
    def __init__(self, db_path: Path | str, *, prefer_duckdb: bool = True) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        backend = "sqlite"
        connect = _load_duckdb_connect() if prefer_duckdb else None
        if connect is not None:
            self._conn = cast(_SqlConnectionLike, connect(str(path)))
            backend = "duckdb"
        else:
            sqlite_conn = sqlite3.connect(path)
            self._conn = cast(_SqlConnectionLike, sqlite_conn)
        self._backend = backend
        self._ensure_schema()

    @property
    def backend(self) -> str:
        return self._backend

    def _ensure_schema(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                instrument_id TEXT NOT NULL,
                ts_ns BIGINT NOT NULL,
                last_price DOUBLE NOT NULL,
                bid_price_1 DOUBLE NOT NULL,
                ask_price_1 DOUBLE NOT NULL,
                volume BIGINT NOT NULL
            )
            """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS order_events (
                account_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                status TEXT NOT NULL,
                total_volume INTEGER NOT NULL,
                filled_volume INTEGER NOT NULL,
                avg_fill_price DOUBLE NOT NULL,
                reason TEXT NOT NULL,
                ts_ns BIGINT NOT NULL,
                trace_id TEXT NOT NULL
            )
            """)
        self._conn.commit()

    def append_market_snapshots(self, rows: Sequence[MarketSnapshotRecord]) -> int:
        payload = [
            (
                row.instrument_id,
                row.ts_ns,
                row.last_price,
                row.bid_price_1,
                row.ask_price_1,
                row.volume,
            )
            for row in rows
        ]
        if not payload:
            return 0
        self._conn.executemany(
            """
            INSERT INTO market_snapshots (
                instrument_id, ts_ns, last_price, bid_price_1, ask_price_1, volume
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        self._conn.commit()
        return len(payload)

    def append_order_events(self, rows: Sequence[OrderEvent]) -> int:
        payload = [
            (
                row.account_id,
                row.client_order_id,
                row.instrument_id,
                row.status,
                row.total_volume,
                row.filled_volume,
                row.avg_fill_price,
                row.reason,
                row.ts_ns,
                row.trace_id,
            )
            for row in rows
        ]
        if not payload:
            return 0
        self._conn.executemany(
            """
            INSERT INTO order_events (
                account_id, client_order_id, instrument_id, status,
                total_volume, filled_volume, avg_fill_price, reason, ts_ns, trace_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        self._conn.commit()
        return len(payload)

    def query_market_snapshots(
        self, instrument_id: str, *, limit: int = 1000
    ) -> list[MarketSnapshotRecord]:
        cursor = self._conn.execute(
            """
            SELECT instrument_id, ts_ns, last_price, bid_price_1, ask_price_1, volume
            FROM market_snapshots
            WHERE instrument_id = ?
            ORDER BY ts_ns DESC
            LIMIT ?
            """,
            (instrument_id, max(1, limit)),
        )
        rows = cursor.fetchall()
        return [
            MarketSnapshotRecord(
                instrument_id=str(item[0]),
                ts_ns=_coerce_int(item[1]),
                last_price=_coerce_float(item[2]),
                bid_price_1=_coerce_float(item[3]),
                ask_price_1=_coerce_float(item[4]),
                volume=_coerce_int(item[5]),
            )
            for item in rows
        ]

    def query_order_events(self, client_order_id: str) -> list[OrderEvent]:
        cursor = self._conn.execute(
            """
            SELECT account_id, client_order_id, instrument_id, status,
                   total_volume, filled_volume, avg_fill_price, reason, ts_ns, trace_id
            FROM order_events
            WHERE client_order_id = ?
            ORDER BY ts_ns ASC
            """,
            (client_order_id,),
        )
        rows = cursor.fetchall()
        return [
            OrderEvent(
                account_id=str(item[0]),
                client_order_id=str(item[1]),
                instrument_id=str(item[2]),
                status=str(item[3]),
                total_volume=_coerce_int(item[4]),
                filled_volume=_coerce_int(item[5]),
                avg_fill_price=_coerce_float(item[6]),
                reason=str(item[7]),
                ts_ns=_coerce_int(item[8]),
                trace_id=str(item[9]),
            )
            for item in rows
        ]

    def export_table_to_csv(self, table: str, destination: Path | str) -> int:
        if not _is_valid_identifier(table):
            raise ValueError(f"invalid table name: {table}")

        cursor = self._conn.execute(f"SELECT * FROM {table}")
        rows = list(cursor.fetchall())
        description = cursor.description or ()
        headers = [str(item[0]) for item in description]

        output = Path(destination)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8", newline="") as fp:
            writer = csv.writer(fp)
            if headers:
                writer.writerow(headers)
            writer.writerows(rows)
        return len(rows)

    def close(self) -> None:
        self._conn.close()


class MinioArchiveStore:
    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        use_ssl: bool = False,
        local_fallback_dir: Path | str | None = None,
    ) -> None:
        self._bucket = bucket
        self._client: Any | None = None
        self._root: Path | None = None
        if local_fallback_dir is not None:
            self._mode = "local_fallback"
            self._root = Path(local_fallback_dir).resolve()
            (self._root / bucket).mkdir(parents=True, exist_ok=True)
            return

        ctor = _load_minio_client_ctor()
        if ctor is None:
            raise RuntimeError("MinIO SDK is not installed and local fallback is disabled")

        self._mode = "minio"
        self._client = ctor(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=use_ssl,
        )
        if not bool(self._client.bucket_exists(bucket)):
            self._client.make_bucket(bucket)

    @property
    def mode(self) -> str:
        return self._mode

    def _validate_object_name(self, object_name: str) -> str:
        normalized = object_name.strip("/")
        if not normalized:
            raise ValueError("object_name is empty")
        if any(part == ".." for part in Path(normalized).parts):
            raise ValueError("object_name contains invalid path traversal segment")
        return normalized

    def _local_path(self, object_name: str) -> Path:
        if self._root is None:
            raise RuntimeError("local fallback root is not configured")
        safe_name = self._validate_object_name(object_name)
        path = (self._root / self._bucket / safe_name).resolve()
        base = (self._root / self._bucket).resolve()
        if base not in path.parents and path != base:
            raise ValueError("object_name resolves outside archive root")
        return path

    def put_text(self, object_name: str, text: str) -> None:
        payload = text.encode("utf-8")
        if self._mode == "local_fallback":
            target = self._local_path(object_name)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
            return

        stream = io.BytesIO(payload)
        assert self._client is not None
        self._client.put_object(
            self._bucket,
            self._validate_object_name(object_name),
            stream,
            len(payload),
            content_type="application/json",
        )

    def put_file(self, object_name: str, source_path: Path | str) -> None:
        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(source)

        if self._mode == "local_fallback":
            target = self._local_path(object_name)
            target.parent.mkdir(parents=True, exist_ok=True)
            copyfile(source, target)
            return

        assert self._client is not None
        self._client.fput_object(
            self._bucket,
            self._validate_object_name(object_name),
            str(source),
        )

    def get_text(self, object_name: str) -> str:
        if self._mode == "local_fallback":
            return self._local_path(object_name).read_text(encoding="utf-8")

        assert self._client is not None
        response = self._client.get_object(self._bucket, self._validate_object_name(object_name))
        try:
            raw = response.read()
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()
            release_conn = getattr(response, "release_conn", None)
            if callable(release_conn):
                release_conn()
        data = raw if isinstance(raw, bytes) else str(raw).encode("utf-8")
        return data.decode("utf-8")

    def list_objects(self, *, prefix: str = "") -> list[str]:
        clean_prefix = prefix.strip("/")
        if self._mode == "local_fallback":
            assert self._root is not None
            root = self._root / self._bucket
            if clean_prefix:
                root = root / clean_prefix
                if not root.exists():
                    return []

            items: list[str] = []
            if not root.exists():
                return items
            for path in root.rglob("*"):
                if path.is_file():
                    assert self._root is not None
                    rel = path.relative_to(self._root / self._bucket)
                    items.append(rel.as_posix())
            items.sort()
            return items

        assert self._client is not None
        items = []
        for obj in self._client.list_objects(self._bucket, prefix=clean_prefix, recursive=True):
            name = getattr(obj, "object_name", "")
            if isinstance(name, str) and name:
                items.append(name)
        items.sort()
        return items
