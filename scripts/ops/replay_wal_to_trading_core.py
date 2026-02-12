#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WalRecord:
    seq: int
    kind: str
    payload: dict[str, Any]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay local WAL records into trading_core order/trade tables."
    )
    parser.add_argument("--wal-path", default="runtime_events.wal")
    parser.add_argument(
        "--checkpoint-file",
        default="docs/results/wal_to_trading_core_checkpoint.json",
    )
    parser.add_argument("--schema", default=os.getenv("QUANT_HFT_TRADING_SCHEMA", "trading_core"))
    parser.add_argument("--timescale-dsn", default=os.getenv("QUANT_HFT_TIMESCALE_DSN", ""))
    parser.add_argument(
        "--timescale-host",
        default=os.getenv("QUANT_HFT_TIMESCALE_HOST", "127.0.0.1"),
    )
    parser.add_argument(
        "--timescale-port",
        type=int,
        default=int(os.getenv("QUANT_HFT_TIMESCALE_PORT", "5432")),
    )
    parser.add_argument("--timescale-db", default=os.getenv("QUANT_HFT_TIMESCALE_DB", "quant_hft"))
    parser.add_argument(
        "--timescale-user",
        default=os.getenv("QUANT_HFT_TIMESCALE_USER", "quant_hft"),
    )
    parser.add_argument(
        "--timescale-password",
        default=os.getenv("QUANT_HFT_TIMESCALE_PASSWORD", ""),
    )
    parser.add_argument("--max-records", type=int, default=0, help="0 means no limit")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _load_checkpoint(path: Path) -> int:
    if not path.exists():
        return 0
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return 0
    value = payload.get("last_seq", 0)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _save_checkpoint(path: Path, wal_path: str, last_seq: int) -> None:
    payload = {
        "wal_path": wal_path,
        "last_seq": last_seq,
        "updated_ts_ns": time.time_ns(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def _safe_int(payload: dict[str, Any], key: str, default: int = 0) -> int:
    raw = payload.get(key, default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _safe_float(payload: dict[str, Any], key: str, default: float = 0.0) -> float:
    raw = payload.get(key, default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _safe_str(payload: dict[str, Any], key: str, default: str = "") -> str:
    raw = payload.get(key, default)
    if raw is None:
        return default
    return str(raw)


def _build_trade_date(ts_ns: int) -> str:
    seconds = ts_ns // 1_000_000_000
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%d")


def _build_idempotency_key(payload: dict[str, Any]) -> str:
    client_order_id = _safe_str(payload, "client_order_id")
    event_source = _safe_str(payload, "event_source")
    ts_ns = _safe_int(payload, "ts_ns")
    filled_volume = _safe_int(payload, "filled_volume")
    trade_id = _safe_str(payload, "trade_id")
    return f"{client_order_id}|{event_source}|{ts_ns}|{filled_volume}|{trade_id}"


def _iter_wal(path: Path, min_seq_exclusive: int, max_records: int) -> list[WalRecord]:
    rows: list[WalRecord] = []
    with path.open("r", encoding="utf-8") as fp:
        for raw_line in fp:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            seq = _safe_int(payload, "seq", 0)
            kind = _safe_str(payload, "kind")
            if seq <= min_seq_exclusive:
                continue
            if kind not in {"order", "trade"}:
                continue
            rows.append(WalRecord(seq=seq, kind=kind, payload=payload))
            if max_records > 0 and len(rows) >= max_records:
                break
    rows.sort(key=lambda item: item.seq)
    return rows


def _load_pg_driver() -> tuple[Any, str] | tuple[None, str]:
    try:
        import psycopg  # type: ignore

        return psycopg, "psycopg"
    except ModuleNotFoundError:
        pass
    try:
        import psycopg2  # type: ignore

        return psycopg2, "psycopg2"
    except ModuleNotFoundError:
        return None, ""


def _connect(args: argparse.Namespace) -> Any:
    driver, _ = _load_pg_driver()
    if driver is None:
        raise RuntimeError("psycopg/psycopg2 is required for --dry-run=false execution")
    if args.timescale_dsn:
        return driver.connect(args.timescale_dsn)
    return driver.connect(
        host=args.timescale_host,
        port=args.timescale_port,
        dbname=args.timescale_db,
        user=args.timescale_user,
        password=args.timescale_password,
    )


def _insert_order(cur: Any, schema: str, payload: dict[str, Any]) -> None:
    ts_ns = _safe_int(payload, "ts_ns")
    exchange_ts_ns = _safe_int(payload, "exchange_ts_ns", ts_ns)
    recv_ts_ns = _safe_int(payload, "recv_ts_ns", ts_ns)
    row = {
        "trade_date": _build_trade_date(recv_ts_ns if recv_ts_ns > 0 else ts_ns),
        "idempotency_key": _build_idempotency_key(payload),
        "account_id": _safe_str(payload, "account_id"),
        "client_order_id": _safe_str(payload, "client_order_id"),
        "exchange_order_id": _safe_str(payload, "exchange_order_id"),
        "instrument_id": _safe_str(payload, "instrument_id"),
        "exchange_id": _safe_str(payload, "exchange_id"),
        "status": str(_safe_int(payload, "status")),
        "total_volume": _safe_int(payload, "total_volume"),
        "filled_volume": _safe_int(payload, "filled_volume"),
        "avg_fill_price": _safe_float(payload, "avg_fill_price"),
        "reason": _safe_str(payload, "reason"),
        "status_msg": _safe_str(payload, "status_msg"),
        "order_submit_status": _safe_str(payload, "order_submit_status"),
        "order_ref": _safe_str(payload, "order_ref", _safe_str(payload, "client_order_id")),
        "front_id": _safe_int(payload, "front_id"),
        "session_id": _safe_int(payload, "session_id"),
        "trade_id": _safe_str(payload, "trade_id"),
        "event_source": _safe_str(payload, "event_source"),
        "exchange_ts_ns": exchange_ts_ns,
        "recv_ts_ns": recv_ts_ns,
        "ts_ns": ts_ns,
        "trace_id": _safe_str(payload, "trace_id"),
        "execution_algo_id": _safe_str(payload, "execution_algo_id"),
        "slice_index": _safe_int(payload, "slice_index"),
        "slice_total": _safe_int(payload, "slice_total"),
        "throttle_applied": _safe_int(payload, "throttle_applied"),
        "venue": _safe_str(payload, "venue"),
        "route_id": _safe_str(payload, "route_id"),
        "slippage_bps": _safe_float(payload, "slippage_bps"),
        "impact_cost": _safe_float(payload, "impact_cost"),
    }
    columns = list(row.keys())
    values = [row[column] for column in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    sql = (
        f"INSERT INTO {schema}.order_events ({', '.join(columns)}) "
        f"VALUES ({placeholders}) ON CONFLICT (trade_date, idempotency_key) DO NOTHING"
    )
    cur.execute(sql, values)


def _insert_trade(cur: Any, schema: str, payload: dict[str, Any]) -> None:
    ts_ns = _safe_int(payload, "ts_ns")
    exchange_ts_ns = _safe_int(payload, "exchange_ts_ns", ts_ns)
    recv_ts_ns = _safe_int(payload, "recv_ts_ns", ts_ns)
    row = {
        "trade_date": _build_trade_date(recv_ts_ns if recv_ts_ns > 0 else ts_ns),
        "idempotency_key": _build_idempotency_key(payload),
        "account_id": _safe_str(payload, "account_id"),
        "client_order_id": _safe_str(payload, "client_order_id"),
        "exchange_order_id": _safe_str(payload, "exchange_order_id"),
        "instrument_id": _safe_str(payload, "instrument_id"),
        "exchange_id": _safe_str(payload, "exchange_id"),
        "trade_id": _safe_str(payload, "trade_id"),
        "filled_volume": _safe_int(payload, "filled_volume"),
        "avg_fill_price": _safe_float(payload, "avg_fill_price"),
        "exchange_ts_ns": exchange_ts_ns,
        "recv_ts_ns": recv_ts_ns,
        "ts_ns": ts_ns,
        "trace_id": _safe_str(payload, "trace_id"),
        "event_source": _safe_str(payload, "event_source"),
    }
    columns = list(row.keys())
    values = [row[column] for column in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    sql = (
        f"INSERT INTO {schema}.trade_events ({', '.join(columns)}) "
        f"VALUES ({placeholders}) ON CONFLICT (trade_date, idempotency_key) DO NOTHING"
    )
    cur.execute(sql, values)


def main() -> int:
    args = _parse_args()
    wal_path = Path(args.wal_path)
    checkpoint_path = Path(args.checkpoint_file)
    if not wal_path.exists():
        print(f"error: wal file not found: {wal_path}", file=sys.stderr)
        return 2

    last_seq = _load_checkpoint(checkpoint_path)
    rows = _iter_wal(wal_path, last_seq, args.max_records)
    if not rows:
        print("no new wal records")
        return 0

    if args.dry_run:
        summary = {
            "mode": "dry_run",
            "wal_path": str(wal_path),
            "from_seq_exclusive": last_seq,
            "to_seq_inclusive": rows[-1].seq,
            "records": len(rows),
            "orders": sum(1 for row in rows if row.kind == "order"),
            "trades": sum(1 for row in rows if row.kind == "trade"),
            "schema": args.schema,
        }
        print(json.dumps(summary, ensure_ascii=True))
        return 0

    conn = _connect(args)
    try:
        inserted_order = 0
        inserted_trade = 0
        with conn:
            with conn.cursor() as cur:
                for row in rows:
                    if row.kind == "order":
                        _insert_order(cur, args.schema, row.payload)
                        inserted_order += 1
                    else:
                        _insert_trade(cur, args.schema, row.payload)
                        inserted_trade += 1
        _save_checkpoint(checkpoint_path, str(wal_path), rows[-1].seq)
    finally:
        conn.close()

    summary = {
        "mode": "execute",
        "wal_path": str(wal_path),
        "from_seq_exclusive": last_seq,
        "to_seq_inclusive": rows[-1].seq,
        "records": len(rows),
        "orders": inserted_order,
        "trades": inserted_trade,
        "checkpoint_file": str(checkpoint_path),
    }
    print(json.dumps(summary, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
