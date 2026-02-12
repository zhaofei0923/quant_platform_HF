#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export PostgreSQL business truth tables into partitioned files for cold archive"
        ),
    )
    parser.add_argument("--dsn", default="", help="PostgreSQL DSN")
    parser.add_argument("--schema", default="trading_core")
    parser.add_argument(
        "--tables",
        default="orders,trades,position_detail,account_funds,risk_events",
        help="Comma-separated table list",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--format", choices=("parquet", "csv"), default="parquet")
    parser.add_argument("--report-json", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _normalize_tables(raw: str) -> list[str]:
    tables = [item.strip() for item in raw.split(",")]
    return [item for item in tables if item]


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    args = _parse_args()
    tables = _normalize_tables(args.tables)
    if not tables:
        print("error: --tables must include at least one table")
        return 2

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, Any]] = []

    if not args.dry_run:
        if not args.dsn.strip():
            print("error: --dsn is required when not using --dry-run")
            return 2
        try:
            import pandas as pd  # type: ignore[import-not-found]
            import psycopg  # type: ignore[import-not-found]
        except ModuleNotFoundError as exc:
            print(f"error: missing dependency for export: {exc}")
            return 2

        with psycopg.connect(args.dsn) as conn:
            for table in tables:
                where_clauses: list[str] = []
                if args.start_date:
                    where_clauses.append(f"update_time >= '{args.start_date}'::timestamptz")
                if args.end_date:
                    where_clauses.append(f"update_time < '{args.end_date}'::timestamptz")
                where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
                query = f"SELECT * FROM {args.schema}.{table}{where_sql}"
                frame = pd.read_sql_query(query, conn)
                extension = "parquet" if args.format == "parquet" else "csv"
                output_file = output_dir / f"{table}.{extension}"
                if args.format == "parquet":
                    frame.to_parquet(output_file, index=False)
                else:
                    frame.to_csv(output_file, index=False)
                manifest.append(
                    {
                        "table_name": f"{args.schema}.{table}",
                        "row_count": int(frame.shape[0]),
                        "file_path": str(output_file),
                        "file_size": int(output_file.stat().st_size),
                        "checksum": _hash_file(output_file),
                    }
                )

    payload = {
        "dry_run": args.dry_run,
        "schema": args.schema,
        "tables": tables,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "format": args.format,
        "output_dir": str(output_dir),
        "manifest": manifest,
    }

    rendered = json.dumps(payload, ensure_ascii=True, indent=2)
    if args.report_json:
        report_path = Path(args.report_json)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(rendered + "\n", encoding="utf-8")
        print(str(report_path))
    else:
        print(rendered)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
