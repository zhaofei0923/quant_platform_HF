#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from quant_hft.data_pipeline.clickhouse_parquet_archive import (
        ClickHouseParquetArchiveConfig,
        archive_ticks_to_parquet,
        build_market_ticks_query,
    )
except ModuleNotFoundError:  # pragma: no cover - script fallback path
    REPO_ROOT = Path(__file__).resolve().parents[2]
    PYTHON_ROOT = REPO_ROOT / "python"
    if str(PYTHON_ROOT) not in sys.path:
        sys.path.insert(0, str(PYTHON_ROOT))
    from quant_hft.data_pipeline.clickhouse_parquet_archive import (  # type: ignore[no-redef]
        ClickHouseParquetArchiveConfig,
        archive_ticks_to_parquet,
        build_market_ticks_query,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export ClickHouse market ticks into trading-day parquet partitions",
    )
    parser.add_argument("--clickhouse-dsn", default="", help="ClickHouse HTTP DSN")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--table", default="quant_hft.market_ticks")
    parser.add_argument("--start-trading-day", default="")
    parser.add_argument("--end-trading-day", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--compression", default="zstd")
    parser.add_argument("--allow-jsonl-fallback", action="store_true")
    parser.add_argument("--report-json", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _report_payload(dry_run: bool, query: str, report: object | None) -> dict[str, object]:
    payload: dict[str, object] = {
        "dry_run": dry_run,
        "query": query,
    }
    if report is None:
        payload.update(
            {
                "row_count": 0,
                "output_files": [],
                "trading_days": [],
                "writer_backend": "none",
            }
        )
        return payload

    payload.update(
        {
            "row_count": int(getattr(report, "row_count", 0)),
            "output_files": [str(path) for path in getattr(report, "output_files", tuple())],
            "trading_days": list(getattr(report, "trading_days", tuple())),
            "writer_backend": str(getattr(report, "writer_backend", "unknown")),
        }
    )
    return payload


def main() -> int:
    args = _build_parser().parse_args()

    config = ClickHouseParquetArchiveConfig(
        clickhouse_dsn=args.clickhouse_dsn,
        output_dir=args.output_dir,
        table=args.table,
        start_trading_day=args.start_trading_day,
        end_trading_day=args.end_trading_day,
        limit=max(0, args.limit),
        timeout_seconds=max(1, args.timeout_seconds),
        compression=args.compression,
        allow_jsonl_fallback=args.allow_jsonl_fallback,
    )
    query = build_market_ticks_query(config)

    report = None
    if not args.dry_run:
        report = archive_ticks_to_parquet(config)

    payload = _report_payload(args.dry_run, query, report)
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
