#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

try:
    from quant_hft.data_pipeline.adapters import DuckDbAnalyticsStore, MinioArchiveStore
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.data_pipeline.adapters import (  # type: ignore[no-redef]
        DuckDbAnalyticsStore,
        MinioArchiveStore,
    )


_DEFAULT_PARTITION_KEYS: dict[str, tuple[str, ...]] = {
    "market_snapshots": ("dt", "instrument_id"),
    "order_events": ("trade_date", "instrument_id"),
    "trade_events": ("trade_date", "instrument_id"),
    "account_snapshots": ("trade_date", "account_id"),
    "position_snapshots": ("trade_date", "account_id"),
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export partitioned parquet artifacts and archive them to "
            "MinIO/S3-compatible storage"
        )
    )
    parser.add_argument("--analytics-db", default="runtime/analytics.duckdb")
    parser.add_argument(
        "--tables",
        default="market_snapshots,order_events,trade_events,account_snapshots,position_snapshots",
    )
    parser.add_argument("--output-dir", default="runtime/parquet_partitions")
    parser.add_argument("--compression", default="zstd")
    parser.add_argument("--archive-endpoint", default="localhost:9000")
    parser.add_argument("--archive-access-key", default="minioadmin")
    parser.add_argument("--archive-secret-key", default="minioadmin")
    parser.add_argument("--archive-bucket", default="quant-archive")
    parser.add_argument("--archive-local-dir", default="runtime/archive")
    parser.add_argument("--archive-prefix", default="parquet")
    parser.add_argument("--prefer-duckdb", action="store_true")
    parser.add_argument("--report-json", default="docs/results/parquet_partitions_report.json")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.execute and args.dry_run:
        parser.error("use either --dry-run or --execute")
    if not args.execute and not args.dry_run:
        args.dry_run = True
    return args


def _normalize_tables(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def main() -> int:
    args = _build_parser()
    tables = _normalize_tables(args.tables)
    if not tables:
        raise SystemExit("empty --tables")

    store = DuckDbAnalyticsStore(Path(args.analytics_db), prefer_duckdb=bool(args.prefer_duckdb))
    archive = MinioArchiveStore(
        endpoint=args.archive_endpoint,
        access_key=args.archive_access_key,
        secret_key=args.archive_secret_key,
        bucket=args.archive_bucket,
        local_fallback_dir=Path(args.archive_local_dir),
    )
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    table_reports: dict[str, object] = {}
    archived_objects: list[str] = []
    success = True
    error = ""

    try:
        for table in tables:
            partition_keys = _DEFAULT_PARTITION_KEYS.get(table)
            if partition_keys is None:
                table_reports[table] = {
                    "status": "skipped",
                    "reason": "no partition policy configured",
                    "partition_keys": [],
                    "artifacts": [],
                }
                continue

            try:
                table_rows = store.read_table_as_dicts(table, limit=None)
            except Exception as exc:
                table_reports[table] = {
                    "status": "skipped",
                    "reason": str(exc),
                    "partition_keys": list(partition_keys),
                    "artifacts": [],
                }
                continue

            if args.dry_run:
                row_count = len(table_rows)
                table_reports[table] = {
                    "status": "simulated",
                    "partition_keys": list(partition_keys),
                    "row_count": row_count,
                    "artifacts": [],
                }
                continue

            table_root = output_root / table
            artifacts = store.export_table_to_parquet_partitions(
                table,
                table_root,
                partition_keys=partition_keys,
                compression=args.compression,
            )
            artifact_reports: list[dict[str, object]] = []
            for artifact in artifacts:
                local_path = table_root / artifact.relative_path
                object_name = (
                    f"{args.archive_prefix.strip('/')}/{table}/hot/{artifact.relative_path}".strip(
                        "/"
                    )
                )
                archive.put_file(object_name, local_path)
                archived_objects.append(object_name)
                artifact_reports.append(
                    {
                        "relative_path": artifact.relative_path,
                        "row_count": artifact.row_count,
                        "sha256": artifact.sha256,
                        "min_ts_ns": artifact.min_ts_ns,
                        "max_ts_ns": artifact.max_ts_ns,
                        "format": artifact.format,
                        "object_name": object_name,
                    }
                )

            table_reports[table] = {
                "status": "ok",
                "partition_keys": list(partition_keys),
                "row_count": sum(item.row_count for item in artifacts),
                "artifact_count": len(artifacts),
                "artifacts": artifact_reports,
            }
    except Exception as exc:
        success = False
        error = str(exc)
    finally:
        store.close()

    report = {
        "generated_ts_ns": time.time_ns(),
        "run_id": run_id,
        "mode": "execute" if args.execute else "dry-run",
        "success": success,
        "error": error,
        "analytics_db": str(Path(args.analytics_db)),
        "output_dir": str(output_root),
        "archive_bucket": args.archive_bucket,
        "archive_mode": archive.mode,
        "archive_prefix": args.archive_prefix.strip("/"),
        "tables": table_reports,
        "archived_objects": archived_objects,
    }

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(report_path))
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
