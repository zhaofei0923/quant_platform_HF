#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from quant_hft.data_pipeline import ArchiveConfig, DataPipelineConfig, DataPipelineProcess


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run quant_hft data pipeline process")
    parser.add_argument("--analytics-db", default="runtime/analytics.duckdb")
    parser.add_argument("--export-dir", default="runtime/exports")
    parser.add_argument(
        "--archive-endpoint",
        default=os.getenv("QUANT_HFT_ARCHIVE_ENDPOINT", "localhost:9000"),
    )
    parser.add_argument(
        "--archive-access-key",
        default=os.getenv("QUANT_HFT_ARCHIVE_ACCESS_KEY", "minioadmin"),
    )
    parser.add_argument(
        "--archive-secret-key",
        default=os.getenv("QUANT_HFT_ARCHIVE_SECRET_KEY", "minioadmin"),
    )
    parser.add_argument(
        "--archive-bucket",
        default=os.getenv("QUANT_HFT_ARCHIVE_BUCKET", "quant-archive"),
    )
    parser.add_argument(
        "--archive-local-dir",
        default=os.getenv("QUANT_HFT_ARCHIVE_LOCAL_DIR", "runtime/archive"),
    )
    parser.add_argument(
        "--archive-prefix",
        default=os.getenv("QUANT_HFT_ARCHIVE_PREFIX", "etl"),
    )
    parser.add_argument("--prefer-duckdb", action="store_true")
    parser.add_argument("--run-once", action="store_true")
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    archive = ArchiveConfig(
        endpoint=args.archive_endpoint,
        access_key=args.archive_access_key,
        secret_key=args.archive_secret_key,
        bucket=args.archive_bucket,
        local_fallback_dir=Path(args.archive_local_dir),
    )
    config = DataPipelineConfig(
        analytics_db_path=Path(args.analytics_db),
        export_dir=Path(args.export_dir),
        archive=archive,
        prefer_duckdb=bool(args.prefer_duckdb),
        interval_seconds=max(0.0, args.interval_seconds),
        archive_prefix=args.archive_prefix,
    )

    process = DataPipelineProcess(config)
    try:
        if args.run_once:
            report = process.run_once()
            print(
                json.dumps(
                    {
                        "run_id": report.run_id,
                        "exported_rows": report.exported_rows,
                        "archived_objects_count": report.archived_objects_count,
                        "manifest_path": str(report.manifest_path),
                    },
                    ensure_ascii=True,
                )
            )
            return 0

        iterations = args.iterations if args.iterations > 0 else None
        reports = process.run_loop(max_iterations=iterations)
        print(
            json.dumps(
                [
                    {
                        "run_id": report.run_id,
                        "exported_rows": report.exported_rows,
                        "archived_objects_count": report.archived_objects_count,
                        "manifest_path": str(report.manifest_path),
                    }
                    for report in reports
                ],
                ensure_ascii=True,
            )
        )
        return 0
    finally:
        process.close()


if __name__ == "__main__":
    raise SystemExit(main())
