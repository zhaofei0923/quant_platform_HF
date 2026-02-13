#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

_REQUIRED_PARTITIONS = ("source=", "trading_day=", "instrument_id=")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate partition layout for converted backtest parquet dataset"
    )
    parser.add_argument("--dataset-root", default="runtime/backtest/parquet")
    parser.add_argument(
        "--report-json",
        default="docs/results/backtest_parquet_validation_report.json",
    )
    parser.add_argument("--require-non-empty", action="store_true")
    return parser


def _has_required_partitions(path: Path) -> bool:
    normalized = path.as_posix()
    return all(segment in normalized for segment in _REQUIRED_PARTITIONS)


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    started_ns = time.time_ns()
    root = Path(args.dataset_root)
    report_path = Path(args.report_json)

    success = True
    errors: list[str] = []
    files: list[Path] = []

    if not root.exists():
        success = False
        errors.append(f"dataset root not found: {root}")
    else:
        files = sorted(root.rglob("*.parquet"))
        if args.require_non_empty and not files:
            success = False
            errors.append("dataset is empty")

    partition_errors: list[str] = []
    instruments: set[str] = set()
    trading_days: set[str] = set()
    for file_path in files:
        relative = file_path.relative_to(root)
        if not _has_required_partitions(relative):
            partition_errors.append(str(relative))
            continue
        for part in relative.parts:
            if part.startswith("instrument_id="):
                instruments.add(part.split("=", 1)[1])
            if part.startswith("trading_day="):
                trading_days.add(part.split("=", 1)[1])

    if partition_errors:
        success = False
        errors.append(
            "invalid partition path: " + ", ".join(partition_errors[:5])
        )

    report = {
        "generated_ts_ns": time.time_ns(),
        "duration_ms": (time.time_ns() - started_ns) / 1_000_000.0,
        "dataset_root": str(root),
        "report_path": str(report_path),
        "success": success,
        "errors": errors,
        "file_count": len(files),
        "instrument_universe": sorted(instruments),
        "trading_days": sorted(trading_days),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(report_path))
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
