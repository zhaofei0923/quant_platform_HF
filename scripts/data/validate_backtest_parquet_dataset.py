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
    parser.add_argument(
        "--conversion-report",
        default="",
        help="Optional conversion report json for cross-check",
    )
    return parser


def _has_required_partitions(path: Path) -> bool:
    normalized = path.as_posix()
    return all(segment in normalized for segment in _REQUIRED_PARTITIONS)


def _extract_symbol_prefix(value: str) -> str:
    prefix = []
    for ch in value.strip():
        if ch.isalpha():
            prefix.append(ch)
            continue
        break
    return "".join(prefix).lower()


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
    prefix_errors: list[str] = []
    instruments: set[str] = set()
    trading_days: set[str] = set()
    source_symbols: set[str] = set()
    source_to_days: dict[str, set[str]] = {}
    for file_path in files:
        relative = file_path.relative_to(root)
        if not _has_required_partitions(relative):
            partition_errors.append(str(relative))
            continue

        source_symbol = ""
        instrument_id = ""
        for part in relative.parts:
            if part.startswith("source="):
                source_symbol = part.split("=", 1)[1]
                source_symbols.add(source_symbol)
            if part.startswith("instrument_id="):
                instrument_id = part.split("=", 1)[1]
                instruments.add(instrument_id)
            if part.startswith("trading_day="):
                day = part.split("=", 1)[1]
                trading_days.add(day)
                if source_symbol:
                    source_to_days.setdefault(source_symbol, set()).add(day)

        if source_symbol and instrument_id:
            source_prefix = _extract_symbol_prefix(source_symbol)
            instrument_prefix = _extract_symbol_prefix(instrument_id)
            if source_prefix and instrument_prefix and source_prefix != instrument_prefix:
                prefix_errors.append(str(relative))

    if partition_errors:
        success = False
        errors.append(
            "invalid partition path: " + ", ".join(partition_errors[:5])
        )

    if prefix_errors:
        success = False
        errors.append(
            "source/instrument prefix mismatch: " + ", ".join(prefix_errors[:5])
        )

    conversion_cross_check: dict[str, object] = {
        "checked": False,
        "success": True,
        "error": "",
        "expected_rollover_event_count": 0,
        "actual_rollover_event_count": 0,
    }
    if args.conversion_report:
        conversion_cross_check["checked"] = True
        conversion_path = Path(args.conversion_report)
        if not conversion_path.exists():
            success = False
            conversion_cross_check["success"] = False
            conversion_cross_check["error"] = f"conversion report not found: {conversion_path}"
            errors.append(f"conversion report not found: {conversion_path}")
        else:
            try:
                payload = json.loads(conversion_path.read_text(encoding="utf-8"))
                expected_rollover = int(payload.get("rollover_event_count", 0))
                actual_rollover = 0
                for source_payload in payload.get("sources", {}).values():
                    if isinstance(source_payload, dict):
                        actual_rollover += int(source_payload.get("contract_switch_count", 0))
                conversion_cross_check["expected_rollover_event_count"] = expected_rollover
                conversion_cross_check["actual_rollover_event_count"] = actual_rollover
                if expected_rollover != actual_rollover:
                    success = False
                    conversion_cross_check["success"] = False
                    conversion_cross_check["error"] = (
                        "rollover counts in conversion report are inconsistent"
                    )
                    errors.append("rollover counts in conversion report are inconsistent")
            except Exception as exc:  # pylint: disable=broad-except
                success = False
                conversion_cross_check["success"] = False
                conversion_cross_check["error"] = str(exc)
                errors.append(f"failed to parse conversion report: {exc}")

    source_day_coverage = {
        source: {
            "day_count": len(days),
            "first_day": min(days) if days else "",
            "last_day": max(days) if days else "",
        }
        for source, days in sorted(source_to_days.items())
    }

    report = {
        "generated_ts_ns": time.time_ns(),
        "duration_ms": (time.time_ns() - started_ns) / 1_000_000.0,
        "dataset_root": str(root),
        "report_path": str(report_path),
        "success": success,
        "errors": errors,
        "file_count": len(files),
        "source_symbols": sorted(source_symbols),
        "instrument_universe": sorted(instruments),
        "trading_days": sorted(trading_days),
        "prefix_mismatch_count": len(prefix_errors),
        "prefix_mismatch_samples": prefix_errors[:20],
        "source_day_coverage": source_day_coverage,
        "conversion_cross_check": conversion_cross_check,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(report_path))
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
