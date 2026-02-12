#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify daily settlement precision against broker statements "
            "using historical sampled trading days."
        )
    )
    parser.add_argument("--dataset-json", required=True)
    parser.add_argument(
        "--result-json",
        default="docs/results/daily_settlement_precision_report.json",
    )
    parser.add_argument("--trading-day-field", default="trading_day")
    parser.add_argument("--local-field", default="local_balance")
    parser.add_argument("--broker-field", default="broker_balance")
    parser.add_argument("--min-days", type=int, default=10)
    parser.add_argument("--tolerance", type=float, default=0.01)
    return parser


def _to_decimal(value: Any, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"invalid decimal for {field_name}: {value}") from exc


def _load_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        rows = payload["rows"]
    else:
        raise ValueError("dataset json must be a list or an object containing 'rows' list")
    if not all(isinstance(row, dict) for row in rows):
        raise ValueError("dataset rows must be json objects")
    return rows


def main() -> int:
    args = _build_parser().parse_args()
    dataset_path = Path(args.dataset_json)
    result_path = Path(args.result_json)
    tolerance = Decimal(str(args.tolerance))
    min_days = max(1, int(args.min_days))

    if not dataset_path.exists():
        print(f"error: dataset file not found: {dataset_path}")
        return 2

    try:
        rows = _load_rows(dataset_path)
        if not rows:
            raise ValueError("dataset rows are empty")

        failed_days: list[dict[str, Any]] = []
        total_diff = Decimal("0")
        max_abs_diff = Decimal("0")
        for row in rows:
            trading_day = str(row.get(args.trading_day_field, ""))
            local_value = _to_decimal(row.get(args.local_field), args.local_field)
            broker_value = _to_decimal(row.get(args.broker_field), args.broker_field)
            abs_diff = abs(local_value - broker_value)
            total_diff += abs_diff
            if abs_diff > max_abs_diff:
                max_abs_diff = abs_diff
            if abs_diff > tolerance:
                failed_days.append(
                    {
                        "trading_day": trading_day,
                        "local_value": str(local_value),
                        "broker_value": str(broker_value),
                        "abs_diff": str(abs_diff),
                    }
                )

        total_days = len(rows)
        mean_abs_diff = total_diff / Decimal(total_days)
        passed = total_days >= min_days and not failed_days

        result: dict[str, Any] = {
            "dataset_json": str(dataset_path),
            "total_days": total_days,
            "min_days_required": min_days,
            "tolerance": str(tolerance),
            "max_abs_diff": str(max_abs_diff),
            "mean_abs_diff": str(mean_abs_diff),
            "failed_days": failed_days,
            "passed": passed,
        }

        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(
            json.dumps(result, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
        print(str(result_path))
        return 0 if passed else 2
    except Exception as exc:  # pragma: no cover
        print(f"error: precision verification failed: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
