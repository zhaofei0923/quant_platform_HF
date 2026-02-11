#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

try:
    from quant_hft.data_pipeline.data_dictionary import DataDictionary
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.data_pipeline.data_dictionary import DataDictionary  # type: ignore[no-redef]


_RECONCILE_FIELDS = (
    "instrument_id",
    "status",
    "total_volume",
    "filled_volume",
    "avg_fill_price",
    "trace_id",
    "execution_algo_id",
    "slice_index",
    "slice_total",
    "throttle_applied",
    "venue",
    "route_id",
    "slippage_bps",
    "impact_cost",
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile Redis order snapshot against Timescale order events"
    )
    parser.add_argument("--redis-json-file", required=True)
    parser.add_argument("--timescale-csv-file", required=True)
    parser.add_argument("--report-json", default="docs/results/data_reconcile_report.json")
    parser.add_argument("--max-mismatches", type=int, default=50)
    return parser


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def _to_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    return int(str(value))


def _to_float(value: object) -> float:
    if isinstance(value, bool):
        return float(value)
    return float(str(value))


def _normalize(row: dict[str, object]) -> dict[str, object]:
    out = dict(row)
    if "ts_ns" in out and str(out["ts_ns"]).strip() != "":
        out["ts_ns"] = _to_int(out["ts_ns"])
    if "total_volume" in out:
        out["total_volume"] = _to_int(out["total_volume"])
    if "filled_volume" in out:
        out["filled_volume"] = _to_int(out["filled_volume"])
    if "slice_index" in out and str(out["slice_index"]).strip() != "":
        out["slice_index"] = _to_int(out["slice_index"])
    if "slice_total" in out and str(out["slice_total"]).strip() != "":
        out["slice_total"] = _to_int(out["slice_total"])
    if "avg_fill_price" in out:
        out["avg_fill_price"] = _to_float(out["avg_fill_price"])
    if "slippage_bps" in out and str(out["slippage_bps"]).strip() != "":
        out["slippage_bps"] = _to_float(out["slippage_bps"])
    if "impact_cost" in out and str(out["impact_cost"]).strip() != "":
        out["impact_cost"] = _to_float(out["impact_cost"])
    if "throttle_applied" in out:
        out["throttle_applied"] = _to_bool(out["throttle_applied"])
    return out


def _load_redis(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        if "records" in payload and isinstance(payload["records"], list):
            return [dict(item) for item in payload["records"] if isinstance(item, dict)]
        records: list[dict[str, object]] = []
        for maybe_fields in payload.values():
            if isinstance(maybe_fields, dict):
                records.append(dict(maybe_fields))
        return records
    return []


def _load_timescale_csv(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        return [dict(row) for row in reader]


def _latest_by_order(records: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for row in records:
        order_id = str(row.get("client_order_id", "")).strip()
        if not order_id:
            continue
        ts_value = _to_int(row.get("ts_ns", 0)) if "ts_ns" in row else 0
        existing = out.get(order_id)
        if existing is None or _to_int(existing.get("ts_ns", 0)) <= ts_value:
            out[order_id] = row
    return out


def main() -> int:
    args = _build_parser().parse_args()

    dictionary = DataDictionary()
    redis_rows = [_normalize(item) for item in _load_redis(Path(args.redis_json_file))]
    timescale_rows = [
        _normalize(item) for item in _load_timescale_csv(Path(args.timescale_csv_file))
    ]

    dictionary_errors: list[str] = []
    for idx, row in enumerate(redis_rows):
        for issue in dictionary.validate("redis_order_event", row):
            dictionary_errors.append(f"redis[{idx}]: {issue}")
    for idx, row in enumerate(timescale_rows):
        for issue in dictionary.validate("timescale_order_event", row):
            dictionary_errors.append(f"timescale[{idx}]: {issue}")

    redis_latest = _latest_by_order(redis_rows)
    timescale_latest = _latest_by_order(timescale_rows)

    missing_in_redis = sorted(set(timescale_latest) - set(redis_latest))
    missing_in_timescale = sorted(set(redis_latest) - set(timescale_latest))

    field_mismatches: list[dict[str, Any]] = []
    for order_id in sorted(set(redis_latest) & set(timescale_latest)):
        left = redis_latest[order_id]
        right = timescale_latest[order_id]
        for field_name in _RECONCILE_FIELDS:
            if field_name not in left and field_name not in right:
                continue
            left_value = left.get(field_name)
            right_value = right.get(field_name)
            if left_value != right_value:
                field_mismatches.append(
                    {
                        "client_order_id": order_id,
                        "field": field_name,
                        "redis": left_value,
                        "timescale": right_value,
                    }
                )
                if len(field_mismatches) >= max(1, args.max_mismatches):
                    break
        if len(field_mismatches) >= max(1, args.max_mismatches):
            break

    mismatch_count = len(missing_in_redis) + len(missing_in_timescale) + len(field_mismatches)
    consistent = mismatch_count == 0 and not dictionary_errors
    classification = "consistent"
    severity = "info"
    auto_fixable = False
    if dictionary_errors:
        classification = "schema_violation"
        severity = "critical"
    elif missing_in_redis or missing_in_timescale:
        classification = "record_presence_mismatch"
        severity = "critical"
    elif field_mismatches:
        classification = "field_mismatch"
        severity = "warn"

    report = {
        "redis_records": len(redis_rows),
        "timescale_records": len(timescale_rows),
        "checked_order_count": len(set(redis_latest) | set(timescale_latest)),
        "missing_in_redis": missing_in_redis,
        "missing_in_timescale": missing_in_timescale,
        "field_mismatches": field_mismatches,
        "dictionary_errors": dictionary_errors,
        "mismatch_count": mismatch_count,
        "consistent": consistent,
        "severity": severity,
        "classification": classification,
        "auto_fixable": auto_fixable,
        "schema_versions": {
            "redis_order_event": dictionary.schema_version("redis_order_event"),
            "timescale_order_event": dictionary.schema_version("timescale_order_event"),
        },
        "migration_strategies": {
            "redis_order_event": dictionary.migration_strategy("redis_order_event"),
            "timescale_order_event": dictionary.migration_strategy("timescale_order_event"),
        },
    }

    output = Path(args.report_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(output))
    if consistent:
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
