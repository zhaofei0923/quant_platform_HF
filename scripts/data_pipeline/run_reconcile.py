#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from itertools import combinations
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
        description="Reconcile Redis/analytics_ts/trading_core order snapshots"
    )
    parser.add_argument("--redis-json-file", required=True)
    parser.add_argument("--analytics-csv-file", default="")
    parser.add_argument("--timescale-csv-file", default="")
    parser.add_argument("--trading-core-csv-file", default="")
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


def _load_csv(path: Path) -> list[dict[str, object]]:
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


def _build_field_mismatch(
    *,
    order_id: str,
    field_name: str,
    values: dict[str, object],
) -> dict[str, object]:
    item: dict[str, object] = {
        "client_order_id": order_id,
        "field": field_name,
        "values": dict(values),
    }
    for source_name, value in values.items():
        alias = "timescale" if source_name == "analytics_ts" else source_name
        item[alias] = value
    return item


def main() -> int:
    args = _build_parser().parse_args()

    analytics_path_raw = args.analytics_csv_file.strip() or args.timescale_csv_file.strip()
    if not analytics_path_raw:
        print(
            "error: one of --analytics-csv-file or --timescale-csv-file is required",
            file=sys.stderr,
        )
        return 2

    dictionary = DataDictionary()
    redis_rows = [_normalize(item) for item in _load_redis(Path(args.redis_json_file))]
    analytics_rows = [_normalize(item) for item in _load_csv(Path(analytics_path_raw))]
    include_trading_core = bool(args.trading_core_csv_file.strip())
    trading_rows: list[dict[str, object]] = []
    if include_trading_core:
        trading_rows = [_normalize(item) for item in _load_csv(Path(args.trading_core_csv_file))]

    dictionary_errors: list[str] = []
    for idx, row in enumerate(redis_rows):
        for issue in dictionary.validate("redis_order_event", row):
            dictionary_errors.append(f"redis[{idx}]: {issue}")
    for idx, row in enumerate(analytics_rows):
        for issue in dictionary.validate("timescale_order_event", row):
            dictionary_errors.append(f"analytics_ts[{idx}]: {issue}")
    for idx, row in enumerate(trading_rows):
        for issue in dictionary.validate("timescale_order_event", row):
            dictionary_errors.append(f"trading_core[{idx}]: {issue}")

    redis_latest = _latest_by_order(redis_rows)
    analytics_latest = _latest_by_order(analytics_rows)
    source_latest: dict[str, dict[str, dict[str, object]]] = {
        "redis": redis_latest,
        "analytics_ts": analytics_latest,
    }
    if include_trading_core:
        source_latest["trading_core"] = _latest_by_order(trading_rows)

    all_order_ids: set[str] = set()
    for latest_rows in source_latest.values():
        all_order_ids.update(latest_rows)

    missing_by_source = {
        source_name: sorted(all_order_ids - set(latest_rows))
        for source_name, latest_rows in source_latest.items()
    }
    missing_in_redis = sorted(set(analytics_latest) - set(redis_latest))
    missing_in_timescale = sorted(set(redis_latest) - set(analytics_latest))
    missing_in_trading_core = missing_by_source.get("trading_core", [])

    field_mismatches: list[dict[str, Any]] = []
    for order_id in sorted(all_order_ids):
        rows_by_source = {
            source_name: latest_rows[order_id]
            for source_name, latest_rows in source_latest.items()
            if order_id in latest_rows
        }
        if len(rows_by_source) < 2:
            continue
        for field_name in _RECONCILE_FIELDS:
            if all(field_name not in row for row in rows_by_source.values()):
                continue
            values = {
                source_name: row.get(field_name) for source_name, row in rows_by_source.items()
            }
            has_mismatch = any(
                values[left] != values[right] for left, right in combinations(sorted(values), 2)
            )
            if has_mismatch:
                field_mismatches.append(
                    _build_field_mismatch(order_id=order_id, field_name=field_name, values=values)
                )
                if len(field_mismatches) >= max(1, args.max_mismatches):
                    break
        if len(field_mismatches) >= max(1, args.max_mismatches):
            break

    presence_mismatch_count = sum(len(items) for items in missing_by_source.values())
    mismatch_count = presence_mismatch_count + len(field_mismatches)
    consistent = mismatch_count == 0 and not dictionary_errors
    classification = "consistent"
    severity = "info"
    auto_fixable = False
    if dictionary_errors:
        classification = "schema_violation"
        severity = "critical"
    elif presence_mismatch_count > 0:
        classification = "record_presence_mismatch"
        severity = "critical"
    elif field_mismatches:
        classification = "field_mismatch"
        severity = "warn"

    schema_versions = {
        "redis_order_event": dictionary.schema_version("redis_order_event"),
        "timescale_order_event": dictionary.schema_version("timescale_order_event"),
        "analytics_ts_order_event": dictionary.schema_version("timescale_order_event"),
    }
    migration_strategies = {
        "redis_order_event": dictionary.migration_strategy("redis_order_event"),
        "timescale_order_event": dictionary.migration_strategy("timescale_order_event"),
        "analytics_ts_order_event": dictionary.migration_strategy("timescale_order_event"),
    }
    source_records: dict[str, int] = {
        "redis": len(redis_rows),
        "analytics_ts": len(analytics_rows),
    }
    if include_trading_core:
        schema_versions["trading_core_order_event"] = dictionary.schema_version(
            "timescale_order_event"
        )
        migration_strategies["trading_core_order_event"] = dictionary.migration_strategy(
            "timescale_order_event"
        )
        source_records["trading_core"] = len(trading_rows)

    report = {
        "source_records": source_records,
        "compared_sources": list(source_latest.keys()),
        "redis_records": len(redis_rows),
        "timescale_records": len(analytics_rows),
        "trading_core_records": len(trading_rows),
        "checked_order_count": len(all_order_ids),
        "missing_by_source": missing_by_source,
        "missing_in_redis": missing_in_redis,
        "missing_in_timescale": missing_in_timescale,
        "missing_in_trading_core": missing_in_trading_core,
        "field_mismatches": field_mismatches,
        "dictionary_errors": dictionary_errors,
        "mismatch_count": mismatch_count,
        "consistent": consistent,
        "severity": severity,
        "classification": classification,
        "auto_fixable": auto_fixable,
        "schema_versions": schema_versions,
        "migration_strategies": migration_strategies,
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
