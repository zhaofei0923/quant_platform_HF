#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify M2 single-host dataflow evidence across ops health, "
            "reconcile, and lifecycle reports"
        )
    )
    parser.add_argument("--ops-health-json", required=True)
    parser.add_argument("--reconcile-json", required=True)
    parser.add_argument("--lifecycle-json", required=True)
    parser.add_argument("--min-kafka-publish-success-rate", type=float, default=0.99)
    parser.add_argument("--max-kafka-spool-backlog", type=float, default=1000.0)
    parser.add_argument("--max-cdc-lag-seconds", type=float, default=5.0)
    parser.add_argument("--max-clickhouse-lag-seconds", type=float, default=3.0)
    parser.add_argument("--min-parquet-lifecycle-success", type=float, default=1.0)
    return parser


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid JSON object: {path}")
    return payload


def _to_float(value: object, *, field: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc


def _collect_sli_values(ops_payload: dict[str, object]) -> dict[str, float]:
    raw_slis = ops_payload.get("slis")
    if not isinstance(raw_slis, list):
        raise ValueError("ops health payload missing slis list")
    values: dict[str, float] = {}
    for item in raw_slis:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        value = item.get("value")
        if not isinstance(name, str) or value is None:
            continue
        values[name] = _to_float(value, field=name)
    return values


def main() -> int:
    args = _build_parser().parse_args()
    try:
        ops_payload = _load_json(Path(args.ops_health_json))
        reconcile_payload = _load_json(Path(args.reconcile_json))
        lifecycle_payload = _load_json(Path(args.lifecycle_json))

        sli_values = _collect_sli_values(ops_payload)
        required = {
            "quant_hft_kafka_publish_success_rate": args.min_kafka_publish_success_rate,
            "quant_hft_kafka_spool_backlog": args.max_kafka_spool_backlog,
            "quant_hft_cdc_lag_seconds": args.max_cdc_lag_seconds,
            "quant_hft_clickhouse_ingest_lag_seconds": args.max_clickhouse_lag_seconds,
            "quant_hft_parquet_lifecycle_success": args.min_parquet_lifecycle_success,
        }
        for key in required:
            if key not in sli_values:
                raise ValueError(f"missing required SLI: {key}")

        if sli_values["quant_hft_kafka_publish_success_rate"] < args.min_kafka_publish_success_rate:
            raise ValueError("kafka publish success rate below threshold")
        if sli_values["quant_hft_kafka_spool_backlog"] > args.max_kafka_spool_backlog:
            raise ValueError("kafka spool backlog exceeds threshold")
        if sli_values["quant_hft_cdc_lag_seconds"] > args.max_cdc_lag_seconds:
            raise ValueError("cdc lag exceeds threshold")
        if sli_values["quant_hft_clickhouse_ingest_lag_seconds"] > args.max_clickhouse_lag_seconds:
            raise ValueError("clickhouse ingest lag exceeds threshold")
        if sli_values["quant_hft_parquet_lifecycle_success"] < args.min_parquet_lifecycle_success:
            raise ValueError("parquet lifecycle success below threshold")

        consistent = bool(reconcile_payload.get("consistent", False))
        if not consistent:
            raise ValueError("reconcile report is not consistent")

        lifecycle_mode = str(lifecycle_payload.get("mode", "")).strip().lower()
        if lifecycle_mode not in {"local", "object-store"}:
            raise ValueError("unknown lifecycle report mode")
        if bool(lifecycle_payload.get("dry_run", True)):
            raise ValueError("lifecycle report indicates dry-run")

    except (OSError, ValueError) as exc:
        print(f"error: m2 dataflow evidence verification failed: {exc}")
        return 2

    print("verification passed: m2 dataflow evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
