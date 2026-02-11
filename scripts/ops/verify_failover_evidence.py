#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

_REQUIRED_KEYS = (
    "FAILOVER_ENV",
    "FAILOVER_DRY_RUN",
    "FAILOVER_SUCCESS",
    "FAILOVER_TOTAL_STEPS",
    "FAILOVER_FAILED_STEP",
    "FAILOVER_DURATION_SECONDS",
    "DATA_SYNC_LAG_EVENTS",
)

_ALLOWED_STEP_STATUS = {"ok", "simulated_ok"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify failover drill evidence contract and threshold constraints"
    )
    parser.add_argument(
        "--evidence-file",
        required=True,
        help="Path to key=value failover evidence file",
    )
    parser.add_argument(
        "--max-failover-seconds",
        type=float,
        default=180.0,
        help="Maximum allowed failover duration in seconds (default: 180)",
    )
    parser.add_argument(
        "--max-data-lag-events",
        type=int,
        default=0,
        help="Maximum allowed data sync lag in events (default: 0)",
    )
    return parser.parse_args()


def _parse_kv_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for idx, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(f"line {idx} has empty key")
        values[key] = value
    return values


def _parse_non_negative_int(value: str, *, key: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if number < 0:
        raise ValueError(f"{key} must be >= 0")
    return number


def _parse_non_negative_float(value: str, *, key: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise ValueError(f"{key} must be a number") from exc
    if number < 0:
        raise ValueError(f"{key} must be >= 0")
    return number


def _parse_bool_token(value: str, *, key: str) -> bool:
    token = value.strip().lower()
    if token in {"true", "1", "yes"}:
        return True
    if token in {"false", "0", "no"}:
        return False
    raise ValueError(f"{key} must be true/false")


def _validate(
    values: dict[str, str], *, max_failover_seconds: float, max_data_lag_events: int
) -> None:
    for key in _REQUIRED_KEYS:
        if key not in values:
            raise ValueError(f"missing required key: {key}")
        if key == "FAILOVER_FAILED_STEP":
            continue
        if not values[key].strip():
            raise ValueError(f"missing required key: {key}")

    success = _parse_bool_token(values["FAILOVER_SUCCESS"], key="FAILOVER_SUCCESS")
    if not success:
        raise ValueError("FAILOVER_SUCCESS must be true")

    total_steps = _parse_non_negative_int(
        values["FAILOVER_TOTAL_STEPS"], key="FAILOVER_TOTAL_STEPS"
    )
    if total_steps <= 0:
        raise ValueError("FAILOVER_TOTAL_STEPS must be > 0")

    for idx in range(1, total_steps + 1):
        name_key = f"STEP_{idx}_NAME"
        status_key = f"STEP_{idx}_STATUS"
        duration_key = f"STEP_{idx}_DURATION_MS"
        if name_key not in values or not values[name_key].strip():
            raise ValueError(f"missing required key: {name_key}")
        if status_key not in values or not values[status_key].strip():
            raise ValueError(f"missing required key: {status_key}")
        if values[status_key] not in _ALLOWED_STEP_STATUS:
            raise ValueError(f"{status_key} has invalid value: {values[status_key]}")
        _parse_non_negative_int(values.get(duration_key, ""), key=duration_key)

    failover_duration_seconds = _parse_non_negative_float(
        values["FAILOVER_DURATION_SECONDS"],
        key="FAILOVER_DURATION_SECONDS",
    )
    if max_failover_seconds < 0:
        raise ValueError("max_failover_seconds must be >= 0")
    if failover_duration_seconds > max_failover_seconds:
        raise ValueError(
            "failover_duration_seconds threshold exceeded: "
            f"actual={failover_duration_seconds} max={max_failover_seconds}"
        )

    data_sync_lag_events = _parse_non_negative_int(
        values["DATA_SYNC_LAG_EVENTS"],
        key="DATA_SYNC_LAG_EVENTS",
    )
    if max_data_lag_events < 0:
        raise ValueError("max_data_lag_events must be >= 0")
    if data_sync_lag_events > max_data_lag_events:
        raise ValueError(
            "data_sync_lag_events threshold exceeded: "
            f"actual={data_sync_lag_events} max={max_data_lag_events}"
        )


def main() -> int:
    args = _parse_args()
    evidence_path = Path(args.evidence_file)
    if not evidence_path.exists():
        print(f"error: evidence file not found: {evidence_path}")
        return 2

    try:
        values = _parse_kv_file(evidence_path)
        _validate(
            values,
            max_failover_seconds=float(args.max_failover_seconds),
            max_data_lag_events=int(args.max_data_lag_events),
        )
    except (OSError, ValueError) as exc:
        print(f"error: failover evidence verification failed: {exc}")
        return 2

    print(f"verification passed: {evidence_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
