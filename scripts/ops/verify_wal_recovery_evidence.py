#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

_REQUIRED_KEYS = (
    "drill_id",
    "release_version",
    "wal_path",
    "failure_start_utc",
    "recovery_complete_utc",
    "rto_seconds",
    "rpo_events",
    "operator",
    "result",
    "notes",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Verify WAL recovery evidence contract and threshold constraints " "(RTO/RPO)")
    )
    parser.add_argument(
        "--evidence-file",
        required=True,
        help="Path to key=value WAL recovery evidence file",
    )
    parser.add_argument(
        "--max-rto-seconds",
        type=float,
        default=10.0,
        help="Maximum allowed RTO in seconds (default: 10)",
    )
    parser.add_argument(
        "--max-rpo-events",
        type=int,
        default=0,
        help="Maximum allowed RPO event count (default: 0)",
    )
    return parser.parse_args()


def _parse_evidence(path: Path) -> dict[str, str]:
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


def _require_non_empty(values: dict[str, str], key: str) -> str:
    value = values.get(key, "").strip()
    if not value:
        raise ValueError(f"missing required key: {key}")
    return value


def _parse_iso8601_utc(value: str, *, key: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise ValueError(f"{key} must be ISO8601 UTC like 2026-02-11T12:00:00Z") from exc


def _parse_non_negative_float(value: str, *, key: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise ValueError(f"{key} must be a number") from exc
    if number < 0:
        raise ValueError(f"{key} must be >= 0")
    return number


def _parse_non_negative_int(value: str, *, key: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if number < 0:
        raise ValueError(f"{key} must be >= 0")
    return number


def _validate(values: dict[str, str], *, max_rto_seconds: float, max_rpo_events: int) -> None:
    for key in _REQUIRED_KEYS:
        _require_non_empty(values, key)

    failure_start = _parse_iso8601_utc(values["failure_start_utc"], key="failure_start_utc")
    recovery_complete = _parse_iso8601_utc(
        values["recovery_complete_utc"], key="recovery_complete_utc"
    )
    if recovery_complete < failure_start:
        raise ValueError("recovery_complete_utc must be >= failure_start_utc")

    rto_seconds = _parse_non_negative_float(values["rto_seconds"], key="rto_seconds")
    rpo_events = _parse_non_negative_int(values["rpo_events"], key="rpo_events")

    result = values["result"].strip().lower()
    if result not in {"pass", "fail"}:
        raise ValueError("result must be pass or fail")

    if max_rto_seconds < 0:
        raise ValueError("max_rto_seconds must be >= 0")
    if max_rpo_events < 0:
        raise ValueError("max_rpo_events must be >= 0")

    if rto_seconds > max_rto_seconds:
        raise ValueError(
            f"rto_seconds threshold exceeded: actual={rto_seconds} max={max_rto_seconds}"
        )
    if rpo_events > max_rpo_events:
        raise ValueError(f"rpo_events threshold exceeded: actual={rpo_events} max={max_rpo_events}")


def main() -> int:
    args = _parse_args()
    evidence_path = Path(args.evidence_file)

    if not evidence_path.exists():
        print(f"error: evidence file not found: {evidence_path}")
        return 2

    try:
        values = _parse_evidence(evidence_path)
        _validate(
            values,
            max_rto_seconds=float(args.max_rto_seconds),
            max_rpo_events=int(args.max_rpo_events),
        )
    except (OSError, ValueError) as exc:
        print(f"error: wal recovery evidence verification failed: {exc}")
        return 2

    print(f"verification passed: {evidence_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
