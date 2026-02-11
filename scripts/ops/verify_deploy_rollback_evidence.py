#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

_REQUIRED_KEYS = (
    "drill_id",
    "platform",
    "release_version",
    "rollback_target",
    "failure_injected",
    "rollback_start_utc",
    "rollback_complete_utc",
    "rollback_seconds",
    "health_check_passed",
    "result",
    "operator",
    "notes",
)

_ALLOWED_PLATFORMS = {"systemd", "k8s"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify deploy/rollback drill evidence contract and threshold constraints"
    )
    parser.add_argument(
        "--evidence-file",
        required=True,
        help="Path to key=value deploy rollback evidence file",
    )
    parser.add_argument(
        "--max-rollback-seconds",
        type=float,
        default=180.0,
        help="Maximum allowed rollback duration in seconds (default: 180)",
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


def _parse_bool_token(value: str, *, key: str) -> bool:
    token = value.strip().lower()
    if token in {"true", "1", "yes"}:
        return True
    if token in {"false", "0", "no"}:
        return False
    raise ValueError(f"{key} must be true/false")


def _validate(values: dict[str, str], *, max_rollback_seconds: float) -> None:
    for key in _REQUIRED_KEYS:
        _require_non_empty(values, key)

    platform = values["platform"].strip().lower()
    if platform not in _ALLOWED_PLATFORMS:
        raise ValueError("platform must be one of: systemd, k8s")

    rollback_start = _parse_iso8601_utc(values["rollback_start_utc"], key="rollback_start_utc")
    rollback_complete = _parse_iso8601_utc(
        values["rollback_complete_utc"], key="rollback_complete_utc"
    )
    if rollback_complete < rollback_start:
        raise ValueError("rollback_complete_utc must be >= rollback_start_utc")

    rollback_seconds = _parse_non_negative_float(values["rollback_seconds"], key="rollback_seconds")
    if max_rollback_seconds < 0:
        raise ValueError("max_rollback_seconds must be >= 0")
    if rollback_seconds > max_rollback_seconds:
        raise ValueError(
            "rollback_seconds threshold exceeded: "
            f"actual={rollback_seconds} max={max_rollback_seconds}"
        )

    health_check_passed = _parse_bool_token(
        values["health_check_passed"], key="health_check_passed"
    )
    if not health_check_passed:
        raise ValueError("health_check_passed must be true")

    result = values["result"].strip().lower()
    if result != "pass":
        raise ValueError("result must be pass")


def main() -> int:
    args = _parse_args()
    evidence_path = Path(args.evidence_file)

    if not evidence_path.exists():
        print(f"error: evidence file not found: {evidence_path}")
        return 2

    try:
        values = _parse_kv_file(evidence_path)
        _validate(values, max_rollback_seconds=float(args.max_rollback_seconds))
    except (OSError, ValueError) as exc:
        print(f"error: deploy rollback evidence verification failed: {exc}")
        return 2

    print(f"verification passed: {evidence_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
