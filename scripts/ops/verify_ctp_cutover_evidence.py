#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

_ALLOWED_STEP_STATUS = {"ok", "simulated_ok", "failed"}

_CUTOVER_REQUIRED_KEYS = (
    "CUTOVER_ENV",
    "CUTOVER_DRY_RUN",
    "CUTOVER_SUCCESS",
    "CUTOVER_TOTAL_STEPS",
    "CUTOVER_FAILED_STEP",
    "CUTOVER_TRIGGERED_ROLLBACK",
    "CUTOVER_DURATION_SECONDS",
)

_ROLLBACK_REQUIRED_KEYS = (
    "ROLLBACK_ENV",
    "ROLLBACK_DRY_RUN",
    "ROLLBACK_TRIGGERED",
    "ROLLBACK_SUCCESS",
    "ROLLBACK_TOTAL_STEPS",
    "ROLLBACK_FAILED_STEP",
    "ROLLBACK_DURATION_SECONDS",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify CTP one-shot cutover and rollback evidence contracts"
    )
    parser.add_argument("--cutover-evidence-file", required=True)
    parser.add_argument("--rollback-evidence-file", required=True)
    parser.add_argument(
        "--allow-cutover-rollback",
        action="store_true",
        help="Allow CUTOVER_SUCCESS=false only when rollback is triggered and successful",
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


def _parse_bool_token(value: str, *, key: str) -> bool:
    token = value.strip().lower()
    if token in {"true", "1", "yes"}:
        return True
    if token in {"false", "0", "no"}:
        return False
    raise ValueError(f"{key} must be true/false")


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


def _require_keys(values: dict[str, str], keys: tuple[str, ...], label: str) -> None:
    for key in keys:
        if key not in values:
            raise ValueError(f"{label} missing required key: {key}")
        if key.endswith("_FAILED_STEP"):
            continue
        if not values[key].strip():
            raise ValueError(f"{label} missing required key: {key}")


def _validate_steps(values: dict[str, str], total_steps: int, label: str) -> None:
    if total_steps <= 0:
        raise ValueError(f"{label}_TOTAL_STEPS must be > 0")
    for idx in range(1, total_steps + 1):
        name_key = f"STEP_{idx}_NAME"
        status_key = f"STEP_{idx}_STATUS"
        duration_key = f"STEP_{idx}_DURATION_MS"
        exit_code_key = f"STEP_{idx}_EXIT_CODE"
        for key in (name_key, status_key, duration_key, exit_code_key):
            if key not in values or not values[key].strip():
                raise ValueError(f"missing required key: {key}")
        if values[status_key] not in _ALLOWED_STEP_STATUS:
            raise ValueError(f"{status_key} has invalid value: {values[status_key]}")
        _parse_non_negative_int(values[duration_key], key=duration_key)
        int(values[exit_code_key])


def _validate_cutover_payload(values: dict[str, str]) -> tuple[bool, bool]:
    _require_keys(values, _CUTOVER_REQUIRED_KEYS, "cutover evidence")
    cutover_success = _parse_bool_token(values["CUTOVER_SUCCESS"], key="CUTOVER_SUCCESS")
    rollback_triggered = _parse_bool_token(
        values["CUTOVER_TRIGGERED_ROLLBACK"],
        key="CUTOVER_TRIGGERED_ROLLBACK",
    )
    total_steps = _parse_non_negative_int(values["CUTOVER_TOTAL_STEPS"], key="CUTOVER_TOTAL_STEPS")
    _validate_steps(values, total_steps, "CUTOVER")
    _parse_non_negative_float(values["CUTOVER_DURATION_SECONDS"], key="CUTOVER_DURATION_SECONDS")

    failed_step = values["CUTOVER_FAILED_STEP"].strip()
    if cutover_success and failed_step:
        raise ValueError("CUTOVER_FAILED_STEP must be empty when CUTOVER_SUCCESS is true")
    if not cutover_success and not failed_step:
        raise ValueError("CUTOVER_FAILED_STEP must be set when CUTOVER_SUCCESS is false")
    return cutover_success, rollback_triggered


def _validate_rollback_payload(values: dict[str, str]) -> tuple[bool, bool, float]:
    _require_keys(values, _ROLLBACK_REQUIRED_KEYS, "rollback evidence")
    rollback_triggered = _parse_bool_token(values["ROLLBACK_TRIGGERED"], key="ROLLBACK_TRIGGERED")
    rollback_success = _parse_bool_token(values["ROLLBACK_SUCCESS"], key="ROLLBACK_SUCCESS")
    total_steps = _parse_non_negative_int(
        values["ROLLBACK_TOTAL_STEPS"], key="ROLLBACK_TOTAL_STEPS"
    )
    rollback_duration_seconds = _parse_non_negative_float(
        values["ROLLBACK_DURATION_SECONDS"], key="ROLLBACK_DURATION_SECONDS"
    )

    failed_step = values["ROLLBACK_FAILED_STEP"].strip()
    if rollback_triggered:
        _validate_steps(values, total_steps, "ROLLBACK")
        if rollback_success and failed_step:
            raise ValueError("ROLLBACK_FAILED_STEP must be empty when ROLLBACK_SUCCESS is true")
        if not rollback_success and not failed_step:
            raise ValueError("ROLLBACK_FAILED_STEP must be set when ROLLBACK_SUCCESS is false")
    else:
        if total_steps != 0:
            raise ValueError("ROLLBACK_TOTAL_STEPS must be 0 when ROLLBACK_TRIGGERED is false")
        if failed_step:
            raise ValueError("ROLLBACK_FAILED_STEP must be empty when ROLLBACK_TRIGGERED is false")
    return rollback_triggered, rollback_success, rollback_duration_seconds


def _validate_cross_contract(
    *,
    cutover_success: bool,
    cutover_triggered_rollback: bool,
    rollback_triggered: bool,
    rollback_success: bool,
    rollback_duration_seconds: float,
    allow_cutover_rollback: bool,
    max_rollback_seconds: float,
) -> None:
    if cutover_triggered_rollback != rollback_triggered:
        raise ValueError(
            "CUTOVER_TRIGGERED_ROLLBACK and ROLLBACK_TRIGGERED must have the same boolean value"
        )

    if max_rollback_seconds < 0:
        raise ValueError("max_rollback_seconds must be >= 0")

    if not allow_cutover_rollback:
        if not cutover_success:
            raise ValueError("CUTOVER_SUCCESS must be true")
        if rollback_triggered:
            raise ValueError("ROLLBACK_TRIGGERED must be false for successful cutover mode")
        return

    if cutover_success:
        if rollback_triggered:
            raise ValueError("ROLLBACK_TRIGGERED must be false when CUTOVER_SUCCESS is true")
        return

    if not rollback_triggered:
        raise ValueError("ROLLBACK_TRIGGERED must be true when CUTOVER_SUCCESS is false")
    if not rollback_success:
        raise ValueError("ROLLBACK_SUCCESS must be true when cutover rollback is allowed")
    if rollback_duration_seconds > max_rollback_seconds:
        raise ValueError(
            "rollback_duration_seconds threshold exceeded: "
            f"actual={rollback_duration_seconds} max={max_rollback_seconds}"
        )


def main() -> int:
    args = _parse_args()
    cutover_path = Path(args.cutover_evidence_file)
    rollback_path = Path(args.rollback_evidence_file)
    if not cutover_path.exists():
        print(f"error: evidence file not found: {cutover_path}")
        return 2
    if not rollback_path.exists():
        print(f"error: evidence file not found: {rollback_path}")
        return 2

    try:
        cutover_values = _parse_kv_file(cutover_path)
        rollback_values = _parse_kv_file(rollback_path)
        cutover_success, cutover_triggered_rollback = _validate_cutover_payload(cutover_values)
        rollback_triggered, rollback_success, rollback_duration_seconds = (
            _validate_rollback_payload(rollback_values)
        )
        _validate_cross_contract(
            cutover_success=cutover_success,
            cutover_triggered_rollback=cutover_triggered_rollback,
            rollback_triggered=rollback_triggered,
            rollback_success=rollback_success,
            rollback_duration_seconds=rollback_duration_seconds,
            allow_cutover_rollback=bool(args.allow_cutover_rollback),
            max_rollback_seconds=float(args.max_rollback_seconds),
        )
    except (OSError, ValueError) as exc:
        print(f"error: ctp cutover evidence verification failed: {exc}")
        return 2

    print(f"verification passed: {cutover_path} {rollback_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
