#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def _parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        out[key.strip()] = value.strip()
    return out


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify rollout orchestration evidence env file")
    parser.add_argument("--evidence-file", required=True)
    parser.add_argument("--require-fault-step", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    evidence_file = Path(args.evidence_file)
    if not evidence_file.exists():
        print(f"error: evidence file not found: {evidence_file}")
        return 2

    payload = _parse_env_file(evidence_file)
    required_keys = [
        "ROLLOUT_ENV",
        "ROLLOUT_DRY_RUN",
        "ROLLOUT_SUCCESS",
        "ROLLOUT_TOTAL_STEPS",
        "ROLLOUT_FAILED_STEP",
    ]
    for key in required_keys:
        if key not in payload:
            print(f"error: missing key: {key}")
            return 2

    try:
        total_steps = int(payload["ROLLOUT_TOTAL_STEPS"])
    except ValueError:
        print("error: ROLLOUT_TOTAL_STEPS must be integer")
        return 2
    if total_steps <= 0:
        print("error: ROLLOUT_TOTAL_STEPS must be > 0")
        return 2

    found_fault_step = False
    for idx in range(1, total_steps + 1):
        name_key = f"STEP_{idx}_NAME"
        status_key = f"STEP_{idx}_STATUS"
        duration_key = f"STEP_{idx}_DURATION_MS"
        for key in (name_key, status_key, duration_key):
            if key not in payload:
                print(f"error: missing key: {key}")
                return 2
        if payload[name_key] == "fault_injection":
            found_fault_step = True
        if payload[status_key] not in {"ok", "simulated_ok"}:
            print(f"error: step {idx} has invalid status: {payload[status_key]}")
            return 2
        try:
            int(payload[duration_key])
        except ValueError:
            print(f"error: step {idx} has invalid duration: {payload[duration_key]}")
            return 2

    if args.require_fault_step and not found_fault_step:
        print("error: fault_injection step is required but not found")
        return 2

    print(f"verification passed: {evidence_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
