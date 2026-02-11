#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StepResult:
    name: str
    status: str
    duration_ms: int
    command: str


def _trim(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1]
    return value


def _load_simple_yaml(path: Path) -> dict[str, str]:
    kv: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", maxsplit=1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", maxsplit=1)
        kv[_trim(key)] = _trim(value)
    return kv


def _run_step(name: str, command: str, *, dry_run: bool) -> StepResult:
    started = time.monotonic_ns()
    status = "simulated_ok"
    if command and not dry_run:
        completed = subprocess.run(command, shell=True, check=False, capture_output=True, text=True)
        status = "ok" if completed.returncode == 0 else "failed"
    elapsed_ms = int((time.monotonic_ns() - started) / 1_000_000)
    return StepResult(name=name, status=status, duration_ms=max(0, elapsed_ms), command=command)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run rollout/rollback orchestration and emit evidence"
    )
    parser.add_argument("--env-config", required=True)
    parser.add_argument("--output-file", default="docs/results/rollout_result.env")
    parser.add_argument("--inject-fault", action="store_true")
    parser.add_argument("--execute", action="store_true", help="Run real commands from env config")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    env_config_path = Path(args.env_config)
    if not env_config_path.exists():
        print(f"error: env config not found: {env_config_path}")
        return 2

    kv = _load_simple_yaml(env_config_path)
    env_name = kv.get("environment", "unknown")
    dry_run = not args.execute

    steps: list[tuple[str, str]] = [
        ("precheck", kv.get("precheck_cmd", "")),
        ("deploy", kv.get("deploy_cmd", "")),
    ]
    if args.inject_fault:
        steps.append(("fault_injection", kv.get("fault_inject_cmd", "")))
    steps.extend(
        [
            ("rollback", kv.get("rollback_cmd", "")),
            ("verify", kv.get("verify_cmd", "")),
        ]
    )

    results: list[StepResult] = []
    failed_step = ""
    for name, command in steps:
        result = _run_step(name, command, dry_run=dry_run)
        results.append(result)
        if result.status == "failed":
            failed_step = name
            break

    success = failed_step == ""
    output = Path(args.output_file)
    output.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        f"ROLLOUT_ENV={env_name}",
        f"ROLLOUT_DRY_RUN={'1' if dry_run else '0'}",
        f"ROLLOUT_SUCCESS={'true' if success else 'false'}",
        f"ROLLOUT_TOTAL_STEPS={len(results)}",
        f"ROLLOUT_FAILED_STEP={failed_step}",
    ]
    for idx, item in enumerate(results, start=1):
        lines.append(f"STEP_{idx}_NAME={item.name}")
        lines.append(f"STEP_{idx}_STATUS={item.status}")
        lines.append(f"STEP_{idx}_DURATION_MS={item.duration_ms}")
        lines.append(f"STEP_{idx}_COMMAND={item.command}")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(str(output))
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
