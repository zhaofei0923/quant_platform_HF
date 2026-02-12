#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


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


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, text=True, capture_output=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-click deploy orchestrator for rollout/failover with evidence verification"
    )
    parser.add_argument("--env-config", required=True, help="Deploy environment yaml path")
    parser.add_argument(
        "--mode",
        choices=("auto", "rollout", "failover"),
        default="auto",
        help="Execution workflow mode",
    )
    parser.add_argument(
        "--output-dir",
        default="docs/results",
        help="Directory for evidence and summary output",
    )
    parser.add_argument("--execute", action="store_true", help="Run real commands from env config")
    parser.add_argument(
        "--inject-fault",
        action="store_true",
        help="Inject fault in rollout workflow",
    )
    return parser.parse_args()


def _select_workflow(mode: str, kv: dict[str, str]) -> str:
    if mode != "auto":
        return mode
    has_failover_keys = (
        "backup_sync_check_cmd" in kv
        or "demote_primary_cmd" in kv
        or "promote_standby_cmd" in kv
    )
    return "failover" if has_failover_keys else "rollout"


def _append_result(lines: list[str], key: str, value: str) -> None:
    lines.append(f"{key}={value}")


def main() -> int:
    args = _parse_args()
    env_config = Path(args.env_config)
    if not env_config.exists():
        print(f"error: env config not found: {env_config}")
        return 2

    kv = _load_simple_yaml(env_config)
    workflow = _select_workflow(args.mode, kv)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    orchestrator = (
        Path("scripts/ops/failover_orchestrator.py")
        if workflow == "failover"
        else Path("scripts/ops/rollout_orchestrator.py")
    )
    verifier = (
        Path("scripts/ops/verify_failover_evidence.py")
        if workflow == "failover"
        else Path("scripts/ops/verify_rollout_evidence.py")
    )
    evidence_name = "failover_result.env" if workflow == "failover" else "rollout_result.env"
    evidence_file = output_dir / evidence_name

    run_cmd = [
        sys.executable,
        str(orchestrator),
        "--env-config",
        str(env_config),
        "--output-file",
        str(evidence_file),
    ]
    if args.execute:
        run_cmd.append("--execute")
    if workflow == "rollout" and args.inject_fault:
        run_cmd.append("--inject-fault")

    run_result = _run(run_cmd)

    verify_cmd = [
        sys.executable,
        str(verifier),
        "--evidence-file",
        str(evidence_file),
    ]
    if workflow == "rollout" and args.inject_fault:
        verify_cmd.append("--require-fault-step")

    verify_result = _run(verify_cmd)

    summary_path = output_dir / "one_click_deploy_result.env"
    lines: list[str] = []
    _append_result(lines, "DEPLOY_WORKFLOW", workflow)
    _append_result(lines, "DEPLOY_ENV_CONFIG", str(env_config))
    _append_result(lines, "DEPLOY_EXECUTE", "1" if args.execute else "0")
    _append_result(lines, "DEPLOY_INJECT_FAULT", "1" if args.inject_fault else "0")
    _append_result(lines, "DEPLOY_EVIDENCE_FILE", str(evidence_file))
    _append_result(lines, "DEPLOY_ORCHESTRATOR_EXIT", str(run_result.returncode))
    _append_result(lines, "DEPLOY_VERIFY_EXIT", str(verify_result.returncode))
    _append_result(lines, "DEPLOY_SUCCESS", "true" if run_result.returncode == 0 and verify_result.returncode == 0 else "false")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    if run_result.stdout:
        print(run_result.stdout.strip())
    if run_result.stderr:
        print(run_result.stderr.strip())
    if verify_result.stdout:
        print(verify_result.stdout.strip())
    if verify_result.stderr:
        print(verify_result.stderr.strip())
    print(str(summary_path))

    if run_result.returncode != 0:
        return run_result.returncode
    return verify_result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
