from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_rollout_orchestrator_dry_run_outputs_verifiable_evidence(tmp_path: Path) -> None:
    env_config = tmp_path / "sim.yaml"
    env_config.write_text(
        "environment: sim\n"
        'precheck_cmd: "echo precheck"\n'
        'deploy_cmd: "echo deploy"\n'
        'fault_inject_cmd: "echo fault"\n'
        'rollback_cmd: "echo rollback"\n'
        'verify_cmd: "echo verify"\n',
        encoding="utf-8",
    )
    evidence_file = tmp_path / "rollout_result.env"

    run_cmd = [
        sys.executable,
        "scripts/ops/rollout_orchestrator.py",
        "--env-config",
        str(env_config),
        "--output-file",
        str(evidence_file),
        "--inject-fault",
    ]
    completed = subprocess.run(run_cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert evidence_file.exists()

    verify_cmd = [
        sys.executable,
        "scripts/ops/verify_rollout_evidence.py",
        "--evidence-file",
        str(evidence_file),
        "--require-fault-step",
    ]
    verified = subprocess.run(verify_cmd, check=False, capture_output=True, text=True)
    assert verified.returncode == 0, verified.stdout + verified.stderr
    assert "verification passed" in (verified.stdout + verified.stderr).lower()


def test_rollout_orchestrator_execute_fails_when_step_command_fails(tmp_path: Path) -> None:
    env_config = tmp_path / "staging.yaml"
    env_config.write_text(
        "environment: staging\n"
        'precheck_cmd: "true"\n'
        'deploy_cmd: "false"\n'
        'rollback_cmd: "true"\n'
        'verify_cmd: "true"\n',
        encoding="utf-8",
    )
    evidence_file = tmp_path / "rollout_result.env"

    run_cmd = [
        sys.executable,
        "scripts/ops/rollout_orchestrator.py",
        "--env-config",
        str(env_config),
        "--output-file",
        str(evidence_file),
        "--execute",
    ]
    completed = subprocess.run(run_cmd, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    payload = evidence_file.read_text(encoding="utf-8")
    assert "ROLLOUT_SUCCESS=false" in payload
    assert "ROLLOUT_FAILED_STEP=deploy" in payload
