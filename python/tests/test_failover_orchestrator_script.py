from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_failover_orchestrator_dry_run_outputs_expected_steps(tmp_path: Path) -> None:
    env_config = tmp_path / "prodlike_multi_host.yaml"
    env_config.write_text(
        "environment: prodlike-multi-host\n"
        'precheck_cmd: "echo precheck"\n'
        'backup_sync_check_cmd: "echo backup_sync"\n'
        'demote_primary_cmd: "echo demote_primary"\n'
        'promote_standby_cmd: "echo promote_standby"\n'
        'verify_cmd: "echo verify"\n',
        encoding="utf-8",
    )
    evidence_file = tmp_path / "failover_result.env"

    command = [
        sys.executable,
        "scripts/ops/failover_orchestrator.py",
        "--env-config",
        str(env_config),
        "--output-file",
        str(evidence_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    payload = evidence_file.read_text(encoding="utf-8")
    assert "FAILOVER_DRY_RUN=1" in payload
    assert "FAILOVER_SUCCESS=true" in payload
    assert "STEP_1_NAME=precheck" in payload
    assert "STEP_2_NAME=backup_sync_check" in payload
    assert "STEP_3_NAME=demote_primary" in payload
    assert "STEP_4_NAME=promote_standby" in payload
    assert "STEP_5_NAME=verify" in payload


def test_failover_orchestrator_execute_records_failed_step(tmp_path: Path) -> None:
    env_config = tmp_path / "prodlike_multi_host.yaml"
    env_config.write_text(
        "environment: prodlike-multi-host\n"
        'precheck_cmd: "true"\n'
        'backup_sync_check_cmd: "true"\n'
        'demote_primary_cmd: "true"\n'
        'promote_standby_cmd: "false"\n'
        'verify_cmd: "true"\n',
        encoding="utf-8",
    )
    evidence_file = tmp_path / "failover_result.env"

    command = [
        sys.executable,
        "scripts/ops/failover_orchestrator.py",
        "--env-config",
        str(env_config),
        "--output-file",
        str(evidence_file),
        "--execute",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0

    payload = evidence_file.read_text(encoding="utf-8")
    assert "FAILOVER_DRY_RUN=0" in payload
    assert "FAILOVER_SUCCESS=false" in payload
    assert "FAILOVER_FAILED_STEP=promote_standby" in payload
