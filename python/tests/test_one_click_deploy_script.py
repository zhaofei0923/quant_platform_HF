from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_one_click_deploy_auto_uses_rollout(tmp_path: Path) -> None:
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
    output_dir = tmp_path / "results"

    command = [
        sys.executable,
        "scripts/ops/one_click_deploy.py",
        "--env-config",
        str(env_config),
        "--output-dir",
        str(output_dir),
        "--inject-fault",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    summary = (output_dir / "one_click_deploy_result.env").read_text(encoding="utf-8")
    assert "DEPLOY_WORKFLOW=rollout" in summary
    assert "DEPLOY_SUCCESS=true" in summary

    rollout_payload = (output_dir / "rollout_result.env").read_text(encoding="utf-8")
    assert "ROLLOUT_SUCCESS=true" in rollout_payload
    assert "STEP_3_NAME=fault_injection" in rollout_payload


def test_one_click_deploy_auto_uses_failover(tmp_path: Path) -> None:
    env_config = tmp_path / "prodlike_multi_host.yaml"
    env_config.write_text(
        "environment: prodlike-multi-host\n"
        'precheck_cmd: "echo precheck"\n'
        'backup_sync_check_cmd: "echo backup_sync"\n'
        'demote_primary_cmd: "echo demote_primary"\n'
        'promote_standby_cmd: "echo promote_standby"\n'
        'verify_cmd: "echo verify"\n'
        'data_sync_lag_events: "0"\n',
        encoding="utf-8",
    )
    output_dir = tmp_path / "results"

    command = [
        sys.executable,
        "scripts/ops/one_click_deploy.py",
        "--env-config",
        str(env_config),
        "--output-dir",
        str(output_dir),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    summary = (output_dir / "one_click_deploy_result.env").read_text(encoding="utf-8")
    assert "DEPLOY_WORKFLOW=failover" in summary
    assert "DEPLOY_SUCCESS=true" in summary

    failover_payload = (output_dir / "failover_result.env").read_text(encoding="utf-8")
    assert "FAILOVER_SUCCESS=true" in failover_payload
    assert "STEP_5_NAME=verify" in failover_payload
