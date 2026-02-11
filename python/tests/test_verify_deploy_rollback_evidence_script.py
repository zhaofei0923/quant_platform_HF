from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _write_evidence(
    path: Path,
    *,
    rollback_seconds: float = 75.0,
    health_check_passed: str = "true",
    result: str = "pass",
) -> None:
    path.write_text(
        "\n".join(
            [
                "drill_id=20260211-1300",
                "platform=systemd",
                "release_version=v0.2.7",
                "rollback_target=v0.2.6",
                "failure_injected=core_engine_service_stop",
                "rollback_start_utc=2026-02-11T13:00:00Z",
                "rollback_complete_utc=2026-02-11T13:01:15Z",
                f"rollback_seconds={rollback_seconds}",
                f"health_check_passed={health_check_passed}",
                f"result={result}",
                "operator=ops-engineer",
                "notes=rollback drill ok",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_verify_deploy_rollback_evidence_passes_when_within_threshold(
    tmp_path: Path,
) -> None:
    evidence_file = tmp_path / "deploy_rollback_result.env"
    _write_evidence(evidence_file, rollback_seconds=75.0)

    command = [
        sys.executable,
        "scripts/ops/verify_deploy_rollback_evidence.py",
        "--evidence-file",
        str(evidence_file),
        "--max-rollback-seconds",
        "180",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "verification passed" in (completed.stdout + completed.stderr).lower()


def test_verify_deploy_rollback_evidence_fails_when_threshold_exceeded(
    tmp_path: Path,
) -> None:
    evidence_file = tmp_path / "deploy_rollback_result.env"
    _write_evidence(evidence_file, rollback_seconds=240.0)

    command = [
        sys.executable,
        "scripts/ops/verify_deploy_rollback_evidence.py",
        "--evidence-file",
        str(evidence_file),
        "--max-rollback-seconds",
        "180",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "rollback_seconds" in (completed.stdout + completed.stderr).lower()


def test_verify_deploy_rollback_evidence_fails_when_health_check_not_passed(
    tmp_path: Path,
) -> None:
    evidence_file = tmp_path / "deploy_rollback_result.env"
    _write_evidence(evidence_file, health_check_passed="false", result="fail")

    command = [
        sys.executable,
        "scripts/ops/verify_deploy_rollback_evidence.py",
        "--evidence-file",
        str(evidence_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "health_check_passed" in (completed.stdout + completed.stderr).lower()
