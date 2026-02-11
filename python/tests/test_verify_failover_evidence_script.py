from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _write_evidence(
    path: Path,
    *,
    success: str = "true",
    duration_seconds: str = "45",
    lag_events: str = "0",
) -> None:
    path.write_text(
        "\n".join(
            [
                "FAILOVER_ENV=prodlike-multi-host",
                "FAILOVER_DRY_RUN=0",
                f"FAILOVER_SUCCESS={success}",
                "FAILOVER_TOTAL_STEPS=5",
                "FAILOVER_FAILED_STEP=",
                f"FAILOVER_DURATION_SECONDS={duration_seconds}",
                f"DATA_SYNC_LAG_EVENTS={lag_events}",
                "STEP_1_NAME=precheck",
                "STEP_1_STATUS=ok",
                "STEP_1_DURATION_MS=10",
                "STEP_2_NAME=backup_sync_check",
                "STEP_2_STATUS=ok",
                "STEP_2_DURATION_MS=10",
                "STEP_3_NAME=demote_primary",
                "STEP_3_STATUS=ok",
                "STEP_3_DURATION_MS=10",
                "STEP_4_NAME=promote_standby",
                "STEP_4_STATUS=ok",
                "STEP_4_DURATION_MS=10",
                "STEP_5_NAME=verify",
                "STEP_5_STATUS=ok",
                "STEP_5_DURATION_MS=10",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_verify_failover_evidence_passes_when_within_threshold(tmp_path: Path) -> None:
    evidence_file = tmp_path / "failover_result.env"
    _write_evidence(evidence_file, duration_seconds="120", lag_events="0")

    command = [
        sys.executable,
        "scripts/ops/verify_failover_evidence.py",
        "--evidence-file",
        str(evidence_file),
        "--max-failover-seconds",
        "180",
        "--max-data-lag-events",
        "0",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "verification passed" in (completed.stdout + completed.stderr).lower()


def test_verify_failover_evidence_fails_when_duration_exceeds_threshold(tmp_path: Path) -> None:
    evidence_file = tmp_path / "failover_result.env"
    _write_evidence(evidence_file, duration_seconds="400", lag_events="0")

    command = [
        sys.executable,
        "scripts/ops/verify_failover_evidence.py",
        "--evidence-file",
        str(evidence_file),
        "--max-failover-seconds",
        "180",
        "--max-data-lag-events",
        "0",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "failover_duration_seconds" in (completed.stdout + completed.stderr).lower()


def test_verify_failover_evidence_fails_when_lag_exceeds_threshold(tmp_path: Path) -> None:
    evidence_file = tmp_path / "failover_result.env"
    _write_evidence(evidence_file, duration_seconds="120", lag_events="3")

    command = [
        sys.executable,
        "scripts/ops/verify_failover_evidence.py",
        "--evidence-file",
        str(evidence_file),
        "--max-failover-seconds",
        "180",
        "--max-data-lag-events",
        "0",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "data_sync_lag_events" in (completed.stdout + completed.stderr).lower()
