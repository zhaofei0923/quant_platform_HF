from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _write_evidence(
    path: Path,
    *,
    rto_seconds: float = 8.5,
    rpo_events: int = 0,
    result: str = "pass",
) -> None:
    path.write_text(
        "\n".join(
            [
                "drill_id=20260211-1200",
                "release_version=v0.2.5",
                "wal_path=runtime/runtime_events.wal",
                "failure_start_utc=2026-02-11T12:00:00Z",
                "recovery_complete_utc=2026-02-11T12:00:08Z",
                f"rto_seconds={rto_seconds}",
                f"rpo_events={rpo_events}",
                "operator=ops-engineer",
                f"result={result}",
                "notes=drill ok",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_verify_wal_recovery_evidence_passes_within_threshold(tmp_path: Path) -> None:
    evidence_file = tmp_path / "wal_recovery_result.env"
    _write_evidence(evidence_file, rto_seconds=8.5, rpo_events=0, result="pass")

    command = [
        sys.executable,
        "scripts/ops/verify_wal_recovery_evidence.py",
        "--evidence-file",
        str(evidence_file),
        "--max-rto-seconds",
        "10",
        "--max-rpo-events",
        "0",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "verification passed" in (completed.stdout + completed.stderr).lower()


def test_verify_wal_recovery_evidence_fails_when_rto_exceeds_threshold(
    tmp_path: Path,
) -> None:
    evidence_file = tmp_path / "wal_recovery_result.env"
    _write_evidence(evidence_file, rto_seconds=12.0, rpo_events=0, result="pass")

    command = [
        sys.executable,
        "scripts/ops/verify_wal_recovery_evidence.py",
        "--evidence-file",
        str(evidence_file),
        "--max-rto-seconds",
        "10",
        "--max-rpo-events",
        "0",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "rto_seconds" in (completed.stdout + completed.stderr).lower()


def test_verify_wal_recovery_evidence_fails_when_required_key_missing(
    tmp_path: Path,
) -> None:
    evidence_file = tmp_path / "wal_recovery_result.env"
    _write_evidence(evidence_file)
    lines = evidence_file.read_text(encoding="utf-8").splitlines()
    lines = [line for line in lines if not line.startswith("operator=")]
    evidence_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    command = [
        sys.executable,
        "scripts/ops/verify_wal_recovery_evidence.py",
        "--evidence-file",
        str(evidence_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "operator" in (completed.stdout + completed.stderr).lower()
