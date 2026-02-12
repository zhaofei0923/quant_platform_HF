from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_evidence(
    path: Path,
    *,
    dry_run: bool,
    success: bool,
    blocked: bool,
    status: str,
    duration_seconds: float = 1.0,
) -> None:
    payload = {
        "trading_day": "2026-02-12",
        "dry_run": dry_run,
        "success": success,
        "blocked": blocked,
        "status": status,
        "duration_seconds": duration_seconds,
        "started_utc": "2026-02-12T03:00:00Z",
        "completed_utc": "2026-02-12T03:00:01Z",
    }
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def test_verify_daily_settlement_evidence_passes_for_completed_status(tmp_path: Path) -> None:
    evidence = tmp_path / "daily_settlement_evidence.json"
    _write_evidence(evidence, dry_run=False, success=True, blocked=False, status="COMPLETED")
    command = [
        sys.executable,
        "scripts/ops/verify_daily_settlement_evidence.py",
        "--evidence-file",
        str(evidence),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_verify_daily_settlement_evidence_blocks_without_allow_flag(tmp_path: Path) -> None:
    evidence = tmp_path / "daily_settlement_evidence.json"
    _write_evidence(evidence, dry_run=False, success=False, blocked=True, status="BLOCKED")
    command = [
        sys.executable,
        "scripts/ops/verify_daily_settlement_evidence.py",
        "--evidence-file",
        str(evidence),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0


def test_verify_daily_settlement_evidence_allows_blocked_with_flag(tmp_path: Path) -> None:
    evidence = tmp_path / "daily_settlement_evidence.json"
    _write_evidence(evidence, dry_run=False, success=False, blocked=True, status="BLOCKED")
    command = [
        sys.executable,
        "scripts/ops/verify_daily_settlement_evidence.py",
        "--evidence-file",
        str(evidence),
        "--allow-blocked",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
