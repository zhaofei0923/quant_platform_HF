from __future__ import annotations

import json
import stat
import subprocess
import sys
from pathlib import Path


def _write_fake_settlement_bin(path: Path, *, payload: dict[str, object], exit_code: int) -> None:
    path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                f"echo '{json.dumps(payload, ensure_ascii=True)}'",
                f"exit {exit_code}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def test_daily_settlement_orchestrator_dry_run_writes_evidence(tmp_path: Path) -> None:
    evidence_file = tmp_path / "evidence.json"
    command = [
        sys.executable,
        "scripts/ops/daily_settlement_orchestrator.py",
        "--trading-day",
        "20260212",
        "--evidence-json",
        str(evidence_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(evidence_file.read_text(encoding="utf-8"))
    assert payload["dry_run"] is True
    assert payload["status"] == "DRY_RUN"
    assert "settlement_price_json" in payload
    assert "diff_report_path" in payload


def test_daily_settlement_orchestrator_execute_writes_success_payload(tmp_path: Path) -> None:
    settlement_bin = tmp_path / "fake_settlement_ok.sh"
    _write_fake_settlement_bin(
        settlement_bin,
        payload={
            "success": True,
            "noop": False,
            "blocked": False,
            "status": "COMPLETED",
            "message": "ok",
            "diff_report_path": "docs/results/settlement_diff_2026-02-12.json",
        },
        exit_code=0,
    )
    evidence_file = tmp_path / "evidence.json"
    command = [
        sys.executable,
        "scripts/ops/daily_settlement_orchestrator.py",
        "--settlement-bin",
        str(settlement_bin),
        "--trading-day",
        "2026-02-12",
        "--evidence-json",
        str(evidence_file),
        "--execute",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(evidence_file.read_text(encoding="utf-8"))
    assert payload["dry_run"] is False
    assert payload["success"] is True
    assert payload["status"] == "COMPLETED"
    assert payload["diff_report_path"] == "docs/results/settlement_diff_2026-02-12.json"


def test_daily_settlement_orchestrator_execute_propagates_failure(tmp_path: Path) -> None:
    settlement_bin = tmp_path / "fake_settlement_fail.sh"
    _write_fake_settlement_bin(
        settlement_bin,
        payload={
            "success": False,
            "noop": False,
            "blocked": True,
            "status": "BLOCKED",
            "message": "query failed",
        },
        exit_code=2,
    )
    evidence_file = tmp_path / "evidence.json"
    command = [
        sys.executable,
        "scripts/ops/daily_settlement_orchestrator.py",
        "--settlement-bin",
        str(settlement_bin),
        "--trading-day",
        "2026-02-12",
        "--evidence-json",
        str(evidence_file),
        "--execute",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    payload = json.loads(evidence_file.read_text(encoding="utf-8"))
    assert payload["success"] is False
    assert payload["blocked"] is True
    assert payload["blocked_reason"] == "query failed"
