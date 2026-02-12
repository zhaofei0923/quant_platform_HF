from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_check_debezium_connectors_passes_on_running_status(tmp_path: Path) -> None:
    status_file = tmp_path / "status.json"
    status_file.write_text(
        json.dumps(
            {
                "quant_hft_trading_core_cdc": {
                    "connector": {"state": "RUNNING"},
                    "tasks": [{"state": "RUNNING"}],
                }
            }
        ),
        encoding="utf-8",
    )
    report_file = tmp_path / "report.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/infra/check_debezium_connectors.py",
            "--status-json-file",
            str(status_file),
            "--output-json",
            str(report_file),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["healthy"] is True
    assert payload["connectors_report"][0]["healthy"] is True


def test_check_debezium_connectors_fails_on_stopped_task(tmp_path: Path) -> None:
    status_file = tmp_path / "status.json"
    status_file.write_text(
        json.dumps(
            {
                "quant_hft_trading_core_cdc": {
                    "connector": {"state": "RUNNING"},
                    "tasks": [{"state": "FAILED"}],
                }
            }
        ),
        encoding="utf-8",
    )
    report_file = tmp_path / "report.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/infra/check_debezium_connectors.py",
            "--status-json-file",
            str(status_file),
            "--output-json",
            str(report_file),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["healthy"] is False
    assert payload["connectors_report"][0]["healthy"] is False
