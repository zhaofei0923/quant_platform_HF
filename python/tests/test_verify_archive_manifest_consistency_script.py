from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_verify_archive_manifest_consistency_passes(tmp_path: Path) -> None:
    data_file = tmp_path / "orders.parquet"
    data_file.write_bytes(b"test-data")
    manifest = {
        "manifest": [
            {
                "table_name": "trading_core.orders",
                "file_path": str(data_file),
                "file_size": data_file.stat().st_size,
            }
        ]
    }
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(manifest), encoding="utf-8")
    report_file = tmp_path / "report.json"

    command = [
        sys.executable,
        "scripts/ops/verify_archive_manifest_consistency.py",
        "--manifest-json",
        str(manifest_file),
        "--report-json",
        str(report_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["success"] is True
    assert payload["failed_entries"] == 0


def test_verify_archive_manifest_consistency_fails_when_missing_file(tmp_path: Path) -> None:
    manifest = {
        "manifest": [
            {
                "table_name": "trading_core.orders",
                "file_path": str(tmp_path / "missing.parquet"),
                "file_size": 1,
            }
        ]
    }
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(manifest), encoding="utf-8")

    command = [
        sys.executable,
        "scripts/ops/verify_archive_manifest_consistency.py",
        "--manifest-json",
        str(manifest_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
