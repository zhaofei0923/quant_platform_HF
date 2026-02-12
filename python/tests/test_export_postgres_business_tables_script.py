from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_export_postgres_business_tables_dry_run(tmp_path: Path) -> None:
    output_dir = tmp_path / "archive"
    report_file = tmp_path / "report.json"
    command = [
        sys.executable,
        "scripts/data_pipeline/export_postgres_business_tables.py",
        "--output-dir",
        str(output_dir),
        "--tables",
        "orders,trades",
        "--dry-run",
        "--report-json",
        str(report_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["dry_run"] is True
    assert payload["tables"] == ["orders", "trades"]
