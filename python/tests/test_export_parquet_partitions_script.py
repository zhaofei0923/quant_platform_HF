from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_export_clickhouse_parquet_partitions_script_dry_run(tmp_path: Path) -> None:
    report_file = tmp_path / "parquet_report.json"
    command = [
        sys.executable,
        "scripts/data_pipeline/export_clickhouse_parquet_partitions.py",
        "--clickhouse-dsn",
        "http://clickhouse:8123/",
        "--output-dir",
        str(tmp_path / "parquet"),
        "--start-trading-day",
        "20260211",
        "--end-trading-day",
        "20260212",
        "--limit",
        "10",
        "--report-json",
        str(report_file),
        "--dry-run",
    ]

    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["dry_run"] is True
    assert payload["row_count"] == 0
    assert payload["writer_backend"] == "none"
    assert "trading_day >= '20260211'" in payload["query"]
