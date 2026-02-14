from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_validate_backtest_parquet_dataset_script(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    partition = dataset_root / "source=rb" / "trading_day=20260101" / "instrument_id=rb2405"
    partition.mkdir(parents=True, exist_ok=True)
    (partition / "part-0000.parquet").write_bytes(b"PAR1")

    report_file = tmp_path / "validate_report.json"
    command = [
        sys.executable,
        "scripts/data/validate_backtest_parquet_dataset.py",
        "--dataset-root",
        str(dataset_root),
        "--report-json",
        str(report_file),
        "--require-non-empty",
    ]

    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["success"] is True
    assert payload["file_count"] == 1
    assert payload["instrument_universe"] == ["rb2405"]
    assert payload["trading_days"] == ["20260101"]


def test_validate_backtest_parquet_dataset_prefix_mismatch_fails(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    partition = dataset_root / "source=rb" / "trading_day=20260101" / "instrument_id=ag2405"
    partition.mkdir(parents=True, exist_ok=True)
    (partition / "part-0000.parquet").write_bytes(b"PAR1")

    conversion_report = tmp_path / "conversion.json"
    conversion_report.write_text(
        json.dumps(
            {
                "rollover_event_count": 0,
                "sources": {
                    "rb.csv": {
                        "contract_switch_count": 0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    report_file = tmp_path / "validate_report_bad.json"
    command = [
        sys.executable,
        "scripts/data/validate_backtest_parquet_dataset.py",
        "--dataset-root",
        str(dataset_root),
        "--report-json",
        str(report_file),
        "--require-non-empty",
        "--conversion-report",
        str(conversion_report),
    ]

    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 2
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["success"] is False
    assert payload["prefix_mismatch_count"] == 1
