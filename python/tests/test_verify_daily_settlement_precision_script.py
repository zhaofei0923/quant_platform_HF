from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=False, capture_output=True, text=True)


def test_verify_daily_settlement_precision_passes_with_10_days(tmp_path: Path) -> None:
    dataset = []
    for index in range(10):
        dataset.append(
            {
                "trading_day": f"2026-02-{index + 1:02d}",
                "local_balance": "1000000.00",
                "broker_balance": "1000000.01",
            }
        )
    dataset_file = tmp_path / "precision_dataset.json"
    dataset_file.write_text(json.dumps(dataset), encoding="utf-8")
    result_file = tmp_path / "precision_result.json"

    completed = _run(
        [
            sys.executable,
            "scripts/ops/verify_daily_settlement_precision.py",
            "--dataset-json",
            str(dataset_file),
            "--result-json",
            str(result_file),
            "--min-days",
            "10",
            "--tolerance",
            "0.01",
        ]
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(result_file.read_text(encoding="utf-8"))
    assert payload["passed"] is True
    assert payload["total_days"] == 10
    assert payload["failed_days"] == []


def test_verify_daily_settlement_precision_fails_when_diff_exceeds_tolerance(
    tmp_path: Path,
) -> None:
    dataset = [
        {
            "trading_day": "2026-02-01",
            "local_balance": "1000000.00",
            "broker_balance": "1000000.02",
        }
    ]
    dataset_file = tmp_path / "precision_dataset_fail.json"
    dataset_file.write_text(json.dumps(dataset), encoding="utf-8")

    completed = _run(
        [
            sys.executable,
            "scripts/ops/verify_daily_settlement_precision.py",
            "--dataset-json",
            str(dataset_file),
            "--min-days",
            "1",
            "--tolerance",
            "0.01",
        ]
    )
    assert completed.returncode == 2


def test_verify_daily_settlement_precision_fails_when_days_below_threshold(tmp_path: Path) -> None:
    dataset = [
        {
            "trading_day": "2026-02-01",
            "local_balance": "1000000.00",
            "broker_balance": "1000000.00",
        }
    ]
    dataset_file = tmp_path / "precision_dataset_days.json"
    dataset_file.write_text(json.dumps(dataset), encoding="utf-8")

    completed = _run(
        [
            sys.executable,
            "scripts/ops/verify_daily_settlement_precision.py",
            "--dataset-json",
            str(dataset_file),
            "--min-days",
            "10",
            "--tolerance",
            "0.01",
        ]
    )
    assert completed.returncode == 2
