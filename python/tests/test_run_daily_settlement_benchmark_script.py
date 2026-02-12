from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=False, capture_output=True, text=True)


def test_run_daily_settlement_benchmark_passes_with_relaxed_threshold(tmp_path: Path) -> None:
    result_file = tmp_path / "settlement_benchmark.json"
    completed = _run(
        [
            sys.executable,
            "scripts/perf/run_daily_settlement_benchmark.py",
            "--positions",
            "2000",
            "--runs",
            "5",
            "--target-p99-ms",
            "500.0",
            "--result-json",
            str(result_file),
        ]
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(result_file.read_text(encoding="utf-8"))
    assert payload["passed"] is True
    assert payload["positions"] == 2000
    assert payload["runs"] == 5


def test_run_daily_settlement_benchmark_fails_with_impossible_threshold(tmp_path: Path) -> None:
    result_file = tmp_path / "settlement_benchmark_fail.json"
    completed = _run(
        [
            sys.executable,
            "scripts/perf/run_daily_settlement_benchmark.py",
            "--positions",
            "2000",
            "--runs",
            "3",
            "--target-p99-ms",
            "0.00001",
            "--result-json",
            str(result_file),
        ]
    )
    assert completed.returncode == 2
    payload = json.loads(result_file.read_text(encoding="utf-8"))
    assert payload["passed"] is False
