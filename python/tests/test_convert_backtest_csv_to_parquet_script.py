from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_sample_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20260101,rb2405,09:00:00,500,3500.5,10,3500.0,2,3501.0,3,3500.2,35000.0,1200000",
                "20260101,rb2405,09:00:01,0,3501.0,11,3500.5,1,3501.5,2,3500.4,38511.0,1200100",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_convert_backtest_csv_to_parquet_script_dry_run(tmp_path: Path) -> None:
    csv_file = tmp_path / "rb.csv"
    _write_sample_csv(csv_file)
    report_file = tmp_path / "convert_report.json"

    command = [
        sys.executable,
        "scripts/data/convert_backtest_csv_to_parquet.py",
        "--input",
        str(csv_file),
        "--output-dir",
        str(tmp_path / "out"),
        "--report-json",
        str(report_file),
        "--dry-run",
    ]

    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["success"] is True
    assert payload["mode"] == "dry-run"
    assert payload["total_rows"] == 2
    assert payload["output_file_count"] == 0
    assert payload["instrument_universe"] == ["rb2405"]
