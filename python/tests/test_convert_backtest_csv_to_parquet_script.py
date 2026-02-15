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


def test_convert_backtest_csv_to_parquet_prefix_mismatch_warn_mode(tmp_path: Path) -> None:
    csv_file = tmp_path / "rb.csv"
    csv_file.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20260101,ag2405,09:00:00,0,5100.0,1,5099.0,1,5101.0,1,5100.0,0.0,0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    report_file = tmp_path / "warn_report.json"
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
        "--filename-prefix-policy",
        "warn",
    ]

    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["success"] is True
    assert payload["prefix_mismatch_count"] == 1


def test_convert_backtest_csv_to_parquet_prefix_mismatch_error_mode(tmp_path: Path) -> None:
    csv_file = tmp_path / "rb.csv"
    csv_file.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20260101,ag2405,09:00:00,0,5100.0,1,5099.0,1,5101.0,1,5100.0,0.0,0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    report_file = tmp_path / "error_report.json"
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
        "--filename-prefix-policy",
        "error",
    ]

    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 2
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["success"] is False


def test_convert_backtest_csv_to_parquet_accepts_float_like_millisec(tmp_path: Path) -> None:
    csv_file = tmp_path / "c.csv"
    csv_file.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20260101,c2501,09:00:00,0.0,2500.0,1,2499.0,1,2501.0,1,2500.0,0.0,0",
                "20260101,c2501,09:00:01,1.0,2500.5,2,2500.0,1,2501.5,1,2500.2,0.0,0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report_file = tmp_path / "float_ms_report.json"
    command = [
        sys.executable,
        "scripts/data/convert_backtest_csv_to_parquet.py",
        "--input",
        str(csv_file),
        "--output-dir",
        str(tmp_path / "out"),
        "--report-json",
        str(report_file),
        "--execute",
        "--no-run-id",
    ]

    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["success"] is True
    assert payload["output_file_count"] == 1
