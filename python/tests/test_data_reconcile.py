from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path


def _write_timescale_csv(path: Path, rows: list[dict[str, object]]) -> None:
    headers = [
        "account_id",
        "client_order_id",
        "instrument_id",
        "status",
        "total_volume",
        "filled_volume",
        "avg_fill_price",
        "reason",
        "ts_ns",
        "trace_id",
        "execution_algo_id",
        "slice_index",
        "slice_total",
        "throttle_applied",
    ]
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def test_run_reconcile_script_passes_for_consistent_records(tmp_path: Path) -> None:
    redis_file = tmp_path / "redis.json"
    analytics_file = tmp_path / "analytics.csv"
    trading_core_file = tmp_path / "trading_core.csv"
    report_file = tmp_path / "report.json"

    row = {
        "account_id": "sim-account",
        "client_order_id": "ord-1",
        "instrument_id": "rb2405",
        "status": "FILLED",
        "total_volume": 2,
        "filled_volume": 2,
        "avg_fill_price": 4101.5,
        "reason": "ok",
        "ts_ns": 100,
        "trace_id": "trace-1",
        "execution_algo_id": "twap",
        "slice_index": 1,
        "slice_total": 2,
        "throttle_applied": False,
    }
    redis_file.write_text(json.dumps([row]), encoding="utf-8")
    _write_timescale_csv(analytics_file, [row])
    _write_timescale_csv(trading_core_file, [row])

    cmd = [
        sys.executable,
        "scripts/data_pipeline/run_reconcile.py",
        "--redis-json-file",
        str(redis_file),
        "--analytics-csv-file",
        str(analytics_file),
        "--trading-core-csv-file",
        str(trading_core_file),
        "--report-json",
        str(report_file),
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["consistent"] is True
    assert payload["mismatch_count"] == 0
    assert payload["severity"] == "info"
    assert payload["classification"] == "consistent"
    assert payload["auto_fixable"] is False
    assert set(payload["compared_sources"]) == {"redis", "analytics_ts", "trading_core"}
    assert payload["missing_by_source"]["redis"] == []
    assert payload["missing_by_source"]["analytics_ts"] == []
    assert payload["missing_by_source"]["trading_core"] == []


def test_run_reconcile_script_fails_for_mismatch(tmp_path: Path) -> None:
    redis_file = tmp_path / "redis.json"
    analytics_file = tmp_path / "analytics.csv"
    trading_core_file = tmp_path / "trading_core.csv"
    report_file = tmp_path / "report.json"

    redis_row = {
        "account_id": "sim-account",
        "client_order_id": "ord-2",
        "instrument_id": "rb2405",
        "status": "FILLED",
        "total_volume": 2,
        "filled_volume": 2,
        "avg_fill_price": 4101.5,
        "reason": "ok",
        "ts_ns": 101,
        "trace_id": "trace-2",
        "execution_algo_id": "twap",
        "slice_index": 1,
        "slice_total": 2,
        "throttle_applied": False,
    }
    analytics_row = dict(redis_row)
    trading_core_row = dict(redis_row)
    trading_core_row["status"] = "REJECTED"

    redis_file.write_text(json.dumps([redis_row]), encoding="utf-8")
    _write_timescale_csv(analytics_file, [analytics_row])
    _write_timescale_csv(trading_core_file, [trading_core_row])

    cmd = [
        sys.executable,
        "scripts/data_pipeline/run_reconcile.py",
        "--redis-json-file",
        str(redis_file),
        "--analytics-csv-file",
        str(analytics_file),
        "--trading-core-csv-file",
        str(trading_core_file),
        "--report-json",
        str(report_file),
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode != 0

    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["consistent"] is False
    assert payload["mismatch_count"] >= 1
    assert any(item["field"] == "status" for item in payload["field_mismatches"])
    assert payload["severity"] in {"warn", "critical"}
    assert payload["classification"] == "field_mismatch"
    status_mismatches = [item for item in payload["field_mismatches"] if item["field"] == "status"]
    assert status_mismatches
    assert status_mismatches[0]["redis"] == "FILLED"
    assert status_mismatches[0]["timescale"] == "FILLED"
    assert status_mismatches[0]["trading_core"] == "REJECTED"


def test_run_reconcile_script_detects_missing_trading_core_record(tmp_path: Path) -> None:
    redis_file = tmp_path / "redis.json"
    analytics_file = tmp_path / "analytics.csv"
    trading_core_file = tmp_path / "trading_core.csv"
    report_file = tmp_path / "report.json"

    row = {
        "account_id": "sim-account",
        "client_order_id": "ord-3",
        "instrument_id": "rb2405",
        "status": "FILLED",
        "total_volume": 3,
        "filled_volume": 3,
        "avg_fill_price": 4102.5,
        "reason": "ok",
        "ts_ns": 102,
        "trace_id": "trace-3",
        "execution_algo_id": "twap",
        "slice_index": 1,
        "slice_total": 3,
        "throttle_applied": False,
    }
    redis_file.write_text(json.dumps([row]), encoding="utf-8")
    _write_timescale_csv(analytics_file, [row])
    _write_timescale_csv(trading_core_file, [])

    cmd = [
        sys.executable,
        "scripts/data_pipeline/run_reconcile.py",
        "--redis-json-file",
        str(redis_file),
        "--analytics-csv-file",
        str(analytics_file),
        "--trading-core-csv-file",
        str(trading_core_file),
        "--report-json",
        str(report_file),
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode != 0

    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["consistent"] is False
    assert payload["classification"] == "record_presence_mismatch"
    assert payload["severity"] == "critical"
    assert payload["missing_in_trading_core"] == ["ord-3"]
