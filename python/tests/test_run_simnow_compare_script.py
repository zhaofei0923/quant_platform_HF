from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path


def _write_csv(path: Path, rows: int = 5) -> None:
    header = (
        "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,"
        "BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest"
    )
    lines = [header]
    for idx in range(rows):
        lines.append(
            ",".join(
                [
                    "20260102",
                    "SHFE.ag2406",
                    "09:30:00",
                    str(idx),
                    f"{4500.0 + idx}",
                    str(idx + 1),
                    f"{4499.0 + idx}",
                    "10",
                    f"{4501.0 + idx}",
                    "12",
                    f"{4500.0 + idx}",
                    f"{10000.0 + idx}",
                    f"{100.0 + idx}",
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_ctp_yaml(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "ctp:",
                "environment: sim",
                "is_production_mode: false",
                "enable_real_api: false",
                "market_front: tcp://sim-md",
                "trader_front: tcp://sim-td",
                "broker_id: 9999",
                "user_id: 191202",
                "investor_id: 191202",
                "password: secret",
                "strategy_ids: demo",
                "instruments: SHFE.ag2406",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_run_simnow_compare_script_dry_run(tmp_path: Path) -> None:
    csv_path = tmp_path / "ticks.csv"
    cfg_path = tmp_path / "ctp.yaml"
    out_path = tmp_path / "simnow_report.json"
    html_path = tmp_path / "simnow_report.html"
    sqlite_path = tmp_path / "simnow_compare.sqlite"
    _write_csv(csv_path, rows=6)
    _write_ctp_yaml(cfg_path)

    cmd = [
        sys.executable,
        "scripts/simnow/run_simnow_compare.py",
        "--config",
        str(cfg_path),
        "--csv-path",
        str(csv_path),
        "--output-json",
        str(out_path),
        "--output-html",
        str(html_path),
        "--sqlite-path",
        str(sqlite_path),
        "--max-ticks",
        "6",
        "--dry-run",
        "--strict",
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "simnow compare:" in completed.stdout

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["dry_run"] is True
    assert payload["delta"]["intents"] == 0
    assert "signal_parity" in payload["attribution"]
    assert html_path.exists()
    assert sqlite_path.exists()

    conn = sqlite3.connect(sqlite_path)
    try:
        row = conn.execute(
            "SELECT run_id, within_threshold FROM simnow_compare_runs LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[1] == 1
