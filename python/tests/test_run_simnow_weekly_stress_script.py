from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_csv(path: Path, rows: int = 10) -> None:
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


def test_run_simnow_weekly_stress_collect_only(tmp_path: Path) -> None:
    csv_path = tmp_path / "ticks.csv"
    cfg_path = tmp_path / "ctp.yaml"
    out_path = tmp_path / "weekly_stress.json"
    _write_csv(csv_path)
    _write_ctp_yaml(cfg_path)

    cmd = [
        sys.executable,
        "scripts/perf/run_simnow_weekly_stress.py",
        "--config",
        str(cfg_path),
        "--csv-path",
        str(csv_path),
        "--max-ticks",
        "10",
        "--samples",
        "3",
        "--dry-run",
        "--collect-only",
        "--result-json",
        str(out_path),
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["collect_only"] is True
    assert payload["samples"] == 3
    assert "delta_abs_mean" in payload
    assert len(payload["samples_detail"]) == 3
