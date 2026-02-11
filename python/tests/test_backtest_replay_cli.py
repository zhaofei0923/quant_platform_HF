from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_sample_csv(csv_path: Path) -> None:
    csv_path.write_text(
        "\n".join(
            [
                "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest",
                "20230103,rb2305,09:00:01,0,4100.0,1,4099.0,10,4101.0,12,4100.0,0.0,100",
                "20230103,rb2305,09:01:02,0,4103.0,5,4102.0,10,4104.0,15,4102.0,0.0,100",
            ]
        ),
        encoding="utf-8",
    )


def test_replay_csv_uses_template_default_max_ticks(tmp_path: Path) -> None:
    csv_path = tmp_path / "ticks.csv"
    report_path = tmp_path / "report.json"
    _write_sample_csv(csv_path)

    cmd = [
        sys.executable,
        "scripts/backtest/replay_csv.py",
        "--csv",
        str(csv_path),
        "--scenario-template",
        "deterministic_regression",
        "--run-id",
        "cli-template-default",
        "--report-json",
        str(report_path),
    ]
    subprocess.run(cmd, check=True, cwd=Path.cwd())
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["spec"]["max_ticks"] == 20000


def test_replay_csv_allows_explicit_max_ticks_override(tmp_path: Path) -> None:
    csv_path = tmp_path / "ticks.csv"
    report_path = tmp_path / "report_override.json"
    _write_sample_csv(csv_path)

    cmd = [
        sys.executable,
        "scripts/backtest/replay_csv.py",
        "--csv",
        str(csv_path),
        "--scenario-template",
        "deterministic_regression",
        "--max-ticks",
        "1234",
        "--run-id",
        "cli-template-override",
        "--report-json",
        str(report_path),
    ]
    subprocess.run(cmd, check=True, cwd=Path.cwd())
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["spec"]["max_ticks"] == 1234
