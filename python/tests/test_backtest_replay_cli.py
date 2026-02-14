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


def test_replay_csv_stdout_contains_data_overview_fields(tmp_path: Path) -> None:
    csv_path = tmp_path / "ticks.csv"
    _write_sample_csv(csv_path)

    cmd = [
        sys.executable,
        "scripts/backtest/replay_csv.py",
        "--csv",
        str(csv_path),
        "--run-id",
        "cli-overview",
    ]
    completed = subprocess.run(
        cmd,
        check=True,
        cwd=Path.cwd(),
        capture_output=True,
        text=True,
    )
    first_line = completed.stdout.splitlines()[0]
    assert "instruments=1" in first_line
    assert "time_range=" in first_line


def test_replay_csv_can_emit_markdown_report(tmp_path: Path) -> None:
    csv_path = tmp_path / "ticks.csv"
    report_json = tmp_path / "report.json"
    report_md = tmp_path / "report.md"
    _write_sample_csv(csv_path)

    cmd = [
        sys.executable,
        "scripts/backtest/replay_csv.py",
        "--csv",
        str(csv_path),
        "--scenario-template",
        "deterministic_regression",
        "--run-id",
        "cli-report-md",
        "--report-json",
        str(report_json),
        "--report-md",
        str(report_md),
    ]
    subprocess.run(cmd, check=True, cwd=Path.cwd())
    text = report_md.read_text(encoding="utf-8")
    assert "# Backtest Replay Result" in text
    assert "- Run ID: `cli-report-md`" in text
    assert "- Instrument Universe: `rb2305`" in text


def test_replay_csv_can_enable_emit_state_snapshots(tmp_path: Path) -> None:
    csv_path = tmp_path / "ticks.csv"
    report_path = tmp_path / "state_enabled.json"
    _write_sample_csv(csv_path)
    cmd = [
        sys.executable,
        "scripts/backtest/replay_csv.py",
        "--csv",
        str(csv_path),
        "--emit-state-snapshots",
        "--report-json",
        str(report_path),
    ]
    subprocess.run(cmd, check=True, cwd=Path.cwd())
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["spec"]["emit_state_snapshots"] is True


def test_replay_csv_core_sim_requires_data_source(tmp_path: Path) -> None:
    cmd = [
        sys.executable,
        "scripts/backtest/replay_csv.py",
        "--engine-mode",
        "core_sim",
    ]
    completed = subprocess.run(
        cmd,
        check=False,
        cwd=Path.cwd(),
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 2
    assert "requires either --dataset-root or --csv" in completed.stderr


def test_replay_csv_core_sim_with_csv_emits_rollover_fields(tmp_path: Path) -> None:
    csv_path = tmp_path / "ticks.csv"
    report_path = tmp_path / "core_sim_report.json"
    _write_sample_csv(csv_path)
    cmd = [
        sys.executable,
        "scripts/backtest/replay_csv.py",
        "--csv",
        str(csv_path),
        "--engine-mode",
        "core_sim",
        "--rollover-price-mode",
        "mid",
        "--rollover-slippage-bps",
        "12.5",
        "--deterministic-fills",
        "--report-json",
        str(report_path),
    ]
    subprocess.run(cmd, check=True, cwd=Path.cwd())
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["engine_mode"] == "core_sim"
    assert payload["data_source"] == "csv"
    assert payload["spec"]["rollover_price_mode"] == "mid"
    assert payload["spec"]["rollover_slippage_bps"] == 12.5
    assert "rollover_events" in payload["deterministic"]


def test_replay_csv_parquet_requires_dataset_root(tmp_path: Path) -> None:
    csv_path = tmp_path / "ticks.csv"
    _write_sample_csv(csv_path)
    cmd = [
        sys.executable,
        "scripts/backtest/replay_csv.py",
        "--csv",
        str(csv_path),
        "--engine-mode",
        "parquet",
    ]
    completed = subprocess.run(
        cmd,
        check=False,
        cwd=Path.cwd(),
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 2
    assert "--dataset-root is required" in completed.stderr


def test_replay_csv_report_includes_engine_and_rollover_fields(tmp_path: Path) -> None:
    csv_path = tmp_path / "ticks.csv"
    report_path = tmp_path / "report_engine_fields.json"
    _write_sample_csv(csv_path)

    cmd = [
        sys.executable,
        "scripts/backtest/replay_csv.py",
        "--csv",
        str(csv_path),
        "--rollover-mode",
        "carry",
        "--report-json",
        str(report_path),
    ]
    subprocess.run(cmd, check=True, cwd=Path.cwd())
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["engine_mode"] == "csv"
    assert payload["rollover_mode"] == "carry"
    assert payload["data_source"] == "csv"
