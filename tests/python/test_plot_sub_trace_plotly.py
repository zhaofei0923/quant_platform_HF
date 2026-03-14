from __future__ import annotations

import contextlib
import csv
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "analysis" / "plot_sub_trace_plotly.py"

TRACE_COLUMNS = [
    "instrument_id",
    "ts_ns",
    "dt_utc",
    "timeframe_minutes",
    "strategy_id",
    "strategy_type",
    "bar_open",
    "bar_high",
    "bar_low",
    "bar_close",
    "bar_volume",
    "kama",
    "atr",
    "adx",
    "er",
    "stop_loss_price",
    "take_profit_price",
    "market_regime",
]


def load_script_module():
    spec = importlib.util.spec_from_file_location("plot_sub_trace_plotly", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load script module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_trace_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=TRACE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def make_trace_row(
    dt_utc: str,
    ts_ns: int,
    strategy_id: str = "kama_trend_1",
    timeframe_minutes: int = 5,
    bar_close: float = 2425.0,
    kama: str = "2424.5",
    atr: str = "3.2",
    adx: str = "25.6",
    er: str = "0.45",
) -> dict[str, str]:
    return {
        "instrument_id": "c2405",
        "ts_ns": str(ts_ns),
        "dt_utc": dt_utc,
        "timeframe_minutes": str(timeframe_minutes),
        "strategy_id": strategy_id,
        "strategy_type": "KamaTrendStrategy",
        "bar_open": "2420",
        "bar_high": "2428",
        "bar_low": "2419",
        "bar_close": str(bar_close),
        "bar_volume": "12345",
        "kama": kama,
        "atr": atr,
        "adx": adx,
        "er": er,
        "stop_loss_price": "",
        "take_profit_price": "",
        "market_regime": "kWeakTrend",
    }


def write_run(root: Path, run_dir_name: str, run_id: str, trace_rows: list[dict[str, str]]) -> Path:
    run_dir = root / run_dir_name
    run_dir.mkdir(parents=True, exist_ok=True)
    trace_path = run_dir / "my_sub_trace.csv"
    write_trace_csv(trace_path, trace_rows)
    payload = {
        "run_id": run_id,
        "spec": {
            "run_id": run_id,
        },
        "replay": {
            "bars_emitted": len(trace_rows),
            "instrument_universe": "c2405",
        },
        "sub_strategy_indicator_trace": {
            "enabled": True,
            "path": str(trace_path),
            "rows": len(trace_rows),
        },
    }
    (run_dir / "backtest_auto.json").write_text(json.dumps(payload), encoding="utf-8")
    return run_dir


class PlotSubTracePlotlyTest(unittest.TestCase):
    def test_list_runs_outputs_latest_first(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            write_run(
                root,
                "backtest-20260313T210013_20260313T210013",
                "backtest-20260313T210013",
                [make_trace_row("2024-01-02 09:00", 1704186000000000000)],
            )
            write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [make_trace_row("2024-01-02 09:05", 1704186300000000000)],
            )
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = module.main(["--list-runs", "--runs-root", str(root)])
        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("backtest-20260313T210119", output)
        self.assertIn("backtest-20260313T210013", output)
        self.assertLess(
            output.index("backtest-20260313T210119"),
            output.index("backtest-20260313T210013"),
        )

    def test_latest_run_requires_strategy_filter_for_ambiguous_csv(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [
                    make_trace_row("2024-01-02 09:00", 1704186000000000000, strategy_id="alpha"),
                    make_trace_row("2024-01-02 09:05", 1704186300000000000, strategy_id="beta"),
                ],
            )
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                exit_code = module.main(["--latest", "--runs-root", str(root)])
        self.assertEqual(exit_code, 1)
        self.assertIn("strategy_id", stderr.getvalue())

    def test_latest_run_generates_html_output(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            output_path = root / "chart.html"
            write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [
                    make_trace_row("2024-01-02 09:00", 1704186000000000000, bar_close=2425.0),
                    make_trace_row("2024-01-02 09:05", 1704186300000000000, bar_close=2427.0),
                    make_trace_row("2024-01-02 09:10", 1704186600000000000, bar_close=2426.0),
                ],
            )
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                exit_code = module.main(
                    [
                        "--latest",
                        "--runs-root",
                        str(root),
                        "--output",
                        str(output_path),
                    ]
                )
            self.assertEqual(exit_code, 0, msg=stderr.getvalue())
            self.assertTrue(output_path.exists())
            html = output_path.read_text(encoding="utf-8")
            self.assertIn("Plotly.newPlot", html)
            self.assertIn("KAMA", html)

    def test_latest_run_rejects_invalid_time_window(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [make_trace_row("2024-01-02 09:00", 1704186000000000000)],
            )
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                exit_code = module.main(
                    [
                        "--latest",
                        "--runs-root",
                        str(root),
                        "--start",
                        "2024-01-03 09:00",
                        "--end",
                        "2024-01-02 09:00",
                    ]
                )
        self.assertEqual(exit_code, 1)
        self.assertIn("start must be less than or equal to end", stderr.getvalue())

    def test_latest_run_rejects_missing_required_columns(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            run_dir = write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [make_trace_row("2024-01-02 09:00", 1704186000000000000)],
            )
            broken_csv = run_dir / "my_sub_trace.csv"
            with broken_csv.open("w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=[column for column in TRACE_COLUMNS if column != "adx"],
                )
                writer.writeheader()
                row = make_trace_row("2024-01-02 09:00", 1704186000000000000)
                row.pop("adx")
                writer.writerow(row)

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                exit_code = module.main(["--latest", "--runs-root", str(root)])
        self.assertEqual(exit_code, 1)
        self.assertIn("missing required columns", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
