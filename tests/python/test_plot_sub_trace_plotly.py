from __future__ import annotations

import contextlib
import csv
import importlib.util
import io
import json
import sys
import tempfile
import types
import unittest
from unittest import mock
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "analysis" / "plot_sub_trace_plotly.py"

TRACE_COLUMNS = [
    "instrument_id",
    "ts_ns",
    "dt_utc",
    "trading_day",
    "action_day",
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

TRADE_COLUMNS = [
    "fill_seq",
    "trade_id",
    "order_id",
    "symbol",
    "exchange",
    "side",
    "offset",
    "volume",
    "price",
    "timestamp_ns",
    "signal_ts_ns",
    "trading_day",
    "action_day",
    "update_time",
    "timestamp_dt_local",
    "signal_dt_local",
    "commission",
    "timestamp_dt_utc",
    "slippage",
    "realized_pnl",
    "strategy_id",
    "signal_type",
    "regime_at_entry",
]


def load_script_module():
    spec = importlib.util.spec_from_file_location("plot_sub_trace_plotly", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load script module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def invoke_main(module, argv: list[str]) -> int:
    try:
        return module.main(argv)
    except SystemExit as exc:
        return int(exc.code)


def write_trace_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=TRACE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_trades_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=TRADE_COLUMNS)
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
    instrument_id: str = "c2405",
    trading_day: str | None = None,
    action_day: str | None = None,
) -> dict[str, str]:
    normalized_day = dt_utc.split(" ", maxsplit=1)[0].replace("-", "")
    return {
        "instrument_id": instrument_id,
        "ts_ns": str(ts_ns),
        "dt_utc": dt_utc,
        "trading_day": trading_day or normalized_day,
        "action_day": action_day or normalized_day,
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


def make_trade_row(
    fill_seq: int,
    trade_id: str,
    signal_dt_local: str,
    *,
    strategy_id: str = "kama_trend_1",
    symbol: str = "c2405",
    side: str = "Buy",
    offset: str = "Open",
    signal_type: str = "kOpen",
    trading_day: str = "20240102",
    action_day: str = "20240102",
    volume: str = "7",
    price: str = "2426.00000000",
    timestamp_dt_local: str | None = None,
    timestamp_dt_utc: str | None = None,
    timestamp_ns: str = "1704204060699000000",
    signal_ts_ns: str = "1704203999889000000",
    update_time: str | None = None,
    realized_pnl: str = "0.00000000",
) -> dict[str, str]:
    timestamp_dt_local = timestamp_dt_local or signal_dt_local
    timestamp_dt_utc = timestamp_dt_utc or timestamp_dt_local
    update_time = update_time or signal_dt_local.split(" ", maxsplit=1)[1]
    return {
        "fill_seq": str(fill_seq),
        "trade_id": trade_id,
        "order_id": f"order-{fill_seq}",
        "symbol": symbol,
        "exchange": "DCE",
        "side": side,
        "offset": offset,
        "volume": volume,
        "price": price,
        "timestamp_ns": timestamp_ns,
        "signal_ts_ns": signal_ts_ns,
        "trading_day": trading_day,
        "action_day": action_day,
        "update_time": update_time,
        "timestamp_dt_local": timestamp_dt_local,
        "signal_dt_local": signal_dt_local,
        "commission": "14.00000000",
        "timestamp_dt_utc": timestamp_dt_utc,
        "slippage": "0.00000000",
        "realized_pnl": realized_pnl,
        "strategy_id": strategy_id,
        "signal_type": signal_type,
        "regime_at_entry": "kWeakTrend",
    }


def write_run(
    root: Path,
    run_dir_name: str,
    run_id: str,
    trace_rows: list[dict[str, str]],
    *,
    trade_rows: list[dict[str, str]] | None = None,
    write_trade_csv_file: bool = False,
    write_trade_json_payload: bool = False,
) -> Path:
    run_dir = root / run_dir_name
    run_dir.mkdir(parents=True, exist_ok=True)
    trace_path = run_dir / "my_sub_trace.csv"
    write_trace_csv(trace_path, trace_rows)
    if trade_rows is not None and write_trade_csv_file:
        write_trades_csv(run_dir / "csv" / "trades.csv", trade_rows)
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
    if trade_rows is not None and write_trade_json_payload:
        payload["trades"] = trade_rows
    (run_dir / "backtest_auto.json").write_text(json.dumps(payload), encoding="utf-8")
    return run_dir


class PlotSubTracePlotlyTest(unittest.TestCase):
    def test_prepare_plot_frame_orders_by_action_day_and_display_time(self) -> None:
        module = load_script_module()
        frame = pd.DataFrame(
            [
                make_trace_row(
                    "2024-01-04 22:55",
                    100,
                    trading_day="20240105",
                    action_day="20240104",
                ),
                make_trace_row(
                    "2024-01-04 23:00",
                    200,
                    trading_day="20240105",
                    action_day="20240104",
                ),
                make_trace_row(
                    "2024-01-04 09:00",
                    300,
                    trading_day="20240105",
                    action_day="20240105",
                ),
                make_trace_row(
                    "2024-01-04 09:05",
                    400,
                    trading_day="20240105",
                    action_day="20240105",
                ),
            ]
        )

        prepared, metadata = module.prepare_plot_frame(
            frame,
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        self.assertEqual(
            prepared["display_dt_text"].tolist(),
            [
                "2024-01-04 22:55",
                "2024-01-04 23:00",
                "2024-01-05 09:00",
                "2024-01-05 09:05",
            ],
        )
        self.assertEqual(prepared["plot_index"].tolist(), [0, 1, 2, 3])
        self.assertEqual(metadata["start_label"], "2024-01-04 22:55")
        self.assertEqual(metadata["end_label"], "2024-01-05 09:05")

    def test_prepare_plot_frame_legacy_trace_falls_back_to_night_session_action_day(self) -> None:
        module = load_script_module()
        frame = (
            pd.DataFrame(
                [
                    make_trace_row("2024-01-03 22:55", 100),
                    make_trace_row("2024-01-03 23:00", 200),
                    make_trace_row("2024-01-03 09:00", 300),
                    make_trace_row("2024-01-03 09:05", 400),
                ]
            )
            .drop(columns=["trading_day", "action_day"])
            .copy()
        )

        prepared, metadata = module.prepare_plot_frame(
            frame,
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        self.assertEqual(
            prepared["display_dt_text"].tolist(),
            [
                "2024-01-02 22:55",
                "2024-01-02 23:00",
                "2024-01-03 09:00",
                "2024-01-03 09:05",
            ],
        )
        self.assertEqual(metadata["start_label"], "2024-01-02 22:55")
        self.assertEqual(metadata["end_label"], "2024-01-03 09:05")

    def test_build_axis_ticks_marks_boundaries_and_thins_labels(self) -> None:
        module = load_script_module()
        frame = pd.DataFrame(
            {
                "display_dt_text": [
                    "2024-01-02 09:00",
                    "2024-01-02 09:05",
                    "2024-01-02 09:30",
                    "2024-01-02 09:35",
                    "2024-01-02 10:00",
                    "2024-01-02 10:05",
                    "2024-01-02 10:30",
                    "2024-01-02 10:35",
                ],
                "display_dt": pd.to_datetime(
                    [
                        "2024-01-02 09:00",
                        "2024-01-02 09:05",
                        "2024-01-02 09:30",
                        "2024-01-02 09:35",
                        "2024-01-02 10:00",
                        "2024-01-02 10:05",
                        "2024-01-02 10:30",
                        "2024-01-02 10:35",
                    ],
                    format="%Y-%m-%d %H:%M",
                ),
                "plot_index": list(range(8)),
            }
        )

        tickvals, ticktext = module.build_axis_ticks(frame, timeframe_minutes=5, max_ticks=4)

        self.assertEqual(tickvals[0], 0)
        self.assertEqual(tickvals[-1], 7)
        self.assertEqual(ticktext[0], "2024-01-02 09:00")
        self.assertEqual(ticktext[-1], "2024-01-02 10:35")
        self.assertLessEqual(len(tickvals), 4)

    def test_load_trade_events_prefers_csv_over_backtest_json(self) -> None:
        module = load_script_module()

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            run_dir = write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [make_trace_row("2024-01-02 13:55", 1704203700000000000)],
                trade_rows=[make_trade_row(1, "csv-trade", "2024-01-02 13:59:59")],
                write_trade_csv_file=True,
                write_trade_json_payload=True,
            )
            payload = json.loads((run_dir / "backtest_auto.json").read_text(encoding="utf-8"))
            payload["trades"] = [make_trade_row(2, "json-trade", "2024-01-02 14:04:59")]
            (run_dir / "backtest_auto.json").write_text(json.dumps(payload), encoding="utf-8")

            trade_frame = module.load_trade_events(
                {"run_dir": run_dir, "metadata_path": run_dir / "backtest_auto.json"},
                strategy_id="kama_trend_1",
                instrument_ids=["c2405"],
            )

        self.assertEqual(trade_frame["trade_id"].tolist(), ["csv-trade"])

    def test_load_trade_events_falls_back_to_backtest_json(self) -> None:
        module = load_script_module()

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            run_dir = write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [make_trace_row("2024-01-02 13:55", 1704203700000000000)],
                trade_rows=[make_trade_row(1, "json-trade", "2024-01-02 13:59:59")],
                write_trade_json_payload=True,
            )

            trade_frame = module.load_trade_events(
                {"run_dir": run_dir, "metadata_path": run_dir / "backtest_auto.json"},
                strategy_id="kama_trend_1",
                instrument_ids=["c2405"],
            )

        self.assertEqual(trade_frame["trade_id"].tolist(), ["json-trade"])

    def test_load_trade_events_includes_trace_instruments_and_rollover_strategy(self) -> None:
        module = load_script_module()

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            run_dir = write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [
                    make_trace_row("2024-01-02 13:55", 1704203700000000000, instrument_id="c2405"),
                    make_trace_row("2024-04-01 09:00", 1711933200000000000, instrument_id="c2407"),
                ],
                trade_rows=[
                    make_trade_row(1, "alpha-open", "2024-01-02 13:59:59", symbol="c2405"),
                    make_trade_row(2, "alpha-open-next", "2024-04-01 09:00:01", symbol="c2407"),
                    make_trade_row(
                        3,
                        "roll-close",
                        "2024-04-01 09:00:00",
                        strategy_id="rollover",
                        symbol="c2405",
                        offset="Close",
                        signal_type="rollover_close",
                        side="Sell",
                    ),
                    make_trade_row(
                        4,
                        "roll-open",
                        "2024-04-01 09:00:00",
                        strategy_id="rollover",
                        symbol="c2407",
                        offset="Open",
                        signal_type="rollover_open",
                        side="Buy",
                    ),
                    make_trade_row(5, "other-strategy", "2024-04-01 09:00:01", strategy_id="beta"),
                    make_trade_row(6, "other-symbol", "2024-04-01 09:00:01", symbol="rb2405"),
                ],
                write_trade_csv_file=True,
            )

            trade_frame = module.load_trade_events(
                {"run_dir": run_dir, "metadata_path": run_dir / "backtest_auto.json"},
                strategy_id="kama_trend_1",
                instrument_ids=["c2405", "c2407"],
            )

        self.assertEqual(
            trade_frame["trade_id"].tolist(),
            ["alpha-open", "alpha-open-next", "roll-close", "roll-open"],
        )

    def test_prepare_trade_markers_maps_update_time_to_bar(self) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row("2024-01-02 13:55", 100),
                    make_trace_row("2024-01-02 14:00", 200),
                ]
            ),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        markers = module.prepare_trade_markers(
            frame,
            pd.DataFrame(
                [
                    make_trade_row(
                        1,
                        "trade-1",
                        "2024-01-02 13:59:59",
                        timestamp_dt_local="2024-01-02 14:00:01",
                        trading_day="20240102",
                        update_time="14:00:01",
                        signal_type="kOpen",
                        offset="Open",
                        side="Buy",
                    )
                ]
            ),
            metadata,
        )

        self.assertEqual(markers["plot_index"].tolist(), [1])
        self.assertEqual(markers["event_bar_text"].tolist(), ["2024-01-02 14:00"])
        self.assertEqual(markers["marker_kind"].tolist(), ["Open"])

    def test_prepare_trade_markers_matches_execution_bar_by_instrument(self) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row("2024-04-01 09:00", 100, instrument_id="c2405"),
                    make_trace_row("2024-04-01 09:00", 200, instrument_id="c2407"),
                ]
            ),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        markers = module.prepare_trade_markers(
            frame,
            pd.DataFrame(
                [
                    make_trade_row(
                        1,
                        "roll-open",
                        "2024-04-01 09:00:00",
                        timestamp_dt_local="2024-04-01 09:00:01",
                        trading_day="20240401",
                        symbol="c2407",
                        strategy_id="rollover",
                        signal_type="rollover_open",
                        offset="Open",
                        side="Buy",
                    )
                ]
            ),
            metadata,
        )

        self.assertEqual(markers["plot_index"].tolist(), [1])
        self.assertEqual(markers["marker_kind"].tolist(), ["RolloverOpen"])

    def test_prepare_trade_markers_maps_rollover_close_to_last_bar_of_previous_contract(self) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row("2024-03-29 14:55", 100, instrument_id="c2405"),
                    make_trace_row(
                        "2024-04-01 21:00",
                        200,
                        instrument_id="c2407",
                        trading_day="20240401",
                        action_day="20240331",
                    ),
                ]
            ),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        markers = module.prepare_trade_markers(
            frame,
            pd.DataFrame(
                [
                    make_trade_row(
                        1,
                        "roll-close",
                        "2024-03-31 21:00:00",
                        trading_day="20240401",
                        action_day="20240331",
                        update_time="21:00:00",
                        symbol="c2405",
                        strategy_id="rollover",
                        signal_type="rollover_close",
                        offset="Close",
                        side="Sell",
                    ),
                    make_trade_row(
                        2,
                        "roll-open",
                        "2024-03-31 21:00:00",
                        trading_day="20240401",
                        action_day="20240331",
                        update_time="21:00:00",
                        symbol="c2407",
                        strategy_id="rollover",
                        signal_type="rollover_open",
                        offset="Open",
                        side="Buy",
                    ),
                ]
            ),
            metadata,
        )

        self.assertEqual(markers["plot_index"].tolist(), [0, 1])
        self.assertEqual(markers["marker_kind"].tolist(), ["RolloverClose", "RolloverOpen"])
        self.assertEqual(markers["event_bar_text"].tolist(), ["2024-03-29 14:55", "2024-03-31 21:00"])

    def test_prepare_trade_markers_classifies_rollover_markers(self) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row("2024-04-01 09:00", 100, instrument_id="c2405"),
                    make_trace_row("2024-04-01 09:05", 200, instrument_id="c2407"),
                ]
            ),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        markers = module.prepare_trade_markers(
            frame,
            pd.DataFrame(
                [
                    make_trade_row(
                        1,
                        "roll-close",
                        "2024-04-01 09:00:00",
                        timestamp_dt_local="2024-04-01 09:00:01",
                        trading_day="20240401",
                        symbol="c2405",
                        strategy_id="rollover",
                        signal_type="rollover_close",
                        offset="Close",
                        side="Sell",
                    ),
                    make_trade_row(
                        2,
                        "roll-open",
                        "2024-04-01 09:05:00",
                        timestamp_dt_local="2024-04-01 09:05:01",
                        trading_day="20240401",
                        symbol="c2407",
                        strategy_id="rollover",
                        signal_type="rollover_open",
                        offset="Open",
                        side="Buy",
                    ),
                ]
            ),
            metadata,
        )

        self.assertEqual(markers["marker_kind"].tolist(), ["RolloverClose", "RolloverOpen"])

    def test_prepare_trade_markers_maps_update_time_at_session_boundary_to_first_new_session_bar(
        self,
    ) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row("2024-01-11 11:25", 100),
                    make_trace_row("2024-01-11 11:30", 200),
                    make_trace_row("2024-01-11 13:30", 300),
                    make_trace_row("2024-01-11 13:35", 400),
                ]
            ),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        markers = module.prepare_trade_markers(
            frame,
            pd.DataFrame(
                [
                    make_trade_row(
                        1,
                        "stop-1",
                        "2024-01-11 11:29:59",
                        timestamp_dt_local="2024-01-11 13:30:00",
                        trading_day="20240111",
                        update_time="13:30:00",
                        signal_type="kStopLoss",
                        offset="Close",
                        side="Buy",
                    )
                ]
            ),
            metadata,
        )

        self.assertEqual(markers["plot_index"].tolist(), [2])
        self.assertEqual(markers["event_bar_text"].tolist(), ["2024-01-11 13:30"])

    def test_prepare_trade_markers_maps_night_session_execution_to_action_day_bar(self) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row("2024-01-31 15:00", 100),
                    make_trace_row(
                        "2024-02-01 21:00",
                        200,
                        trading_day="20240201",
                        action_day="20240131",
                    ),
                    make_trace_row(
                        "2024-02-01 21:05",
                        300,
                        trading_day="20240201",
                        action_day="20240131",
                    ),
                ]
            ),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        markers = module.prepare_trade_markers(
            frame,
            pd.DataFrame(
                [
                    make_trade_row(
                        1,
                        "night-stop",
                        "2024-01-31 15:00:00",
                        timestamp_dt_local="2024-02-01 21:02:05",
                        trading_day="20240201",
                        action_day="20240131",
                        update_time="21:02:05",
                        signal_type="kStopLoss",
                        offset="Close",
                        side="Sell",
                    )
                ]
            ),
            metadata,
        )

        self.assertEqual(markers["plot_index"].tolist(), [1])
        self.assertEqual(markers["event_bar_text"].tolist(), ["2024-01-31 21:00"])

    def test_prepare_trade_markers_legacy_trace_infers_action_day_for_night_session_bar(self) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row("2024-05-13 21:55", 100, instrument_id="c2407"),
                    make_trace_row("2024-05-13 22:00", 200, instrument_id="c2407"),
                ]
            )
            .drop(columns=["trading_day", "action_day"])
            .copy(),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        markers = module.prepare_trade_markers(
            frame,
            pd.DataFrame(
                [
                    make_trade_row(
                        1,
                        "trade-130",
                        "2024-05-12 21:54:59",
                        timestamp_dt_local="2024-05-12 21:56:04",
                        trading_day="20240513",
                        action_day="20240512",
                        update_time="21:56:04",
                        symbol="c2407",
                        signal_type="kOpen",
                        offset="Open",
                        side="Sell",
                    )
                ]
            ),
            metadata,
        )

        self.assertEqual(markers["plot_index"].tolist(), [0])
        self.assertEqual(markers["event_bar_text"].tolist(), ["2024-05-12 21:55"])
        self.assertEqual(markers["marker_kind"].tolist(), ["Open"])

    def test_prepare_trade_markers_prefers_explicit_action_day_bar_when_present(self) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row(
                        "2024-05-12 21:55",
                        100,
                        instrument_id="c2407",
                        trading_day="20240513",
                        action_day="20240512",
                    ),
                    make_trace_row(
                        "2024-05-13 21:55",
                        200,
                        instrument_id="c2407",
                        trading_day="20240514",
                        action_day="20240513",
                    ),
                ]
            ),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        markers = module.prepare_trade_markers(
            frame,
            pd.DataFrame(
                [
                    make_trade_row(
                        1,
                        "action-day-mismatch",
                        "2024-05-12 21:54:59",
                        timestamp_dt_local="2024-05-12 21:56:04",
                        trading_day="20240513",
                        action_day="20240512",
                        update_time="21:56:04",
                        symbol="c2407",
                        signal_type="kOpen",
                        offset="Open",
                        side="Sell",
                    )
                ]
            ),
            metadata,
        )

        self.assertEqual(markers["plot_index"].tolist(), [0])
        self.assertEqual(markers["event_bar_text"].tolist(), ["2024-05-12 21:55"])

    def test_prepare_trade_markers_skips_unmatched_bar_and_warns(self) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row("2024-05-13 21:55", 100, instrument_id="c2407"),
                    make_trace_row("2024-05-13 22:00", 200, instrument_id="c2407"),
                ]
            ),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            markers = module.prepare_trade_markers(
                frame,
                pd.DataFrame(
                    [
                        make_trade_row(
                            1,
                            "missing-bar",
                            "2024-05-12 21:54:59",
                            timestamp_dt_local="2024-05-12 21:56:04",
                            trading_day="20240513",
                            action_day="20240512",
                            update_time="20:56:04",
                            symbol="c2407",
                            signal_type="kOpen",
                            offset="Open",
                            side="Sell",
                        )
                    ]
                ),
                metadata,
            )

        self.assertTrue(markers.empty)
        self.assertIn("trade markers skipped", stderr.getvalue())
        self.assertIn("missing-bar", stderr.getvalue())

    def test_prepare_trade_markers_keeps_buy_and_sell_on_same_execution_bar(self) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row("2024-01-03 09:00", 100),
                    make_trace_row("2024-01-03 09:05", 200),
                ]
            ),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        markers = module.prepare_trade_markers(
            frame,
            pd.DataFrame(
                [
                    make_trade_row(
                        1,
                        "buy-open",
                        "2024-01-03 08:59:58",
                        timestamp_dt_local="2024-01-03 09:04:59",
                        trading_day="20240103",
                        update_time="09:04:59",
                        signal_type="kOpen",
                        offset="Open",
                        side="Buy",
                    ),
                    make_trade_row(
                        2,
                        "sell-open",
                        "2024-01-03 08:59:59",
                        timestamp_dt_local="2024-01-03 09:04:59",
                        trading_day="20240103",
                        update_time="09:04:59",
                        signal_type="kOpen",
                        offset="Open",
                        side="Sell",
                    ),
                ]
            ),
            metadata,
        )

        self.assertEqual(markers["plot_index"].tolist(), [0, 0])
        self.assertEqual(markers["marker_kind"].tolist(), ["Open", "Open"])
        self.assertEqual(markers["side"].tolist(), ["Buy", "Sell"])
        self.assertTrue(markers.iloc[0]["marker_y"] < 2419.0)
        self.assertTrue(markers.iloc[1]["marker_y"] > 2425.0)

    def test_prepare_trade_markers_aggregates_same_bar_and_separates_reverse_close_and_open(self) -> None:
        module = load_script_module()
        frame, metadata = module.prepare_plot_frame(
            pd.DataFrame(
                [
                    make_trace_row("2024-01-03 09:00", 100),
                    make_trace_row("2024-01-03 09:05", 200),
                ]
            ),
            strategy_id=None,
            timeframe_minutes=None,
            start=None,
            end=None,
        )

        markers = module.prepare_trade_markers(
            frame,
            pd.DataFrame(
                [
                    make_trade_row(
                        1,
                        "close-1",
                        "2024-01-03 09:04:59",
                        trading_day="20240103",
                        signal_type="kClose",
                        offset="Close",
                        side="Sell",
                    ),
                    make_trade_row(
                        2,
                        "open-1",
                        "2024-01-03 09:04:59",
                        trading_day="20240103",
                        signal_type="kOpen",
                        offset="Open",
                        side="Sell",
                    ),
                    make_trade_row(
                        3,
                        "open-2",
                        "2024-01-03 09:04:59",
                        trading_day="20240103",
                        signal_type="kOpen",
                        offset="Open",
                        side="Sell",
                    ),
                ]
            ),
            metadata,
        )

        self.assertEqual(markers["marker_kind"].tolist(), ["Close", "Open"])
        self.assertEqual(markers["event_count"].tolist(), [1, 2])
        self.assertIn("open-1", markers.loc[markers["marker_kind"] == "Open", "hover_html"].iloc[0])
        self.assertIn("open-2", markers.loc[markers["marker_kind"] == "Open", "hover_html"].iloc[0])

    def test_build_figure_uses_sequence_axis_and_display_time_hover(self) -> None:
        module = load_script_module()

        class FakeTrace:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs

        class FakeCandlestick(FakeTrace):
            pass

        class FakeScatter(FakeTrace):
            pass

        class FakeFigure:
            def __init__(self, **kwargs) -> None:
                self.subplot_kwargs = kwargs
                self.traces: list[dict[str, object]] = []
                self.layout: dict[str, object] = {}
                self.xaxis_updates: list[dict[str, object]] = []

            def add_trace(self, trace, row: int, col: int) -> None:
                self.traces.append(
                    {
                        "type": trace.__class__.__name__,
                        "kwargs": trace.kwargs,
                        "row": row,
                        "col": col,
                    }
                )

            def update_layout(self, **kwargs) -> None:
                self.layout.update(kwargs)

            def update_yaxes(self, **kwargs) -> None:
                pass

            def update_xaxes(self, **kwargs) -> None:
                self.xaxis_updates.append(kwargs)

        fake_plotly = types.ModuleType("plotly")
        fake_go = types.ModuleType("plotly.graph_objects")
        fake_subplots = types.ModuleType("plotly.subplots")
        fake_go.Candlestick = FakeCandlestick
        fake_go.Scatter = FakeScatter
        fake_subplots.make_subplots = lambda **kwargs: FakeFigure(**kwargs)
        fake_plotly.graph_objects = fake_go
        fake_plotly.subplots = fake_subplots

        frame = pd.DataFrame(
            [
                {
                    "instrument_id": "c2405",
                    "strategy_id": "kama_trend_1",
                    "timeframe_minutes": 5,
                    "market_regime": "kRanging",
                    "bar_volume": 1000.0,
                    "plot_index": 0,
                    "display_dt_text": "2024-01-03 22:55",
                    "dt_utc": pd.Timestamp("2024-01-03 22:55"),
                    "bar_open": 1.0,
                    "bar_high": 2.0,
                    "bar_low": 0.5,
                    "bar_close": 1.5,
                    "kama": 1.4,
                    "atr": 0.2,
                    "er": 0.3,
                    "adx": 20.0,
                },
                {
                    "instrument_id": "c2405",
                    "strategy_id": "kama_trend_1",
                    "timeframe_minutes": 5,
                    "market_regime": "kRanging",
                    "bar_volume": 1005.0,
                    "plot_index": 1,
                    "display_dt_text": "2024-01-03 23:00",
                    "dt_utc": pd.Timestamp("2024-01-03 23:00"),
                    "bar_open": 1.5,
                    "bar_high": 2.1,
                    "bar_low": 1.2,
                    "bar_close": 1.8,
                    "kama": 1.6,
                    "atr": 0.25,
                    "er": 0.31,
                    "adx": 22.0,
                },
            ]
        )
        metadata = {
            "instrument_id": "c2405",
            "strategy_id": "kama_trend_1",
            "timeframe_minutes": 5,
            "start_label": "2024-01-03 22:55",
            "end_label": "2024-01-03 23:00",
            "tickvals": [0, 1],
            "ticktext": ["2024-01-03 22:55", "2024-01-03 23:00"],
        }
        marker_traces = [
            {
                "x": [1],
                "y": [2.4],
                "name": "Open",
                "mode": "markers+text",
                "text": [""],
                "textposition": "middle center",
                "marker": {"symbol": "triangle-up", "color": "#2ca02c", "size": 10},
                "customdata": [["trade detail"]],
                "hovertemplate": "%{customdata[0]}<extra></extra>",
            }
        ]

        with mock.patch.dict(
            sys.modules,
            {
                "plotly": fake_plotly,
                "plotly.graph_objects": fake_go,
                "plotly.subplots": fake_subplots,
            },
        ):
            figure = module.build_figure(
                frame,
                "backtest-20260314T191513",
                metadata,
                trade_marker_traces=marker_traces,
            )

        self.assertEqual(figure.traces[0]["kwargs"]["x"], [0, 1])
        self.assertEqual(figure.traces[1]["kwargs"]["x"], [0, 1])
        self.assertEqual(figure.traces[2]["kwargs"]["x"], [1])
        self.assertEqual(figure.traces[2]["row"], 1)
        self.assertEqual(figure.traces[2]["col"], 1)
        self.assertIn("%{customdata[5]}", figure.traces[0]["kwargs"]["hovertemplate"])
        self.assertEqual(figure.layout["xaxis"]["tickvals"], [0, 1])
        self.assertEqual(
            figure.layout["xaxis"]["ticktext"],
            ["2024-01-03 22:55", "2024-01-03 23:00"],
        )
        self.assertFalse(figure.layout["xaxis"]["rangeslider"]["visible"])
        self.assertEqual(figure.layout["title"]["x"], 0.02)
        self.assertEqual(figure.layout["title"]["xanchor"], "left")
        self.assertEqual(figure.layout["legend"]["orientation"], "h")
        self.assertEqual(figure.layout["legend"]["yanchor"], "top")
        self.assertGreaterEqual(figure.layout["legend"]["y"], 1.03)
        self.assertGreaterEqual(figure.layout["margin"]["t"], 130)
        self.assertNotIn("rangeselector", figure.layout["xaxis"])

    def test_build_figure_uses_instrument_chain_label_for_multi_instrument_trace(self) -> None:
        module = load_script_module()

        class FakeTrace:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs

        class FakeCandlestick(FakeTrace):
            pass

        class FakeScatter(FakeTrace):
            pass

        class FakeFigure:
            def __init__(self, **kwargs) -> None:
                self.layout: dict[str, object] = {}

            def add_trace(self, trace, row: int, col: int) -> None:
                pass

            def update_layout(self, **kwargs) -> None:
                self.layout.update(kwargs)

            def update_yaxes(self, **kwargs) -> None:
                pass

            def update_xaxes(self, **kwargs) -> None:
                pass

        fake_plotly = types.ModuleType("plotly")
        fake_go = types.ModuleType("plotly.graph_objects")
        fake_subplots = types.ModuleType("plotly.subplots")
        fake_go.Candlestick = FakeCandlestick
        fake_go.Scatter = FakeScatter
        fake_subplots.make_subplots = lambda **kwargs: FakeFigure(**kwargs)
        fake_plotly.graph_objects = fake_go
        fake_plotly.subplots = fake_subplots

        frame = pd.DataFrame(
            [
                {
                    "instrument_id": "c2405",
                    "strategy_id": "kama_trend_1",
                    "timeframe_minutes": 5,
                    "market_regime": "kRanging",
                    "bar_volume": 1000.0,
                    "plot_index": 0,
                    "display_dt_text": "2024-03-29 14:55",
                    "dt_utc": pd.Timestamp("2024-03-29 14:55"),
                    "bar_open": 1.0,
                    "bar_high": 2.0,
                    "bar_low": 0.5,
                    "bar_close": 1.5,
                    "kama": 1.4,
                    "atr": 0.2,
                    "er": 0.3,
                    "adx": 20.0,
                },
                {
                    "instrument_id": "c2407",
                    "strategy_id": "kama_trend_1",
                    "timeframe_minutes": 5,
                    "market_regime": "kRanging",
                    "bar_volume": 1005.0,
                    "plot_index": 1,
                    "display_dt_text": "2024-04-01 09:00",
                    "dt_utc": pd.Timestamp("2024-04-01 09:00"),
                    "bar_open": 1.5,
                    "bar_high": 2.1,
                    "bar_low": 1.2,
                    "bar_close": 1.8,
                    "kama": 1.6,
                    "atr": 0.25,
                    "er": 0.31,
                    "adx": 22.0,
                },
            ]
        )
        metadata = {
            "instrument_id": "c2405",
            "instrument_ids": ["c2405", "c2407"],
            "instrument_label": "c2405->c2407",
            "strategy_id": "kama_trend_1",
            "timeframe_minutes": 5,
            "start_label": "2024-03-29 14:55",
            "end_label": "2024-04-01 09:00",
            "tickvals": [0, 1],
            "ticktext": ["2024-03-29 14:55", "2024-04-01 09:00"],
        }

        with mock.patch.dict(
            sys.modules,
            {
                "plotly": fake_plotly,
                "plotly.graph_objects": fake_go,
                "plotly.subplots": fake_subplots,
            },
        ):
            figure = module.build_figure(frame, "backtest-20260314T191513", metadata)

        self.assertIn("c2405->c2407", str(figure.layout["title"]["text"]))

    def test_build_figure_prefers_analysis_bars_and_hover_shows_raw_prices(self) -> None:
        module = load_script_module()

        class FakeTrace:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs

        class FakeCandlestick(FakeTrace):
            pass

        class FakeScatter(FakeTrace):
            pass

        class FakeFigure:
            def __init__(self, **kwargs) -> None:
                self.traces: list[dict[str, object]] = []
                self.layout: dict[str, object] = {}

            def add_trace(self, trace, row: int, col: int) -> None:
                self.traces.append(
                    {
                        "type": trace.__class__.__name__,
                        "kwargs": trace.kwargs,
                        "row": row,
                        "col": col,
                    }
                )

            def update_layout(self, **kwargs) -> None:
                self.layout.update(kwargs)

            def update_yaxes(self, **kwargs) -> None:
                pass

            def update_xaxes(self, **kwargs) -> None:
                pass

        fake_plotly = types.ModuleType("plotly")
        fake_go = types.ModuleType("plotly.graph_objects")
        fake_subplots = types.ModuleType("plotly.subplots")
        fake_go.Candlestick = FakeCandlestick
        fake_go.Scatter = FakeScatter
        fake_subplots.make_subplots = lambda **kwargs: FakeFigure(**kwargs)
        fake_plotly.graph_objects = fake_go
        fake_plotly.subplots = fake_subplots

        frame = pd.DataFrame(
            [
                {
                    "instrument_id": "c2407",
                    "strategy_id": "kama_trend_1",
                    "timeframe_minutes": 5,
                    "market_regime": "kWeakTrend",
                    "bar_volume": 1000.0,
                    "plot_index": 0,
                    "display_dt_text": "2024-04-01 21:00",
                    "dt_utc": pd.Timestamp("2024-04-01 21:00"),
                    "bar_open": 200.0,
                    "bar_high": 205.0,
                    "bar_low": 198.0,
                    "bar_close": 204.0,
                    "analysis_bar_open": 104.0,
                    "analysis_bar_high": 109.0,
                    "analysis_bar_low": 102.0,
                    "analysis_bar_close": 108.0,
                    "analysis_price_offset": -96.0,
                    "kama": 107.0,
                    "atr": 3.0,
                    "er": 0.45,
                    "adx": 25.0,
                }
            ]
        )
        metadata = {
            "instrument_id": "c2407",
            "instrument_ids": ["c2407"],
            "instrument_label": "c2407",
            "strategy_id": "kama_trend_1",
            "timeframe_minutes": 5,
            "start_label": "2024-04-01 21:00",
            "end_label": "2024-04-01 21:00",
            "tickvals": [0],
            "ticktext": ["2024-04-01 21:00"],
        }

        with mock.patch.dict(
            sys.modules,
            {
                "plotly": fake_plotly,
                "plotly.graph_objects": fake_go,
                "plotly.subplots": fake_subplots,
            },
        ):
            figure = module.build_figure(frame, "backtest-20260316T152348", metadata)

        candlestick = figure.traces[0]["kwargs"]
        self.assertEqual(list(candlestick["open"]), [104.0])
        self.assertEqual(list(candlestick["close"]), [108.0])
        self.assertIn("Raw Close=%{customdata[9]}", candlestick["hovertemplate"])
        self.assertIn("Analysis Close=%{close}", candlestick["hovertemplate"])

    def test_resolve_runs_root_defaults_relative_to_repo_root(self) -> None:
        module = load_script_module()

        resolved = module.resolve_runs_root(Path("docs/results/backtest_runs"))

        self.assertEqual(resolved, REPO_ROOT / "docs" / "results" / "backtest_runs")

    def test_defaults_to_latest_run_and_writes_chart_in_run_directory(self) -> None:
        module = load_script_module()

        class FakeFigure:
            def write_html(self, path: Path, include_plotlyjs: bool, full_html: bool) -> None:
                Path(path).write_text("fake html", encoding="utf-8")

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            older_run = write_run(
                root,
                "backtest-20260313T210013_20260313T210013",
                "backtest-20260313T210013",
                [make_trace_row("2024-01-02 09:00", 1704186000000000000)],
            )
            latest_run = write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [make_trace_row("2024-01-02 09:05", 1704186300000000000)],
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                mock.patch.object(module, "build_figure", return_value=FakeFigure()),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                exit_code = invoke_main(module, ["--runs-root", str(root)])

            expected_output = latest_run / "my_sub_trace_backtest-20260313T210119.html"
            self.assertEqual(exit_code, 0, msg=stderr.getvalue())
            self.assertFalse((older_run / expected_output.name).exists())
            self.assertTrue(expected_output.exists())
            self.assertIn(str(expected_output), stdout.getvalue())

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

    def test_default_output_path_is_stable_and_overwrites_previous_html(self) -> None:
        module = load_script_module()

        class FakeFigure:
            def __init__(self, content: str) -> None:
                self.content = content

            def write_html(self, path: Path, include_plotlyjs: bool, full_html: bool) -> None:
                Path(path).write_text(self.content, encoding="utf-8")

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            latest_run = write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [make_trace_row("2024-01-02 09:05", 1704186300000000000)],
            )

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                with mock.patch.object(module, "build_figure", return_value=FakeFigure("first")):
                    first_exit_code = invoke_main(module, ["--runs-root", str(root)])
                with mock.patch.object(module, "build_figure", return_value=FakeFigure("second")):
                    second_exit_code = invoke_main(module, ["--runs-root", str(root)])

            expected_output = latest_run / "my_sub_trace_backtest-20260313T210119.html"
            self.assertEqual(first_exit_code, 0)
            self.assertEqual(second_exit_code, 0)
            self.assertTrue(expected_output.exists())
            self.assertEqual(expected_output.read_text(encoding="utf-8"), "second")

    def test_latest_run_without_trade_data_still_generates_html_output(self) -> None:
        module = load_script_module()

        class FakeFigure:
            def write_html(self, path: Path, include_plotlyjs: bool, full_html: bool) -> None:
                Path(path).write_text("<html>fake plotly chart</html>", encoding="utf-8")

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            output_path = root / "chart.html"
            write_run(
                root,
                "backtest-20260313T210119_20260313T210119",
                "backtest-20260313T210119",
                [make_trace_row("2024-01-02 09:00", 1704186000000000000)],
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                mock.patch.object(module, "build_figure", return_value=FakeFigure()),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
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

        class FakeFigure:
            def write_html(self, path: Path, include_plotlyjs: bool, full_html: bool) -> None:
                Path(path).write_text("<html>fake plotly chart KAMA</html>", encoding="utf-8")

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
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                mock.patch.object(module, "build_figure", return_value=FakeFigure()),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
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
            self.assertIn("fake plotly chart", html)
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
