#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUNS_ROOT = Path("docs/results/backtest_runs")
DEFAULT_TRACE_NAME = "my_sub_trace.csv"
DISPLAY_DT_FORMAT = "%Y-%m-%d %H:%M"
MAX_AXIS_TICKS = 12
REQUIRED_COLUMNS = {
    "ts_ns",
    "dt_utc",
    "instrument_id",
    "strategy_id",
    "timeframe_minutes",
    "bar_open",
    "bar_high",
    "bar_low",
    "bar_close",
    "bar_volume",
    "kama",
    "atr",
    "er",
    "adx",
    "market_regime",
}
NUMERIC_COLUMNS = [
    "timeframe_minutes",
    "bar_open",
    "bar_high",
    "bar_low",
    "bar_close",
    "bar_volume",
    "kama",
    "atr",
    "er",
    "adx",
]
ANALYSIS_COLUMNS = [
    "analysis_bar_open",
    "analysis_bar_high",
    "analysis_bar_low",
    "analysis_bar_close",
    "analysis_price_offset",
]
TRADE_EVENT_COLUMNS = [
    "fill_seq",
    "trade_id",
    "symbol",
    "side",
    "offset",
    "volume",
    "price",
    "timestamp_dt_local",
    "timestamp_dt_utc",
    "trading_day",
    "action_day",
    "update_time",
    "signal_dt_local",
    "strategy_id",
    "signal_type",
    "realized_pnl",
]
MARKER_STYLE_MAP = {
    "Open": {"symbol": "triangle-up", "color": "#2ca02c", "size": 12},
    "Close": {"symbol": "diamond", "color": "#5f6b7a", "size": 11},
    "StopLoss": {"symbol": "x", "color": "#d62728", "size": 12},
    "TakeProfit": {"symbol": "star", "color": "#d4a017", "size": 13},
    "RolloverOpen": {"symbol": "triangle-right", "color": "#1f77b4", "size": 12},
    "RolloverClose": {"symbol": "triangle-left", "color": "#9467bd", "size": 12},
}
MARKER_TRACE_ORDER = ["Open", "Close", "StopLoss", "TakeProfit", "RolloverOpen", "RolloverClose"]
MARKER_OFFSET_SCALE = 0.6


def empty_trade_events_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=TRADE_EVENT_COLUMNS)


def trades_csv_path(run: dict[str, Any]) -> Path:
    return Path(run["run_dir"]) / "csv" / "trades.csv"


def has_trade_event_source(run: dict[str, Any]) -> bool:
    trade_csv = trades_csv_path(run)
    if trade_csv.exists():
        return True
    payload = load_json(Path(run["metadata_path"]))
    trades = payload.get("trades")
    return isinstance(trades, list) and bool(trades)


def normalize_trade_events(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return empty_trade_events_frame()

    normalized = frame.copy()
    for column in TRADE_EVENT_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = ""

    numeric_columns = ("fill_seq", "volume", "price", "realized_pnl")
    for column in numeric_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    return normalized[TRADE_EVENT_COLUMNS].copy()


def load_trade_events(
    run: dict[str, Any], strategy_id: str, instrument_ids: Sequence[str]
) -> pd.DataFrame:
    trade_csv = trades_csv_path(run)
    if trade_csv.exists():
        frame = pd.read_csv(trade_csv)
    else:
        payload = load_json(Path(run["metadata_path"]))
        trades = payload.get("trades")
        if not isinstance(trades, list) or not trades:
            return empty_trade_events_frame()
        frame = pd.DataFrame(trades)

    if frame.empty:
        return empty_trade_events_frame()

    normalized = normalize_trade_events(frame)
    filtered = normalized.copy()
    filtered["strategy_id"] = filtered["strategy_id"].astype(str)
    filtered["symbol"] = filtered["symbol"].astype(str)
    allowed_symbols = {str(symbol) for symbol in instrument_ids if str(symbol).strip()}
    allowed_strategies = {str(strategy_id), "rollover"}
    filtered = filtered[
        filtered["strategy_id"].isin(allowed_strategies) & filtered["symbol"].isin(allowed_symbols)
    ].copy()
    if filtered.empty:
        return empty_trade_events_frame()
    return filtered.reset_index(drop=True)


def normalize_clock_time(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if " " in text:
        text = text.split(" ", maxsplit=1)[1]
    if len(text) == 5:
        return f"{text}:00"
    return text if len(text) == 8 else None


def normalize_day_text(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    digits = "".join(character for character in text if character.isdigit())
    return digits if len(digits) == 8 else None


def format_day_text(day_text: str) -> str:
    return f"{day_text[:4]}-{day_text[4:6]}-{day_text[6:8]}"


def infer_legacy_action_day(raw_dt: pd.Timestamp) -> str:
    display_dt = raw_dt - pd.Timedelta(days=1) if raw_dt.hour >= 20 else raw_dt
    return display_dt.strftime("%Y%m%d")


def build_trace_display_dt_text(dt_value: Any, action_day: Any) -> str:
    raw_dt = pd.to_datetime(str(dt_value), format=DISPLAY_DT_FORMAT, errors="raise")
    normalized_action_day = normalize_day_text(action_day)
    if normalized_action_day is None:
        normalized_action_day = infer_legacy_action_day(raw_dt)
    return f"{format_day_text(normalized_action_day)} {raw_dt.strftime('%H:%M')}"


def build_trade_event_display_dt(day_value: Any, update_time: Any) -> pd.Timestamp | pd.NaT:
    normalized_day = normalize_day_text(day_value)
    normalized_time = normalize_clock_time(update_time)
    if normalized_day is None or normalized_time is None:
        return pd.NaT
    return pd.to_datetime(
        f"{format_day_text(normalized_day)} {normalized_time}",
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )


def classify_marker_kind(signal_type: Any, offset: Any) -> str:
    signal_text = str(signal_type or "").casefold()
    offset_text = str(offset or "").casefold()
    if signal_text == "rollover_close":
        return "RolloverClose"
    if signal_text == "rollover_open":
        return "RolloverOpen"
    if "stop" in signal_text:
        return "StopLoss"
    if "takeprofit" in signal_text or "take_profit" in signal_text or "profit" in signal_text:
        return "TakeProfit"
    if offset_text == "open" or signal_text.endswith("open"):
        return "Open"
    return "Close"


def build_marker_hover_html(group: pd.DataFrame, marker_kind: str) -> str:
    first = group.iloc[0]
    details = []
    for _, row in group.iterrows():
        price = "" if pd.isna(row["price"]) else f"{float(row['price']):g}"
        volume = "" if pd.isna(row["volume"]) else f"{float(row['volume']):g}"
        pnl = "" if pd.isna(row["realized_pnl"]) else f"{float(row['realized_pnl']):g}"
        details.append(
            " | ".join(
                [
                    str(row["trade_id"]),
                    str(row["signal_type"]),
                    f"vol={volume}",
                    f"price={price}",
                    f"pnl={pnl}",
                    f"signal={row['signal_dt_local']}",
                    f"fill={row['timestamp_dt_local']}",
                ]
            )
        )

    total_volume = group["volume"].fillna(0).sum()
    return (
        f"Time={first['event_bar_text']}<br>"
        f"Instrument={first['symbol']}<br>"
        f"Type={marker_kind}<br>"
        f"Side={first['side']}<br>"
        f"Offset={first['offset']}<br>"
        f"Count={len(group)}<br>"
        f"TotalVolume={total_volume:g}<br>"
        + "<br>".join(details)
    )


def build_instrument_label(instrument_ids: Sequence[str]) -> str:
    ordered = [str(instrument_id) for instrument_id in instrument_ids if str(instrument_id).strip()]
    if not ordered:
        return ""
    if len(ordered) == 1:
        return ordered[0]
    if len(ordered) <= 4:
        return "->".join(ordered)
    return f"{ordered[0]}->{ordered[-1]} ({len(ordered)} instruments)"


def emit_trade_marker_skip_warning(reason: str, rows: pd.DataFrame) -> None:
    if rows.empty:
        return
    examples = ", ".join(rows["trade_id"].astype(str).head(5).tolist())
    print(
        f"trade markers skipped ({reason}): {len(rows)} rows; examples: {examples}",
        file=sys.stderr,
    )


def resolve_runs_root(runs_root: Path) -> Path:
    if runs_root.is_absolute():
        return runs_root
    return REPO_ROOT / runs_root


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate interactive Plotly HTML charts from sub-strategy trace CSV files."
    )
    selection = parser.add_mutually_exclusive_group(required=False)
    selection.add_argument(
        "--list-runs",
        action="store_true",
        help="List available backtest runs and their trace files.",
    )
    selection.add_argument(
        "--latest",
        action="store_true",
        help="Use the most recent backtest run under the runs root.",
    )
    selection.add_argument(
        "--run-id",
        help="Select a run by spec.run_id / run_id from backtest_auto.json.",
    )
    parser.add_argument(
        "--runs-root",
        type=Path,
        help=f"Backtest runs root directory. Defaults to {DEFAULT_RUNS_ROOT}.",
    )
    parser.add_argument(
        "--trace-name",
        default=DEFAULT_TRACE_NAME,
        help=f"Trace file name inside each run directory. Defaults to {DEFAULT_TRACE_NAME}.",
    )
    parser.add_argument("--strategy-id", help="Filter a specific strategy_id from the trace CSV.")
    parser.add_argument(
        "--timeframe-minutes",
        type=int,
        help="Filter a specific timeframe_minutes from the trace CSV.",
    )
    parser.add_argument(
        "--start",
        help='Optional inclusive start time, format "YYYY-MM-DD HH:MM".',
    )
    parser.add_argument(
        "--end",
        help='Optional inclusive end time, format "YYYY-MM-DD HH:MM".',
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Optional HTML output path. Defaults to <trace_stem>_<run_id>.html in the selected "
            "run directory."
        ),
    )
    args = parser.parse_args(argv)
    if args.runs_root is None:
        args.runs_root = resolve_runs_root(DEFAULT_RUNS_ROOT)
    if not args.list_runs and not args.latest and not args.run_id:
        args.latest = True
    return args


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def discover_runs(root_dir: Path, trace_name: str) -> list[dict[str, Any]]:
    if not root_dir.exists():
        raise FileNotFoundError(f"backtest runs root does not exist: {root_dir}")

    runs: list[dict[str, Any]] = []
    for run_dir in sorted(path for path in root_dir.glob("backtest-*") if path.is_dir()):
        metadata_path = run_dir / "backtest_auto.json"
        if not metadata_path.exists():
            continue
        payload = load_json(metadata_path)
        spec = payload.get("spec", {}) or {}
        replay = payload.get("replay", {}) or {}
        trace_cfg = payload.get("sub_strategy_indicator_trace", {}) or {}
        configured_trace = Path(str(trace_cfg.get("path", ""))) if trace_cfg.get("path") else None
        trace_path = run_dir / Path(trace_name).name
        if configured_trace is not None and configured_trace.name == Path(trace_name).name:
            trace_path = run_dir / configured_trace.name

        run_id = str(spec.get("run_id") or payload.get("run_id") or run_dir.name)
        instrument = replay.get("instrument_universe", "")
        if isinstance(instrument, list):
            instrument = ",".join(str(item) for item in instrument)
        runs.append(
            {
                "run_id": run_id,
                "run_dir": run_dir,
                "metadata_path": metadata_path,
                "trace_path": trace_path,
                "trace_exists": trace_path.exists(),
                "bars_emitted": int(replay.get("bars_emitted", trace_cfg.get("rows", 0)) or 0),
                "instrument_universe": str(instrument or ""),
            }
        )

    runs.sort(key=lambda item: item["run_dir"].name, reverse=True)
    return runs


def format_runs_table(runs: list[dict[str, Any]]) -> str:
    if not runs:
        return "No runs found."

    lines = [
        "run_id | run_dir | trace_exists | bars | instrument_universe | trace_path",
        "-" * 120,
    ]
    for run in runs:
        lines.append(
            f"{run['run_id']} | {run['run_dir'].name} | "
            f"{'yes' if run['trace_exists'] else 'no'} | {run['bars_emitted']} | "
            f"{run['instrument_universe']} | {run['trace_path']}"
        )
    return "\n".join(lines)


def select_run(runs: list[dict[str, Any]], run_id: str | None, latest: bool) -> dict[str, Any]:
    if latest:
        if not runs:
            raise FileNotFoundError("no backtest runs available")
        return runs[0]

    matches = [run for run in runs if run["run_id"] == run_id]
    if not matches:
        raise FileNotFoundError(f"run_id not found: {run_id}")
    if len(matches) > 1:
        run_dirs = ", ".join(str(item["run_dir"]) for item in matches)
        raise ValueError(f"run_id {run_id} is ambiguous across run directories: {run_dirs}")
    return matches[0]


def require_columns(frame: pd.DataFrame, required: set[str]) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"trace CSV is missing required columns: {', '.join(missing)}")


def parse_optional_time(value: str | None, name: str) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        return pd.to_datetime(value, format=DISPLAY_DT_FORMAT, errors="raise")
    except ValueError as exc:
        raise ValueError(f"{name} must match YYYY-MM-DD HH:MM: {value}") from exc


def thin_tick_positions(positions: Sequence[int], max_ticks: int) -> list[int]:
    ordered = sorted(dict.fromkeys(int(position) for position in positions))
    if len(ordered) <= max_ticks:
        return ordered
    if max_ticks <= 1:
        return ordered[:1]
    if max_ticks == 2:
        return [ordered[0], ordered[-1]]

    middle = ordered[1:-1]
    slots = max_ticks - 2
    if not middle or slots <= 0:
        return [ordered[0], ordered[-1]]

    selected = [ordered[0]]
    if slots == 1:
        selected.append(middle[len(middle) // 2])
    else:
        for slot in range(slots):
            middle_index = round(slot * (len(middle) - 1) / (slots - 1))
            selected.append(middle[middle_index])
    selected.append(ordered[-1])
    return sorted(dict.fromkeys(selected))


def build_axis_ticks(
    frame: pd.DataFrame, timeframe_minutes: int, max_ticks: int = MAX_AXIS_TICKS
) -> tuple[list[int], list[str]]:
    if frame.empty:
        return [], []

    expected_gap = pd.Timedelta(minutes=timeframe_minutes)
    candidate_positions: list[int] = [0, len(frame) - 1]
    display_dt = frame["display_dt"].reset_index(drop=True)
    for index in range(1, len(frame)):
        gap = display_dt.iloc[index] - display_dt.iloc[index - 1]
        if gap <= pd.Timedelta(0) or gap != expected_gap:
            candidate_positions.extend([index - 1, index])

    chosen_positions = thin_tick_positions(candidate_positions, max_ticks)
    tickvals = [int(frame.iloc[position]["plot_index"]) for position in chosen_positions]
    ticktext = [str(frame.iloc[position]["display_dt_text"]) for position in chosen_positions]
    return tickvals, ticktext


def filter_unique_value(
    frame: pd.DataFrame, column: str, requested_value: str | int | None
) -> tuple[pd.DataFrame, str | int]:
    unique_values = list(frame[column].dropna().unique())
    if requested_value is None:
        if len(unique_values) > 1:
            available = ", ".join(str(item) for item in unique_values)
            raise ValueError(
                f"trace CSV contains multiple {column} values; specify --{column.replace('_', '-')}: "
                f"{available}"
            )
        if not unique_values:
            raise ValueError(f"trace CSV has no non-null values for {column}")
        requested_value = unique_values[0]

    filtered = frame[frame[column] == requested_value].copy()
    if filtered.empty:
        raise ValueError(f"no rows found for {column}={requested_value}")
    return filtered, requested_value


def prepare_plot_frame(
    frame: pd.DataFrame,
    strategy_id: str | None,
    timeframe_minutes: int | None,
    start: str | None,
    end: str | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = frame.copy()
    require_columns(working, REQUIRED_COLUMNS)

    action_day_series = (
        working["action_day"] if "action_day" in working.columns else pd.Series([""] * len(working))
    )
    working["display_dt_text"] = [
        build_trace_display_dt_text(dt_value, action_day)
        for dt_value, action_day in zip(working["dt_utc"], action_day_series)
    ]
    working["display_dt"] = pd.to_datetime(
        working["display_dt_text"], format=DISPLAY_DT_FORMAT, errors="raise"
    )
    working["ts_ns"] = pd.to_numeric(working["ts_ns"], errors="raise")
    for column in NUMERIC_COLUMNS:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    analysis_available = all(column in working.columns for column in ANALYSIS_COLUMNS)
    if analysis_available:
        for column in ANALYSIS_COLUMNS:
            working[column] = pd.to_numeric(working[column], errors="coerce")

    working["plot_bar_open"] = working["bar_open"]
    working["plot_bar_high"] = working["bar_high"]
    working["plot_bar_low"] = working["bar_low"]
    working["plot_bar_close"] = working["bar_close"]
    if analysis_available:
        working["plot_bar_open"] = working["analysis_bar_open"].where(
            working["analysis_bar_open"].notna(), working["bar_open"]
        )
        working["plot_bar_high"] = working["analysis_bar_high"].where(
            working["analysis_bar_high"].notna(), working["bar_high"]
        )
        working["plot_bar_low"] = working["analysis_bar_low"].where(
            working["analysis_bar_low"].notna(), working["bar_low"]
        )
        working["plot_bar_close"] = working["analysis_bar_close"].where(
            working["analysis_bar_close"].notna(), working["bar_close"]
        )

    working, selected_strategy = filter_unique_value(working, "strategy_id", strategy_id)
    working, selected_timeframe = filter_unique_value(
        working, "timeframe_minutes", timeframe_minutes
    )
    selected_timeframe = int(selected_timeframe)

    start_ts = parse_optional_time(start, "start")
    end_ts = parse_optional_time(end, "end")
    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("start must be less than or equal to end")
    if start_ts is not None:
        working = working[working["display_dt"] >= start_ts].copy()
    if end_ts is not None:
        working = working[working["display_dt"] <= end_ts].copy()
    if working.empty:
        raise ValueError("no trace rows remain after applying filters")

    working = working.sort_values(["display_dt", "ts_ns", "instrument_id"], kind="stable").reset_index(
        drop=True
    )
    working["plot_index"] = list(range(len(working)))
    tickvals, ticktext = build_axis_ticks(working, timeframe_minutes=selected_timeframe)
    instrument_ids = list(dict.fromkeys(working["instrument_id"].astype(str).tolist()))

    metadata = {
        "instrument_id": instrument_ids[0] if instrument_ids else "",
        "instrument_ids": instrument_ids,
        "instrument_label": build_instrument_label(instrument_ids),
        "strategy_id": selected_strategy,
        "timeframe_minutes": selected_timeframe,
        "uses_analysis_bars": analysis_available,
        "start_label": str(working["display_dt_text"].iloc[0]),
        "end_label": str(working["display_dt_text"].iloc[-1]),
        "tickvals": tickvals,
        "ticktext": ticktext,
    }
    return working, metadata


def load_trace_frame(
    trace_path: Path,
    strategy_id: str | None,
    timeframe_minutes: int | None,
    start: str | None,
    end: str | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not trace_path.exists():
        raise FileNotFoundError(f"trace file does not exist: {trace_path}")

    frame = pd.read_csv(trace_path)
    return prepare_plot_frame(
        frame,
        strategy_id=strategy_id,
        timeframe_minutes=timeframe_minutes,
        start=start,
        end=end,
    )


def prepare_trade_markers(
    frame: pd.DataFrame, trade_events: pd.DataFrame, metadata: dict[str, Any]
) -> pd.DataFrame:
    if trade_events.empty:
        return pd.DataFrame(
            columns=[
                "plot_index",
                "event_bar_text",
                "marker_kind",
                "side",
                "marker_y",
                "event_count",
                "count_text",
                "hover_html",
            ]
        )

    working = trade_events.copy()
    for column in ("fill_seq", "volume", "price", "realized_pnl"):
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce")
    fallback_time = working["update_time"].map(normalize_clock_time)
    working["event_time_text"] = fallback_time
    working["action_event_display_dt"] = [
        build_trade_event_display_dt(action_day, update_time)
        for action_day, update_time in zip(working["action_day"], working["update_time"])
    ]
    working["trading_event_display_dt"] = [
        build_trade_event_display_dt(trading_day, update_time)
        for trading_day, update_time in zip(working["trading_day"], working["update_time"])
    ]
    working["event_display_dt"] = working["action_event_display_dt"].where(
        working["action_event_display_dt"].notna(), working["trading_event_display_dt"]
    )
    invalid_time_rows = working[working["event_display_dt"].isna()].copy()
    emit_trade_marker_skip_warning("invalid trading_day/update_time", invalid_time_rows)
    working = working[working["event_display_dt"].notna()].copy()
    if working.empty:
        return pd.DataFrame()

    timeframe_minutes = int(metadata["timeframe_minutes"])
    bar_lookup = frame[
        [
            "display_dt",
            "display_dt_text",
            "instrument_id",
            "plot_index",
            "plot_bar_low",
            "plot_bar_high",
            "plot_bar_close",
            "atr",
        ]
    ].copy()
    bar_lookup["bar_key"] = (
        bar_lookup["instrument_id"].astype(str) + "|" + bar_lookup["display_dt_text"].astype(str)
    )
    available_bar_keys = set(bar_lookup["bar_key"].tolist())

    working["event_bar_dt"] = working["event_display_dt"].dt.floor(f"{timeframe_minutes}min")
    working["event_bar_text"] = working["event_bar_dt"].dt.strftime(DISPLAY_DT_FORMAT)
    working["bar_key"] = working["symbol"].astype(str) + "|" + working["event_bar_text"].astype(str)
    unmatched_mask = ~working["bar_key"].isin(available_bar_keys)
    if unmatched_mask.any():
        for row_index in working.index[unmatched_mask]:
            row = working.loc[row_index]
            alternate_event_display_dt = row["trading_event_display_dt"]
            if pd.notna(alternate_event_display_dt):
                alternate_event_bar_dt = alternate_event_display_dt.floor(f"{timeframe_minutes}min")
                alternate_event_bar_text = alternate_event_bar_dt.strftime(DISPLAY_DT_FORMAT)
                alternate_bar_key = f"{str(row['symbol'])}|{alternate_event_bar_text}"
                if alternate_bar_key in available_bar_keys:
                    working.at[row_index, "event_display_dt"] = alternate_event_display_dt
                    working.at[row_index, "event_bar_dt"] = alternate_event_bar_dt
                    working.at[row_index, "event_bar_text"] = alternate_event_bar_text
                    working.at[row_index, "bar_key"] = alternate_bar_key
                    continue
            signal_type = str(row.get("signal_type", ""))
            if signal_type != "rollover_close":
                continue
            symbol = str(row["symbol"])
            event_bar_dt = row["event_bar_dt"]
            if pd.isna(event_bar_dt):
                continue
            candidates = bar_lookup[
                (bar_lookup["instrument_id"].astype(str) == symbol)
                & (bar_lookup["display_dt"] <= event_bar_dt)
            ]
            if candidates.empty:
                continue
            fallback = candidates.sort_values(["display_dt", "plot_index"], kind="stable").iloc[-1]
            working.at[row_index, "event_bar_text"] = str(fallback["display_dt_text"])
            working.at[row_index, "bar_key"] = (
                f"{symbol}|{str(fallback['display_dt_text'])}"
            )

    unmatched_rows = working[~working["bar_key"].isin(available_bar_keys)].copy()
    emit_trade_marker_skip_warning("no matching trace bar", unmatched_rows)
    working = working[working["bar_key"].isin(available_bar_keys)].copy()
    if working.empty:
        return pd.DataFrame()

    working["marker_kind"] = [
        classify_marker_kind(signal_type, offset)
        for signal_type, offset in zip(working["signal_type"], working["offset"])
    ]
    working = working.merge(
        bar_lookup,
        left_on=["symbol", "event_bar_text"],
        right_on=["instrument_id", "display_dt_text"],
        how="inner",
    )
    if working.empty:
        return pd.DataFrame()

    bar_low = pd.to_numeric(working["plot_bar_low"], errors="coerce")
    bar_high = pd.to_numeric(working["plot_bar_high"], errors="coerce")
    atr = pd.to_numeric(working["atr"], errors="coerce").abs()
    close = pd.to_numeric(working["plot_bar_close"], errors="coerce").abs()
    bar_range = (bar_high - bar_low).abs()
    y_offset = pd.concat([bar_range, atr, close * 0.001], axis=1).max(axis=1).fillna(1.0)
    y_offset = y_offset.where(y_offset > 0, 1.0) * MARKER_OFFSET_SCALE
    working["marker_y"] = bar_high + y_offset
    buy_mask = working["side"].astype(str).str.casefold() == "buy"
    working.loc[buy_mask, "marker_y"] = bar_low[buy_mask] - y_offset[buy_mask]

    grouped_rows: list[dict[str, Any]] = []
    group_columns = ["plot_index", "event_bar_text", "marker_kind", "side"]
    sort_columns = ["plot_index", "marker_kind", "side", "fill_seq", "trade_id"]
    for group_key, group in working.sort_values(sort_columns, kind="stable").groupby(
        group_columns, sort=True
    ):
        plot_index, event_bar_text, marker_kind, side = group_key
        first = group.iloc[0]
        grouped_rows.append(
            {
                "plot_index": int(plot_index),
                "event_bar_text": str(event_bar_text),
                "marker_kind": str(marker_kind),
                "side": str(side),
                "marker_y": float(first["marker_y"]),
                "event_count": int(len(group)),
                "count_text": str(len(group)) if len(group) > 1 else "",
                "hover_html": build_marker_hover_html(group, str(marker_kind)),
            }
        )

    if not grouped_rows:
        return pd.DataFrame()

    marker_frame = pd.DataFrame(grouped_rows)
    return marker_frame.sort_values(["plot_index", "marker_kind", "side"], kind="stable").reset_index(
        drop=True
    )


def build_trade_marker_traces(marker_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if marker_frame.empty:
        return []

    traces: list[dict[str, Any]] = []
    for marker_kind in MARKER_TRACE_ORDER:
        marker_rows = marker_frame[marker_frame["marker_kind"] == marker_kind]
        if marker_rows.empty:
            continue
        style = MARKER_STYLE_MAP[marker_kind]
        traces.append(
            {
                "x": marker_rows["plot_index"].tolist(),
                "y": marker_rows["marker_y"].tolist(),
                "name": marker_kind,
                "mode": "markers+text",
                "text": marker_rows["count_text"].tolist(),
                "textposition": "middle center",
                "textfont": {"color": "#ffffff", "size": 10},
                "marker": {
                    "symbol": style["symbol"],
                    "color": style["color"],
                    "size": style["size"],
                    "line": {"color": "#ffffff", "width": 1},
                },
                "customdata": marker_rows[["hover_html"]].to_numpy(),
                "hovertemplate": "%{customdata[0]}<extra></extra>",
            }
        )
    return traces


def build_figure(
    frame: pd.DataFrame,
    run_id: str,
    metadata: dict[str, Any],
    trade_marker_traces: Sequence[dict[str, Any]] | None = None,
):
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except Exception as exc:
        raise RuntimeError(
            "plotly is required to generate HTML charts. Install it with: "
            "python3 -m pip install pandas plotly"
        ) from exc

    x_values = frame["plot_index"].tolist()
    analysis_columns_present = all(column in frame.columns for column in ANALYSIS_COLUMNS)
    uses_analysis_bars = bool(metadata.get("uses_analysis_bars")) or analysis_columns_present
    plot_open = (
        frame["plot_bar_open"]
        if "plot_bar_open" in frame.columns
        else frame["analysis_bar_open"].where(frame["analysis_bar_open"].notna(), frame["bar_open"])
        if analysis_columns_present
        else frame["bar_open"]
    )
    plot_high = (
        frame["plot_bar_high"]
        if "plot_bar_high" in frame.columns
        else frame["analysis_bar_high"].where(frame["analysis_bar_high"].notna(), frame["bar_high"])
        if analysis_columns_present
        else frame["bar_high"]
    )
    plot_low = (
        frame["plot_bar_low"]
        if "plot_bar_low" in frame.columns
        else frame["analysis_bar_low"].where(frame["analysis_bar_low"].notna(), frame["bar_low"])
        if analysis_columns_present
        else frame["bar_low"]
    )
    plot_close = (
        frame["plot_bar_close"]
        if "plot_bar_close" in frame.columns
        else frame["analysis_bar_close"].where(
            frame["analysis_bar_close"].notna(), frame["bar_close"]
        )
        if analysis_columns_present
        else frame["bar_close"]
    )
    analysis_offset = (
        frame["analysis_price_offset"]
        if "analysis_price_offset" in frame.columns
        else pd.Series([0.0] * len(frame))
    )
    customdata = frame[
        [
            "instrument_id",
            "strategy_id",
            "timeframe_minutes",
            "market_regime",
            "bar_volume",
            "display_dt_text",
            "bar_open",
            "bar_high",
            "bar_low",
            "bar_close",
        ]
    ].copy()
    customdata["analysis_price_offset"] = analysis_offset
    customdata = customdata.fillna("")
    customdata = customdata.to_numpy()

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.52, 0.16, 0.16, 0.16],
        subplot_titles=(
            "Price / KAMA",
            "ATR",
            "ER",
            "ADX",
        ),
    )

    hover_template = (
        "Time=%{customdata[5]}<br>"
        "Analysis Open=%{open}<br>"
        "Analysis High=%{high}<br>"
        "Analysis Low=%{low}<br>"
        "Analysis Close=%{close}<br>"
        "Raw Open=%{customdata[6]}<br>"
        "Raw High=%{customdata[7]}<br>"
        "Raw Low=%{customdata[8]}<br>"
        "Raw Close=%{customdata[9]}<br>"
        "Offset=%{customdata[10]}<br>"
        "Volume=%{customdata[4]}<br>"
        "Instrument=%{customdata[0]}<br>"
        "Strategy=%{customdata[1]}<br>"
        "Timeframe=%{customdata[2]}m<br>"
        "Regime=%{customdata[3]}<extra></extra>"
    )
    if not uses_analysis_bars:
        hover_template = (
            "Time=%{customdata[5]}<br>"
            "Open=%{open}<br>"
            "High=%{high}<br>"
            "Low=%{low}<br>"
            "Close=%{close}<br>"
            "Volume=%{customdata[4]}<br>"
            "Instrument=%{customdata[0]}<br>"
            "Strategy=%{customdata[1]}<br>"
            "Timeframe=%{customdata[2]}m<br>"
            "Regime=%{customdata[3]}<extra></extra>"
        )
    fig.add_trace(
        go.Candlestick(
            x=x_values,
            open=plot_open,
            high=plot_high,
            low=plot_low,
            close=plot_close,
            name="OHLC",
            customdata=customdata,
            hovertemplate=hover_template,
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=x_values,
            y=frame["kama"],
            name="KAMA",
            mode="lines",
            line={"color": "#ff7f0e", "width": 1.8},
            customdata=customdata,
            hovertemplate=(
                "Time=%{customdata[5]}<br>KAMA=%{y:.6f}<br>"
                "Strategy=%{customdata[1]}<br>Regime=%{customdata[3]}<extra></extra>"
            ),
        ),
        row=1,
        col=1,
    )
    for marker_trace in trade_marker_traces or []:
        fig.add_trace(go.Scatter(**marker_trace), row=1, col=1)
    for row_index, field, color in (
        (2, "atr", "#2ca02c"),
        (3, "er", "#1f77b4"),
        (4, "adx", "#d62728"),
    ):
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=frame[field],
                name=field.upper(),
                mode="lines",
                line={"color": color, "width": 1.4},
                customdata=customdata,
                hovertemplate=(
                    f"Time=%{{customdata[5]}}<br>{field.upper()}=%{{y:.6f}}<br>"
                    "Strategy=%{customdata[1]}<br>Regime=%{customdata[3]}<extra></extra>"
                ),
            ),
            row=row_index,
            col=1,
        )

    start_label = metadata["start_label"]
    end_label = metadata["end_label"]
    title_text = (
        f"Sub Strategy Trace: {metadata.get('instrument_label', metadata['instrument_id'])} / "
        f"{metadata['strategy_id']} / "
        f"{metadata['timeframe_minutes']}m / {run_id}<br>"
        f"<sup>{start_label} to {end_label}</sup>"
    )
    fig.update_layout(
        title={
            "text": title_text,
            "x": 0.02,
            "xanchor": "left",
            "y": 0.985,
            "yanchor": "top",
            "pad": {"b": 18},
        },
        hovermode="x unified",
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": 1.04,
            "x": 0.0,
            "xanchor": "left",
            "bgcolor": "rgba(255,255,255,0.85)",
        },
        xaxis={
            "type": "linear",
            "tickmode": "array",
            "tickvals": metadata["tickvals"],
            "ticktext": metadata["ticktext"],
            "rangeslider": {"visible": False},
        },
        margin={"l": 60, "r": 20, "t": 150, "b": 60},
        template="plotly_white",
        height=1100,
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="ATR", row=2, col=1)
    fig.update_yaxes(title_text="ER", range=[0, 1], row=3, col=1)
    fig.update_yaxes(title_text="ADX", range=[0, 100], row=4, col=1)
    fig.update_xaxes(
        type="linear",
        tickmode="array",
        tickvals=metadata["tickvals"],
        ticktext=metadata["ticktext"],
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
    )
    return fig


def default_output_path(run: dict[str, Any]) -> Path:
    safe_run_id = str(run["run_id"]).replace("/", "_")
    return run["run_dir"] / f"{run['trace_path'].stem}_{safe_run_id}.html"


def generate_chart(
    run: dict[str, Any],
    output_path: Path | None,
    strategy_id: str | None,
    timeframe_minutes: int | None,
    start: str | None,
    end: str | None,
) -> Path:
    frame, metadata = load_trace_frame(
        run["trace_path"],
        strategy_id=strategy_id,
        timeframe_minutes=timeframe_minutes,
        start=start,
        end=end,
    )
    trade_events = load_trade_events(
        run,
        strategy_id=str(metadata["strategy_id"]),
        instrument_ids=metadata.get("instrument_ids", [str(metadata["instrument_id"])]),
    )
    if trade_events.empty and not has_trade_event_source(run):
        print("trade markers unavailable: no trades data found for selected run")

    marker_frame = prepare_trade_markers(frame, trade_events, metadata)
    trade_marker_traces = build_trade_marker_traces(marker_frame)
    fig = build_figure(
        frame,
        str(run["run_id"]),
        metadata,
        trade_marker_traces=trade_marker_traces,
    )
    destination = output_path if output_path is not None else default_output_path(run)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(destination, include_plotlyjs=True, full_html=True)
    return destination


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        runs = discover_runs(args.runs_root, args.trace_name)
        if args.list_runs:
            print(format_runs_table(runs))
            return 0

        run = select_run(runs, run_id=args.run_id, latest=args.latest)
        if not run["trace_exists"]:
            raise FileNotFoundError(f"trace file not found for selected run: {run['trace_path']}")

        output_path = generate_chart(
            run,
            output_path=args.output,
            strategy_id=args.strategy_id,
            timeframe_minutes=args.timeframe_minutes,
            start=args.start,
            end=args.end,
        )
        print(f"generated chart: {output_path}")
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
