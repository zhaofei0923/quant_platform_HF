#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

import pandas as pd


DEFAULT_RUNS_ROOT = Path("docs/results/backtest_runs")
DEFAULT_TRACE_NAME = "my_sub_trace.csv"
REQUIRED_COLUMNS = {
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
        default=DEFAULT_RUNS_ROOT,
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
        help="Optional HTML output path. Defaults to <trace_stem>_<run_id>.html in run directory.",
    )
    args = parser.parse_args(argv)
    if not args.list_runs and not args.latest and not args.run_id:
        parser.error("one of --list-runs, --latest, or --run-id is required")
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
        return pd.to_datetime(value, format="%Y-%m-%d %H:%M", errors="raise")
    except ValueError as exc:
        raise ValueError(f"{name} must match YYYY-MM-DD HH:MM: {value}") from exc


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
    require_columns(frame, REQUIRED_COLUMNS)

    frame["dt_utc"] = pd.to_datetime(frame["dt_utc"], format="%Y-%m-%d %H:%M", errors="raise")
    for column in NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.sort_values("dt_utc").reset_index(drop=True)

    frame, selected_strategy = filter_unique_value(frame, "strategy_id", strategy_id)
    frame, selected_timeframe = filter_unique_value(frame, "timeframe_minutes", timeframe_minutes)

    start_ts = parse_optional_time(start, "start")
    end_ts = parse_optional_time(end, "end")
    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("start must be less than or equal to end")
    if start_ts is not None:
        frame = frame[frame["dt_utc"] >= start_ts].copy()
    if end_ts is not None:
        frame = frame[frame["dt_utc"] <= end_ts].copy()
    if frame.empty:
        raise ValueError("no trace rows remain after applying filters")

    metadata = {
        "instrument_id": str(frame.get("instrument_id", pd.Series([""])).iloc[0]),
        "strategy_id": selected_strategy,
        "timeframe_minutes": int(selected_timeframe),
        "start_dt": frame["dt_utc"].iloc[0],
        "end_dt": frame["dt_utc"].iloc[-1],
    }
    return frame, metadata


def build_figure(frame: pd.DataFrame, run_id: str, metadata: dict[str, Any]):
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except Exception as exc:
        raise RuntimeError(
            "plotly is required to generate HTML charts. Install it with: "
            "python3 -m pip install pandas plotly"
        ) from exc

    customdata = frame[
        ["instrument_id", "strategy_id", "timeframe_minutes", "market_regime", "bar_volume"]
    ].fillna("")
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
        "Time=%{x}<br>"
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
            x=frame["dt_utc"],
            open=frame["bar_open"],
            high=frame["bar_high"],
            low=frame["bar_low"],
            close=frame["bar_close"],
            name="OHLC",
            customdata=customdata,
            hovertemplate=hover_template,
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=frame["dt_utc"],
            y=frame["kama"],
            name="KAMA",
            mode="lines",
            line={"color": "#ff7f0e", "width": 1.8},
            customdata=customdata,
            hovertemplate=(
                "Time=%{x}<br>KAMA=%{y:.6f}<br>"
                "Strategy=%{customdata[1]}<br>Regime=%{customdata[3]}<extra></extra>"
            ),
        ),
        row=1,
        col=1,
    )
    for row_index, field, color in (
        (2, "atr", "#2ca02c"),
        (3, "er", "#1f77b4"),
        (4, "adx", "#d62728"),
    ):
        fig.add_trace(
            go.Scatter(
                x=frame["dt_utc"],
                y=frame[field],
                name=field.upper(),
                mode="lines",
                line={"color": color, "width": 1.4},
                customdata=customdata,
                hovertemplate=(
                    f"Time=%{{x}}<br>{field.upper()}=%{{y:.6f}}<br>"
                    "Strategy=%{customdata[1]}<br>Regime=%{customdata[3]}<extra></extra>"
                ),
            ),
            row=row_index,
            col=1,
        )

    start_label = metadata["start_dt"].strftime("%Y-%m-%d %H:%M")
    end_label = metadata["end_dt"].strftime("%Y-%m-%d %H:%M")
    title = (
        f"Sub Strategy Trace: {metadata['instrument_id']} / {metadata['strategy_id']} / "
        f"{metadata['timeframe_minutes']}m / {run_id}<br>"
        f"<sup>{start_label} to {end_label}</sup>"
    )
    fig.update_layout(
        title=title,
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0.0},
        xaxis4={
            "rangeslider": {"visible": True},
            "rangeselector": {
                "buttons": [
                    {"count": 1, "label": "1D", "step": "day", "stepmode": "backward"},
                    {"count": 3, "label": "3D", "step": "day", "stepmode": "backward"},
                    {"count": 7, "label": "1W", "step": "day", "stepmode": "backward"},
                    {"count": 1, "label": "1M", "step": "month", "stepmode": "backward"},
                    {"label": "All", "step": "all"},
                ]
            },
        },
        margin={"l": 60, "r": 20, "t": 90, "b": 60},
        template="plotly_white",
        height=1100,
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="ATR", row=2, col=1)
    fig.update_yaxes(title_text="ER", range=[0, 1], row=3, col=1)
    fig.update_yaxes(title_text="ADX", range=[0, 100], row=4, col=1)
    fig.update_xaxes(showspikes=True, spikemode="across", spikesnap="cursor")
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
    fig = build_figure(frame, str(run["run_id"]), metadata)
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
