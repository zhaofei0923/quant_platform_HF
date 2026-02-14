#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    from quant_hft.backtest.replay import (
        BacktestRunSpec,
        build_backtest_spec_from_template,
        load_backtest_run_spec,
        run_backtest_spec,
    )
    from quant_hft.runtime.engine import StrategyRuntime
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.backtest.replay import (  # type: ignore[no-redef]
        BacktestRunSpec,
        build_backtest_spec_from_template,
        load_backtest_run_spec,
        run_backtest_spec,
    )
    from quant_hft.runtime.engine import StrategyRuntime  # type: ignore[no-redef]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay CSV ticks into minute bars.")
    parser.add_argument("--csv", default="", help="path to csv file")
    parser.add_argument(
        "--dataset-root",
        default="",
        help="dataset root (reserved for parquet/core_sim modes)",
    )
    parser.add_argument(
        "--engine-mode",
        choices=("csv", "parquet", "core_sim"),
        default="csv",
        help="replay engine mode",
    )
    parser.add_argument(
        "--rollover-mode",
        choices=("strict", "carry"),
        default="strict",
        help="rollover handling mode",
    )
    parser.add_argument(
        "--rollover-price-mode",
        choices=("bbo", "mid", "last"),
        default="bbo",
        help="rollover pricing source for core_sim (bbo|mid|last)",
    )
    parser.add_argument(
        "--rollover-slippage-bps",
        type=float,
        default=0.0,
        help="extra rollover slippage in basis points for core_sim",
    )
    parser.add_argument("--start-date", default="", help="inclusive start trading day (YYYYMMDD)")
    parser.add_argument("--end-date", default="", help="inclusive end trading day (YYYYMMDD)")
    parser.add_argument(
        "--max-ticks",
        type=int,
        default=None,
        help="max ticks to replay (defaults: template preset, or 50000 without template)",
    )
    parser.add_argument("--spec-file", default="", help="path to JSON backtest spec")
    parser.add_argument(
        "--scenario-template",
        default="",
        help="scenario template name (smoke_fast | baseline_replay | deterministic_regression)",
    )
    parser.add_argument("--report-json", default="", help="optional output path for JSON report")
    parser.add_argument("--report-md", default="", help="optional output path for markdown report")
    parser.add_argument("--run-id", default="", help="override run_id in spec/cli")
    parser.add_argument(
        "--strategy-template",
        choices=("trend", "arbitrage", "market_making"),
        default="trend",
        help="strategy template tag for experiment tracking metadata",
    )
    parser.add_argument(
        "--emit-state-snapshots",
        action="store_true",
        help="emit synthetic StateSnapshot7D from replay bars before on_bar",
    )
    parser.add_argument(
        "--deterministic-fills",
        action="store_true",
        help="emit deterministic order events and pnl report",
    )
    return parser


def _render_markdown_report(result: Any) -> str:
    # Keep script self-contained; report object shape is produced by run_backtest_spec.
    replay = result.replay
    lines = [
        "# Backtest Replay Result",
        "",
        "## Metadata",
        f"- Run ID: `{result.run_id}`",
        f"- Mode: `{result.mode}`",
        f"- Input Signature: `{result.input_signature}`",
        f"- Data Signature: `{result.data_signature}`",
        "",
        "## Replay Overview",
        f"- Ticks Read: `{replay.ticks_read}`",
        f"- Bars Emitted: `{replay.bars_emitted}`",
        f"- Intents Emitted: `{replay.intents_emitted}`",
        f"- Instrument Count: `{replay.instrument_count}`",
        f"- Instrument Universe: `{','.join(replay.instrument_universe)}`",
        f"- Time Range (ns): `{replay.first_ts_ns}:{replay.last_ts_ns}`",
        "",
    ]

    deterministic = result.deterministic
    if deterministic is not None:
        lines.extend(
            [
                "## Deterministic Summary",
                f"- Order Events: `{deterministic.order_events_emitted}`",
                f"- WAL Records: `{deterministic.wal_records}`",
                f"- Total PnL: `{deterministic.performance.total_pnl:.4f}`",
                f"- Max Drawdown: `{deterministic.performance.max_drawdown:.4f}`",
                "",
            ]
        )

    return "\n".join(lines)


def main() -> int:
    args = build_parser().parse_args()

    if args.engine_mode == "parquet" and not args.dataset_root:
        print("error: --dataset-root is required when --engine-mode=parquet", file=sys.stderr)
        return 2

    if args.engine_mode == "core_sim" and not args.dataset_root and not args.csv:
        print(
            "error: --engine-mode=core_sim requires either --dataset-root or --csv",
            file=sys.stderr,
        )
        return 2

    effective_csv_path = args.csv
    if args.dataset_root and args.engine_mode == "csv":
        if not args.csv:
            print(
                "error: --dataset-root currently requires --csv when --engine-mode=csv",
                file=sys.stderr,
            )
            return 2
        effective_csv_path = args.csv

    runtime = StrategyRuntime()

    def with_cli_overrides(base: BacktestRunSpec) -> BacktestRunSpec:
        return BacktestRunSpec(
            csv_path=base.csv_path,
            dataset_root=args.dataset_root or base.dataset_root,
            engine_mode=args.engine_mode or base.engine_mode,
            rollover_mode=args.rollover_mode or base.rollover_mode,
            rollover_price_mode=args.rollover_price_mode or base.rollover_price_mode,
            rollover_slippage_bps=args.rollover_slippage_bps,
            start_date=args.start_date or base.start_date,
            end_date=args.end_date or base.end_date,
            max_ticks=base.max_ticks,
            deterministic_fills=base.deterministic_fills,
            wal_path=base.wal_path,
            account_id=base.account_id,
            run_id=base.run_id,
            emit_state_snapshots=base.emit_state_snapshots,
        )

    if args.spec_file:
        spec = load_backtest_run_spec(args.spec_file)
        spec = with_cli_overrides(spec)
        if args.run_id:
            spec = BacktestRunSpec(
                csv_path=spec.csv_path,
                dataset_root=spec.dataset_root,
                engine_mode=spec.engine_mode,
                rollover_mode=spec.rollover_mode,
                rollover_price_mode=spec.rollover_price_mode,
                rollover_slippage_bps=spec.rollover_slippage_bps,
                start_date=spec.start_date,
                end_date=spec.end_date,
                max_ticks=spec.max_ticks,
                deterministic_fills=spec.deterministic_fills,
                wal_path=spec.wal_path,
                account_id=spec.account_id,
                run_id=args.run_id,
                emit_state_snapshots=args.emit_state_snapshots or spec.emit_state_snapshots,
            )
        elif args.emit_state_snapshots and not spec.emit_state_snapshots:
            spec = BacktestRunSpec(
                csv_path=spec.csv_path,
                dataset_root=spec.dataset_root,
                engine_mode=spec.engine_mode,
                rollover_mode=spec.rollover_mode,
                rollover_price_mode=spec.rollover_price_mode,
                rollover_slippage_bps=spec.rollover_slippage_bps,
                start_date=spec.start_date,
                end_date=spec.end_date,
                max_ticks=spec.max_ticks,
                deterministic_fills=spec.deterministic_fills,
                wal_path=spec.wal_path,
                account_id=spec.account_id,
                run_id=spec.run_id,
                emit_state_snapshots=True,
            )
    else:
        if args.engine_mode == "csv" and not effective_csv_path:
            print("error: --csv is required when --spec-file is not provided", file=sys.stderr)
            return 2
        if args.scenario_template:
            spec = build_backtest_spec_from_template(
                args.scenario_template,
                csv_path=effective_csv_path,
                run_id=args.run_id or None,
                wal_dir=Path("runtime/backtest"),
            )
            spec = with_cli_overrides(spec)
            if args.max_ticks is not None and args.max_ticks > 0:
                spec = BacktestRunSpec(
                    csv_path=spec.csv_path,
                    dataset_root=args.dataset_root,
                    engine_mode=args.engine_mode,
                    rollover_mode=args.rollover_mode,
                    rollover_price_mode=args.rollover_price_mode,
                    rollover_slippage_bps=args.rollover_slippage_bps,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    max_ticks=args.max_ticks,
                    deterministic_fills=spec.deterministic_fills,
                    wal_path=spec.wal_path,
                    account_id=spec.account_id,
                    run_id=spec.run_id,
                    emit_state_snapshots=args.emit_state_snapshots or spec.emit_state_snapshots,
                )
            elif args.emit_state_snapshots:
                spec = BacktestRunSpec(
                    csv_path=spec.csv_path,
                    dataset_root=args.dataset_root,
                    engine_mode=args.engine_mode,
                    rollover_mode=args.rollover_mode,
                    rollover_price_mode=args.rollover_price_mode,
                    rollover_slippage_bps=args.rollover_slippage_bps,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    max_ticks=spec.max_ticks,
                    deterministic_fills=spec.deterministic_fills,
                    wal_path=spec.wal_path,
                    account_id=spec.account_id,
                    run_id=spec.run_id,
                    emit_state_snapshots=True,
                )
        else:
            spec = BacktestRunSpec(
                csv_path=effective_csv_path,
                dataset_root=args.dataset_root,
                engine_mode=args.engine_mode,
                rollover_mode=args.rollover_mode,
                rollover_price_mode=args.rollover_price_mode,
                rollover_slippage_bps=args.rollover_slippage_bps,
                start_date=args.start_date,
                end_date=args.end_date,
                max_ticks=args.max_ticks if args.max_ticks is not None else 50000,
                deterministic_fills=args.deterministic_fills,
                wal_path=None,
                account_id="sim-account",
                run_id=args.run_id or "run-default",
                emit_state_snapshots=args.emit_state_snapshots,
            )

    ctx: dict[str, object] = {"strategy_template": args.strategy_template}
    result = run_backtest_spec(spec, runtime, ctx=ctx)
    report = result.replay

    print(
        "csv_replay_report "
        f"run_id={result.run_id} "
        f"mode={result.mode} "
        f"input_sig={result.input_signature[:12]} "
        f"data_sig={result.data_signature[:12]} "
        f"ticks={report.ticks_read} "
        f"bars={report.bars_emitted} "
        f"intents={report.intents_emitted} "
        f"instruments={report.instrument_count} "
        f"template={args.strategy_template} "
        f"time_range={report.first_ts_ns}:{report.last_ts_ns} "
        f"first_instrument={report.first_instrument} "
        f"last_instrument={report.last_instrument}"
    )
    if result.deterministic is not None:
        print(
            "csv_replay_deterministic "
            f"order_events={result.deterministic.order_events_emitted} "
            f"wal_records={result.deterministic.wal_records} "
            f"invariant_violations={len(result.deterministic.invariant_violations)} "
            f"total_pnl={result.deterministic.performance.total_pnl:.4f} "
            f"max_drawdown={result.deterministic.performance.max_drawdown:.4f}"
        )

    if args.report_json:
        output = Path(args.report_json)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(result.to_dict(), ensure_ascii=True, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    if args.report_md:
        output = Path(args.report_md)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(_render_markdown_report(result), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
