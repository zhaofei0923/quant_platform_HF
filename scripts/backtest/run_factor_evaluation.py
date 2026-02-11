#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    from quant_hft.backtest.replay import BacktestRunSpec, load_backtest_run_spec, run_backtest_spec
    from quant_hft.research.experiment_tracker import ExperimentTracker
    from quant_hft.research.metric_dictionary import METRIC_DICTIONARY
    from quant_hft.runtime.engine import StrategyRuntime
    from quant_hft.strategy.demo import DemoStrategy
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.backtest.replay import (  # type: ignore[no-redef]
        BacktestRunSpec,
        load_backtest_run_spec,
        run_backtest_spec,
    )
    from quant_hft.research.experiment_tracker import ExperimentTracker  # type: ignore[no-redef]
    from quant_hft.research.metric_dictionary import METRIC_DICTIONARY  # type: ignore[no-redef]
    from quant_hft.runtime.engine import StrategyRuntime  # type: ignore[no-redef]
    from quant_hft.strategy.demo import DemoStrategy  # type: ignore[no-redef]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run factor evaluation and append experiment record"
    )
    parser.add_argument("--spec-file", default="")
    parser.add_argument("--csv-path", default="")
    parser.add_argument("--run-id", default="factor-eval")
    parser.add_argument("--factor-id", required=True)
    parser.add_argument(
        "--template",
        choices=("trend", "arbitrage", "market_making"),
        default="trend",
    )
    parser.add_argument("--output-jsonl", default="docs/results/experiment_tracker.jsonl")
    return parser.parse_args()


def _build_spec(args: argparse.Namespace) -> BacktestRunSpec:
    if args.spec_file:
        return load_backtest_run_spec(args.spec_file)
    if not args.csv_path:
        raise ValueError("either --spec-file or --csv-path is required")
    return BacktestRunSpec(
        csv_path=args.csv_path,
        max_ticks=5000,
        deterministic_fills=True,
        run_id=args.run_id,
        account_id="sim-account",
    )


def main() -> int:
    args = _parse_args()
    spec = _build_spec(args)

    runtime = StrategyRuntime()
    runtime.add_strategy(DemoStrategy("demo"))
    result = run_backtest_spec(spec, runtime, ctx={})

    total_pnl = 0.0
    max_drawdown = 0.0
    fill_rate = 0.0
    win_rate = 0.0
    capital_efficiency = 0.0
    if result.deterministic is not None:
        perf = result.deterministic.performance
        total_pnl = perf.total_pnl
        max_drawdown = perf.max_drawdown
        accepted = float(perf.order_status_counts.get("ACCEPTED", 0))
        filled = float(perf.order_status_counts.get("FILLED", 0))
        fill_rate = filled / accepted if accepted > 0 else 0.0
        win_rate = 1.0 if total_pnl > 0 else 0.0
        capital_efficiency = total_pnl / max(1.0, abs(perf.max_equity))

    metrics = {
        "total_pnl": total_pnl,
        "max_drawdown": max_drawdown,
        "win_rate": win_rate,
        "fill_rate": fill_rate,
        "capital_efficiency": capital_efficiency,
    }
    for key in metrics:
        if key not in METRIC_DICTIONARY:
            raise ValueError(f"metric key is not registered: {key}")

    tracker = ExperimentTracker(args.output_jsonl)
    record = tracker.append(
        run_id=result.run_id,
        template=args.template,
        factor_id=args.factor_id,
        spec_signature=result.input_signature,
        metrics=metrics,
    )
    print(f"factor evaluation recorded: {args.output_jsonl} run_id={record.run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
