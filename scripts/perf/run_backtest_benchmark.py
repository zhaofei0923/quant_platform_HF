#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path

try:
    from quant_hft.backtest.replay import BacktestRunSpec, run_backtest_spec
    from quant_hft.runtime.engine import StrategyRuntime
    from quant_hft.strategy.demo import DemoStrategy
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.backtest.replay import BacktestRunSpec, run_backtest_spec  # type: ignore[no-redef]
    from quant_hft.runtime.engine import StrategyRuntime  # type: ignore[no-redef]
    from quant_hft.strategy.demo import DemoStrategy  # type: ignore[no-redef]


@dataclass(frozen=True)
class BacktestBenchmarkSummary:
    runs: int
    max_ticks: int
    mean_ms: float
    p95_ms: float
    min_ticks_read: int
    max_ticks_read: int
    sample_total_pnl: float


def run_backtest_benchmark(csv_path: Path, *, max_ticks: int, runs: int, warmup_runs: int) -> BacktestBenchmarkSummary:
    if runs <= 0:
        raise ValueError("runs must be positive")
    if warmup_runs < 0:
        raise ValueError("warmup_runs must be non-negative")

    latencies_ms: list[float] = []
    ticks_read_values: list[int] = []
    sample_total_pnl = 0.0

    total_runs = warmup_runs + runs
    for idx in range(total_runs):
        runtime = StrategyRuntime()
        runtime.add_strategy(DemoStrategy("demo"))
        spec = BacktestRunSpec(
            csv_path=str(csv_path),
            max_ticks=max_ticks,
            deterministic_fills=True,
            run_id=f"bench-{idx:03d}",
            account_id="sim-account",
        )

        started = time.perf_counter_ns()
        result = run_backtest_spec(spec, runtime, ctx={})
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000.0

        if idx < warmup_runs:
            continue

        latencies_ms.append(elapsed_ms)
        ticks_read_values.append(result.replay.ticks_read)
        if result.deterministic is not None:
            sample_total_pnl = float(result.deterministic.performance.total_pnl)

    sorted_lat = sorted(latencies_ms)
    p95_index = max(0, min(len(sorted_lat) - 1, int(round((len(sorted_lat) - 1) * 0.95))))

    return BacktestBenchmarkSummary(
        runs=runs,
        max_ticks=max_ticks,
        mean_ms=statistics.fmean(latencies_ms),
        p95_ms=sorted_lat[p95_index],
        min_ticks_read=min(ticks_read_values),
        max_ticks_read=max(ticks_read_values),
        sample_total_pnl=sample_total_pnl,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic backtest benchmark for CI gate")
    parser.add_argument("--csv-path", default="runtime/benchmarks/backtest/rb_ci_sample.csv")
    parser.add_argument("--baseline", default="configs/perf/backtest_benchmark_baseline.json")
    parser.add_argument("--result-json", default="docs/results/backtest_benchmark_result.json")
    parser.add_argument("--max-ticks", type=int, default=1200)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup-runs", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    baseline = json.loads(Path(args.baseline).read_text(encoding="utf-8"))

    max_ticks = int(args.max_ticks or baseline.get("max_ticks", 1200))
    runs = int(args.runs or baseline.get("runs", 3))
    warmup_runs = int(args.warmup_runs or baseline.get("warmup_runs", 1))

    summary = run_backtest_benchmark(
        Path(args.csv_path),
        max_ticks=max_ticks,
        runs=runs,
        warmup_runs=warmup_runs,
    )

    p95_threshold_ms = float(baseline.get("max_p95_ms", 2000.0))
    min_ticks_required = int(baseline.get("min_ticks_read", max_ticks))
    passed = summary.p95_ms <= p95_threshold_ms and summary.min_ticks_read >= min_ticks_required

    payload = {
        "benchmark": "backtest_deterministic",
        "csv_path": str(Path(args.csv_path)),
        "runs": summary.runs,
        "warmup_runs": warmup_runs,
        "max_ticks": summary.max_ticks,
        "mean_ms": summary.mean_ms,
        "p95_ms": summary.p95_ms,
        "max_p95_ms_threshold": p95_threshold_ms,
        "min_ticks_read": summary.min_ticks_read,
        "max_ticks_read": summary.max_ticks_read,
        "min_ticks_required": min_ticks_required,
        "sample_total_pnl": summary.sample_total_pnl,
        "passed": passed,
    }

    result_path = Path(args.result_json)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(result_path))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
