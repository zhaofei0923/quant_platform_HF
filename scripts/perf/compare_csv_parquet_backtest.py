#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from dataclasses import asdict, dataclass
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
class ModeSummary:
    engine_mode: str
    runs: int
    warmup_runs: int
    max_ticks: int
    elapsed_ms_values: list[float]
    mean_ms: float
    p95_ms: float
    min_ms: float
    max_ms: float
    ticks_read_min: int
    ticks_read_max: int
    mean_ticks_per_sec: float


@dataclass(frozen=True)
class CompareSummary:
    csv: ModeSummary
    parquet: ModeSummary
    parquet_vs_csv_speedup: float
    ticks_read_consistent: bool


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare backtest replay speed between CSV and Parquet for one symbol"
    )
    parser.add_argument("--csv-path", default="backtest_data/c.csv")
    parser.add_argument("--parquet-root", default="backtest_data/parquet/source=c")
    parser.add_argument("--max-ticks", type=int, default=20000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument(
        "--deterministic-fills",
        action="store_true",
        help="Enable deterministic order/fill simulation (higher CPU/memory)",
    )
    parser.add_argument("--result-json", default="docs/results/csv_parquet_speed_compare_c.json")
    return parser


def _run_one_mode(
    *,
    engine_mode: str,
    csv_path: str,
    parquet_root: str,
    max_ticks: int,
    runs: int,
    warmup_runs: int,
    deterministic_fills: bool,
) -> ModeSummary:
    elapsed_ms_values: list[float] = []
    ticks_read_values: list[int] = []

    total_runs = warmup_runs + runs
    for idx in range(total_runs):
        runtime = StrategyRuntime()
        runtime.add_strategy(DemoStrategy("demo"))

        spec = BacktestRunSpec(
            csv_path=csv_path,
            dataset_root=parquet_root,
            engine_mode=engine_mode,
            max_ticks=max_ticks,
            deterministic_fills=deterministic_fills,
            run_id=f"cmp-{engine_mode}-{idx:03d}",
            account_id="sim-account",
        )

        started = time.perf_counter_ns()
        result = run_backtest_spec(spec, runtime, ctx={})
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000.0

        if idx < warmup_runs:
            continue

        elapsed_ms_values.append(elapsed_ms)
        ticks_read_values.append(result.replay.ticks_read)

    sorted_elapsed = sorted(elapsed_ms_values)
    p95_index = max(0, min(len(sorted_elapsed) - 1, int(round((len(sorted_elapsed) - 1) * 0.95))))

    mean_ms = statistics.fmean(elapsed_ms_values)
    mean_ticks = statistics.fmean(ticks_read_values)
    mean_ticks_per_sec = mean_ticks / (mean_ms / 1000.0) if mean_ms > 0 else 0.0

    return ModeSummary(
        engine_mode=engine_mode,
        runs=runs,
        warmup_runs=warmup_runs,
        max_ticks=max_ticks,
        elapsed_ms_values=elapsed_ms_values,
        mean_ms=mean_ms,
        p95_ms=sorted_elapsed[p95_index],
        min_ms=min(elapsed_ms_values),
        max_ms=max(elapsed_ms_values),
        ticks_read_min=min(ticks_read_values),
        ticks_read_max=max(ticks_read_values),
        mean_ticks_per_sec=mean_ticks_per_sec,
    )


def main() -> int:
    args = _build_parser().parse_args()

    if args.runs <= 0:
        raise ValueError("--runs must be > 0")
    if args.warmup_runs < 0:
        raise ValueError("--warmup-runs must be >= 0")
    if args.max_ticks <= 0:
        raise ValueError("--max-ticks must be > 0")

    csv_path = str(Path(args.csv_path))
    parquet_root = str(Path(args.parquet_root))

    csv_summary = _run_one_mode(
        engine_mode="csv",
        csv_path=csv_path,
        parquet_root=parquet_root,
        max_ticks=int(args.max_ticks),
        runs=int(args.runs),
        warmup_runs=int(args.warmup_runs),
        deterministic_fills=bool(args.deterministic_fills),
    )
    parquet_summary = _run_one_mode(
        engine_mode="parquet",
        csv_path=csv_path,
        parquet_root=parquet_root,
        max_ticks=int(args.max_ticks),
        runs=int(args.runs),
        warmup_runs=int(args.warmup_runs),
        deterministic_fills=bool(args.deterministic_fills),
    )

    speedup = csv_summary.mean_ms / parquet_summary.mean_ms if parquet_summary.mean_ms > 0 else 0.0
    ticks_consistent = (
        csv_summary.ticks_read_min == parquet_summary.ticks_read_min
        and csv_summary.ticks_read_max == parquet_summary.ticks_read_max
    )

    summary = CompareSummary(
        csv=csv_summary,
        parquet=parquet_summary,
        parquet_vs_csv_speedup=speedup,
        ticks_read_consistent=ticks_consistent,
    )

    payload = {
        "benchmark": "csv_parquet_compare",
        "csv_path": csv_path,
        "parquet_root": parquet_root,
        "runs": int(args.runs),
        "warmup_runs": int(args.warmup_runs),
        "max_ticks": int(args.max_ticks),
        "deterministic_fills": bool(args.deterministic_fills),
        "summary": asdict(summary),
    }

    result_path = Path(args.result_json)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

    print(str(result_path))
    print(
        "csv_vs_parquet: "
        f"csv_mean_ms={csv_summary.mean_ms:.2f} "
        f"parquet_mean_ms={parquet_summary.mean_ms:.2f} "
        f"parquet_speedup={speedup:.3f}x "
        f"deterministic_fills={bool(args.deterministic_fills)} "
        f"ticks_consistent={ticks_consistent}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
