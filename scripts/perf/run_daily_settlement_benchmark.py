#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run synthetic daily settlement benchmark and enforce " "P99 latency threshold."
        )
    )
    parser.add_argument("--positions", type=int, default=10_000)
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--target-p99-ms", type=float, default=200.0)
    parser.add_argument("--result-json", default="docs/results/daily_settlement_benchmark.json")
    return parser


def _percentile(sorted_values: list[float], quantile: float) -> float:
    if not sorted_values:
        return 0.0
    index = max(0, min(len(sorted_values) - 1, math.ceil(quantile * len(sorted_values)) - 1))
    return sorted_values[index]


def _build_positions(count: int) -> list[tuple[int, int, int, int]]:
    positions: list[tuple[int, int, int, int]] = []
    for i in range(count):
        open_price_1e4 = 350000 + (i % 5000)
        settlement_price_1e4 = open_price_1e4 + ((i % 9) - 4) * 3
        volume = 1 + (i % 5)
        multiplier = 5 + (i % 6) * 5
        positions.append((open_price_1e4, settlement_price_1e4, volume, multiplier))
    return positions


def _run_once(positions: list[tuple[int, int, int, int]]) -> tuple[int, int]:
    pnl_scaled = 0
    margin_scaled = 0
    for open_price_1e4, settlement_price_1e4, volume, multiplier in positions:
        delta_price_1e4 = settlement_price_1e4 - open_price_1e4
        pnl_scaled += delta_price_1e4 * volume * multiplier

        # Margin rule simulation:
        # intermediate keeps 6 decimal rate precision (0.123456 -> 123456 / 1e6),
        # final amount rounds up to cent-like precision (2 decimals).
        notional_scaled = settlement_price_1e4 * volume * multiplier
        intermediate = notional_scaled * 123456
        margin_scaled += (intermediate + 1_000_000 - 1) // 1_000_000
    return pnl_scaled, margin_scaled


def main() -> int:
    args = _build_parser().parse_args()
    positions = max(1, int(args.positions))
    runs = max(1, int(args.runs))
    target_p99_ms = max(0.0, float(args.target_p99_ms))

    dataset = _build_positions(positions)
    durations_ms: list[float] = []
    checksum = 0

    for _ in range(runs):
        start_ns = time.perf_counter_ns()
        pnl_scaled, margin_scaled = _run_once(dataset)
        elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
        durations_ms.append(elapsed_ms)
        checksum ^= pnl_scaled ^ margin_scaled

    durations_ms.sort()
    p50_ms = _percentile(durations_ms, 0.50)
    p95_ms = _percentile(durations_ms, 0.95)
    p99_ms = _percentile(durations_ms, 0.99)
    passed = p99_ms <= target_p99_ms

    result = {
        "positions": positions,
        "runs": runs,
        "target_p99_ms": target_p99_ms,
        "p50_ms": p50_ms,
        "p95_ms": p95_ms,
        "p99_ms": p99_ms,
        "passed": passed,
        "checksum": int(checksum),
    }

    result_path = Path(args.result_json)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(result_path))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
