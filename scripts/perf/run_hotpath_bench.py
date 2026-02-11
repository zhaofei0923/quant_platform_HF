#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run hotpath benchmark and enforce baseline regression threshold"
    )
    parser.add_argument("--benchmark-bin", default="build/hotpath_benchmark")
    parser.add_argument("--baseline", default="configs/perf/baseline.json")
    parser.add_argument("--result-json", default="docs/results/hotpath_bench_result.json")
    parser.add_argument("--iterations", type=int, default=0)
    parser.add_argument("--buffer-size", type=int, default=0)
    parser.add_argument("--pool-capacity", type=int, default=0)
    return parser.parse_args()


def _parse_key_values(raw: str) -> dict[str, str]:
    payload: dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        payload[key.strip()] = value.strip()
    return payload


def main() -> int:
    args = _parse_args()
    baseline = json.loads(Path(args.baseline).read_text(encoding="utf-8"))

    iterations = int(args.iterations or baseline.get("iterations", 100000))
    buffer_size = int(args.buffer_size or baseline.get("buffer_size", 256))
    pool_capacity = int(args.pool_capacity or baseline.get("pool_capacity", 1024))

    cmd = [
        str(Path(args.benchmark_bin)),
        "--iterations",
        str(max(1, iterations)),
        "--buffer-size",
        str(max(1, buffer_size)),
        "--pool-capacity",
        str(max(1, pool_capacity)),
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        print(completed.stdout)
        print(completed.stderr)
        return completed.returncode

    measured = _parse_key_values(completed.stdout)
    baseline_ns_per_op = float(baseline.get("baseline_ns_per_op", 1.0))
    pooled_ns_per_op = float(measured["pooled_ns_per_op"])
    regression_ratio = (pooled_ns_per_op - baseline_ns_per_op) / max(baseline_ns_per_op, 1e-9)
    max_regression_ratio = float(baseline.get("max_regression_ratio", 0.10))
    passed = regression_ratio <= max_regression_ratio

    result = {
        "benchmark_bin": str(Path(args.benchmark_bin)),
        "iterations": int(measured["iterations"]),
        "buffer_size": int(measured["buffer_size"]),
        "pool_capacity": int(measured["pool_capacity"]),
        "baseline_ns_per_op": float(measured["baseline_ns_per_op"]),
        "pooled_ns_per_op": pooled_ns_per_op,
        "baseline_threshold_ns_per_op": baseline_ns_per_op,
        "regression_ratio": regression_ratio,
        "max_regression_ratio": max_regression_ratio,
        "passed": passed,
    }

    result_path = Path(args.result_json)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(result_path))
    if passed:
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
