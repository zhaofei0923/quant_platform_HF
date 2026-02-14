#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hybrid hotpath stress runner")
    parser.add_argument("--tick-rate", type=int, default=2000)
    parser.add_argument("--order-rate", type=int, default=20)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--python-queue-size", type=int, default=5000)
    parser.add_argument("--output", default="stats.json")
    parser.add_argument("--benchmark-bin", default="build-check/hotpath_hybrid")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    benchmark = Path(args.benchmark_bin)
    if not benchmark.exists():
        print(f"missing benchmark binary: {benchmark}")
        return 2

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(benchmark),
        "--tick-rate",
        str(max(0, args.tick_rate)),
        "--order-rate",
        str(max(0, args.order_rate)),
        "--duration",
        str(max(1, args.duration)),
        "--python-queue-size",
        str(max(1, args.python_queue_size)),
        "--output",
        str(output_path),
    ]

    completed = subprocess.run(cmd, check=False)
    if completed.returncode != 0:
        return completed.returncode

    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
