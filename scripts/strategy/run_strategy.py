#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.runtime.redis_hash import TcpRedisHashClient
from quant_hft.runtime.strategy_runner import (
    StrategyRunner,
    load_redis_client_from_env,
    load_runner_config,
)
from quant_hft.strategy.demo import DemoStrategy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run quant_hft strategy runner over Redis bridge")
    parser.add_argument(
        "--config",
        default="configs/sim/ctp.yaml",
        help="Path to ctp yaml config (simple key-value parser)",
    )
    parser.add_argument(
        "--strategy-id",
        default="",
        help="Override strategy id (defaults to first item in strategy_ids)",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Execute one polling cycle and exit",
    )
    parser.add_argument(
        "--run-seconds",
        type=int,
        default=0,
        help="Optional runtime in seconds for loop mode; <=0 means run forever",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runner_config = load_runner_config(args.config, args.strategy_id)

    runtime = StrategyRuntime()
    runtime.add_strategy(DemoStrategy(runner_config.strategy_id))

    redis_client = load_redis_client_from_env(TcpRedisHashClient)
    if not redis_client.ping():
        print("redis ping failed", file=sys.stderr)
        return 2

    runner = StrategyRunner(
        runtime=runtime,
        redis_client=redis_client,
        strategy_id=runner_config.strategy_id,
        instruments=runner_config.instruments,
        poll_interval_ms=runner_config.poll_interval_ms,
    )

    if args.run_once:
        emitted = runner.run_once()
        print(f"strategy runner emitted intents={emitted}")
        return 0

    run_seconds = args.run_seconds if args.run_seconds > 0 else None
    runner.run_forever(run_seconds=run_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
