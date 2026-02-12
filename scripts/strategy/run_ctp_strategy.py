#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from decimal import Decimal

from quant_hft.runtime.ctp_direct_runner import (
    UnifiedCtpDirectRunner,
    load_ctp_direct_runner_config,
)
from quant_hft.runtime.unified import Bar, Offset, Order, OrderType, Strategy, Tick, Trade


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run quant_hft strategy directly on CTP pybind wrapper "
            "(Redis bridge is optional compatibility path)"
        ),
    )
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


class DemoUnifiedStrategy(Strategy):
    def __init__(self, strategy_id: str, instruments: list[str]) -> None:
        super().__init__(strategy_id)
        self._instruments = instruments
        self._submitted_symbols: set[str] = set()

    def initialize(self) -> None:
        if self.data is None:
            raise RuntimeError("strategy datafeed is not injected")
        self.data.subscribe(self._instruments, on_tick=self.on_tick, on_bar=self.on_bar)

    def on_tick(self, tick: Tick) -> None:
        if self.broker is None:
            raise RuntimeError("strategy broker is not injected")
        if tick.symbol in self._submitted_symbols:
            return
        self._submitted_symbols.add(tick.symbol)
        self.broker.buy(
            symbol=tick.symbol,
            price=tick.last_price + Decimal("1"),
            quantity=1,
            offset=Offset.OPEN,
            order_type=OrderType.LIMIT,
            trace_id=f"{self.strategy_id}-{tick.symbol}-{tick.datetime.timestamp()}",
        )

    def on_bar(self, bar: Bar) -> None:
        del bar

    def on_order(self, order: Order) -> None:
        del order

    def on_trade(self, trade: Trade) -> None:
        del trade


def main() -> int:
    args = parse_args()
    runner_config = load_ctp_direct_runner_config(args.config, args.strategy_id)

    strategy = DemoUnifiedStrategy(
        strategy_id=runner_config.strategy_id,
        instruments=list(runner_config.instruments),
    )
    runner = UnifiedCtpDirectRunner(strategy, runner_config)
    if not runner.start():
        print("ctp direct runner failed to start", file=sys.stderr)
        return 2

    try:
        if args.run_once:
            emitted = runner.run_once()
            print(f"ctp direct runner emitted intents={emitted}")
            return 0

        run_seconds = args.run_seconds if args.run_seconds > 0 else None
        runner.run_forever(run_seconds=run_seconds)
        return 0
    finally:
        runner.stop()


if __name__ == "__main__":
    raise SystemExit(main())
