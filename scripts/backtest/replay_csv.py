#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    from quant_hft.backtest.replay import replay_csv_minute_bars
    from quant_hft.runtime.engine import StrategyRuntime
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.backtest.replay import replay_csv_minute_bars  # type: ignore[no-redef]
    from quant_hft.runtime.engine import StrategyRuntime  # type: ignore[no-redef]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay CSV ticks into minute bars.")
    parser.add_argument("--csv", required=True, help="path to csv file")
    parser.add_argument("--max-ticks", type=int, default=50000, help="max ticks to replay")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    runtime = StrategyRuntime()
    report = replay_csv_minute_bars(args.csv, runtime, ctx={}, max_ticks=args.max_ticks)

    print(
        "csv_replay_report "
        f"ticks={report.ticks_read} "
        f"bars={report.bars_emitted} "
        f"intents={report.intents_emitted} "
        f"first_instrument={report.first_instrument} "
        f"last_instrument={report.last_instrument}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
