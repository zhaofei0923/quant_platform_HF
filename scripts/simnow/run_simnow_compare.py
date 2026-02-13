#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from quant_hft.simnow import (
        SimNowComparatorRunner,
        load_simnow_compare_config,
        persist_compare_sqlite,
        write_compare_html,
        write_compare_report,
    )
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.simnow import (  # type: ignore[no-redef]
        SimNowComparatorRunner,
        load_simnow_compare_config,
        persist_compare_sqlite,
        write_compare_html,
        write_compare_report,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run SimNow/backtest parity compare and emit JSON report"
    )
    parser.add_argument("--config", default="configs/sim/ctp.yaml", help="CTP yaml config path")
    parser.add_argument("--csv-path", default="backtest_data/rb.csv", help="baseline csv path")
    parser.add_argument("--output-json", default="docs/results/simnow_compare_report.json")
    parser.add_argument("--output-html", default="docs/results/simnow_compare_report.html")
    parser.add_argument("--sqlite-path", default="runtime/simnow/simnow_compare.sqlite")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--strategy-id", default="")
    parser.add_argument("--max-ticks", type=int, default=300)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="return non-zero when mismatch exists",
    )
    return parser


def _should_force_dry_run() -> bool:
    required = (
        "CTP_SIM_BROKER_ID",
        "CTP_SIM_USER_ID",
        "CTP_SIM_INVESTOR_ID",
        "CTP_SIM_PASSWORD",
    )
    return any(not os.getenv(name) for name in required)


def main() -> int:
    args = _build_parser().parse_args()

    run_id = args.run_id or datetime.now(timezone.utc).strftime("simnow-compare-%Y%m%d-%H%M%S")
    dry_run = bool(args.dry_run) or _should_force_dry_run()

    config = load_simnow_compare_config(
        ctp_config_path=args.config,
        backtest_csv_path=args.csv_path,
        output_json_path=args.output_json,
        run_id=run_id,
        strategy_id_override=args.strategy_id,
        max_ticks=args.max_ticks,
        dry_run=dry_run,
    )

    runner = SimNowComparatorRunner(config)
    result = runner.run()
    payload = result.to_dict()
    report_file = write_compare_report(payload, config.output_json_path)
    report_html = write_compare_html(payload, args.output_html)
    sqlite_file = persist_compare_sqlite(payload, args.sqlite_path)

    print(
        "simnow compare: "
        f"run_id={result.run_id} dry_run={result.dry_run} "
        f"delta_intents={result.delta_intents} report={report_file} "
        f"html={report_html} sqlite={sqlite_file}"
    )

    if args.strict and not result.within_threshold:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
