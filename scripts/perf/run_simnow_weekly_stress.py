#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from quant_hft.simnow import SimNowComparatorRunner, load_simnow_compare_config
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.simnow import (  # type: ignore[no-redef]
        SimNowComparatorRunner,
        load_simnow_compare_config,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run weekly SimNow stress collection (non-blocking by default)."
    )
    parser.add_argument("--config", default="configs/sim/ctp.yaml")
    parser.add_argument("--csv-path", default="backtest_data/rb.csv")
    parser.add_argument("--max-ticks", type=int, default=1200)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--collect-only", action="store_true", default=True)
    parser.add_argument("--result-json", default="docs/results/simnow_weekly_stress.json")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    samples = max(1, int(args.samples))
    run_root = datetime.now(timezone.utc).strftime("simnow-stress-%Y%m%d-%H%M%S")

    sample_payloads: list[dict[str, object]] = []
    delta_abs_values: list[int] = []
    delta_ratio_values: list[float] = []

    for index in range(samples):
        run_id = f"{run_root}-{index + 1:02d}"
        cfg = load_simnow_compare_config(
            ctp_config_path=args.config,
            backtest_csv_path=args.csv_path,
            output_json_path="",
            run_id=run_id,
            max_ticks=int(args.max_ticks),
            dry_run=bool(args.dry_run),
        )
        result = SimNowComparatorRunner(cfg).run()
        payload = result.to_dict()
        sample_payloads.append(payload)

        delta_obj = payload.get("delta", {})
        delta_abs = int(abs(int((delta_obj or {}).get("intents", 0) or 0)))
        delta_ratio = float((delta_obj or {}).get("intents_ratio", 0.0) or 0.0)
        delta_abs_values.append(delta_abs)
        delta_ratio_values.append(delta_ratio)

    summary = {
        "benchmark": "simnow_weekly_stress",
        "collect_only": bool(args.collect_only),
        "samples": samples,
        "max_ticks": int(args.max_ticks),
        "dry_run": bool(args.dry_run),
        "delta_abs_mean": statistics.fmean(delta_abs_values),
        "delta_abs_p95": sorted(delta_abs_values)[max(0, int(round((samples - 1) * 0.95)))],
        "delta_ratio_mean": statistics.fmean(delta_ratio_values),
        "delta_ratio_p95": sorted(delta_ratio_values)[max(0, int(round((samples - 1) * 0.95)))],
        "all_within_threshold": all(
            bool((item.get("threshold", {}) or {}).get("within_threshold", False))
            for item in sample_payloads
        ),
        "samples_detail": sample_payloads,
    }

    output = Path(args.result_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(output))

    if args.collect_only:
        return 0
    return 0 if bool(summary["all_within_threshold"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
