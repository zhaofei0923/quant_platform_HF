#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from quant_hft.data_pipeline.lifecycle_policy import (
        LifecyclePolicy,
        LifecyclePolicyConfig,
    )
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.data_pipeline.lifecycle_policy import (  # type: ignore[no-redef]
        LifecyclePolicy,
        LifecyclePolicyConfig,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run local data lifecycle policy for hot/warm/cold tiers"
    )
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--warm-dir", default="runtime/lifecycle/warm")
    parser.add_argument("--cold-dir", default="runtime/lifecycle/cold")
    parser.add_argument("--hot-retention-days", type=int, default=1)
    parser.add_argument("--warm-retention-days", type=int, default=7)
    parser.add_argument("--report-json", default="docs/results/data_lifecycle_report.json")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--now-epoch-seconds", type=float, default=0.0)
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    now_fn = None
    if args.now_epoch_seconds > 0:
        fixed = float(args.now_epoch_seconds)

        def _fixed_now() -> float:
            return fixed

        now_fn = _fixed_now

    policy = LifecyclePolicy(
        source_root=Path(args.source_dir),
        config=LifecyclePolicyConfig(
            hot_retention_days=args.hot_retention_days,
            warm_retention_days=args.warm_retention_days,
        ),
        now_epoch_seconds_fn=now_fn,
    )
    decisions = policy.plan()
    report = policy.apply(
        decisions,
        warm_root=Path(args.warm_dir),
        cold_root=Path(args.cold_dir),
        execute=bool(args.execute),
    )

    payload = {
        "source_dir": str(Path(args.source_dir)),
        "warm_dir": str(Path(args.warm_dir)),
        "cold_dir": str(Path(args.cold_dir)),
        "scanned_files": report.scanned_files,
        "hot_files": report.hot_files,
        "warm_files": report.warm_files,
        "cold_files": report.cold_files,
        "moved_files": report.moved_files,
        "dry_run": report.dry_run,
        "replication_status": "placeholder",  # cross-region DR placeholder
        "decisions": [
            {
                "relative_path": str(item.relative_path),
                "age_days": round(item.age_days, 6),
                "tier": item.tier.value,
                "action": item.action,
            }
            for item in decisions
        ],
    }

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(str(report_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
