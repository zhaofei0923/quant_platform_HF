#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from quant_hft.data_pipeline.adapters import MinioArchiveStore
    from quant_hft.data_pipeline.lifecycle_policy import (
        LifecyclePolicy,
        LifecyclePolicyConfig,
        ObjectLifecycleRule,
        ObjectStoreLifecyclePolicy,
    )
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.data_pipeline.adapters import MinioArchiveStore  # type: ignore[no-redef]
    from quant_hft.data_pipeline.lifecycle_policy import (  # type: ignore[no-redef]
        LifecyclePolicy,
        LifecyclePolicyConfig,
        ObjectLifecycleRule,
        ObjectStoreLifecyclePolicy,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run local or object-store lifecycle policy for hot/warm/cold tiers"
    )
    parser.add_argument("--mode", choices=("local", "object-store"), default="local")

    parser.add_argument("--source-dir", default="")
    parser.add_argument("--warm-dir", default="runtime/lifecycle/warm")
    parser.add_argument("--cold-dir", default="runtime/lifecycle/cold")
    parser.add_argument("--hot-retention-days", type=int, default=1)
    parser.add_argument("--warm-retention-days", type=int, default=7)

    parser.add_argument("--archive-endpoint", default="localhost:9000")
    parser.add_argument("--archive-access-key", default="minioadmin")
    parser.add_argument("--archive-secret-key", default="minioadmin")
    parser.add_argument("--archive-bucket", default="quant-archive")
    parser.add_argument("--archive-local-dir", default="runtime/archive")
    parser.add_argument("--policies-file", default="configs/data_lifecycle/policies.yaml")

    parser.add_argument("--report-json", default="docs/results/data_lifecycle_report.json")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--now-epoch-seconds", type=float, default=0.0)
    return parser


def _load_object_rules(path: Path) -> list[ObjectLifecycleRule]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("policies file must be a JSON object")

    rules: list[ObjectLifecycleRule] = []
    for dataset, config in payload.items():
        if not isinstance(dataset, str) or not isinstance(config, dict):
            continue
        rules.append(
            ObjectLifecycleRule(
                dataset=dataset,
                base_prefix=str(config.get("base_prefix", "")).strip("/"),
                hot_retention_days=int(config.get("hot_retention_days", 7)),
                warm_retention_days=int(config.get("warm_retention_days", 90)),
                cold_retention_days=int(config.get("cold_retention_days", 365)),
                delete_after_days=int(config.get("delete_after_days", 365)),
            )
        )
    return rules


def main() -> int:
    args = _build_parser().parse_args()

    now_fn = None
    if args.now_epoch_seconds > 0:
        fixed = float(args.now_epoch_seconds)

        def _fixed_now() -> float:
            return fixed

        now_fn = _fixed_now

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    if args.mode == "local":
        if not args.source_dir:
            print("error: --source-dir is required when --mode=local", file=sys.stderr)
            return 2
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
            "mode": "local",
            "source_dir": str(Path(args.source_dir)),
            "warm_dir": str(Path(args.warm_dir)),
            "cold_dir": str(Path(args.cold_dir)),
            "scanned_files": report.scanned_files,
            "hot_files": report.hot_files,
            "warm_files": report.warm_files,
            "cold_files": report.cold_files,
            "moved_files": report.moved_files,
            "dry_run": report.dry_run,
            "replication_status": "placeholder",
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
        report_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
        print(str(report_path))
        return 0

    rules = _load_object_rules(Path(args.policies_file))
    archive = MinioArchiveStore(
        endpoint=args.archive_endpoint,
        access_key=args.archive_access_key,
        secret_key=args.archive_secret_key,
        bucket=args.archive_bucket,
        local_fallback_dir=Path(args.archive_local_dir),
    )
    policy = ObjectStoreLifecyclePolicy(archive, rules, now_epoch_seconds_fn=now_fn)
    decisions = policy.plan()
    report = policy.apply(decisions, execute=bool(args.execute))
    payload = {
        "mode": "object-store",
        "archive_bucket": args.archive_bucket,
        "archive_mode": archive.mode,
        "policies_file": str(Path(args.policies_file)),
        "scanned_objects": report.scanned_objects,
        "moved_objects": report.moved_objects,
        "deleted_objects": report.deleted_objects,
        "kept_objects": report.kept_objects,
        "dry_run": report.dry_run,
        "decisions": [
            {
                "dataset": item.dataset,
                "object_name": item.object_name,
                "age_days": round(item.age_days, 6),
                "current_tier": item.current_tier,
                "action": item.action,
                "target_object": item.target_object,
            }
            for item in decisions
        ],
    }
    report_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(str(report_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
