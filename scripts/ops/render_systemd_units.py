#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_hft.ops.systemd import SystemdRenderConfig, write_systemd_bundle


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render systemd unit files for quant_hft services")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-dir", default="deploy/systemd")
    parser.add_argument("--build-dir", default="build")
    parser.add_argument("--python-bin", default=".venv/bin/python")
    parser.add_argument("--core-config", default="configs/sim/ctp.yaml")
    parser.add_argument("--analytics-db", default="runtime/analytics.duckdb")
    parser.add_argument("--export-dir", default="runtime/exports")
    parser.add_argument("--archive-local-dir", default="runtime/archive")
    parser.add_argument("--archive-endpoint", default="localhost:9000")
    parser.add_argument("--archive-bucket", default="quant-archive")
    parser.add_argument("--archive-prefix", default="etl")
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    parser.add_argument("--service-user")
    parser.add_argument("--service-group")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    config = SystemdRenderConfig(
        repo_root=Path(args.repo_root),
        output_dir=Path(args.output_dir),
        build_dir=Path(args.build_dir),
        python_bin=Path(args.python_bin),
        core_config=Path(args.core_config),
        analytics_db=Path(args.analytics_db),
        export_dir=Path(args.export_dir),
        archive_local_dir=Path(args.archive_local_dir),
        archive_endpoint=args.archive_endpoint,
        archive_bucket=args.archive_bucket,
        archive_prefix=args.archive_prefix,
        interval_seconds=max(1.0, args.interval_seconds),
        service_user=args.service_user,
        service_group=args.service_group,
    )
    written = write_systemd_bundle(config)
    print(
        json.dumps(
            {
                "output_dir": str(Path(args.output_dir)),
                "files": [str(path) for path in written],
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
