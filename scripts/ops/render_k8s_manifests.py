#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from quant_hft.ops.k8s import K8sRenderConfig, write_k8s_bundle
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.ops.k8s import K8sRenderConfig, write_k8s_bundle  # type: ignore[no-redef]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render Kubernetes manifests for quant_hft non-hotpath components"
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-dir", default="deploy/k8s")
    parser.add_argument("--namespace", default="quant-hft")
    parser.add_argument("--image-repository", default="quant-hft")
    parser.add_argument("--image-tag", default="latest")
    parser.add_argument("--replicas", type=int, default=1)
    parser.add_argument("--runtime-pvc-size", default="20Gi")
    parser.add_argument("--container-workdir", default="/app")
    parser.add_argument("--python-bin", default=".venv/bin/python")
    parser.add_argument("--pipeline-entrypoint", default="scripts/data_pipeline/run_pipeline.py")
    parser.add_argument("--analytics-db", default="runtime/analytics.duckdb")
    parser.add_argument("--export-dir", default="runtime/exports")
    parser.add_argument("--archive-local-dir", default="runtime/archive")
    parser.add_argument("--archive-endpoint", default="minio:9000")
    parser.add_argument("--archive-bucket", default="quant-archive")
    parser.add_argument("--archive-prefix", default="etl")
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    config = K8sRenderConfig(
        repo_root=Path(args.repo_root),
        output_dir=Path(args.output_dir),
        namespace=args.namespace,
        image_repository=args.image_repository,
        image_tag=args.image_tag,
        replicas=max(1, args.replicas),
        runtime_pvc_size=args.runtime_pvc_size,
        container_workdir=args.container_workdir,
        python_bin=args.python_bin,
        pipeline_entrypoint=args.pipeline_entrypoint,
        analytics_db=args.analytics_db,
        export_dir=args.export_dir,
        archive_local_dir=args.archive_local_dir,
        archive_endpoint=args.archive_endpoint,
        archive_bucket=args.archive_bucket,
        archive_prefix=args.archive_prefix,
        interval_seconds=max(1.0, args.interval_seconds),
    )
    written = write_k8s_bundle(config)
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
