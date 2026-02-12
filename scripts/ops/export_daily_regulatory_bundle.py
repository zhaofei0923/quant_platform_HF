#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from quant_hft.data_pipeline.adapters import MinioArchiveStore
except ModuleNotFoundError:  # pragma: no cover
    repo_python = Path(__file__).resolve().parents[2] / "python"
    if str(repo_python) not in sys.path:
        sys.path.insert(0, str(repo_python))
    from quant_hft.data_pipeline.adapters import MinioArchiveStore  # type: ignore[no-redef]


_EXPORT_FILES = (
    "order_events.csv",
    "trade_events.csv",
    "account_snapshots.csv",
    "position_snapshots.csv",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export daily trading_core compliance bundle (CSV + SHA256 manifest)."
    )
    parser.add_argument("--trade-date", required=True, help="YYYYMMDD")
    parser.add_argument("--output-dir", default="runtime/compliance_exports")
    parser.add_argument("--schema", default="trading_core")
    parser.add_argument(
        "--source-dir",
        default="",
        help="Use pre-exported CSV directory for local/testing",
    )
    parser.add_argument("--timescale-dsn", default="")
    parser.add_argument("--timescale-host", default="127.0.0.1")
    parser.add_argument("--timescale-port", type=int, default=5432)
    parser.add_argument("--timescale-db", default="quant_hft")
    parser.add_argument("--timescale-user", default="quant_hft")
    parser.add_argument("--timescale-password", default="")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--archive-endpoint", default="localhost:9000")
    parser.add_argument("--archive-access-key", default="minioadmin")
    parser.add_argument("--archive-secret-key", default="minioadmin")
    parser.add_argument("--archive-use-ssl", action="store_true")
    parser.add_argument("--archive-local-dir", default="")
    parser.add_argument("--skip-archive", action="store_true")
    return parser.parse_args()


def _validate_trade_date(raw: str) -> str:
    normalized = raw.strip()
    if len(normalized) != 8 or not normalized.isdigit():
        raise ValueError(f"trade date must be YYYYMMDD, got: {raw}")
    return normalized


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.reader(fp)
        rows = list(reader)
    if not rows:
        return 0
    return max(0, len(rows) - 1)


def _psql_copy_csv(args: argparse.Namespace, query: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if args.timescale_dsn:
        base = ["psql", args.timescale_dsn]
    else:
        base = [
            "psql",
            "-h",
            args.timescale_host,
            "-p",
            str(args.timescale_port),
            "-U",
            args.timescale_user,
            "-d",
            args.timescale_db,
        ]
    command = [
        *base,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        f"\\COPY ({query}) TO '{output_path.resolve()}' WITH CSV HEADER",
    ]
    env = dict(os.environ)
    if args.timescale_password:
        env["PGPASSWORD"] = args.timescale_password
    completed = subprocess.run(command, check=False, capture_output=True, text=True, env=env)
    if completed.returncode != 0:
        raise RuntimeError(completed.stdout + completed.stderr)


def _export_from_source_dir(source_dir: Path, destination_dir: Path) -> list[Path]:
    exported: list[Path] = []
    for filename in _EXPORT_FILES:
        source = source_dir / filename
        if not source.exists():
            raise FileNotFoundError(f"missing source CSV: {source}")
        target = destination_dir / filename
        shutil.copyfile(source, target)
        exported.append(target)
    return exported


def _export_from_postgres(
    args: argparse.Namespace,
    destination_dir: Path,
    trade_date: str,
) -> list[Path]:
    trade_date_iso = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
    schema = args.schema
    queries = {
        "order_events.csv": (
            f"SELECT * FROM {schema}.order_events WHERE trade_date = DATE '{trade_date_iso}' "
            "ORDER BY ts_ns ASC"
        ),
        "trade_events.csv": (
            f"SELECT * FROM {schema}.trade_events WHERE trade_date = DATE '{trade_date_iso}' "
            "ORDER BY ts_ns ASC"
        ),
        "account_snapshots.csv": (
            f"SELECT * FROM {schema}.account_snapshots "
            "WHERE to_char("
            "to_timestamp(ts_ns / 1000000000.0) AT TIME ZONE 'UTC', "
            f"'YYYYMMDD') = '{trade_date}' "
            "ORDER BY ts_ns ASC"
        ),
        "position_snapshots.csv": (
            f"SELECT * FROM {schema}.position_snapshots "
            "WHERE to_char("
            "to_timestamp(ts_ns / 1000000000.0) AT TIME ZONE 'UTC', "
            f"'YYYYMMDD') = '{trade_date}' "
            "ORDER BY ts_ns ASC"
        ),
    }
    exported: list[Path] = []
    for filename, query in queries.items():
        target = destination_dir / filename
        _psql_copy_csv(args, query, target)
        exported.append(target)
    return exported


def _build_manifest(exported_files: list[Path], trade_date: str, bucket: str) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for path in exported_files:
        files.append(
            {
                "file": path.name,
                "rows": _row_count(path),
                "sha256": _sha256(path),
                "size_bytes": path.stat().st_size,
            }
        )
    return {
        "trade_date": trade_date,
        "exported_at_utc": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bucket": bucket,
        "archive_prefix": f"compliance/{trade_date}",
        "files": files,
    }


def _archive_bundle(args: argparse.Namespace, bundle_dir: Path, trade_date: str) -> int:
    local_fallback = Path(args.archive_local_dir) if args.archive_local_dir else None
    archive = MinioArchiveStore(
        endpoint=args.archive_endpoint,
        access_key=args.archive_access_key,
        secret_key=args.archive_secret_key,
        bucket=args.bucket,
        use_ssl=args.archive_use_ssl,
        local_fallback_dir=local_fallback,
    )
    uploaded = 0
    for filename in [*_EXPORT_FILES, "manifest.json"]:
        path = bundle_dir / filename
        object_name = f"compliance/{trade_date}/{filename}"
        archive.put_file(object_name, path)
        uploaded += 1
    return uploaded


def main() -> int:
    args = _parse_args()
    try:
        trade_date = _validate_trade_date(args.trade_date)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    bundle_dir = Path(args.output_dir) / trade_date
    bundle_dir.mkdir(parents=True, exist_ok=True)

    try:
        if args.source_dir:
            exported_files = _export_from_source_dir(Path(args.source_dir), bundle_dir)
        else:
            exported_files = _export_from_postgres(args, bundle_dir, trade_date)
    except Exception as exc:  # pragma: no cover
        print(f"error: export failed: {exc}", file=sys.stderr)
        return 2

    manifest = _build_manifest(exported_files, trade_date, args.bucket)
    manifest_path = bundle_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )

    archived_files = 0
    if not args.skip_archive:
        try:
            archived_files = _archive_bundle(args, bundle_dir, trade_date)
        except Exception as exc:  # pragma: no cover
            print(f"error: archive failed: {exc}", file=sys.stderr)
            return 2

    result = {
        "bundle_dir": str(bundle_dir),
        "manifest_path": str(manifest_path),
        "trade_date": trade_date,
        "files": len(exported_files),
        "archived_files": archived_files,
        "archive_uri_prefix": f"minio://{args.bucket}/compliance/{trade_date}",
    }
    print(json.dumps(result, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
