#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

try:
    from quant_hft.data_pipeline.adapters import MinioArchiveStore
except ModuleNotFoundError:  # pragma: no cover
    repo_python = Path(__file__).resolve().parents[2] / "python"
    if str(repo_python) not in sys.path:
        sys.path.insert(0, str(repo_python))
    from quant_hft.data_pipeline.adapters import MinioArchiveStore  # type: ignore[no-redef]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify daily compliance CSV bundle and SHA256 manifest."
    )
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--verify-archive", action="store_true")
    parser.add_argument("--bucket", default="")
    parser.add_argument("--archive-endpoint", default="localhost:9000")
    parser.add_argument("--archive-access-key", default="minioadmin")
    parser.add_argument("--archive-secret-key", default="minioadmin")
    parser.add_argument("--archive-use-ssl", action="store_true")
    parser.add_argument("--archive-local-dir", default="")
    return parser.parse_args()


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


def _verify_archive(manifest: dict[str, Any], args: argparse.Namespace) -> tuple[bool, str]:
    bucket = args.bucket or str(manifest.get("bucket", ""))
    if not bucket:
        return False, "bucket is required for archive verification"
    local_fallback = Path(args.archive_local_dir) if args.archive_local_dir else None
    archive = MinioArchiveStore(
        endpoint=args.archive_endpoint,
        access_key=args.archive_access_key,
        secret_key=args.archive_secret_key,
        bucket=bucket,
        use_ssl=args.archive_use_ssl,
        local_fallback_dir=local_fallback,
    )
    prefix = str(manifest.get("archive_prefix", "")).strip("/")
    objects = set(archive.list_objects(prefix=prefix))
    required = {f"{prefix}/{item['file']}" for item in manifest.get("files", [])}
    required.add(f"{prefix}/manifest.json")
    missing = sorted(required - objects)
    if missing:
        return False, f"missing archived objects: {missing}"
    return True, ""


def main() -> int:
    args = _parse_args()
    bundle_dir = Path(args.bundle_dir)
    manifest_path = bundle_dir / "manifest.json"
    if not manifest_path.exists():
        print(f"error: missing manifest: {manifest_path}", file=sys.stderr)
        return 2

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        print("error: manifest payload must be an object", file=sys.stderr)
        return 2

    files = payload.get("files", [])
    if not isinstance(files, list):
        print("error: manifest files must be a list", file=sys.stderr)
        return 2

    mismatches: list[str] = []
    for item in files:
        if not isinstance(item, dict):
            mismatches.append("manifest file entry is not an object")
            continue
        filename = str(item.get("file", ""))
        expected_sha = str(item.get("sha256", ""))
        expected_rows = int(item.get("rows", 0))
        file_path = bundle_dir / filename
        if not file_path.exists():
            mismatches.append(f"missing file: {filename}")
            continue
        actual_sha = _sha256(file_path)
        actual_rows = _row_count(file_path)
        if actual_sha != expected_sha:
            mismatches.append(
                f"sha256 mismatch for {filename}: expected={expected_sha} actual={actual_sha}"
            )
        if actual_rows != expected_rows:
            mismatches.append(
                f"row count mismatch for {filename}: expected={expected_rows} actual={actual_rows}"
            )

    archive_ok = True
    archive_error = ""
    if args.verify_archive:
        archive_ok, archive_error = _verify_archive(payload, args)
        if not archive_ok:
            mismatches.append(archive_error)

    result = {
        "bundle_dir": str(bundle_dir),
        "manifest_path": str(manifest_path),
        "verified_files": len(files),
        "archive_verified": args.verify_archive,
        "consistent": not mismatches,
        "mismatches": mismatches,
    }
    print(json.dumps(result, ensure_ascii=True))
    return 0 if not mismatches and archive_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
