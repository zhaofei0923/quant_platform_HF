#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify archive manifest entries (file existence/size/checksum).",
    )
    parser.add_argument("--manifest-json", required=True)
    parser.add_argument("--report-json", default="")
    parser.add_argument("--check-checksum", action="store_true")
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _iter_manifest_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("manifest", [])
    if isinstance(rows, list):
        return [item for item in rows if isinstance(item, dict)]
    return []


def main() -> int:
    args = _parse_args()
    manifest_file = Path(args.manifest_json)
    if not manifest_file.exists():
        print(f"error: manifest file not found: {manifest_file}")
        return 2

    payload = json.loads(manifest_file.read_text(encoding="utf-8"))
    rows = _iter_manifest_rows(payload)
    failures: list[dict[str, Any]] = []
    checked = 0
    for row in rows:
        path = Path(str(row.get("file_path", "")))
        checked += 1
        if not path.exists():
            failures.append({"file_path": str(path), "reason": "missing_file"})
            continue
        expected_size = int(row.get("file_size", 0) or 0)
        if expected_size > 0 and path.stat().st_size != expected_size:
            failures.append(
                {
                    "file_path": str(path),
                    "reason": "size_mismatch",
                    "expected_size": expected_size,
                    "actual_size": path.stat().st_size,
                }
            )
            continue
        if args.check_checksum:
            expected_checksum = str(row.get("checksum", "")).strip()
            if expected_checksum:
                actual_checksum = _sha256(path)
                if actual_checksum != expected_checksum:
                    failures.append(
                        {
                            "file_path": str(path),
                            "reason": "checksum_mismatch",
                            "expected_checksum": expected_checksum,
                            "actual_checksum": actual_checksum,
                        }
                    )

    success = len(failures) == 0
    report = {
        "manifest_json": str(manifest_file),
        "checked_entries": checked,
        "failed_entries": len(failures),
        "success": success,
        "failures": failures,
    }
    rendered = json.dumps(report, ensure_ascii=True, indent=2)
    if args.report_json:
        report_file = Path(args.report_json)
        report_file.parent.mkdir(parents=True, exist_ok=True)
        report_file.write_text(rendered + "\n", encoding="utf-8")
        print(str(report_file))
    else:
        print(rendered)
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
