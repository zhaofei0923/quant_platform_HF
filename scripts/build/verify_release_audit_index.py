#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

_HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_KEYS = (
    "release_version",
    "git_commit",
    "sha256",
    "component_count",
    "bundle_name",
    "build_ts_utc",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify release audit index JSONL contract and summary consistency"
    )
    parser.add_argument("--summary-json", required=True, help="Path to release summary JSON")
    parser.add_argument("--index-jsonl", required=True, help="Path to release index JSONL")
    parser.add_argument(
        "--expect-version",
        default="",
        help="Expected release version (optional)",
    )
    parser.add_argument(
        "--expect-git-commit",
        default="",
        help="Expected short git commit hash (optional)",
    )
    return parser.parse_args()


def _load_object(path: Path, *, name: str) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{name} must be a JSON object")
    return payload


def _load_index_record(path: Path) -> dict[str, object]:
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(lines) != 1:
        raise ValueError(f"index jsonl must contain exactly 1 non-empty line, got {len(lines)}")
    payload = json.loads(lines[0])
    if not isinstance(payload, dict):
        raise ValueError("index record must be a JSON object")
    return payload


def _must_non_empty_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value.strip()


def _must_positive_int(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"{key} must be a positive integer")
    return value


def _validate_index_contract(index_record: dict[str, object]) -> None:
    for key in _REQUIRED_KEYS:
        if key not in index_record:
            raise ValueError(f"missing required key: {key}")

    sha256 = _must_non_empty_str(index_record, "sha256").lower()
    if not _HEX_64_RE.match(sha256):
        raise ValueError("sha256 must be 64 lowercase hex characters")

    _must_non_empty_str(index_record, "release_version")
    _must_non_empty_str(index_record, "git_commit")
    _must_non_empty_str(index_record, "bundle_name")
    _must_non_empty_str(index_record, "build_ts_utc")
    _must_positive_int(index_record, "component_count")


def _validate_consistency(
    *,
    summary: dict[str, object],
    index_record: dict[str, object],
    expect_version: str,
    expect_git_commit: str,
) -> None:
    for key in _REQUIRED_KEYS:
        summary_value = summary.get(key)
        index_value = index_record.get(key)
        if summary_value != index_value:
            raise ValueError(
                f"mismatch for {key}: summary={summary_value!r} index={index_value!r}"
            )

    release_version = str(index_record["release_version"])
    git_commit = str(index_record["git_commit"])

    if expect_version and release_version != expect_version:
        raise ValueError(
            f"mismatch for release_version: expected={expect_version} actual={release_version}"
        )
    if expect_git_commit and git_commit != expect_git_commit:
        raise ValueError(
            f"mismatch for git_commit: expected={expect_git_commit} actual={git_commit}"
        )


def main() -> int:
    args = _parse_args()
    summary_path = Path(args.summary_json)
    index_path = Path(args.index_jsonl)

    if not summary_path.exists():
        print(f"error: summary json not found: {summary_path}")
        return 2
    if not index_path.exists():
        print(f"error: index jsonl not found: {index_path}")
        return 2

    try:
        summary = _load_object(summary_path, name="summary payload")
        index_record = _load_index_record(index_path)
        _validate_index_contract(index_record)
        _validate_consistency(
            summary=summary,
            index_record=index_record,
            expect_version=args.expect_version.strip(),
            expect_git_commit=args.expect_git_commit.strip(),
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"error: release audit index verification failed: {exc}")
        return 2

    print(f"verification passed: {index_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
