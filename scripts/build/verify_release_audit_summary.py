#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

_HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify structured release audit summary JSON contract"
    )
    parser.add_argument("--summary-json", required=True, help="Path to summary JSON file")
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


def _validate(payload: dict[str, object], *, expect_version: str, expect_git_commit: str) -> None:
    required_keys = (
        "bundle_name",
        "bundle_size_bytes",
        "bundle_root_directory",
        "release_version",
        "build_ts_utc",
        "git_commit",
        "manifest_bundle_name",
        "sha256",
        "component_count",
        "components",
    )
    for key in required_keys:
        if key not in payload:
            raise ValueError(f"missing required key: {key}")

    bundle_name = _must_non_empty_str(payload, "bundle_name")
    bundle_root_directory = _must_non_empty_str(payload, "bundle_root_directory")
    release_version = _must_non_empty_str(payload, "release_version")
    _must_non_empty_str(payload, "build_ts_utc")
    git_commit = _must_non_empty_str(payload, "git_commit")
    manifest_bundle_name = _must_non_empty_str(payload, "manifest_bundle_name")
    sha256 = _must_non_empty_str(payload, "sha256").lower()
    component_count = _must_positive_int(payload, "component_count")
    bundle_size_bytes = _must_positive_int(payload, "bundle_size_bytes")
    _ = bundle_size_bytes

    components = payload.get("components")
    if not isinstance(components, list):
        raise ValueError("components must be a list")
    if len(components) != component_count:
        raise ValueError(
            f"component_count mismatch: declared={component_count} actual={len(components)}"
        )
    for idx, component in enumerate(components):
        if not isinstance(component, str) or not component.strip():
            raise ValueError(f"components[{idx}] must be a non-empty string")

    if not bundle_name.endswith(".tar.gz"):
        raise ValueError("bundle_name must end with .tar.gz")
    if manifest_bundle_name + ".tar.gz" != bundle_name:
        raise ValueError("manifest_bundle_name and bundle_name are inconsistent")
    if bundle_root_directory != manifest_bundle_name:
        raise ValueError("bundle_root_directory must match manifest_bundle_name")
    if release_version not in bundle_name:
        raise ValueError("release_version must be embedded in bundle_name")

    if not _HEX_64_RE.match(sha256):
        raise ValueError("sha256 must be 64 lowercase hex characters")

    if expect_version and release_version != expect_version:
        raise ValueError(
            f"release_version mismatch: expected={expect_version} actual={release_version}"
        )
    if expect_git_commit and git_commit != expect_git_commit:
        raise ValueError(
            f"git_commit mismatch: expected={expect_git_commit} actual={git_commit}"
        )


def main() -> int:
    args = _parse_args()
    summary_json_path = Path(args.summary_json)

    if not summary_json_path.exists():
        print(f"error: summary json not found: {summary_json_path}")
        return 2

    try:
        payload = json.loads(summary_json_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("summary json must be an object")
        _validate(
            payload,
            expect_version=args.expect_version.strip(),
            expect_git_commit=args.expect_git_commit.strip(),
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"error: release audit summary verification failed: {exc}")
        return 2

    print(f"verification passed: {summary_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
