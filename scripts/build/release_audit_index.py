#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

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
        description="Generate one-line release audit index JSONL from summary JSON"
    )
    parser.add_argument("--summary-json", required=True, help="Path to release summary JSON")
    parser.add_argument(
        "--output-jsonl",
        required=True,
        help="Path to output JSONL file with one index line",
    )
    return parser.parse_args()


def _load_summary(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("summary payload must be a JSON object")
    return payload


def _require(payload: dict[str, object], key: str) -> object:
    if key not in payload:
        raise ValueError(f"missing required key: {key}")
    value = payload[key]
    if value is None:
        raise ValueError(f"required key is null: {key}")
    if isinstance(value, str) and not value.strip():
        raise ValueError(f"required key is empty: {key}")
    return value


def _build_index_record(payload: dict[str, object]) -> dict[str, object]:
    for key in _REQUIRED_KEYS:
        _require(payload, key)
    return {
        "release_version": str(payload["release_version"]),
        "git_commit": str(payload["git_commit"]),
        "sha256": str(payload["sha256"]),
        "component_count": int(payload["component_count"]),
        "bundle_name": str(payload["bundle_name"]),
        "build_ts_utc": str(payload["build_ts_utc"]),
    }


def main() -> int:
    args = _parse_args()
    summary_path = Path(args.summary_json)
    output_path = Path(args.output_jsonl)
    if not summary_path.exists():
        print(f"error: summary json not found: {summary_path}")
        return 2
    try:
        payload = _load_summary(summary_path)
        record = _build_index_record(payload)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"error: release audit index generation failed: {exc}")
        return 2

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(record, ensure_ascii=True, sort_keys=True) + "\n")
    print(f"index generated: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
