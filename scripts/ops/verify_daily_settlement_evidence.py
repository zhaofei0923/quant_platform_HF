#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify daily settlement evidence JSON contract.",
    )
    parser.add_argument("--evidence-file", required=True)
    parser.add_argument("--allow-blocked", action="store_true")
    parser.add_argument("--max-duration-seconds", type=float, default=900.0)
    return parser


def _require_keys(payload: dict[str, Any], keys: tuple[str, ...]) -> None:
    missing = [key for key in keys if key not in payload]
    if missing:
        raise ValueError(f"missing required evidence keys: {missing}")


def main() -> int:
    args = _build_parser().parse_args()
    evidence_path = Path(args.evidence_file)
    if not evidence_path.exists():
        print(f"error: evidence file not found: {evidence_path}")
        return 2

    try:
        payload = json.loads(evidence_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("evidence payload must be a JSON object")
        _require_keys(
            payload,
            (
                "trading_day",
                "dry_run",
                "success",
                "blocked",
                "status",
                "duration_seconds",
                "started_utc",
                "completed_utc",
            ),
        )
        duration_seconds = float(payload["duration_seconds"])
        if duration_seconds < 0:
            raise ValueError("duration_seconds must be >= 0")
        if duration_seconds > args.max_duration_seconds:
            raise ValueError(
                f"duration_seconds exceeds limit: {duration_seconds} > {args.max_duration_seconds}"
            )

        blocked = bool(payload["blocked"])
        success = bool(payload["success"])
        status = str(payload["status"])
        dry_run = bool(payload["dry_run"])

        if dry_run:
            if status != "DRY_RUN":
                raise ValueError("dry-run evidence must have status=DRY_RUN")
        elif success:
            if status != "COMPLETED":
                raise ValueError("successful settlement evidence must have status=COMPLETED")
            if blocked:
                raise ValueError("blocked must be false when success=true")
        else:
            if blocked and not args.allow_blocked:
                raise ValueError("blocked settlement is not allowed without --allow-blocked")
            if not blocked and status not in {"FAILED", "BLOCKED"}:
                raise ValueError("unsuccessful settlement must have FAILED or BLOCKED status")
    except Exception as exc:  # pragma: no cover
        print(f"error: daily settlement evidence verification failed: {exc}")
        return 2

    print(f"verification passed: {evidence_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
