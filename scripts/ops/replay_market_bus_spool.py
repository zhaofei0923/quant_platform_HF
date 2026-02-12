#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay locally spooled market bus payloads back to Kafka"
    )
    parser.add_argument(
        "--spool-file",
        default="runtime/market_bus_spool/market_snapshots.jsonl",
        help="Path to market bus spool JSONL file",
    )
    parser.add_argument(
        "--offset-file",
        default="",
        help="Path to checkpoint offset file (default: <spool-file>.offset)",
    )
    parser.add_argument("--brokers", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="quant_hft.market.snapshots.v1")
    parser.add_argument(
        "--producer-command-template",
        default="kcat -P -b {brokers} -t {topic}",
        help="Shell command template used for publish, supports {brokers} and {topic}",
    )
    parser.add_argument(
        "--output-json",
        default="docs/results/market_bus_spool_replay_result.json",
        help="JSON report output path",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--truncate-on-success",
        action="store_true",
        help="Clear spool file after successful replay and reset offset",
    )
    args = parser.parse_args()
    if args.execute and args.dry_run:
        parser.error("use either --dry-run or --execute")
    if not args.execute and not args.dry_run:
        args.dry_run = True
    return args


def _render_command(template: str, brokers: str, topic: str) -> str:
    return template.replace("{brokers}", brokers).replace("{topic}", topic)


def _read_offset(path: Path) -> int:
    if not path.exists():
        return 0
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return 0
    try:
        return max(0, int(text))
    except ValueError:
        return 0


def _write_offset(path: Path, value: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{max(0, value)}\n", encoding="utf-8")


def _write_report(path: Path, report: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(path))


def main() -> int:
    args = _parse_args()
    spool_file = Path(args.spool_file)
    offset_file = Path(args.offset_file) if args.offset_file else Path(f"{args.spool_file}.offset")
    output_json = Path(args.output_json)

    if not spool_file.exists():
        report = {
            "generated_ts_ns": time.time_ns(),
            "success": False,
            "error": f"spool file not found: {spool_file}",
            "spool_file": str(spool_file),
            "offset_file": str(offset_file),
            "mode": "execute" if args.execute else "dry-run",
        }
        _write_report(output_json, report)
        return 2

    lines = spool_file.read_text(encoding="utf-8").splitlines()
    start_offset = _read_offset(offset_file)
    if start_offset > len(lines):
        start_offset = len(lines)
    replay_lines = [line for line in lines[start_offset:] if line.strip()]

    replayed = 0
    command = _render_command(args.producer_command_template, args.brokers, args.topic)
    error = ""
    if args.execute and replay_lines:
        process = subprocess.Popen(
            command,
            shell=True,
            stdin=subprocess.PIPE,
            text=True,
        )
        assert process.stdin is not None
        try:
            for line in replay_lines:
                # Keep payload format strict JSONEachRow-compatible.
                json.loads(line)
                process.stdin.write(line + "\n")
                replayed += 1
            process.stdin.close()
            rc = process.wait()
            if rc != 0:
                error = f"producer command exited with code {rc}"
        except Exception as exc:  # pragma: no cover - defensive path
            error = str(exc)
            process.kill()
            process.wait()

    if args.dry_run:
        replayed = len(replay_lines)

    success = error == ""
    next_offset = start_offset + replayed if success else start_offset
    if success and args.execute:
        if args.truncate_on_success:
            spool_file.write_text("", encoding="utf-8")
            next_offset = 0
        _write_offset(offset_file, next_offset)

    report = {
        "generated_ts_ns": time.time_ns(),
        "success": success,
        "mode": "execute" if args.execute else "dry-run",
        "spool_file": str(spool_file),
        "offset_file": str(offset_file),
        "command": command,
        "line_count_total": len(lines),
        "line_count_replayable": len(replay_lines),
        "line_count_replayed": replayed,
        "start_offset": start_offset,
        "next_offset": next_offset,
        "truncate_on_success": args.truncate_on_success,
        "error": error,
    }
    _write_report(output_json, report)
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
