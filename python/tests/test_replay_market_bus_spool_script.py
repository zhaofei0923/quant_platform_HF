from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _sample_lines() -> str:
    return "\n".join(
        (
            '{"instrument_id":"SHFE.ag2406","exchange_ts_ns":1,"recv_ts_ns":2}',
            '{"instrument_id":"SHFE.ag2406","exchange_ts_ns":3,"recv_ts_ns":4}',
        )
    )


def test_replay_market_bus_spool_dry_run(tmp_path: Path) -> None:
    spool_file = tmp_path / "market_snapshots.jsonl"
    spool_file.write_text(_sample_lines() + "\n", encoding="utf-8")
    report_file = tmp_path / "replay_report.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/ops/replay_market_bus_spool.py",
            "--spool-file",
            str(spool_file),
            "--output-json",
            str(report_file),
            "--dry-run",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["success"] is True
    assert payload["line_count_replayable"] == 2
    assert payload["line_count_replayed"] == 2


def test_replay_market_bus_spool_execute_updates_offset_and_truncates(tmp_path: Path) -> None:
    spool_file = tmp_path / "market_snapshots.jsonl"
    spool_file.write_text(_sample_lines() + "\n", encoding="utf-8")
    offset_file = tmp_path / "market_snapshots.offset"
    report_file = tmp_path / "replay_report.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/ops/replay_market_bus_spool.py",
            "--spool-file",
            str(spool_file),
            "--offset-file",
            str(offset_file),
            "--output-json",
            str(report_file),
            "--producer-command-template",
            "cat >/dev/null",
            "--truncate-on-success",
            "--execute",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["success"] is True
    assert payload["line_count_replayed"] == 2
    assert payload["next_offset"] == 0
    assert spool_file.read_text(encoding="utf-8") == ""
    assert offset_file.read_text(encoding="utf-8").strip() == "0"
