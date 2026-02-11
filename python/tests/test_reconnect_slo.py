from __future__ import annotations

import json
from pathlib import Path

from quant_hft.ops.reconnect_slo import (
    evaluate_reconnect_slo,
    load_fault_events,
    load_probe_health_events,
)


def _write_fault_events(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=True) + "\n")


def _write_probe_log(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_evaluate_reconnect_slo_computes_recovery_p99(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    probe_log_path = tmp_path / "probe.log"
    _write_fault_events(
        events_path,
        [
            {
                "ts_ns": 1_000_000_000,
                "mode": "run",
                "scenario": "disconnect",
                "phase": "apply",
                "execute": True,
            },
            {
                "ts_ns": 2_000_000_000,
                "mode": "run",
                "scenario": "disconnect",
                "phase": "clear",
                "execute": True,
            },
        ],
    )
    _write_probe_log(
        probe_log_path,
        [
            "[health] ts_ns=900000000 state=healthy",
            "[health] ts_ns=1200000000 state=unhealthy",
            "[health] ts_ns=2100000000 state=unhealthy",
            "[health] ts_ns=3000000000 state=healthy",
        ],
    )

    report = evaluate_reconnect_slo(
        fault_events=load_fault_events(events_path),
        health_events=load_probe_health_events(probe_log_path),
        target_p99_seconds=10.0,
    )

    assert len(report.samples) == 1
    sample = report.samples[0]
    assert sample.scenario == "disconnect"
    assert sample.recovered is True
    assert sample.unhealthy_observed is True
    assert sample.recovery_seconds == 1.0
    assert report.p99_recovery_seconds == 1.0
    assert report.meets_target is True


def test_evaluate_reconnect_slo_marks_unrecovered_sample(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    probe_log_path = tmp_path / "probe.log"
    _write_fault_events(
        events_path,
        [
            {
                "ts_ns": 5_000_000_000,
                "mode": "run",
                "scenario": "loss",
                "phase": "apply",
                "execute": True,
            },
            {
                "ts_ns": 6_000_000_000,
                "mode": "run",
                "scenario": "loss",
                "phase": "clear",
                "execute": True,
            },
        ],
    )
    _write_probe_log(
        probe_log_path,
        [
            "[health] ts_ns=5900000000 state=unhealthy",
            "[health] ts_ns=6100000000 state=unhealthy",
        ],
    )

    report = evaluate_reconnect_slo(
        fault_events=load_fault_events(events_path),
        health_events=load_probe_health_events(probe_log_path),
        target_p99_seconds=10.0,
    )

    assert len(report.samples) == 1
    sample = report.samples[0]
    assert sample.recovered is False
    assert sample.recovery_seconds is None
    assert report.p99_recovery_seconds is None
    assert report.meets_target is False
