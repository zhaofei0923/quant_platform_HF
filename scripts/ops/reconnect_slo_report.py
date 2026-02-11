#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from quant_hft.ops.monitoring import (
        build_ops_health_report,
        ops_health_report_to_dict,
        render_ops_health_markdown,
    )
    from quant_hft.ops.reconnect_slo import (
        evaluate_reconnect_slo,
        load_fault_events,
        load_probe_health_events,
    )
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.ops.monitoring import (  # type: ignore[no-redef]
        build_ops_health_report,
        ops_health_report_to_dict,
        render_ops_health_markdown,
    )
    from quant_hft.ops.reconnect_slo import (  # type: ignore[no-redef]
        evaluate_reconnect_slo,
        load_fault_events,
        load_probe_health_events,
    )


def _ts_iso(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000.0, tz=timezone.utc).isoformat()


def _checkmark(value: bool) -> str:
    return "[x]" if value else "[ ]"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate reconnect SLO evidence markdown report")
    parser.add_argument("--fault-events-file", required=True)
    parser.add_argument("--probe-log-file", required=True)
    parser.add_argument("--output-file", default="")
    parser.add_argument("--target-p99-sec", type=float, default=10.0)
    parser.add_argument("--date", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--operator", default="")
    parser.add_argument("--host", default="")
    parser.add_argument("--build", default="")
    parser.add_argument("--config-profile", default="")
    parser.add_argument("--interface", default="")
    parser.add_argument("--health-json-file", default="")
    parser.add_argument("--health-markdown-file", default="")
    parser.add_argument("--strategy-bridge-target-ms", type=float, default=1500.0)
    parser.add_argument(
        "--storage-redis-health",
        choices=("unknown", "healthy", "unhealthy"),
        default="unknown",
    )
    parser.add_argument(
        "--storage-timescale-health",
        choices=("unknown", "healthy", "unhealthy"),
        default="unknown",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    fault_events = load_fault_events(Path(args.fault_events_file))
    health_events = load_probe_health_events(Path(args.probe_log_file))
    report = evaluate_reconnect_slo(
        fault_events=fault_events,
        health_events=health_events,
        target_p99_seconds=args.target_p99_sec,
    )

    rows: list[str] = []
    for sample in report.samples:
        recovery = f"{sample.recovery_seconds:.3f}" if sample.recovery_seconds is not None else ""
        notes: list[str] = []
        params_text = ",".join(f"{key}={value}" for key, value in sorted(sample.parameters.items()))
        if sample.parameters:
            notes.append(params_text)
        if not sample.unhealthy_observed:
            notes.append("unhealthy_not_observed")
        if not sample.recovered:
            notes.append("not_recovered")
        row = (
            "| {scenario} | {params} | {start} | {end} | " "{recovered} | {recovery} | {notes} |"
        ).format(
            scenario=sample.scenario,
            params=params_text,
            start=_ts_iso(sample.apply_ts_ns),
            end=_ts_iso(sample.clear_ts_ns),
            recovered="yes" if sample.recovered else "no",
            recovery=recovery,
            notes=";".join(notes),
        )
        rows.append(row)
    if not rows:
        rows.append("| n/a | n/a | n/a | n/a | no |  | no valid apply/clear window |")

    unhealthy_observed = bool(report.samples) and all(
        sample.unhealthy_observed for sample in report.samples
    )
    auto_reconnect_observed = bool(report.samples) and all(
        sample.recovered for sample in report.samples
    )
    p99_ok = report.p99_recovery_seconds is not None and report.meets_target
    no_crash_placeholder = False
    no_stuck_placeholder = auto_reconnect_observed

    p99_line = (
        f"- Recovery P99 (s): {report.p99_recovery_seconds:.3f}"
        if report.p99_recovery_seconds is not None
        else "- Recovery P99 (s): n/a"
    )
    markdown = (
        "# Reconnect Fault Injection Result\n\n"
        "## Metadata\n"
        f"- Date: {args.date}\n"
        f"- Operator: {args.operator}\n"
        f"- Host: {args.host}\n"
        f"- Build: {args.build}\n"
        f"- Config profile: {args.config_profile}\n"
        f"- Interface: {args.interface}\n\n"
        "## Summary\n"
        f"- Samples: {len(report.samples)}\n"
        f"{p99_line}\n"
    )

    markdown += (
        f"- Target P99 (s): {report.target_p99_seconds:.3f}\n"
        f"- Meets target: {'yes' if report.meets_target else 'no'}\n\n"
        "## Scenario Matrix\n"
        "| Scenario | Parameters | Start Time (UTC) | End Time (UTC) | "
        "Recovered | Recovery Seconds | Notes |\n"
        "|---|---|---|---|---|---:|---|\n" + "\n".join(rows) + "\n\n"
        "## Acceptance\n"
        f"- {_checkmark(unhealthy_observed)} unhealthy state observed during fault\n"
        f"- {_checkmark(auto_reconnect_observed)} auto-reconnect observed after clear\n"
        f"- {_checkmark(p99_ok)} P99 recovery `< {report.target_p99_seconds:g}s`\n"
        f"- {_checkmark(no_crash_placeholder)} no crash (manual confirmation required)\n"
        f"- {_checkmark(no_stuck_placeholder)} no stuck state after repeated runs\n\n"
        "## Logs and Evidence\n"
        f"- Probe log file: {args.probe_log_file}\n"
        f"- Fault events file: {args.fault_events_file}\n"
    )

    if args.output_file:
        output = Path(args.output_file)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(markdown + "\n", encoding="utf-8")
        print(str(output))
    else:
        print(markdown)

    core_process_alive = bool(health_events)
    strategy_bridge_latency_ms = (
        None if report.p99_recovery_seconds is None else report.p99_recovery_seconds * 1000.0
    )
    ops_health_report = build_ops_health_report(
        strategy_bridge_latency_ms=strategy_bridge_latency_ms,
        strategy_bridge_target_ms=args.strategy_bridge_target_ms,
        core_process_alive=core_process_alive,
        redis_health=args.storage_redis_health,
        timescale_health=args.storage_timescale_health,
        metadata={
            "operator": args.operator,
            "host": args.host,
            "build": args.build,
            "config_profile": args.config_profile,
            "interface": args.interface,
        },
    )

    if args.health_json_file:
        health_json = Path(args.health_json_file)
        health_json.parent.mkdir(parents=True, exist_ok=True)
        health_json.write_text(
            json.dumps(ops_health_report_to_dict(ops_health_report), ensure_ascii=True, indent=2)
            + "\n",
            encoding="utf-8",
        )
        print(str(health_json))

    if args.health_markdown_file:
        health_md = Path(args.health_markdown_file)
        health_md.parent.mkdir(parents=True, exist_ok=True)
        health_md.write_text(render_ops_health_markdown(ops_health_report), encoding="utf-8")
        print(str(health_md))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
