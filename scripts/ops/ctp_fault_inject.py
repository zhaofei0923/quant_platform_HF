#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path

try:
    from quant_hft.ops.fault_injection import (
        append_fault_event,
        build_disconnect_plan,
        build_disconnect_reset_plan,
        build_fault_event,
        build_netem_plan,
        build_netem_reset_plan,
        parse_ports,
        resolve_command_binary,
    )
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.ops.fault_injection import (  # type: ignore[no-redef]
        append_fault_event,
        build_disconnect_plan,
        build_disconnect_reset_plan,
        build_fault_event,
        build_netem_plan,
        build_netem_reset_plan,
        parse_ports,
        resolve_command_binary,
    )


SCENARIOS = ("disconnect", "latency", "loss", "jitter", "combined")


def _resolve_firewall_command(args: argparse.Namespace) -> str:
    candidates: list[str] = []
    if args.firewall_cmd:
        candidates.append(args.firewall_cmd)
    candidates.extend(["iptables", "iptables-nft"])
    return resolve_command_binary(candidates)


def _resolve_tc_command(args: argparse.Namespace) -> str:
    candidates: list[str] = []
    if args.tc_cmd:
        candidates.append(args.tc_cmd)
    candidates.append("tc")
    return resolve_command_binary(candidates)


def build_apply_commands(args: argparse.Namespace) -> list[str]:
    if args.scenario == "disconnect":
        firewall_cmd = _resolve_firewall_command(args)
        return build_disconnect_plan(
            args.target_ip,
            parse_ports(args.ports),
            firewall_cmd=firewall_cmd,
            disconnect_mode=args.disconnect_mode,
        )
    tc_cmd = _resolve_tc_command(args)
    if args.scenario == "latency":
        return build_netem_plan(
            args.iface, delay_ms=args.delay_ms, jitter_ms=0, loss_percent=0.0, tc_cmd=tc_cmd
        )
    if args.scenario == "loss":
        return build_netem_plan(
            args.iface, delay_ms=0, jitter_ms=0, loss_percent=args.loss_percent, tc_cmd=tc_cmd
        )
    if args.scenario == "jitter":
        return build_netem_plan(
            args.iface,
            delay_ms=args.delay_ms,
            jitter_ms=args.jitter_ms,
            loss_percent=0.0,
            tc_cmd=tc_cmd,
        )
    if args.scenario == "combined":
        return build_netem_plan(
            args.iface,
            delay_ms=args.delay_ms,
            jitter_ms=args.jitter_ms,
            loss_percent=args.loss_percent,
            tc_cmd=tc_cmd,
        )
    raise ValueError(f"unsupported scenario: {args.scenario}")


def build_clear_commands(args: argparse.Namespace) -> list[str]:
    if args.scenario == "disconnect":
        firewall_cmd = _resolve_firewall_command(args)
        return build_disconnect_reset_plan(
            args.target_ip,
            parse_ports(args.ports),
            firewall_cmd=firewall_cmd,
            disconnect_mode=args.disconnect_mode,
        )
    tc_cmd = _resolve_tc_command(args)
    return build_netem_reset_plan(args.iface, tc_cmd=tc_cmd)


def run_commands(commands: Sequence[str], *, dry_run: bool) -> None:
    for cmd in commands:
        print(cmd)
        if dry_run:
            continue
        subprocess.run(cmd, shell=True, check=True)


def _event_parameters(args: argparse.Namespace) -> dict[str, str]:
    params = {
        "duration_sec": str(args.duration_sec),
        "target_ip": str(args.target_ip),
        "ports": str(args.ports),
        "disconnect_mode": str(args.disconnect_mode),
        "iface": str(args.iface),
        "delay_ms": str(args.delay_ms),
        "jitter_ms": str(args.jitter_ms),
        "loss_percent": str(args.loss_percent),
    }
    return params


def record_event(
    args: argparse.Namespace,
    *,
    phase: str,
    execute: bool,
) -> None:
    if not args.event_log_file:
        return
    event = build_fault_event(
        mode=args.mode,
        scenario=args.scenario,
        phase=phase,
        execute=execute,
        ts_ns=time.time_ns(),
        parameters=_event_parameters(args),
    )
    append_fault_event(args.event_log_file, event)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SimNow reconnect fault-injection helper (iptables/tc based)."
    )
    parser.add_argument("mode", choices=("plan", "run", "clear"))
    parser.add_argument("--scenario", choices=SCENARIOS, default="disconnect")
    parser.add_argument(
        "--disconnect-mode",
        choices=("drop", "reset"),
        default="drop",
        help="disconnect action for iptables scenario: drop packets or reject with tcp-reset",
    )
    parser.add_argument("--iface", default="eth0", help="network interface for tc netem")
    parser.add_argument(
        "--firewall-cmd",
        default="",
        help="optional firewall command override for disconnect scenario",
    )
    parser.add_argument(
        "--tc-cmd",
        default="",
        help="optional tc command override for netem scenarios",
    )
    parser.add_argument("--target-ip", default="182.254.243.31", help="SimNow endpoint IP")
    parser.add_argument(
        "--ports",
        default="40001,40011,30001,30011,30002,30012,30003,30013",
        help="comma-separated TCP ports used by SimNow",
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=250,
        help="delay for latency/jitter/combined",
    )
    parser.add_argument(
        "--jitter-ms",
        type=int,
        default=30,
        help="jitter for jitter/combined",
    )
    parser.add_argument(
        "--loss-percent",
        type=float,
        default=2.5,
        help="packet loss for loss/combined",
    )
    parser.add_argument("--duration-sec", type=int, default=20, help="hold time in run mode")
    parser.add_argument(
        "--event-log-file",
        default="",
        help="optional JSONL file to record apply/clear event timestamps",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="actually run privileged commands; without this, only print command plan",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    dry_run = not args.execute

    try:
        if args.mode == "plan":
            print("# apply")
            run_commands(build_apply_commands(args), dry_run=True)
            print("# clear")
            run_commands(build_clear_commands(args), dry_run=True)
            return 0

        if args.mode == "clear":
            print("# clear")
            run_commands(build_clear_commands(args), dry_run=dry_run)
            record_event(args, phase="clear", execute=not dry_run)
            return 0

        print("# apply")
        run_commands(build_apply_commands(args), dry_run=dry_run)
        record_event(args, phase="apply", execute=not dry_run)
        if dry_run:
            print(f"# dry-run: would keep fault for {args.duration_sec}s before clear")
            print("# clear")
            run_commands(build_clear_commands(args), dry_run=True)
            record_event(args, phase="clear", execute=False)
            return 0

        interrupted = False
        try:
            print(f"# hold {args.duration_sec}s")
            time.sleep(max(1, args.duration_sec))
        except KeyboardInterrupt:
            interrupted = True
        finally:
            # Best-effort cleanup so Ctrl-C doesn't leave tc/iptables state behind.
            print("# clear")
            run_commands(build_clear_commands(args), dry_run=False)
            record_event(args, phase="clear", execute=True)

        if interrupted:
            return 130
        return 0
    except (ValueError, subprocess.CalledProcessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
