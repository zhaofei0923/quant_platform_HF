#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Iterable
from pathlib import Path

try:
    from quant_hft.ops.ctp_preflight import (
        CtpPreflightConfig,
        CtpPreflightReport,
        run_ctp_preflight,
    )
    from quant_hft.ops.fault_injection import resolve_command_binary
    from quant_hft.ops.reconnect_evidence import (
        build_fault_inject_command,
        build_probe_command,
        has_reachable_group_hint,
        parse_fallback_config_paths,
        required_tools_for_scenarios,
        select_scenarios,
    )
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.ops.ctp_preflight import (  # type: ignore[no-redef]
        CtpPreflightConfig,
        CtpPreflightReport,
        run_ctp_preflight,
    )
    from quant_hft.ops.fault_injection import resolve_command_binary  # type: ignore[no-redef]
    from quant_hft.ops.reconnect_evidence import (  # type: ignore[no-redef]
        build_fault_inject_command,
        build_probe_command,
        has_reachable_group_hint,
        parse_fallback_config_paths,
        required_tools_for_scenarios,
        select_scenarios,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect SimNow reconnect evidence and generate markdown report"
    )
    parser.add_argument("--probe-bin", default="build-real/simnow_probe")
    parser.add_argument("--config", default="configs/sim/ctp.yaml")
    parser.add_argument("--probe-log", default="runtime/reconnect_probe.log")
    parser.add_argument("--event-log", default="runtime/fault_events.jsonl")
    parser.add_argument(
        "--report-file",
        default="docs/results/reconnect_fault_result.md",
    )
    parser.add_argument(
        "--health-json-file",
        default="docs/results/ops_health_report.json",
    )
    parser.add_argument(
        "--health-markdown-file",
        default="docs/results/ops_health_report.md",
    )
    parser.add_argument(
        "--fault-script",
        default="scripts/ops/ctp_fault_inject.py",
    )
    parser.add_argument(
        "--report-script",
        default="scripts/ops/reconnect_slo_report.py",
    )
    parser.add_argument(
        "--ctp-lib-dir",
        default="ctp_api/v6.7.11_20250617_api_traderapi_se_linux64",
    )
    parser.add_argument("--iface", default="eth0")
    parser.add_argument(
        "--disconnect-mode",
        choices=("drop", "reset"),
        default="reset",
        help=(
            "disconnect action for iptables scenario "
            "(recommended: reset for fast disconnect detection)"
        ),
    )
    parser.add_argument("--target-ip", default="182.254.243.31")
    parser.add_argument("--ports", default="40001,40011,30001,30011,30002,30012,30003,30013")
    parser.add_argument("--monitor-seconds", type=int, default=900)
    parser.add_argument("--health-interval-ms", type=int, default=1000)
    parser.add_argument("--warmup-seconds", type=int, default=8)
    parser.add_argument("--target-p99-sec", type=float, default=10.0)
    parser.add_argument("--strategy-bridge-target-ms", type=float, default=1500.0)
    parser.add_argument(
        "--strategy-bridge-chain-status",
        choices=("unknown", "complete", "incomplete"),
        default="unknown",
    )
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
    parser.add_argument("--operator", default=os.getenv("USER", ""))
    parser.add_argument("--host", default="")
    parser.add_argument("--build", default="")
    parser.add_argument("--config-profile", default="")
    parser.add_argument(
        "--scenarios",
        default="disconnect,jitter,loss,combined",
        help="comma-separated scenarios to execute",
    )
    parser.add_argument("--skip-preflight", action="store_true")
    parser.add_argument("--preflight-timeout-ms", type=int, default=1200)
    parser.add_argument(
        "--fallback-configs",
        default=(
            "configs/sim/ctp_trading_hours_group2.yaml," "configs/sim/ctp_trading_hours_group3.yaml"
        ),
        help="comma-separated fallback configs for trading-hours groups",
    )
    parser.set_defaults(auto_fallback_trading_groups=True)
    parser.add_argument(
        "--auto-fallback-trading-groups",
        dest="auto_fallback_trading_groups",
        action="store_true",
        help="enable automatic fallback to alternate trading-hours groups",
    )
    parser.add_argument(
        "--no-auto-fallback-trading-groups",
        dest="auto_fallback_trading_groups",
        action="store_false",
        help="disable automatic fallback to alternate trading-hours groups",
    )
    parser.add_argument("--execute-faults", action="store_true")
    parser.add_argument("--use-sudo", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _print_command(command: Iterable[str]) -> None:
    rendered = " ".join(command)
    print(f"$ {rendered}")


def _run(command: list[str], *, dry_run: bool) -> None:
    _print_command(command)
    if dry_run:
        return
    subprocess.run(command, check=True)


def _run_preflight(
    *, config_path: Path, ctp_lib_dir: Path, connect_timeout_ms: int
) -> CtpPreflightReport:
    return run_ctp_preflight(
        CtpPreflightConfig(
            config_path=config_path,
            ctp_lib_dir=ctp_lib_dir,
            connect_timeout_ms=max(100, connect_timeout_ms),
            skip_network_check=False,
        )
    )


def _print_preflight_report(report: CtpPreflightReport) -> None:
    for item in report.items:
        status = "PASS" if item.ok else "FAIL"
        if item.skipped:
            status = "SKIP"
        print(f"[{status}] {item.name}: {item.detail}", file=sys.stderr)


def _run_and_capture(
    command: list[str], *, env: dict[str, str], log_path: Path, dry_run: bool
) -> subprocess.Popen[str] | None:
    _print_command(command)
    if dry_run:
        return None
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(
        command,
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )


def _tool_available(tool: str) -> bool:
    try:
        if tool == "iptables":
            resolve_command_binary(("iptables", "iptables-nft"))
            return True
        if tool == "tc":
            resolve_command_binary(("tc",))
            return True
        return shutil.which(tool) is not None
    except ValueError:
        return False


def main() -> int:
    args = _build_parser().parse_args()
    probe_bin = Path(args.probe_bin)
    config = Path(args.config)
    probe_log = Path(args.probe_log)
    event_log = Path(args.event_log)
    report_file = Path(args.report_file)
    fault_script = Path(args.fault_script)
    report_script = Path(args.report_script)
    ctp_lib_dir = Path(args.ctp_lib_dir)

    if not probe_bin.exists():
        print(f"error: probe binary not found: {probe_bin}", file=sys.stderr)
        return 2
    if not config.exists():
        print(f"error: config file not found: {config}", file=sys.stderr)
        return 2
    if not fault_script.exists():
        print(f"error: fault script not found: {fault_script}", file=sys.stderr)
        return 2
    if not report_script.exists():
        print(f"error: report script not found: {report_script}", file=sys.stderr)
        return 2

    if args.execute_faults and not os.getenv("CTP_SIM_PASSWORD"):
        print("error: CTP_SIM_PASSWORD is required when execute-faults is enabled", file=sys.stderr)
        return 2

    try:
        scenarios = select_scenarios(args.scenarios)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    active_config = config
    if not args.skip_preflight:
        preflight = _run_preflight(
            config_path=active_config,
            ctp_lib_dir=ctp_lib_dir,
            connect_timeout_ms=args.preflight_timeout_ms,
        )
        if (
            not preflight.ok
            and args.auto_fallback_trading_groups
            and has_reachable_group_hint(preflight)
        ):
            fallback_candidates = parse_fallback_config_paths(args.fallback_configs)
            for candidate in fallback_candidates:
                if candidate == active_config:
                    continue
                if not candidate.exists():
                    print(f"[SKIP] fallback_config: missing {candidate}", file=sys.stderr)
                    continue
                candidate_report = _run_preflight(
                    config_path=candidate,
                    ctp_lib_dir=ctp_lib_dir,
                    connect_timeout_ms=args.preflight_timeout_ms,
                )
                if candidate_report.ok:
                    print(
                        f"info: preflight fallback selected config: {candidate}",
                        file=sys.stderr,
                    )
                    active_config = candidate
                    preflight = candidate_report
                    break

        if not preflight.ok:
            print("error: preflight failed, stop evidence run", file=sys.stderr)
            _print_preflight_report(preflight)
            return 2
        if active_config != config:
            print(f"info: using fallback config for run: {active_config}", file=sys.stderr)
            config = active_config

    missing_tools = [
        tool
        for tool in required_tools_for_scenarios(scenarios, execute_faults=args.execute_faults)
        if not _tool_available(tool)
    ]
    if missing_tools:
        print(
            "error: required tools not found: "
            + ",".join(missing_tools)
            + ". install tools or exclude dependent scenarios via --scenarios",
            file=sys.stderr,
        )
        return 2

    probe_command = build_probe_command(
        probe_bin=probe_bin,
        config_path=config,
        monitor_seconds=args.monitor_seconds,
        health_interval_ms=args.health_interval_ms,
    )

    env = os.environ.copy()
    current_ld = env.get("LD_LIBRARY_PATH", "")
    ctp_lib = str(ctp_lib_dir)
    env["LD_LIBRARY_PATH"] = f"{ctp_lib}:{current_ld}" if current_ld else ctp_lib

    probe_process = _run_and_capture(
        probe_command,
        env=env,
        log_path=probe_log,
        dry_run=args.dry_run,
    )

    exit_code = 0
    try:
        if not args.dry_run:
            time.sleep(max(1, args.warmup_seconds))
            if probe_process is not None and callable(getattr(probe_process, "poll", None)):
                poll_code = probe_process.poll()
                if poll_code is not None:
                    print(
                        f"error: probe exited early (exit {poll_code}); see {probe_log}",
                        file=sys.stderr,
                    )
                    exit_code = 2
                    return exit_code

        for scenario in scenarios:
            command = build_fault_inject_command(
                launcher_python=sys.executable,
                fault_script=fault_script,
                scenario=scenario,
                event_log_file=event_log,
                iface=args.iface,
                target_ip=args.target_ip,
                ports=args.ports,
                disconnect_mode=args.disconnect_mode,
                execute=args.execute_faults,
                use_sudo=args.use_sudo,
            )
            _run(command, dry_run=args.dry_run)

        report_command = [
            sys.executable,
            str(report_script),
            "--fault-events-file",
            str(event_log),
            "--probe-log-file",
            str(probe_log),
            "--output-file",
            str(report_file),
            "--health-json-file",
            str(args.health_json_file),
            "--health-markdown-file",
            str(args.health_markdown_file),
            "--target-p99-sec",
            f"{args.target_p99_sec:g}",
            "--strategy-bridge-target-ms",
            f"{args.strategy_bridge_target_ms:g}",
            "--strategy-bridge-chain-status",
            args.strategy_bridge_chain_status,
            "--storage-redis-health",
            args.storage_redis_health,
            "--storage-timescale-health",
            args.storage_timescale_health,
            "--operator",
            args.operator,
            "--host",
            args.host or os.uname().nodename,
            "--build",
            args.build,
            "--config-profile",
            args.config_profile or str(config),
            "--interface",
            args.iface,
        ]
        _run(report_command, dry_run=args.dry_run)
    except KeyboardInterrupt:
        print("info: interrupted by user (SIGINT)", file=sys.stderr)
        exit_code = 130
    except subprocess.CalledProcessError as exc:
        if exc.returncode == 130:
            print("info: aborted by user (SIGINT)", file=sys.stderr)
            exit_code = 130
        else:
            print(f"error: command failed (exit {exc.returncode}): {exc.cmd}", file=sys.stderr)
            exit_code = 2
    finally:
        if probe_process is not None:
            probe_process.terminate()
            try:
                probe_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                probe_process.kill()

    if exit_code == 0 and not args.dry_run:
        print(f"report generated: {report_file}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
