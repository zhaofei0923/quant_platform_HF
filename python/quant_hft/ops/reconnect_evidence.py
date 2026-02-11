from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .ctp_preflight import CtpPreflightReport


@dataclass(frozen=True)
class ScenarioConfig:
    name: str
    duration_sec: int
    delay_ms: int | None = None
    jitter_ms: int | None = None
    loss_percent: float | None = None


def default_scenarios() -> tuple[ScenarioConfig, ...]:
    return (
        ScenarioConfig(name="disconnect", duration_sec=20),
        ScenarioConfig(name="jitter", duration_sec=30, delay_ms=250, jitter_ms=30),
        ScenarioConfig(name="loss", duration_sec=30, loss_percent=5.0),
        ScenarioConfig(
            name="combined",
            duration_sec=30,
            delay_ms=200,
            jitter_ms=40,
            loss_percent=3.0,
        ),
    )


def select_scenarios(raw: str | None) -> tuple[ScenarioConfig, ...]:
    scenario_map = {item.name: item for item in default_scenarios()}
    if raw is None or not raw.strip():
        return tuple(scenario_map.values())
    selected: list[ScenarioConfig] = []
    for token in raw.split(","):
        name = token.strip()
        if not name:
            continue
        item = scenario_map.get(name)
        if item is None:
            raise ValueError(f"unknown scenario: {name}")
        selected.append(item)
    if not selected:
        raise ValueError("at least one scenario must be selected")
    return tuple(selected)


def parse_fallback_config_paths(raw: str | None) -> tuple[Path, ...]:
    if raw is None or not raw.strip():
        return tuple()
    candidates: list[Path] = []
    seen: set[str] = set()
    for token in raw.split(","):
        entry = token.strip()
        if not entry:
            continue
        if entry in seen:
            continue
        seen.add(entry)
        candidates.append(Path(entry))
    return tuple(candidates)


def has_reachable_group_hint(report: CtpPreflightReport) -> bool:
    return any(
        item.name == "service_window_hint" and "reachable groups:" in item.detail
        for item in report.items
    )


def required_tools_for_scenarios(
    scenarios: tuple[ScenarioConfig, ...], *, execute_faults: bool
) -> tuple[str, ...]:
    if not execute_faults:
        return tuple()
    required: list[str] = []
    if any(item.name == "disconnect" for item in scenarios):
        required.append("iptables")
    if any(item.name in {"latency", "jitter", "loss", "combined"} for item in scenarios):
        required.append("tc")
    dedup: list[str] = []
    for tool in required:
        if tool not in dedup:
            dedup.append(tool)
    return tuple(dedup)


def build_probe_command(
    *,
    probe_bin: Path,
    config_path: Path,
    monitor_seconds: int,
    health_interval_ms: int,
) -> list[str]:
    return [
        str(probe_bin),
        str(config_path),
        "--monitor-seconds",
        str(monitor_seconds),
        "--health-interval-ms",
        str(health_interval_ms),
    ]


def build_fault_inject_command(
    *,
    launcher_python: str,
    fault_script: Path,
    scenario: ScenarioConfig,
    event_log_file: Path,
    iface: str,
    target_ip: str,
    ports: str,
    disconnect_mode: str = "drop",
    execute: bool,
    use_sudo: bool,
) -> list[str]:
    if not scenario.name:
        raise ValueError("scenario.name is required")
    if scenario.duration_sec <= 0:
        raise ValueError("scenario.duration_sec must be > 0")
    command: list[str] = []
    if use_sudo:
        command.append("sudo")
    command.extend([launcher_python, str(fault_script), "run"])
    command.extend(["--scenario", scenario.name])
    command.extend(["--duration-sec", str(scenario.duration_sec)])
    command.extend(["--event-log-file", str(event_log_file)])

    if scenario.name == "disconnect":
        command.extend(["--target-ip", target_ip, "--ports", ports])
        command.extend(["--disconnect-mode", disconnect_mode])
    else:
        command.extend(["--iface", iface])
        if scenario.delay_ms is not None:
            command.extend(["--delay-ms", str(scenario.delay_ms)])
        if scenario.jitter_ms is not None:
            command.extend(["--jitter-ms", str(scenario.jitter_ms)])
        if scenario.loss_percent is not None:
            command.extend(["--loss-percent", f"{scenario.loss_percent:g}"])

    if execute:
        command.append("--execute")
    return command
