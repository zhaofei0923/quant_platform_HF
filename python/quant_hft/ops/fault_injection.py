from __future__ import annotations

import json
import os
import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_SYSTEM_BIN_DIRS: tuple[Path, ...] = (
    Path("/usr/local/sbin"),
    Path("/usr/local/bin"),
    Path("/usr/sbin"),
    Path("/usr/bin"),
    Path("/sbin"),
    Path("/bin"),
)


def parse_ports(raw_ports: str) -> list[int]:
    ports: list[int] = []
    for token in raw_ports.split(","):
        value = token.strip()
        if not value:
            continue
        port = int(value)
        if port <= 0 or port > 65535:
            raise ValueError(f"invalid TCP port: {port}")
        ports.append(port)
    if not ports:
        raise ValueError("at least one TCP port is required")
    return ports


def _format_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:g}"


def _is_executable(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def resolve_command_binary(
    candidates: Sequence[str], *, search_dirs: Sequence[Path] = DEFAULT_SYSTEM_BIN_DIRS
) -> str:
    if not candidates:
        raise ValueError("at least one tool candidate is required")
    checked: list[str] = []
    for candidate in candidates:
        tool = candidate.strip()
        if not tool:
            continue
        checked.append(tool)
        if "/" in tool:
            path = Path(tool)
            if _is_executable(path):
                return str(path)
            continue

        discovered = shutil.which(tool)
        if discovered:
            return discovered

        for root in search_dirs:
            path = root / tool
            if _is_executable(path):
                return str(path)

    raise ValueError(f"none of command candidates found: {','.join(checked)}")


def build_disconnect_plan(
    target_ip: str,
    ports: Sequence[int],
    *,
    firewall_cmd: str = "iptables",
    disconnect_mode: str = "drop",
) -> list[str]:
    if not target_ip.strip():
        raise ValueError("target_ip is required")
    if not ports:
        raise ValueError("ports are required")
    if not firewall_cmd.strip():
        raise ValueError("firewall_cmd is required")

    mode = disconnect_mode.strip().lower()
    if mode not in {"drop", "reset"}:
        raise ValueError(f"unsupported disconnect_mode: {disconnect_mode}")
    action = "-j DROP"
    if mode == "reset":
        action = "-j REJECT --reject-with tcp-reset"

    commands: list[str] = []
    for port in ports:
        if port <= 0 or port > 65535:
            raise ValueError(f"invalid TCP port: {port}")
        commands.append(f"{firewall_cmd} -I OUTPUT -p tcp -d {target_ip} --dport {port} {action}")
        commands.append(f"{firewall_cmd} -I INPUT -p tcp -s {target_ip} --sport {port} {action}")
    return commands


def build_disconnect_reset_plan(
    target_ip: str,
    ports: Sequence[int],
    *,
    firewall_cmd: str = "iptables",
    disconnect_mode: str = "drop",
) -> list[str]:
    if not target_ip.strip():
        raise ValueError("target_ip is required")
    if not ports:
        raise ValueError("ports are required")
    if not firewall_cmd.strip():
        raise ValueError("firewall_cmd is required")

    mode = disconnect_mode.strip().lower()
    if mode not in {"drop", "reset"}:
        raise ValueError(f"unsupported disconnect_mode: {disconnect_mode}")
    action = "-j DROP"
    if mode == "reset":
        action = "-j REJECT --reject-with tcp-reset"

    commands: list[str] = []
    for port in ports:
        if port <= 0 or port > 65535:
            raise ValueError(f"invalid TCP port: {port}")
        commands.append(
            f"{firewall_cmd} -D OUTPUT -p tcp -d {target_ip} --dport {port} {action} || true"
        )
        commands.append(
            f"{firewall_cmd} -D INPUT -p tcp -s {target_ip} --sport {port} {action} || true"
        )
    return commands


def build_netem_plan(
    iface: str,
    *,
    delay_ms: int = 0,
    jitter_ms: int = 0,
    loss_percent: float = 0.0,
    tc_cmd: str = "tc",
) -> list[str]:
    if not iface.strip():
        raise ValueError("iface is required")
    if delay_ms < 0:
        raise ValueError("delay_ms must be >= 0")
    if jitter_ms < 0:
        raise ValueError("jitter_ms must be >= 0")
    if loss_percent < 0.0 or loss_percent > 100.0:
        raise ValueError("loss_percent must be within [0, 100]")
    if not tc_cmd.strip():
        raise ValueError("tc_cmd is required")

    cmd = f"{tc_cmd} qdisc replace dev {iface} root netem"
    if delay_ms > 0:
        cmd += f" delay {delay_ms}ms"
        if jitter_ms > 0:
            cmd += f" {jitter_ms}ms"
    if loss_percent > 0.0:
        cmd += f" loss {_format_number(loss_percent)}%"
    return [cmd]


def build_netem_reset_plan(iface: str, *, tc_cmd: str = "tc") -> list[str]:
    if not iface.strip():
        raise ValueError("iface is required")
    if not tc_cmd.strip():
        raise ValueError("tc_cmd is required")
    return [f"{tc_cmd} qdisc del dev {iface} root || true"]


@dataclass(frozen=True)
class FaultEvent:
    ts_ns: int
    mode: str
    scenario: str
    phase: str
    execute: bool
    parameters: dict[str, str]


def build_fault_event(
    *,
    mode: str,
    scenario: str,
    phase: str,
    execute: bool,
    ts_ns: int,
    parameters: dict[str, str] | None = None,
) -> FaultEvent:
    if not mode:
        raise ValueError("mode is required")
    if not scenario:
        raise ValueError("scenario is required")
    if not phase:
        raise ValueError("phase is required")
    if ts_ns <= 0:
        raise ValueError("ts_ns must be > 0")
    return FaultEvent(
        ts_ns=ts_ns,
        mode=mode,
        scenario=scenario,
        phase=phase,
        execute=execute,
        parameters=parameters or {},
    )


def append_fault_event(path: Path | str, event: FaultEvent) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts_ns": event.ts_ns,
        "mode": event.mode,
        "scenario": event.scenario,
        "phase": event.phase,
        "execute": event.execute,
        "parameters": event.parameters,
    }
    with target.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=True) + "\n")
