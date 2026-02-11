from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CtpPreflightConfig:
    config_path: Path
    ctp_lib_dir: Path
    connect_timeout_ms: int = 1500
    skip_network_check: bool = False


@dataclass(frozen=True)
class CtpPreflightItem:
    name: str
    ok: bool
    detail: str
    skipped: bool = False


@dataclass(frozen=True)
class CtpPreflightReport:
    ok: bool
    items: tuple[CtpPreflightItem, ...]


def _trim(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return text[1:-1]
    return text


def _load_simple_ctp_yaml(path: Path) -> dict[str, str]:
    kv: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or line == "ctp:":
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        kv[_trim(key)] = _trim(value)
    return kv


def _parse_front(front: str) -> tuple[str, int]:
    if not front.startswith("tcp://"):
        raise ValueError(f"front must start with tcp://, got {front}")
    payload = front[len("tcp://") :]
    if ":" not in payload:
        raise ValueError(f"front must include host:port, got {front}")
    host, port_text = payload.rsplit(":", 1)
    port = int(port_text)
    if not host:
        raise ValueError(f"front host is empty: {front}")
    if port <= 0 or port > 65535:
        raise ValueError(f"front port out of range: {front}")
    return host, port


def _parse_bool(value: str, default: bool) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    return default


def _check_tcp_connect(host: str, port: int, timeout_ms: int) -> tuple[bool, str]:
    timeout_sec = max(0.05, timeout_ms / 1000.0)
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True, f"connected {host}:{port}"
    except OSError as exc:
        return False, f"connect failed {host}:{port} ({exc})"


def _is_timeout_detail(detail: str) -> bool:
    text = detail.lower()
    return "timed out" in text or "timeout" in text


def _looks_like_trading_hours_port(port: int) -> bool:
    return 30001 <= port <= 30013


def _is_simnow_trading_hours_front_pair(
    md_front: str, td_front: str, *, allowed_host: str = "182.254.243.31"
) -> bool:
    try:
        md_host, md_port = _parse_front(md_front)
        td_host, td_port = _parse_front(td_front)
    except ValueError:
        return False
    if md_host != td_host:
        return False
    if md_host != allowed_host:
        return False
    return (td_port, md_port) in {
        (30001, 30011),
        (30002, 30012),
        (30003, 30013),
    }


def _discover_reachable_trading_hours_groups(
    host: str, timeout_ms: int
) -> tuple[tuple[int, int, int], ...]:
    groups = (
        (1, 30001, 30011),
        (2, 30002, 30012),
        (3, 30003, 30013),
    )
    probe_timeout_ms = max(200, min(timeout_ms, 600))
    reachable: list[tuple[int, int, int]] = []
    for group, trader_port, market_port in groups:
        trader_ok, _ = _check_tcp_connect(host, trader_port, probe_timeout_ms)
        market_ok, _ = _check_tcp_connect(host, market_port, probe_timeout_ms)
        if trader_ok and market_ok:
            reachable.append((group, trader_port, market_port))
    return tuple(reachable)


def run_ctp_preflight(config: CtpPreflightConfig) -> CtpPreflightReport:
    items: list[CtpPreflightItem] = []

    cfg_path = config.config_path
    if not cfg_path.exists():
        items.append(CtpPreflightItem("config_exists", False, f"missing {cfg_path}"))
        return CtpPreflightReport(False, tuple(items))
    items.append(CtpPreflightItem("config_exists", True, str(cfg_path)))

    try:
        kv = _load_simple_ctp_yaml(cfg_path)
    except OSError as exc:
        items.append(CtpPreflightItem("config_parse", False, f"read failed: {exc}"))
        return CtpPreflightReport(False, tuple(items))
    items.append(CtpPreflightItem("config_parse", True, "parsed simple YAML format"))

    required_keys = [
        "environment",
        "is_production_mode",
        "broker_id",
        "user_id",
        "market_front",
        "trader_front",
    ]
    enable_terminal_auth = _parse_bool(kv.get("enable_terminal_auth", "true"), True)
    if enable_terminal_auth:
        required_keys.extend(["auth_code", "app_id"])
    missing = [key for key in required_keys if not kv.get(key)]
    if missing:
        items.append(CtpPreflightItem("required_fields", False, f"missing: {','.join(missing)}"))
    else:
        items.append(CtpPreflightItem("required_fields", True, "all required fields present"))

    env_value = kv.get("environment", "").lower()
    is_production_mode = kv.get("is_production_mode", "").lower() in {"1", "true", "yes"}
    md_front = kv.get("market_front", "")
    td_front = kv.get("trader_front", "")
    simnow_trading_hours_front = _is_simnow_trading_hours_front_pair(md_front, td_front)

    if env_value in {"sim", "simnow"}:
        if is_production_mode and not simnow_trading_hours_front:
            items.append(
                CtpPreflightItem(
                    "production_mode_guard",
                    False,
                    (
                        "sim environment requires is_production_mode=false unless using "
                        "SimNow trading-hours "
                        "fronts (30001/30011,30002/30012,30003/30013 "
                        "on 182.254.243.31)"
                    ),
                )
            )
        elif (not is_production_mode) and simnow_trading_hours_front:
            items.append(
                CtpPreflightItem(
                    "production_mode_guard",
                    False,
                    (
                        "SimNow trading-hours fronts require is_production_mode=true "
                        "(CTP v6.7.11 production secret key)"
                    ),
                )
            )
        else:
            items.append(
                CtpPreflightItem("production_mode_guard", True, "mode/environment coherent")
            )
    elif env_value in {"prod", "production"}:
        if not is_production_mode:
            items.append(
                CtpPreflightItem(
                    "production_mode_guard",
                    False,
                    "production environment requires is_production_mode=true",
                )
            )
        else:
            items.append(
                CtpPreflightItem("production_mode_guard", True, "mode/environment coherent")
            )
    else:
        items.append(
            CtpPreflightItem(
                "production_mode_guard",
                False,
                f"unknown environment: {env_value}",
            )
        )

    password_inline = kv.get("password", "")
    password_env_key = kv.get("password_env", "CTP_SIM_PASSWORD")
    resolved_password = password_inline or os.getenv(password_env_key, "")
    if not resolved_password:
        items.append(
            CtpPreflightItem(
                "password_source",
                False,
                f"missing password and env {password_env_key}",
            )
        )
    else:
        source = "config.password" if password_inline else f"env:{password_env_key}"
        items.append(CtpPreflightItem("password_source", True, source))

    lib_dir = config.ctp_lib_dir
    required_lib_files = (
        "ThostFtdcTraderApi.h",
        "thostmduserapi_se.so",
        "thosttraderapi_se.so",
        "error.xml",
    )
    missing_lib = [name for name in required_lib_files if not (lib_dir / name).exists()]
    if missing_lib:
        items.append(
            CtpPreflightItem(
                "ctp_lib_files", False, f"missing under {lib_dir}: {','.join(missing_lib)}"
            )
        )
    else:
        items.append(CtpPreflightItem("ctp_lib_files", True, str(lib_dir)))

    front_ports: list[int] = []
    front_hosts: list[str] = []
    tcp_items: list[CtpPreflightItem] = []

    for key, check_name in (
        ("market_front", "tcp_connect_market_front"),
        ("trader_front", "tcp_connect_trader_front"),
    ):
        front = kv.get(key, "")
        if not front:
            tcp_items.append(CtpPreflightItem(check_name, False, f"{key} missing"))
            continue
        try:
            host, port = _parse_front(front)
            front_hosts.append(host)
            front_ports.append(port)
        except ValueError as exc:
            tcp_items.append(CtpPreflightItem(check_name, False, str(exc)))
            continue

        if config.skip_network_check:
            tcp_items.append(
                CtpPreflightItem(
                    check_name,
                    True,
                    f"skipped {host}:{port}",
                    skipped=True,
                )
            )
            continue

        ok, detail = _check_tcp_connect(host, port, config.connect_timeout_ms)
        tcp_items.append(CtpPreflightItem(check_name, ok, detail))

    items.extend(tcp_items)

    tcp_failures = [item for item in tcp_items if not item.ok]
    all_tcp_timeout = bool(tcp_failures) and all(
        _is_timeout_detail(item.detail) for item in tcp_failures
    )
    trading_hours_profile = "trading" in kv.get("profile", "").lower()
    trading_hours_front = any(_looks_like_trading_hours_port(port) for port in front_ports)
    if all_tcp_timeout and (trading_hours_profile or trading_hours_front):
        hint_detail = (
            "tcp timeout may indicate out-of-session hours; retry in trading window, "
            "switch to 7x24 profile, or use --skip-network-check for static checks"
        )
        if trading_hours_front and front_hosts and len(set(front_hosts)) == 1:
            reachable_groups = _discover_reachable_trading_hours_groups(
                front_hosts[0], config.connect_timeout_ms
            )
            if reachable_groups:
                rendered_groups = ", ".join(
                    f"group{group}({trader_port}/{market_port})"
                    for group, trader_port, market_port in reachable_groups
                )
                hint_detail = f"configured group timed out; reachable groups: {rendered_groups}"
        items.append(
            CtpPreflightItem(
                "service_window_hint",
                True,
                hint_detail,
                skipped=True,
            )
        )

    overall_ok = all(item.ok for item in items)
    return CtpPreflightReport(ok=overall_ok, items=tuple(items))
