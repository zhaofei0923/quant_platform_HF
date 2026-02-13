from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from quant_hft.runtime.ctp_direct_runner import CtpDirectRunnerConfig, load_ctp_direct_runner_config

_ENV_PATTERN = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


@dataclass(frozen=True)
class SimNowCompareConfig:
    run_id: str
    ctp_config_path: str
    backtest_csv_path: str
    output_json_path: str
    strategy_id: str
    instruments: list[str]
    max_ticks: int
    dry_run: bool
    broker_mode: str
    poll_interval_ms: int
    connect_config: dict[str, object]
    settlement_confirm_required: bool


def _trim(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return text[1:-1]
    return text


def _load_simple_yaml_kv(path: str) -> dict[str, str]:
    kv: dict[str, str] = {}
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", maxsplit=1)[0].strip()
        if not line or line == "ctp:":
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", maxsplit=1)
        key = _trim(key)
        value = _trim(value)
        if key:
            kv[key] = value
    return kv


def _parse_bool(raw: str, default: bool) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    return default


def _resolve_env_placeholder(value: object) -> object:
    if not isinstance(value, str):
        return value
    text = value.strip()
    match = _ENV_PATTERN.match(text)
    if not match:
        return value
    env_name = match.group(1)
    return os.getenv(env_name, "")


def _resolve_connect_config(raw: dict[str, object]) -> dict[str, object]:
    return {key: _resolve_env_placeholder(value) for key, value in raw.items()}


def _broker_mode(kv: dict[str, str]) -> str:
    enable_real = _parse_bool(kv.get("enable_real_api", "false"), False)
    return "live" if enable_real else "paper"


def load_simnow_compare_config(
    *,
    ctp_config_path: str,
    backtest_csv_path: str,
    output_json_path: str,
    run_id: str,
    strategy_id_override: str = "",
    max_ticks: int = 500,
    dry_run: bool = True,
) -> SimNowCompareConfig:
    direct_cfg: CtpDirectRunnerConfig = load_ctp_direct_runner_config(
        ctp_config_path,
        strategy_id_override=strategy_id_override,
    )
    kv = _load_simple_yaml_kv(ctp_config_path)
    return SimNowCompareConfig(
        run_id=run_id,
        ctp_config_path=ctp_config_path,
        backtest_csv_path=backtest_csv_path,
        output_json_path=output_json_path,
        strategy_id=direct_cfg.strategy_id,
        instruments=list(direct_cfg.instruments),
        max_ticks=max(1, int(max_ticks)),
        dry_run=dry_run,
        broker_mode=_broker_mode(kv),
        poll_interval_ms=max(1, int(direct_cfg.poll_interval_ms)),
        connect_config=_resolve_connect_config(dict(direct_cfg.connect_config)),
        settlement_confirm_required=bool(direct_cfg.settlement_confirm_required),
    )
