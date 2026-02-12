from __future__ import annotations

import argparse
import json
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DailySettlementOrchestratorConfig:
    settlement_bin: str
    ctp_config_path: str
    trading_day: str
    evidence_json: str
    diff_json: str
    execute: bool
    force: bool
    shadow: bool
    strict_order_trade_backfill: bool


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_trading_day(raw: str) -> str:
    text = raw.strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text
    raise ValueError("trading day must be YYYYMMDD or YYYY-MM-DD")


def _build_command(config: DailySettlementOrchestratorConfig) -> list[str]:
    command = [
        config.settlement_bin,
        "--config",
        config.ctp_config_path,
        "--trading-day",
        config.trading_day,
        "--evidence-path",
        config.evidence_json,
    ]
    if config.force:
        command.append("--force")
    if config.shadow:
        command.append("--shadow")
    if config.strict_order_trade_backfill:
        command.append("--strict-order-trade-backfill")
    return command


def _safe_json_parse(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def run_daily_settlement_orchestrator(
    config: DailySettlementOrchestratorConfig,
) -> tuple[int, dict[str, Any]]:
    started_utc = _utc_now()
    started = time.monotonic()
    command = _build_command(config)

    if not config.execute:
        payload: dict[str, Any] = {
            "dry_run": True,
            "trading_day": config.trading_day,
            "ctp_config_path": config.ctp_config_path,
            "command": command,
            "exit_code": 0,
            "success": True,
            "noop": True,
            "blocked": False,
            "status": "DRY_RUN",
            "message": "dry-run only",
            "started_utc": started_utc,
            "completed_utc": _utc_now(),
            "duration_seconds": 0.0,
            "stdout": "",
            "stderr": "",
            "diff_report_path": config.diff_json,
        }
        return 0, payload

    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    result_obj = _safe_json_parse(completed.stdout)
    duration_seconds = round(max(0.0, time.monotonic() - started), 6)
    payload = {
        "dry_run": False,
        "trading_day": config.trading_day,
        "ctp_config_path": config.ctp_config_path,
        "command": command,
        "exit_code": completed.returncode,
        "success": bool(result_obj.get("success", False)) and completed.returncode == 0,
        "noop": bool(result_obj.get("noop", False)),
        "blocked": bool(result_obj.get("blocked", False)),
        "status": str(result_obj.get("status", "")),
        "message": str(result_obj.get("message", "")),
        "started_utc": started_utc,
        "completed_utc": _utc_now(),
        "duration_seconds": duration_seconds,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "diff_report_path": config.diff_json,
    }
    return completed.returncode, payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one-shot daily settlement and emit structured evidence JSON.",
    )
    parser.add_argument("--settlement-bin", default="build/daily_settlement")
    parser.add_argument("--ctp-config-path", default="configs/prod/ctp.yaml")
    parser.add_argument("--trading-day", required=True)
    parser.add_argument(
        "--evidence-json",
        default="docs/results/daily_settlement_evidence.json",
    )
    parser.add_argument(
        "--diff-json",
        default="docs/results/settlement_diff.json",
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--shadow", action="store_true")
    parser.add_argument("--strict-order-trade-backfill", action="store_true")
    parser.add_argument("--execute", action="store_true")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        trading_day = _parse_trading_day(args.trading_day)
    except ValueError as exc:
        print(f"error: {exc}")
        return 2

    config = DailySettlementOrchestratorConfig(
        settlement_bin=args.settlement_bin,
        ctp_config_path=args.ctp_config_path,
        trading_day=trading_day,
        evidence_json=args.evidence_json,
        diff_json=args.diff_json,
        execute=bool(args.execute),
        force=bool(args.force),
        shadow=bool(args.shadow),
        strict_order_trade_backfill=bool(args.strict_order_trade_backfill),
    )
    exit_code, payload = run_daily_settlement_orchestrator(config)
    evidence_path = Path(config.evidence_json)
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(str(evidence_path))
    if not config.execute:
        return 0
    return 0 if payload.get("success", False) else (2 if exit_code == 0 else exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
