#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

_DEFAULT_REQUIRED = (
    "redis-primary",
    "timescale-primary",
    "redis-replica",
    "timescale-replica",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check multi-host prodlike compose service health and emit a JSON report"
    )
    parser.add_argument("--ps-json-file", default="")
    parser.add_argument("--compose-file", default="infra/docker-compose.prodlike.multi-host.yaml")
    parser.add_argument("--project-name", default="quant-hft-prodlike-multi-host")
    parser.add_argument("--docker-bin", default="docker")
    parser.add_argument(
        "--report-json",
        default="docs/results/prodlike_multi_host_health_report.json",
    )
    parser.add_argument(
        "--required-services",
        default=",".join(_DEFAULT_REQUIRED),
        help="Comma-separated service names that must be present and healthy",
    )
    return parser.parse_args()


def _normalize_required_services(raw: str) -> tuple[str, ...]:
    values = [item.strip() for item in raw.split(",")]
    return tuple(item for item in values if item)


def _run_compose_ps(compose_file: str, project_name: str, docker_bin: str) -> tuple[bool, str]:
    command = [
        docker_bin,
        "compose",
        "-f",
        compose_file,
        "--project-name",
        project_name,
        "ps",
        "--format",
        "json",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        return False, (completed.stdout + completed.stderr).strip()
    return True, completed.stdout


def _load_ps_entries(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = [json.loads(line) for line in text.splitlines() if line.strip()]
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _parse_labels(raw: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        key, value = item.split("=", maxsplit=1)
        result[key.strip()] = value.strip()
    return result


def _extract_row(item: dict[str, Any]) -> dict[str, str]:
    def get(*keys: str) -> str:
        for key in keys:
            value = item.get(key)
            if value is not None:
                return str(value)
        return ""

    labels = _parse_labels(get("Labels", "labels"))
    return {
        "service": get("Service", "service", "Name", "name").strip(),
        "state": get("State", "state", "Status", "status").strip().lower(),
        "health": get("Health", "health").strip().lower(),
        "host": labels.get("quant_hft.host", ""),
        "role": labels.get("quant_hft.role", ""),
    }


def _is_healthy(state: str, health: str) -> bool:
    if state not in {"running", "up"}:
        return False
    if not health:
        return True
    return health == "healthy"


def main() -> int:
    args = _parse_args()
    required_services = _normalize_required_services(args.required_services)

    command_ok = True
    command_error = ""
    if args.ps_json_file:
        raw_entries = _load_ps_entries(Path(args.ps_json_file))
    else:
        command_ok, raw = _run_compose_ps(args.compose_file, args.project_name, args.docker_bin)
        if command_ok:
            try:
                payload = json.loads(raw or "[]")
            except json.JSONDecodeError:
                payload = [json.loads(line) for line in raw.splitlines() if line.strip()]
            if isinstance(payload, dict):
                raw_entries = [payload]
            elif isinstance(payload, list):
                raw_entries = [item for item in payload if isinstance(item, dict)]
            else:
                raw_entries = []
        else:
            raw_entries = []
            command_error = raw

    service_rows = [_extract_row(item) for item in raw_entries]
    service_map = {row["service"]: row for row in service_rows if row["service"]}

    missing_services = [service for service in required_services if service not in service_map]
    unhealthy_services = []
    for service in required_services:
        if service not in service_map:
            continue
        row = service_map[service]
        if not _is_healthy(row["state"], row["health"]):
            unhealthy_services.append(row)

    host_summary: dict[str, dict[str, str]] = {}
    for row in service_rows:
        host = row["host"]
        if not host:
            continue
        host_summary.setdefault(host, {"role": row["role"], "status": "healthy"})
        if row["role"] and not host_summary[host]["role"]:
            host_summary[host]["role"] = row["role"]
        if not _is_healthy(row["state"], row["health"]):
            host_summary[host]["status"] = "degraded"

    healthy = command_ok and not missing_services and not unhealthy_services
    classification = "healthy"
    if not command_ok:
        classification = "compose_command_failed"
    elif missing_services and unhealthy_services:
        classification = "missing_and_unhealthy"
    elif missing_services:
        classification = "missing_services"
    elif unhealthy_services:
        classification = "unhealthy_services"

    report = {
        "generated_ts_ns": time.time_ns(),
        "healthy": healthy,
        "classification": classification,
        "required_services": list(required_services),
        "missing_services": missing_services,
        "unhealthy_services": unhealthy_services,
        "service_rows": service_rows,
        "host_summary": host_summary,
        "compose_file": args.compose_file,
        "project_name": args.project_name,
        "command_error": command_error,
    }

    output = Path(args.report_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(output))
    return 0 if healthy else 2


if __name__ == "__main__":
    raise SystemExit(main())
