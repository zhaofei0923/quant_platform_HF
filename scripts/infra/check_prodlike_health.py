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
    "prometheus",
    "grafana",
    "minio",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check prodlike docker compose service health and emit a JSON report"
    )
    parser.add_argument(
        "--profile",
        default="single-host",
        choices=("single-host", "prodlike"),
        help="Deployment profile used to derive compose/project defaults",
    )
    parser.add_argument("--ps-json-file", default="")
    parser.add_argument("--compose-file", default="")
    parser.add_argument("--project-name", default="")
    parser.add_argument("--docker-bin", default="docker")
    parser.add_argument("--report-json", default="docs/results/prodlike_health_report.json")
    parser.add_argument(
        "--required-services",
        default=",".join(_DEFAULT_REQUIRED),
        help="Comma-separated service names that must be present and healthy",
    )
    return parser.parse_args()


def _extract_service_row(item: dict[str, Any]) -> dict[str, str]:
    def get(*keys: str) -> str:
        for key in keys:
            value = item.get(key)
            if value is not None:
                return str(value)
        return ""

    return {
        "service": get("Service", "service", "Name", "name").strip(),
        "state": get("State", "state", "Status", "status").strip().lower(),
        "health": get("Health", "health").strip().lower(),
    }


def _load_ps_entries_from_file(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            payload.append(json.loads(line))
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


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


def _normalize_required_services(raw: str) -> tuple[str, ...]:
    values = [item.strip() for item in raw.split(",")]
    return tuple(item for item in values if item)


def _is_service_healthy(state: str, health: str) -> bool:
    if state not in {"running", "up"}:
        return False
    if not health:
        return True
    return health in {"healthy"}


def main() -> int:
    args = _parse_args()
    if not args.compose_file:
        args.compose_file = (
            "infra/docker-compose.single-host.yaml"
            if args.profile == "single-host"
            else "infra/docker-compose.prodlike.yaml"
        )
    if not args.project_name:
        args.project_name = (
            "quant-hft-single-host" if args.profile == "single-host" else "quant-hft-prodlike"
        )

    required_services = _normalize_required_services(args.required_services)

    command_ok = True
    command_error = ""
    if args.ps_json_file:
        raw_entries = _load_ps_entries_from_file(Path(args.ps_json_file))
    else:
        command_ok, raw_output = _run_compose_ps(
            args.compose_file,
            args.project_name,
            args.docker_bin,
        )
        if command_ok:
            try:
                payload = json.loads(raw_output or "[]")
            except json.JSONDecodeError:
                payload = [json.loads(line) for line in raw_output.splitlines() if line.strip()]
            if isinstance(payload, dict):
                raw_entries = [payload]
            elif isinstance(payload, list):
                raw_entries = [item for item in payload if isinstance(item, dict)]
            else:
                raw_entries = []
        else:
            raw_entries = []
            command_error = raw_output

    rows = [_extract_service_row(item) for item in raw_entries]
    service_map = {row["service"]: row for row in rows if row["service"]}

    missing_services = [item for item in required_services if item not in service_map]
    unhealthy_services: list[dict[str, str]] = []
    for service in required_services:
        row = service_map.get(service)
        if row is None:
            continue
        if not _is_service_healthy(row["state"], row["health"]):
            unhealthy_services.append(row)

    healthy = command_ok and not missing_services and not unhealthy_services
    classification = "healthy"
    severity = "info"
    if not command_ok:
        classification = "compose_command_failed"
        severity = "critical"
    elif missing_services and unhealthy_services:
        classification = "missing_and_unhealthy"
        severity = "critical"
    elif missing_services:
        classification = "missing_services"
        severity = "critical"
    elif unhealthy_services:
        classification = "unhealthy_services"
        severity = "critical"

    report = {
        "generated_ts_ns": time.time_ns(),
        "healthy": healthy,
        "severity": severity,
        "classification": classification,
        "required_services": list(required_services),
        "missing_services": missing_services,
        "unhealthy_services": unhealthy_services,
        "service_states": {
            key: {
                "state": value["state"],
                "health": value["health"],
            }
            for key, value in service_map.items()
        },
        "compose_file": args.compose_file,
        "project_name": args.project_name,
        "profile": args.profile,
        "command_error": command_error,
    }

    output = Path(args.report_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(output))

    return 0 if healthy else 2


if __name__ == "__main__":
    raise SystemExit(main())
