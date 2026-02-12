#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Debezium connector status via Kafka Connect REST API"
    )
    parser.add_argument("--connect-url", default="http://127.0.0.1:8083")
    parser.add_argument("--connectors", default="quant_hft_trading_core_cdc")
    parser.add_argument("--status-json-file", default="")
    parser.add_argument(
        "--output-json",
        default="docs/results/debezium_connectors_health_report.json",
    )
    return parser.parse_args()


def _normalize_connectors(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _load_statuses_from_file(path: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return {key: value for key, value in payload.items() if isinstance(value, dict)}
    return {}


def _fetch_connector_status(connect_url: str, connector_name: str) -> dict[str, Any]:
    url = f"{connect_url.rstrip('/')}/connectors/{connector_name}/status"
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=5) as response:
        text = response.read().decode("utf-8")
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise RuntimeError("invalid connector status payload")
    return payload


def _evaluate_status(payload: dict[str, Any]) -> tuple[bool, str, list[str]]:
    connector = payload.get("connector", {})
    connector_state = str(connector.get("state", "")).upper()
    tasks = payload.get("tasks", [])
    task_states = []
    for task in tasks if isinstance(tasks, list) else []:
        if isinstance(task, dict):
            task_states.append(str(task.get("state", "")).upper())

    if connector_state != "RUNNING":
        return False, connector_state, task_states
    if any(state != "RUNNING" for state in task_states):
        return False, connector_state, task_states
    return True, connector_state, task_states


def main() -> int:
    args = _parse_args()
    connector_names = _normalize_connectors(args.connectors)
    if not connector_names:
        raise SystemExit("connector list is empty")

    by_file: dict[str, dict[str, Any]] = {}
    if args.status_json_file:
        by_file = _load_statuses_from_file(Path(args.status_json_file))

    connector_reports: list[dict[str, Any]] = []
    healthy = True
    for connector_name in connector_names:
        payload: dict[str, Any] | None = None
        fetch_error = ""
        if by_file:
            payload = by_file.get(connector_name)
            if payload is None:
                fetch_error = "missing connector in status file"
        else:
            try:
                payload = _fetch_connector_status(args.connect_url, connector_name)
            except (RuntimeError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                fetch_error = str(exc)

        status_ok = False
        connector_state = ""
        task_states: list[str] = []
        if payload is not None and not fetch_error:
            status_ok, connector_state, task_states = _evaluate_status(payload)
        if not status_ok:
            healthy = False

        connector_reports.append(
            {
                "connector": connector_name,
                "healthy": status_ok,
                "connector_state": connector_state,
                "task_states": task_states,
                "error": fetch_error,
            }
        )

    report = {
        "generated_ts_ns": time.time_ns(),
        "healthy": healthy,
        "connect_url": args.connect_url,
        "connectors": connector_names,
        "connectors_report": connector_reports,
    }
    output = Path(args.output_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(output))
    return 0 if healthy else 2


if __name__ == "__main__":
    raise SystemExit(main())
