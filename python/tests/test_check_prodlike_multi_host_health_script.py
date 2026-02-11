from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_check_prodlike_multi_host_health_passes_on_healthy_services(tmp_path: Path) -> None:
    ps_file = tmp_path / "ps.json"
    ps_file.write_text(
        json.dumps(
            [
                {
                    "Service": "redis-primary",
                    "State": "running",
                    "Health": "healthy",
                    "Labels": "quant_hft.host=host-a,quant_hft.role=primary",
                },
                {
                    "Service": "timescale-primary",
                    "State": "running",
                    "Health": "healthy",
                    "Labels": "quant_hft.host=host-a,quant_hft.role=primary",
                },
                {
                    "Service": "redis-replica",
                    "State": "running",
                    "Health": "healthy",
                    "Labels": "quant_hft.host=host-b,quant_hft.role=standby",
                },
                {
                    "Service": "timescale-replica",
                    "State": "running",
                    "Health": "healthy",
                    "Labels": "quant_hft.host=host-b,quant_hft.role=standby",
                },
            ]
        ),
        encoding="utf-8",
    )
    report_file = tmp_path / "health.json"

    command = [
        sys.executable,
        "scripts/infra/check_prodlike_multi_host_health.py",
        "--ps-json-file",
        str(ps_file),
        "--report-json",
        str(report_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["healthy"] is True
    assert payload["host_summary"]["host-a"]["role"] == "primary"
    assert payload["host_summary"]["host-b"]["role"] == "standby"


def test_check_prodlike_multi_host_health_fails_on_missing_or_unhealthy(tmp_path: Path) -> None:
    ps_file = tmp_path / "ps.json"
    ps_file.write_text(
        json.dumps(
            [
                {
                    "Service": "redis-primary",
                    "State": "running",
                    "Health": "unhealthy",
                    "Labels": "quant_hft.host=host-a,quant_hft.role=primary",
                },
                {
                    "Service": "timescale-primary",
                    "State": "running",
                    "Health": "healthy",
                    "Labels": "quant_hft.host=host-a,quant_hft.role=primary",
                },
            ]
        ),
        encoding="utf-8",
    )
    report_file = tmp_path / "health.json"

    command = [
        sys.executable,
        "scripts/infra/check_prodlike_multi_host_health.py",
        "--ps-json-file",
        str(ps_file),
        "--report-json",
        str(report_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["healthy"] is False
    assert payload["missing_services"]
