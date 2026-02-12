from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_ops_health(path: Path) -> None:
    payload = {
        "slis": [
            {"name": "quant_hft_kafka_publish_success_rate", "value": 0.999},
            {"name": "quant_hft_kafka_spool_backlog", "value": 10},
            {"name": "quant_hft_cdc_lag_seconds", "value": 2.0},
            {"name": "quant_hft_clickhouse_ingest_lag_seconds", "value": 1.2},
            {"name": "quant_hft_parquet_lifecycle_success", "value": 1.0},
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_verify_m2_dataflow_evidence_script_passes(tmp_path: Path) -> None:
    ops_health = tmp_path / "ops.json"
    reconcile = tmp_path / "reconcile.json"
    lifecycle = tmp_path / "lifecycle.json"
    _write_ops_health(ops_health)
    reconcile.write_text(json.dumps({"consistent": True}), encoding="utf-8")
    lifecycle.write_text(json.dumps({"mode": "object-store", "dry_run": False}), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/ops/verify_m2_dataflow_evidence.py",
            "--ops-health-json",
            str(ops_health),
            "--reconcile-json",
            str(reconcile),
            "--lifecycle-json",
            str(lifecycle),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_verify_m2_dataflow_evidence_script_fails_on_reconcile(tmp_path: Path) -> None:
    ops_health = tmp_path / "ops.json"
    reconcile = tmp_path / "reconcile.json"
    lifecycle = tmp_path / "lifecycle.json"
    _write_ops_health(ops_health)
    reconcile.write_text(json.dumps({"consistent": False}), encoding="utf-8")
    lifecycle.write_text(json.dumps({"mode": "object-store", "dry_run": False}), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/ops/verify_m2_dataflow_evidence.py",
            "--ops-health-json",
            str(ops_health),
            "--reconcile-json",
            str(reconcile),
            "--lifecycle-json",
            str(lifecycle),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
