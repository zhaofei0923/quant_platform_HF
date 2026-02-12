from __future__ import annotations

import json
import subprocess
from pathlib import Path


def _write_connector(path: Path, name: str) -> None:
    path.write_text(
        json.dumps(
            {
                "name": name,
                "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "database.hostname": "timescale-primary",
                },
            }
        ),
        encoding="utf-8",
    )


def test_init_debezium_connectors_dry_run_writes_evidence(tmp_path: Path) -> None:
    trading_file = tmp_path / "trading.json"
    ops_file = tmp_path / "ops.json"
    _write_connector(trading_file, "trading-core-connector")
    _write_connector(ops_file, "ops-audit-connector")
    evidence_file = tmp_path / "debezium_connector_init_result.env"

    command = [
        "bash",
        "scripts/infra/init_debezium_connectors.sh",
        "--trading-core-connector-file",
        str(trading_file),
        "--ops-audit-connector-file",
        str(ops_file),
        "--output-file",
        str(evidence_file),
        "--dry-run",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = evidence_file.read_text(encoding="utf-8")
    assert "DEBEZIUM_CONNECTOR_INIT_DRY_RUN=1" in payload
    assert "DEBEZIUM_CONNECTOR_INIT_SUCCESS=true" in payload
    assert "STEP_1_NAME=apply_trading_core" in payload
    assert "STEP_4_NAME=verify_ops_audit" in payload


def test_init_debezium_connectors_execute_records_failure(tmp_path: Path) -> None:
    trading_file = tmp_path / "trading.json"
    ops_file = tmp_path / "ops.json"
    _write_connector(trading_file, "trading-core-connector")
    _write_connector(ops_file, "ops-audit-connector")
    evidence_file = tmp_path / "debezium_connector_init_result.env"

    command = [
        "bash",
        "scripts/infra/init_debezium_connectors.sh",
        "--trading-core-connector-file",
        str(trading_file),
        "--ops-audit-connector-file",
        str(ops_file),
        "--output-file",
        str(evidence_file),
        "--curl-bin",
        "/bin/false",
        "--execute",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    payload = evidence_file.read_text(encoding="utf-8")
    assert "DEBEZIUM_CONNECTOR_INIT_SUCCESS=false" in payload
    assert "DEBEZIUM_CONNECTOR_INIT_FAILED_STEP=apply_trading_core" in payload
