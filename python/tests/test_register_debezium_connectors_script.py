from __future__ import annotations

import subprocess
from pathlib import Path


def test_register_debezium_connectors_dry_run_writes_evidence(tmp_path: Path) -> None:
    connector_file = tmp_path / "connector.json"
    connector_file.write_text(
        '{"name":"quant_hft_trading_core_cdc","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector"}}\n',
        encoding="utf-8",
    )
    evidence_file = tmp_path / "debezium_register.env"

    completed = subprocess.run(
        [
            "bash",
            "scripts/infra/register_debezium_connectors.sh",
            "--connector-file",
            str(connector_file),
            "--output-file",
            str(evidence_file),
            "--dry-run",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = evidence_file.read_text(encoding="utf-8")
    assert "DEBEZIUM_REGISTER_DRY_RUN=1" in payload
    assert "DEBEZIUM_REGISTER_SUCCESS=true" in payload
    assert "DEBEZIUM_REGISTER_CONNECTOR_NAME=quant_hft_trading_core_cdc" in payload
