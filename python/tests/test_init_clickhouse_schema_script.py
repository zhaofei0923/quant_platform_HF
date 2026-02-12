from __future__ import annotations

import subprocess
from pathlib import Path


def test_init_clickhouse_schema_dry_run_writes_evidence(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  clickhouse:\n" "    image: clickhouse/clickhouse-server:24.8\n",
        encoding="utf-8",
    )
    schema_dir = tmp_path / "sql"
    schema_dir.mkdir(parents=True, exist_ok=True)
    (schema_dir / "001_test.sql").write_text("SELECT 1;\n", encoding="utf-8")
    evidence_file = tmp_path / "clickhouse_schema_init_result.env"

    completed = subprocess.run(
        [
            "bash",
            "scripts/infra/init_clickhouse_schema.sh",
            "--compose-file",
            str(compose_file),
            "--schema-dir",
            str(schema_dir),
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
    assert "CLICKHOUSE_SCHEMA_INIT_DRY_RUN=1" in payload
    assert "CLICKHOUSE_SCHEMA_INIT_SUCCESS=true" in payload
    assert "STEP_1_NAME=apply_001_test.sql" in payload
