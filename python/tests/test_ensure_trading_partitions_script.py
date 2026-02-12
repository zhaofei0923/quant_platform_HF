from __future__ import annotations

import subprocess
from pathlib import Path


def test_ensure_trading_partitions_dry_run_writes_evidence(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  timescale-primary:\n" "    image: timescale/timescaledb:2.14.2-pg16\n",
        encoding="utf-8",
    )
    output_file = tmp_path / "ensure_partitions.env"

    completed = subprocess.run(
        [
            "bash",
            "scripts/infra/ensure_trading_partitions.sh",
            "--compose-file",
            str(compose_file),
            "--output-file",
            str(output_file),
            "--dry-run",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = output_file.read_text(encoding="utf-8")
    assert "ENSURE_TRADING_PARTITIONS_DRY_RUN=1" in payload
    assert "ENSURE_TRADING_PARTITIONS_SUCCESS=true" in payload
