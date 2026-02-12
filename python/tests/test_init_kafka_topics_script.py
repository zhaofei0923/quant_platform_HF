from __future__ import annotations

import subprocess
from pathlib import Path


def test_init_kafka_topics_dry_run_writes_evidence(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  kafka:\n" "    image: bitnami/kafka:3.7\n",
        encoding="utf-8",
    )
    evidence_file = tmp_path / "kafka_topics_init_result.env"

    command = [
        "bash",
        "scripts/infra/init_kafka_topics.sh",
        "--compose-file",
        str(compose_file),
        "--output-file",
        str(evidence_file),
        "--dry-run",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = evidence_file.read_text(encoding="utf-8")
    assert "KAFKA_TOPICS_INIT_DRY_RUN=1" in payload
    assert "KAFKA_TOPICS_INIT_SUCCESS=true" in payload
    assert "KAFKA_TOPICS_INIT_MARKET_TOPIC=quant_hft.market.snapshots.v1" in payload
    assert "quant_hft.trading_core.order_events" in payload
