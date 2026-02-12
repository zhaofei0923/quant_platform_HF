from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_run_ctp_readiness_evidence_script_outputs_json(tmp_path: Path) -> None:
    output_path = tmp_path / "ctp_readiness.json"
    command = [
        sys.executable,
        "scripts/ops/run_ctp_readiness_evidence.py",
        "--output-json",
        str(output_path),
        "--query-latency-ms",
        "1200",
        "--flow-control-hits",
        "3",
        "--disconnect-recovery-success-rate",
        "1.0",
        "--reject-classified-ratio",
        "1.0",
        "--core-process-alive",
        "true",
        "--redis-health",
        "healthy",
        "--timescale-health",
        "healthy",
        "--strategy-bridge-chain-status",
        "complete",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["overall_healthy"] is True
    sli_names = [item["name"] for item in payload["slis"]]
    assert "quant_hft_ctp_query_latency_p99_ms" in sli_names
    assert "quant_hft_ctp_flow_control_hits" in sli_names
