from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path


def test_run_reconnect_evidence_auto_detects_complete_with_fake_redis(tmp_path: Path) -> None:
    fake_redis_script = Path("scripts/ops/fake_redis_bridge_server.py")
    assert fake_redis_script.exists()

    port = 16379
    server_log = tmp_path / "fake_redis.log"
    server_proc = subprocess.Popen(
        [
            sys.executable,
            str(fake_redis_script),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        stdout=server_log.open("w", encoding="utf-8"),
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        time.sleep(0.2)
        env = os.environ.copy()
        env["QUANT_HFT_REDIS_HOST"] = "127.0.0.1"
        env["QUANT_HFT_REDIS_PORT"] = str(port)
        run = subprocess.run(
            [
                sys.executable,
                "scripts/ops/run_reconnect_evidence.py",
                "--dry-run",
                "--skip-preflight",
                "--probe-bin",
                "/bin/echo",
                "--config",
                "configs/sim/ctp.yaml",
                "--scenarios",
                "disconnect",
                "--strategy-bridge-chain-status",
                "auto",
                "--report-file",
                str(tmp_path / "report.md"),
                "--health-json-file",
                str(tmp_path / "ops_health.json"),
                "--health-markdown-file",
                str(tmp_path / "ops_health.md"),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server_proc.kill()

    output = run.stdout + run.stderr
    assert "--strategy-bridge-chain-status complete" in output
    assert "--strategy-bridge-chain-source auto_detected" in output
    assert "--strategy-bridge-state-key-count 2" in output
    assert "--strategy-bridge-intent-count 1" in output
    assert "--strategy-bridge-order-key-count 1" in output
