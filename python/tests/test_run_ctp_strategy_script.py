from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_run_ctp_strategy_script_run_once(tmp_path: Path) -> None:
    config_file = tmp_path / "ctp.yaml"
    config_file.write_text(
        "\n".join(
            [
                "ctp:",
                "environment: sim",
                "is_production_mode: false",
                "market_front: tcp://sim-md",
                "trader_front: tcp://sim-td",
                "broker_id: 9999",
                "user_id: 191202",
                "investor_id: 191202",
                "password: secret",
                "strategy_ids: demo",
                "instruments: SHFE.ag2406",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    command = [
        sys.executable,
        "scripts/strategy/run_ctp_strategy.py",
        "--config",
        str(config_file),
        "--run-once",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "ctp direct runner emitted intents=" in completed.stdout
