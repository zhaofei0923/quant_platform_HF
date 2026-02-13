from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from quant_hft.simnow.reporter import (
    persist_compare_sqlite,
    write_compare_html,
    write_compare_report,
)


def _payload() -> dict[str, object]:
    return {
        "run_id": "ut-simnow-report",
        "strategy_id": "demo",
        "dry_run": True,
        "broker_mode": "paper",
        "max_ticks": 6,
        "simnow": {"intents_emitted": 6, "order_events": 6},
        "backtest": {"intents_emitted": 6, "ticks_read": 6},
        "delta": {"intents": 0, "intents_ratio": 0.0},
        "threshold": {"intents_abs_max": 0, "within_threshold": True},
        "attribution": {
            "signal_parity": 1.0,
            "execution_coverage": 1.0,
            "threshold_stability": 1.0,
        },
        "risk_decomposition": {
            "model_drift": 0.0,
            "execution_gap": 0.0,
            "consistency_gap": 0.0,
        },
    }


def test_reporter_writes_json_html_and_sqlite(tmp_path: Path) -> None:
    payload = _payload()
    json_path = tmp_path / "report.json"
    html_path = tmp_path / "report.html"
    db_path = tmp_path / "report.sqlite"

    write_compare_report(payload, str(json_path))
    write_compare_html(payload, str(html_path))
    persist_compare_sqlite(payload, str(db_path))

    loaded = json.loads(json_path.read_text(encoding="utf-8"))
    assert loaded["run_id"] == "ut-simnow-report"

    html_text = html_path.read_text(encoding="utf-8")
    assert "SimNow Compare Report" in html_text
    assert "ut-simnow-report" in html_text

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT run_id, dry_run, within_threshold FROM simnow_compare_runs"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "ut-simnow-report"
    assert row[1] == 1
    assert row[2] == 1
