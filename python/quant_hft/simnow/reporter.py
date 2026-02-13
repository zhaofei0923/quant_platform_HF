from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def write_compare_report(payload: dict[str, Any], output_path: str) -> Path:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return target


def write_compare_html(payload: dict[str, Any], output_path: str) -> Path:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    run_id = str(payload.get("run_id", ""))
    strategy_id = str(payload.get("strategy_id", ""))
    dry_run = bool(payload.get("dry_run", False))
    delta = payload.get("delta", {})
    threshold = payload.get("threshold", {})
    attribution = payload.get("attribution", {})
    risk = payload.get("risk_decomposition", {})

    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>SimNow Compare Report</title>
</head>
<body>
  <h1>SimNow Compare Report</h1>
  <p>run_id={run_id} strategy_id={strategy_id} dry_run={dry_run}</p>
  <h2>Delta</h2>
  <pre>{json.dumps(delta, ensure_ascii=False, indent=2)}</pre>
  <h2>Threshold</h2>
  <pre>{json.dumps(threshold, ensure_ascii=False, indent=2)}</pre>
  <h2>Attribution</h2>
  <pre>{json.dumps(attribution, ensure_ascii=False, indent=2)}</pre>
  <h2>Risk Decomposition</h2>
  <pre>{json.dumps(risk, ensure_ascii=False, indent=2)}</pre>
</body>
</html>
"""
    target.write_text(html, encoding="utf-8")
    return target


def persist_compare_sqlite(payload: dict[str, Any], sqlite_path: str) -> Path:
    target = Path(sqlite_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(target)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS simnow_compare_runs (
                run_id TEXT PRIMARY KEY,
                created_at_utc TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                dry_run INTEGER NOT NULL,
                broker_mode TEXT NOT NULL,
                max_ticks INTEGER NOT NULL,
                simnow_intents INTEGER NOT NULL,
                backtest_intents INTEGER NOT NULL,
                delta_intents INTEGER NOT NULL,
                delta_ratio REAL NOT NULL,
                within_threshold INTEGER NOT NULL,
                attribution_json TEXT NOT NULL,
                risk_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO simnow_compare_runs (
                run_id,
                created_at_utc,
                strategy_id,
                dry_run,
                broker_mode,
                max_ticks,
                simnow_intents,
                backtest_intents,
                delta_intents,
                delta_ratio,
                within_threshold,
                attribution_json,
                risk_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(payload.get("run_id", "")),
                datetime.now(timezone.utc).isoformat(),
                str(payload.get("strategy_id", "")),
                1 if bool(payload.get("dry_run", False)) else 0,
                str(payload.get("broker_mode", "")),
                int(payload.get("max_ticks", 0) or 0),
                int((payload.get("simnow", {}) or {}).get("intents_emitted", 0) or 0),
                int((payload.get("backtest", {}) or {}).get("intents_emitted", 0) or 0),
                int((payload.get("delta", {}) or {}).get("intents", 0) or 0),
                float((payload.get("delta", {}) or {}).get("intents_ratio", 0.0) or 0.0),
                1
                if bool((payload.get("threshold", {}) or {}).get("within_threshold", False))
                else 0,
                json.dumps(payload.get("attribution", {}), ensure_ascii=False),
                json.dumps(payload.get("risk_decomposition", {}), ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return target
