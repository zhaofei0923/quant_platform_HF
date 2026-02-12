from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from quant_hft.contracts import OrderEvent
from quant_hft.data_pipeline.adapters import DuckDbAnalyticsStore, MarketSnapshotRecord


def _seed_store(db_path: Path) -> None:
    store = DuckDbAnalyticsStore(db_path, prefer_duckdb=False)
    store.append_market_snapshots(
        [
            MarketSnapshotRecord(
                instrument_id="rb2405",
                ts_ns=1_707_628_800_000_000_000,
                last_price=4100.0,
                bid_price_1=4099.0,
                ask_price_1=4101.0,
                volume=10,
            )
        ]
    )
    store.append_order_events(
        [
            OrderEvent(
                account_id="sim-account",
                client_order_id="ord-1",
                instrument_id="rb2405",
                status="FILLED",
                total_volume=1,
                filled_volume=1,
                avg_fill_price=4100.0,
                reason="ok",
                ts_ns=1_707_628_800_000_000_000,
                trace_id="trace-1",
            )
        ]
    )
    store.close()


def test_export_parquet_partitions_script_execute(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    _seed_store(db_path)

    report_file = tmp_path / "parquet_report.json"
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/data_pipeline/export_parquet_partitions.py",
            "--analytics-db",
            str(db_path),
            "--tables",
            "market_snapshots,order_events",
            "--output-dir",
            str(tmp_path / "output"),
            "--archive-local-dir",
            str(tmp_path / "archive"),
            "--archive-bucket",
            "quant-archive",
            "--report-json",
            str(report_file),
            "--execute",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["success"] is True
    assert payload["tables"]["market_snapshots"]["status"] == "ok"
    assert payload["tables"]["order_events"]["status"] == "ok"
    assert payload["archived_objects"]
