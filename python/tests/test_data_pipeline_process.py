from __future__ import annotations

import json
from pathlib import Path

from quant_hft.contracts import OrderEvent
from quant_hft.data_pipeline.adapters import DuckDbAnalyticsStore, MarketSnapshotRecord
from quant_hft.data_pipeline.process import (
    ArchiveConfig,
    DataPipelineConfig,
    DataPipelineProcess,
)
from quant_hft.ops.monitoring import InMemoryObservability


def _seed_store(db_path: Path) -> DuckDbAnalyticsStore:
    store = DuckDbAnalyticsStore(db_path, prefer_duckdb=False)
    store.append_market_snapshots(
        [
            MarketSnapshotRecord(
                instrument_id="rb2405",
                ts_ns=1_000,
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
                client_order_id="ord-0001",
                instrument_id="rb2405",
                status="FILLED",
                total_volume=1,
                filled_volume=1,
                avg_fill_price=4100.0,
                reason="ok",
                ts_ns=2_000,
                trace_id="trace-0001",
            )
        ]
    )
    return store


def test_data_pipeline_run_once_exports_and_archives(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    _seed_store(db_path).close()

    config = DataPipelineConfig(
        analytics_db_path=db_path,
        export_dir=tmp_path / "exports",
        archive=ArchiveConfig(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="quant-archive",
            local_fallback_dir=tmp_path / "archive",
        ),
        prefer_duckdb=False,
    )
    process = DataPipelineProcess(config)
    report = process.run_once()
    process.close()

    assert report.exported_rows["market_snapshots"] == 1
    assert report.exported_rows["order_events"] == 1
    assert report.archived_objects_count >= 3
    assert report.data_dictionary_violations == ()

    assert report.manifest_path.exists()
    manifest = json.loads(report.manifest_path.read_text(encoding="utf-8"))
    assert manifest["exported_rows"]["market_snapshots"] == 1
    assert manifest["exported_rows"]["order_events"] == 1

    archive_root = tmp_path / "archive" / "quant-archive"
    archived_manifest = list(archive_root.rglob("manifest.json"))
    assert len(archived_manifest) == 1


def test_data_pipeline_run_loop_honors_max_iterations(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    _seed_store(db_path).close()

    config = DataPipelineConfig(
        analytics_db_path=db_path,
        export_dir=tmp_path / "exports",
        archive=ArchiveConfig(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="quant-archive",
            local_fallback_dir=tmp_path / "archive",
        ),
        prefer_duckdb=False,
        interval_seconds=0.0,
    )
    process = DataPipelineProcess(config)
    reports = process.run_loop(max_iterations=2)
    process.close()

    assert len(reports) == 2
    assert all(report.exported_rows["market_snapshots"] == 1 for report in reports)
    assert all(report.data_dictionary_violations == () for report in reports)


def test_data_pipeline_emits_observability_metrics_and_alerts(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    empty_store = DuckDbAnalyticsStore(db_path, prefer_duckdb=False)
    empty_store.close()

    obs = InMemoryObservability()
    config = DataPipelineConfig(
        analytics_db_path=db_path,
        export_dir=tmp_path / "exports",
        archive=ArchiveConfig(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="quant-archive",
            local_fallback_dir=tmp_path / "archive",
        ),
        prefer_duckdb=False,
    )
    process = DataPipelineProcess(config, observability=obs)
    report = process.run_once()
    process.close()

    snapshot = obs.snapshot()
    assert any(item.name == "quant_hft_data_pipeline_run_latency_ms" for item in snapshot.metrics)
    assert any(item.trace_id == report.trace_id for item in snapshot.traces)
    assert "PIPELINE_EXPORT_EMPTY" in report.alert_codes
    assert any(item.code == "PIPELINE_EXPORT_EMPTY" for item in snapshot.alerts)
