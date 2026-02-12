from __future__ import annotations

from pathlib import Path

from quant_hft.contracts import OrderEvent
from quant_hft.data_pipeline.adapters import (
    DuckDbAnalyticsStore,
    MarketSnapshotRecord,
    MinioArchiveStore,
)


def test_duckdb_adapter_sqlite_fallback_round_trip(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    store = DuckDbAnalyticsStore(db_path, prefer_duckdb=False)

    count = store.append_market_snapshots(
        [
            MarketSnapshotRecord(
                instrument_id="rb2405",
                ts_ns=100,
                last_price=4100.5,
                bid_price_1=4100.0,
                ask_price_1=4101.0,
                volume=10,
            ),
            MarketSnapshotRecord(
                instrument_id="rb2405",
                ts_ns=200,
                last_price=4101.5,
                bid_price_1=4101.0,
                ask_price_1=4102.0,
                volume=11,
            ),
        ]
    )
    assert count == 2

    order_count = store.append_order_events(
        [
            OrderEvent(
                account_id="sim-account",
                client_order_id="ord-1",
                instrument_id="rb2405",
                status="FILLED",
                total_volume=2,
                filled_volume=2,
                avg_fill_price=4101.5,
                reason="ok",
                ts_ns=300,
                trace_id="trace-1",
            )
        ]
    )
    assert order_count == 1

    snapshots = store.query_market_snapshots("rb2405", limit=10)
    assert [item.ts_ns for item in snapshots] == [200, 100]

    events = store.query_order_events("ord-1")
    assert len(events) == 1
    assert events[0].status == "FILLED"
    assert events[0].filled_volume == 2

    csv_path = tmp_path / "market_snapshots.csv"
    written = store.export_table_to_csv("market_snapshots", csv_path)
    assert written == 2
    assert csv_path.exists()
    assert "rb2405" in csv_path.read_text(encoding="utf-8")

    store.close()


def test_minio_adapter_local_fallback_round_trip(tmp_path: Path) -> None:
    archive = MinioArchiveStore(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="quant-archive",
        local_fallback_dir=tmp_path,
    )

    archive.put_text("snapshots/2026-02-10/report.json", '{"ok":true}')
    (tmp_path / "source.wal").write_text("line-1\nline-2\n", encoding="utf-8")
    archive.put_file("snapshots/2026-02-10/raw.wal", tmp_path / "source.wal")

    objects = archive.list_objects(prefix="snapshots/2026-02-10")
    assert objects == [
        "snapshots/2026-02-10/raw.wal",
        "snapshots/2026-02-10/report.json",
    ]

    content = archive.get_text("snapshots/2026-02-10/report.json")
    assert content == '{"ok":true}'

    archive.copy_object("snapshots/2026-02-10/report.json", "snapshots/2026-02-10/report.copy.json")
    copied = archive.get_text("snapshots/2026-02-10/report.copy.json")
    assert copied == '{"ok":true}'
    stat = archive.stat_object("snapshots/2026-02-10/report.copy.json")
    assert stat["exists"] is True
    assert int(stat["size"]) > 0
    archive.remove_object("snapshots/2026-02-10/report.copy.json")
    assert archive.stat_object("snapshots/2026-02-10/report.copy.json")["exists"] is False


def test_minio_adapter_requires_fallback_or_sdk(tmp_path: Path) -> None:
    # The test environment intentionally does not install MinIO SDK.
    try:
        MinioArchiveStore(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="quant-archive",
            local_fallback_dir=None,
        )
    except RuntimeError as exc:
        assert "minio sdk is not installed" in str(exc).lower()
    else:
        raise AssertionError("expected RuntimeError when MinIO SDK is missing")


def test_duckdb_export_table_to_partitioned_parquet_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    store = DuckDbAnalyticsStore(db_path, prefer_duckdb=False)
    store.append_market_snapshots(
        [
            MarketSnapshotRecord(
                instrument_id="rb2405",
                ts_ns=1_707_628_800_000_000_000,
                last_price=4100.5,
                bid_price_1=4100.0,
                ask_price_1=4101.0,
                volume=10,
            )
        ]
    )

    artifacts = store.export_table_to_parquet_partitions(
        "market_snapshots",
        tmp_path / "partitions",
        partition_keys=("dt", "instrument_id"),
        compression="zstd",
    )
    store.close()

    assert len(artifacts) == 1
    assert artifacts[0].table == "market_snapshots"
    assert artifacts[0].row_count == 1
    assert artifacts[0].sha256
    assert artifacts[0].format in {"parquet", "jsonl_fallback"}
