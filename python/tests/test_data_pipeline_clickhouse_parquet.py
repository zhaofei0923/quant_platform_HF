from __future__ import annotations

from pathlib import Path

from quant_hft.data_pipeline.clickhouse_parquet_archive import (
    ClickHouseParquetArchiveConfig,
    archive_ticks_to_parquet,
    build_market_ticks_query,
)


def test_build_market_ticks_query_with_filters() -> None:
    config = ClickHouseParquetArchiveConfig(
        clickhouse_dsn="http://clickhouse:8123/",
        output_dir="/tmp/parquet",
        table="quant_hft.market_ticks",
        start_trading_day="20260210",
        end_trading_day="20260212",
        limit=100,
    )

    query = build_market_ticks_query(config)
    assert "FROM quant_hft.market_ticks" in query
    assert "trading_day >= '20260210'" in query
    assert "trading_day <= '20260212'" in query
    assert "LIMIT 100" in query
    assert query.endswith("FORMAT JSONEachRow")


def test_archive_ticks_to_parquet_groups_by_trading_day(tmp_path: Path) -> None:
    rows = [
        {
            "instrument_id": "SHFE.ag2406",
            "trading_day": "20260211",
            "recv_ts_ns": 1,
            "last_price": 100.0,
        },
        {
            "instrument_id": "SHFE.ag2406",
            "trading_day": "20260212",
            "recv_ts_ns": 2,
            "last_price": 101.0,
        },
        {
            "instrument_id": "SHFE.ag2406",
            "trading_day": "20260212",
            "recv_ts_ns": 3,
            "last_price": 102.0,
        },
    ]

    config = ClickHouseParquetArchiveConfig(
        clickhouse_dsn="http://clickhouse:8123/",
        output_dir=tmp_path,
        allow_jsonl_fallback=True,
    )

    report = archive_ticks_to_parquet(config, fetch_rows=lambda query: rows)
    assert report.row_count == 3
    assert report.trading_days == ("20260211", "20260212")
    assert len(report.output_files) == 2
    assert report.writer_backend in {"pyarrow", "jsonl_fallback"}

    for output in report.output_files:
        assert output.exists()
        assert output.parent.name.startswith("trading_day=")
