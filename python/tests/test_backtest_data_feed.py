from __future__ import annotations

import tempfile

import pytest
from quant_hft.data_feed import BacktestDataFeed, Timestamp


@pytest.mark.skip(reason="Requires pre-generated parquet conversion pipeline in CI artifact")
def test_backtest_data_feed_loads_ticks() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        feed = BacktestDataFeed(tmpdir, "2024-01-01", "2024-01-02")
        ticks = []

        def on_tick(tick) -> None:
            ticks.append(tick)

        feed.subscribe(["rb2405"], on_tick)
        feed.run()
        assert len(ticks) > 0


def test_backtest_data_feed_get_history_ticks_api() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        feed = BacktestDataFeed(tmpdir, "2024-01-01", "2024-01-02")
        ticks = feed.get_history_ticks(
            "rb2405",
            Timestamp.from_sql("2024-01-01"),
            Timestamp.from_sql("2024-01-02"),
        )
        assert isinstance(ticks, list)
