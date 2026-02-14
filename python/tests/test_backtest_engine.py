from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

pytest.importorskip("quant_hft_strategy")
pytest.importorskip("quant_hft_data_feed")

from quant_hft_data_feed import Timestamp
from quant_hft_strategy import BacktestEngine, BrokerConfig, Strategy


class _OnceBuyStrategy(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self._ordered = False

    def on_tick(self, tick) -> None:
        if self._ordered:
            return
        if getattr(tick, "last_price", 0.0) <= 0.0:
            return
        self.buy(
            getattr(tick, "symbol", "rb2405"),
            float(getattr(tick, "last_price", 0.0)) + 1.0,
            1,
        )
        self._ordered = True


def _prepare_sample_dataset(root: Path) -> None:
    partition = root / "source=rb" / "trading_day=2024-01-01" / "instrument_id=rb2405"
    partition.mkdir(parents=True, exist_ok=True)

    parquet_file = partition / "part-0000.parquet"
    parquet_file.write_text("PAR1", encoding="utf-8")
    (Path(str(parquet_file) + ".meta")).write_text(
        "min_ts_ns=1704067200000000000\nmax_ts_ns=1704067201000000000\nrow_count=2\n",
        encoding="utf-8",
    )
    (Path(str(parquet_file) + ".ticks.csv")).write_text(
        "symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,ask_volume1,volume,turnover,open_interest\n"
        "rb2405,SHFE,1704067200000000000,3500.0,10,3499.0,5,3501.0,5,100,350000.0,1200000\n"
        "rb2405,SHFE,1704067201000000000,3502.0,10,3501.0,5,3503.0,5,110,385220.0,1200010\n",
        encoding="utf-8",
    )


def test_backtest_engine_runs_with_python_strategy() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        _prepare_sample_dataset(root)

        strategy = _OnceBuyStrategy()
        engine = BacktestEngine(
            str(root),
            Timestamp.from_sql("2024-01-01"),
            Timestamp.from_sql("2024-01-02"),
            strategy,
            BrokerConfig(),
        )
        engine.run()
        result = engine.get_result()

        assert result["orders"] > 0
        assert result["trades"] > 0
        assert "performance" in result
        perf = result["performance"]
        assert perf["order_count"] == result["orders"]
        assert perf["trade_count"] == result["trades"]
        assert perf["final_balance"] == pytest.approx(result["last_balance"])
