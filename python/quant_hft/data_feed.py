"""Data Feed Python convenience wrapper."""

try:
    from quant_hft_data_feed import BacktestDataFeed as _BacktestDataFeed
    from quant_hft_data_feed import LiveDataFeed as _LiveDataFeed
    from quant_hft_data_feed import Timestamp
except ModuleNotFoundError:  # pragma: no cover
    class Timestamp:
        def __init__(self, text: str = "1970-01-01 00:00:00") -> None:
            self._text = text

        @staticmethod
        def from_sql(text: str) -> "Timestamp":
            return Timestamp(text)

        @staticmethod
        def now() -> "Timestamp":
            return Timestamp()

        def to_sql(self) -> str:
            return self._text

    class _BacktestDataFeed:
        def __init__(self, parquet_root: str, start: Timestamp, end: Timestamp) -> None:
            self._parquet_root = parquet_root
            self._start = start
            self._end = end

        def subscribe(self, symbols, on_tick, on_bar=None):
            self._symbols = symbols
            self._on_tick = on_tick
            self._on_bar = on_bar

        def get_history_bars(self, symbol, start, end, timeframe):
            return []

        def get_history_ticks(self, symbol, start, end):
            return []

        def run(self):
            return None

        def stop(self):
            return None

        def current_time(self):
            return self._start

        def is_live(self):
            return False

    class _LiveDataFeed:
        def subscribe(self, symbols, on_tick, on_bar=None):
            self._symbols = symbols
            self._on_tick = on_tick
            self._on_bar = on_bar

        def get_history_bars(self, symbol, start, end, timeframe):
            return []

        def get_history_ticks(self, symbol, start, end):
            return []

        def run(self):
            return None

        def stop(self):
            return None

        def current_time(self):
            return Timestamp.now()

        def is_live(self):
            return True


class BacktestDataFeed(_BacktestDataFeed):
    """Backtest data feed with convenient string datetime support."""

    def __init__(self, parquet_root: str, start, end):
        if isinstance(start, str):
            start = Timestamp.from_sql(start)
        if isinstance(end, str):
            end = Timestamp.from_sql(end)
        super().__init__(parquet_root, start, end)


class LiveDataFeed(_LiveDataFeed):
    """Live data feed stub (placeholder for real-time data)."""

    pass
