from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from quant_hft.runtime.unified.broker import BacktestBroker
from quant_hft.runtime.unified.datafeed import BacktestDataFeed
from quant_hft.runtime.unified.engine import StrategyEngine
from quant_hft.runtime.unified.history_reader import HistoryDataReader
from quant_hft.runtime.unified.models import Bar, Offset, OrderType, Tick
from quant_hft.runtime.unified.strategy import Strategy


class _OneTickReader(HistoryDataReader):
    def __init__(self) -> None:
        self._dt = datetime(2026, 1, 1, 9, 0, 0)

    def read_ticks(self, symbol: str, start: datetime, end: datetime):  # type: ignore[override]
        tick = Tick(
            symbol=symbol,
            exchange="SHFE",
            datetime=self._dt,
            exchange_timestamp=self._dt,
            last_price=Decimal("100"),
            last_volume=1,
            ask_price1=Decimal("101"),
            ask_volume1=1,
            bid_price1=Decimal("99"),
            bid_volume1=1,
            volume=1,
            turnover=Decimal("100"),
            open_interest=1,
        )
        if start <= tick.datetime <= end:
            return iter([tick])
        return iter(())

    def read_bars(self, symbol: str, timeframe: str, start: datetime, end: datetime):  # type: ignore[override]
        del symbol
        del timeframe
        del start
        del end
        return iter(())


class _BuyOnFirstTickStrategy(Strategy):
    def __init__(self, strategy_id: str, symbol: str) -> None:
        super().__init__(strategy_id)
        self._symbol = symbol
        self.ticks = 0
        self.orders = 0

    def initialize(self) -> None:
        if self.data is None:
            raise RuntimeError("datafeed missing")
        self.data.subscribe([self._symbol], on_tick=self.on_tick, on_bar=self.on_bar)

    def on_tick(self, tick: Tick) -> None:
        self.ticks += 1
        if self.broker is None:
            raise RuntimeError("broker missing")
        if self.orders == 0:
            self.orders += 1
            self.broker.buy(
                symbol=tick.symbol,
                price=tick.last_price,
                quantity=1,
                offset=Offset.OPEN,
                order_type=OrderType.LIMIT,
            )

    def on_bar(self, bar: Bar) -> None:
        del bar


def test_unified_strategy_engine_runs_backtest_strategy() -> None:
    reader = _OneTickReader()
    feed = BacktestDataFeed(reader)
    broker = BacktestBroker(account_id="acc-1", strategy_id="s1")
    engine = StrategyEngine(mode="backtest", datafeed=feed, broker=broker)
    strategy = _BuyOnFirstTickStrategy("s1", "ag2406")
    engine.add_strategy(strategy)

    engine.run(start=reader._dt - timedelta(minutes=1), end=reader._dt + timedelta(minutes=1))

    assert strategy.ticks == 1
    assert strategy.orders == 1
    assert broker.get_orders()
    assert engine.clock.now().tzinfo == timezone.utc


def test_unified_strategy_engine_live_clock_uses_utc_timezone() -> None:
    engine = StrategyEngine(mode="live")
    assert engine.clock.now().tzinfo == timezone.utc
