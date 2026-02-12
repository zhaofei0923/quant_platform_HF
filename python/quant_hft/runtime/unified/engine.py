from __future__ import annotations

from datetime import datetime, timezone

from quant_hft.runtime.unified.broker import BacktestBroker, Broker
from quant_hft.runtime.unified.datafeed import BacktestDataFeed, DataFeed
from quant_hft.runtime.unified.history_reader import ParquetHistoryReader
from quant_hft.runtime.unified.strategy import Strategy


class SystemClock:
    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc)


class BacktestClock:
    def __init__(self) -> None:
        self._now = datetime.now(timezone.utc)

    def set_now(self, value: datetime) -> None:
        self._now = value

    def now(self) -> datetime:
        return self._now


class StrategyEngine:
    def __init__(
        self,
        mode: str = "backtest",
        *,
        datafeed: DataFeed | None = None,
        broker: Broker | None = None,
    ) -> None:
        if mode not in {"backtest", "paper", "live"}:
            raise ValueError("mode must be one of: backtest, paper, live")
        self.mode = mode
        self.strategies: list[Strategy] = []
        self.datafeed: DataFeed | None = datafeed
        self.broker: Broker | None = broker
        self.clock: SystemClock | BacktestClock = (
            SystemClock() if mode == "live" else BacktestClock()
        )

        if self.datafeed is None and mode == "backtest":
            self.datafeed = BacktestDataFeed(ParquetHistoryReader("./backtest_data"))
        if self.broker is None and mode == "backtest":
            self.broker = BacktestBroker()

    def add_strategy(self, strategy: Strategy) -> None:
        if self.datafeed is None or self.broker is None:
            raise RuntimeError("strategy engine requires datafeed and broker")
        strategy.data = self.datafeed
        strategy.broker = self.broker
        strategy.clock = self.clock
        self.broker.on_order_status(strategy.on_order)
        self.broker.on_trade(strategy.on_trade)
        strategy.initialize()
        self.strategies.append(strategy)

    def run(self, **kwargs: object) -> None:
        if self.datafeed is None:
            raise RuntimeError("datafeed is not configured")
        self.datafeed.run(**kwargs)

    def stop(self) -> None:
        if self.datafeed is not None:
            self.datafeed.stop()
