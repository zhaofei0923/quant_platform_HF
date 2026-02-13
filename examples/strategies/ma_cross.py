from __future__ import annotations

from collections import deque

from quant_hft.strategy import Strategy


class MaCrossStrategy(Strategy):
    def __init__(self, symbol: str = "rb2405", fast: int = 5, slow: int = 20) -> None:
        super().__init__()
        self.symbol = symbol
        self.fast = fast
        self.slow = slow
        self._prices: deque[float] = deque(maxlen=slow)
        self._has_position = False

    def on_tick(self, tick) -> None:
        if getattr(tick, "symbol", "") != self.symbol:
            return
        price = float(getattr(tick, "last_price", 0.0))
        if price <= 0.0:
            return

        self._prices.append(price)
        if len(self._prices) < self.slow:
            return

        fast_ma = sum(list(self._prices)[-self.fast :]) / self.fast
        slow_ma = sum(self._prices) / self.slow

        if fast_ma > slow_ma and not self._has_position:
            self.buy(self.symbol, price + 1.0, 1)
            self._has_position = True
        elif fast_ma < slow_ma and self._has_position:
            self.sell(self.symbol, price - 1.0, 1)
            self._has_position = False
