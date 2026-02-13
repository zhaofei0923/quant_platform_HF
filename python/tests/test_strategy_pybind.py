from __future__ import annotations

import pytest

pytest.importorskip("quant_hft_strategy")

from quant_hft_strategy import Strategy


class _CallbackStrategy(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self.tick_called = False

    def on_tick(self, tick) -> None:
        self.tick_called = True


def test_strategy_subclass_instantiation() -> None:
    strategy = _CallbackStrategy()
    assert strategy is not None


def test_strategy_order_methods_callable() -> None:
    strategy = _CallbackStrategy()
    with pytest.raises(RuntimeError):
        strategy.buy("rb2405", 3500.0, 1)
    with pytest.raises(RuntimeError):
        strategy.sell("rb2405", 3501.0, 1)
    with pytest.raises(RuntimeError):
        strategy.cancel_order("ord-1")
