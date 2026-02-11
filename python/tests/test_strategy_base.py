import pytest
from quant_hft.strategy.base import StrategyBase


def test_strategy_base_cannot_instantiate() -> None:
    with pytest.raises(TypeError):
        StrategyBase("s1")
