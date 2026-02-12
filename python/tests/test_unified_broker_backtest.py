from __future__ import annotations

from datetime import timezone
from decimal import Decimal

from quant_hft.runtime.unified.broker import BacktestBroker
from quant_hft.runtime.unified.models import Offset, OrderStatus, OrderType


def test_backtest_broker_fills_order_and_updates_position() -> None:
    broker = BacktestBroker(account_id="acc-1", strategy_id="s1")
    orders = []
    trades = []
    broker.on_order_status(lambda order: orders.append(order))
    broker.on_trade(lambda trade: trades.append(trade))

    order = broker.buy(
        symbol="ag2406",
        price=Decimal("5000"),
        quantity=2,
        offset=Offset.OPEN,
        order_type=OrderType.LIMIT,
    )

    assert order.status == OrderStatus.FILLED
    assert len(orders) == 1
    assert len(trades) == 1
    position = broker.get_position("ag2406")["ag2406"]
    assert position.long_qty == 2
    assert order.created_at is not None
    assert order.updated_at is not None
    assert order.created_at.tzinfo == timezone.utc
    assert order.updated_at.tzinfo == timezone.utc
    assert trades[0].trade_time.tzinfo == timezone.utc
    assert position.update_time.tzinfo == timezone.utc
    assert broker.get_account().update_time.tzinfo == timezone.utc
    assert broker.cancel_order(order.order_id) is False


def test_backtest_broker_close_reduces_position() -> None:
    broker = BacktestBroker(account_id="acc-1", strategy_id="s1")
    broker.buy("ag2406", Decimal("5000"), 3, offset=Offset.OPEN)
    broker.sell("ag2406", Decimal("5001"), 2, offset=Offset.CLOSE)
    position = broker.get_position("ag2406")["ag2406"]
    assert position.long_qty == 1
