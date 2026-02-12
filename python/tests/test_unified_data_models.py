from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from quant_hft.runtime.unified.models import (
    Account,
    Direction,
    Exchange,
    Offset,
    Order,
    OrderStatus,
    OrderType,
    Position,
    Tick,
    Trade,
)


def test_unified_models_basic_fields() -> None:
    exchange = Exchange(id="SHFE", name="Shanghai Futures Exchange")
    now = datetime.now(timezone.utc)
    tick = Tick(
        symbol="ag2406",
        exchange=exchange.id,
        datetime=now,
        exchange_timestamp=now,
        last_price=Decimal("5234.5"),
        last_volume=10,
        ask_price1=Decimal("5235"),
        ask_volume1=5,
        bid_price1=Decimal("5234"),
        bid_volume1=6,
        volume=100,
        turnover=Decimal("123456"),
        open_interest=200,
    )
    assert tick.symbol == "ag2406"
    assert tick.last_price == Decimal("5234.5")

    order = Order(
        order_id="ord-1",
        strategy_id="s1",
        symbol="ag2406",
        exchange="SHFE",
        direction=Direction.BUY,
        offset=Offset.OPEN,
        order_type=OrderType.LIMIT,
        price=Decimal("5234.5"),
        quantity=2,
    )
    assert order.status == OrderStatus.PENDING
    assert order.commission == Decimal("0")

    trade = Trade(
        trade_id="tr-1",
        order_id="ord-1",
        strategy_id="s1",
        symbol="ag2406",
        exchange="SHFE",
        direction=Direction.BUY,
        offset=Offset.OPEN,
        price=Decimal("5234.5"),
        quantity=2,
        trade_time=now,
        commission=Decimal("1.2"),
    )
    assert trade.price == Decimal("5234.5")

    position = Position(
        symbol="ag2406",
        exchange="SHFE",
        strategy_id="s1",
        account_id="acc-1",
    )
    account = Account(account_id="acc-1")
    assert position.long_qty == 0
    assert account.balance == Decimal("0")
    assert position.update_time.tzinfo == timezone.utc
    assert account.update_time.tzinfo == timezone.utc
