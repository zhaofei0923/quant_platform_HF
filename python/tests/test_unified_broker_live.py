from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from decimal import Decimal
from typing import Any

from quant_hft.runtime.unified.broker import LiveBroker
from quant_hft.runtime.unified.models import Offset, OrderStatus, OrderType


@dataclass
class _FakeTrader:
    callback: Any | None = None
    connected: bool = False

    def connect(self, config: dict[str, object]) -> bool:
        del config
        self.connected = True
        return True

    def disconnect(self) -> None:
        self.connected = False

    def confirm_settlement(self) -> bool:
        return True

    def on_order_status(self, callback: Any) -> None:
        self.callback = callback

    def place_order(self, request: dict[str, object]) -> bool:
        if self.callback is not None:
            self.callback(
                {
                    "account_id": request["account_id"],
                    "client_order_id": request["client_order_id"],
                    "instrument_id": request["instrument_id"],
                    "status": "filled",
                    "total_volume": request["volume"],
                    "filled_volume": request["volume"],
                    "avg_fill_price": request.get("price", 0.0),
                    "trace_id": request.get("trace_id", ""),
                    "trade_id": f"tr-{request['client_order_id']}",
                    "offset": request.get("offset", "open"),
                    "side": request.get("side", "buy"),
                }
            )
        return True

    def cancel_order(self, client_order_id: str, trace_id: str) -> bool:
        del trace_id
        if self.callback is not None:
            self.callback(
                {
                    "client_order_id": client_order_id,
                    "status": "canceled",
                    "filled_volume": 0,
                    "total_volume": 0,
                    "avg_fill_price": 0,
                }
            )
        return True


def test_live_broker_connect_place_and_cancel() -> None:
    trader = _FakeTrader()
    broker = LiveBroker(trader, account_id="acc-1", strategy_id="s1")
    order_events = []
    trade_events = []
    broker.on_order_status(lambda order: order_events.append(order))
    broker.on_trade(lambda trade: trade_events.append(trade))

    assert broker.connect(
        {"market_front_address": "tcp://sim-md"},
        settlement_confirm_required=True,
    )
    order = broker.buy(
        symbol="ag2406",
        price=Decimal("5000"),
        quantity=1,
        offset=Offset.OPEN,
        order_type=OrderType.LIMIT,
    )
    assert order.order_id
    assert broker.cancel_order(order.order_id) is True

    orders = broker.get_orders()
    assert orders
    assert any(item.status in {OrderStatus.FILLED, OrderStatus.CANCELED} for item in orders)
    assert order_events
    assert trade_events
    assert any(
        item.updated_at is not None and item.updated_at.tzinfo == timezone.utc
        for item in order_events
    )
    assert all(item.trade_time.tzinfo == timezone.utc for item in trade_events)
    broker.disconnect()
