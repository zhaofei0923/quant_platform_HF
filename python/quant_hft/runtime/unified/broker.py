from __future__ import annotations

import threading
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol

from quant_hft.runtime.unified.models import (
    Account,
    Direction,
    Offset,
    Order,
    OrderStatus,
    OrderType,
    Position,
    Trade,
)


def _status_from_raw(raw: object) -> OrderStatus:
    if isinstance(raw, int):
        mapping = {
            0: OrderStatus.PENDING,
            1: OrderStatus.SUBMITTED,
            2: OrderStatus.PARTIAL_FILLED,
            3: OrderStatus.FILLED,
            4: OrderStatus.CANCELED,
            5: OrderStatus.REJECTED,
        }
        return mapping.get(raw, OrderStatus.PENDING)
    text = str(raw).strip().lower()
    mapping_text = {
        "new": OrderStatus.PENDING,
        "pending": OrderStatus.PENDING,
        "accepted": OrderStatus.SUBMITTED,
        "submitted": OrderStatus.SUBMITTED,
        "partial_filled": OrderStatus.PARTIAL_FILLED,
        "partially_filled": OrderStatus.PARTIAL_FILLED,
        "filled": OrderStatus.FILLED,
        "canceled": OrderStatus.CANCELED,
        "cancelled": OrderStatus.CANCELED,
        "rejected": OrderStatus.REJECTED,
    }
    return mapping_text.get(text, OrderStatus.PENDING)


def _offset_from_text(raw: object) -> Offset:
    text = str(raw).strip().lower()
    if text in {"close_today", "closetoday"}:
        return Offset.CLOSE_TODAY
    if text in {"close_yesterday", "closeyesterday"}:
        return Offset.CLOSE_YESTERDAY
    if text == "close":
        return Offset.CLOSE
    return Offset.OPEN


def _direction_from_text(raw: object) -> Direction:
    text = str(raw).strip().lower()
    return Direction.SELL if text in {"sell", "1", "s"} else Direction.BUY


def _to_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return default
    return default


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TraderAdapter(Protocol):
    def connect(self, config: dict[str, object]) -> bool: ...

    def disconnect(self) -> None: ...

    def confirm_settlement(self) -> bool: ...

    def place_order(self, request: dict[str, object]) -> bool: ...

    def cancel_order(self, client_order_id: str, trace_id: str) -> bool: ...

    def on_order_status(self, callback: Callable[[dict[str, object]], None]) -> None: ...


class Broker(ABC):
    @abstractmethod
    def buy(
        self,
        symbol: str,
        price: Decimal | None,
        quantity: int,
        offset: Offset = Offset.OPEN,
        order_type: OrderType = OrderType.LIMIT,
        **kwargs: object,
    ) -> Order:
        raise NotImplementedError

    @abstractmethod
    def sell(
        self,
        symbol: str,
        price: Decimal | None,
        quantity: int,
        offset: Offset = Offset.CLOSE,
        order_type: OrderType = OrderType.LIMIT,
        **kwargs: object,
    ) -> Order:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_orders(self, strategy_id: str | None = None) -> list[Order]:
        raise NotImplementedError

    @abstractmethod
    def get_position(
        self,
        symbol: str | None = None,
        strategy_id: str | None = None,
    ) -> dict[str, Position]:
        raise NotImplementedError

    @abstractmethod
    def get_account(self) -> Account:
        raise NotImplementedError

    @abstractmethod
    def on_order_status(self, callback: Callable[[Order], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def on_trade(self, callback: Callable[[Trade], None]) -> None:
        raise NotImplementedError


class BacktestBroker(Broker):
    def __init__(self, account_id: str = "backtest", strategy_id: str = "backtest") -> None:
        self._account = Account(account_id=account_id)
        self._strategy_id = strategy_id
        self._orders: dict[str, Order] = {}
        self._positions: dict[str, Position] = {}
        self._order_callbacks: list[Callable[[Order], None]] = []
        self._trade_callbacks: list[Callable[[Trade], None]] = []
        self._lock = threading.Lock()

    def buy(
        self,
        symbol: str,
        price: Decimal | None,
        quantity: int,
        offset: Offset = Offset.OPEN,
        order_type: OrderType = OrderType.LIMIT,
        **kwargs: object,
    ) -> Order:
        del kwargs
        return self._submit(Direction.BUY, symbol, price, quantity, offset, order_type)

    def sell(
        self,
        symbol: str,
        price: Decimal | None,
        quantity: int,
        offset: Offset = Offset.CLOSE,
        order_type: OrderType = OrderType.LIMIT,
        **kwargs: object,
    ) -> Order:
        del kwargs
        return self._submit(Direction.SELL, symbol, price, quantity, offset, order_type)

    def cancel_order(self, order_id: str) -> bool:
        with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                return False
            if order.status in {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED}:
                return False
            order.status = OrderStatus.CANCELED
            order.updated_at = _utc_now()
            callbacks = list(self._order_callbacks)
        for callback in callbacks:
            callback(order)
        return True

    def get_orders(self, strategy_id: str | None = None) -> list[Order]:
        with self._lock:
            orders = list(self._orders.values())
        if strategy_id is None:
            return orders
        return [order for order in orders if order.strategy_id == strategy_id]

    def get_position(
        self,
        symbol: str | None = None,
        strategy_id: str | None = None,
    ) -> dict[str, Position]:
        del strategy_id
        with self._lock:
            positions = dict(self._positions)
        if symbol is None:
            return positions
        return {symbol: positions[symbol]} if symbol in positions else {}

    def get_account(self) -> Account:
        with self._lock:
            return Account(**self._account.__dict__)

    def on_order_status(self, callback: Callable[[Order], None]) -> None:
        with self._lock:
            self._order_callbacks.append(callback)

    def on_trade(self, callback: Callable[[Trade], None]) -> None:
        with self._lock:
            self._trade_callbacks.append(callback)

    def _submit(
        self,
        direction: Direction,
        symbol: str,
        price: Decimal | None,
        quantity: int,
        offset: Offset,
        order_type: OrderType,
    ) -> Order:
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        order_id = f"bt-{uuid.uuid4().hex}"
        now = _utc_now()
        execution_price = price if price is not None else Decimal("0")
        order = Order(
            order_id=order_id,
            strategy_id=self._strategy_id,
            symbol=symbol,
            exchange="",
            direction=direction,
            offset=offset,
            order_type=order_type,
            price=price,
            quantity=quantity,
            filled_quantity=quantity,
            avg_fill_price=execution_price,
            status=OrderStatus.FILLED,
            created_at=now,
            updated_at=now,
            account_id=self._account.account_id,
        )
        trade = Trade(
            trade_id=f"bt-trade-{uuid.uuid4().hex}",
            order_id=order_id,
            strategy_id=order.strategy_id,
            symbol=symbol,
            exchange="",
            direction=direction,
            offset=offset,
            price=execution_price,
            quantity=quantity,
            trade_time=now,
            commission=Decimal("0"),
            account_id=self._account.account_id,
        )
        with self._lock:
            self._orders[order_id] = order
            self._apply_trade_to_position_locked(trade)
            order_callbacks = list(self._order_callbacks)
            trade_callbacks = list(self._trade_callbacks)
        for order_callback in order_callbacks:
            order_callback(order)
        for trade_callback in trade_callbacks:
            trade_callback(trade)
        return order

    def _apply_trade_to_position_locked(self, trade: Trade) -> None:
        position = self._positions.get(
            trade.symbol,
            Position(
                symbol=trade.symbol,
                exchange=trade.exchange,
                strategy_id=trade.strategy_id,
                account_id=self._account.account_id,
            ),
        )
        qty = int(trade.quantity)
        if trade.offset == Offset.OPEN:
            if trade.direction == Direction.BUY:
                position.long_qty += qty
                position.long_today_qty += qty
            else:
                position.short_qty += qty
                position.short_today_qty += qty
        else:
            if trade.direction == Direction.BUY:
                position.short_qty = max(0, position.short_qty - qty)
            else:
                position.long_qty = max(0, position.long_qty - qty)
        position.update_time = _utc_now()
        self._positions[trade.symbol] = position
        self._account.update_time = _utc_now()


class LiveBroker(Broker):
    def __init__(self, trader_adapter: TraderAdapter, account_id: str, strategy_id: str) -> None:
        self._trader = trader_adapter
        self._account = Account(account_id=account_id)
        self._strategy_id = strategy_id
        self._orders: dict[str, Order] = {}
        self._positions: dict[str, Position] = {}
        self._order_callbacks: list[Callable[[Order], None]] = []
        self._trade_callbacks: list[Callable[[Trade], None]] = []
        self._lock = threading.Lock()
        self._bind_callbacks()

    def connect(self, config: dict[str, object], settlement_confirm_required: bool = True) -> bool:
        connect = self._trader.connect
        if not connect(config):
            return False
        if settlement_confirm_required:
            confirm = self._trader.confirm_settlement
            return bool(confirm())
        return True

    def disconnect(self) -> None:
        disconnect = self._trader.disconnect
        disconnect()

    def buy(
        self,
        symbol: str,
        price: Decimal | None,
        quantity: int,
        offset: Offset = Offset.OPEN,
        order_type: OrderType = OrderType.LIMIT,
        **kwargs: object,
    ) -> Order:
        return self._place(Direction.BUY, symbol, price, quantity, offset, order_type, kwargs)

    def sell(
        self,
        symbol: str,
        price: Decimal | None,
        quantity: int,
        offset: Offset = Offset.CLOSE,
        order_type: OrderType = OrderType.LIMIT,
        **kwargs: object,
    ) -> Order:
        return self._place(Direction.SELL, symbol, price, quantity, offset, order_type, kwargs)

    def cancel_order(self, order_id: str) -> bool:
        order = self._orders.get(order_id)
        if order is None:
            return False
        cancel = self._trader.cancel_order
        return bool(cancel(order_id, order.message or order_id))

    def get_orders(self, strategy_id: str | None = None) -> list[Order]:
        with self._lock:
            orders = list(self._orders.values())
        if strategy_id is None:
            return orders
        return [order for order in orders if order.strategy_id == strategy_id]

    def get_position(
        self,
        symbol: str | None = None,
        strategy_id: str | None = None,
    ) -> dict[str, Position]:
        del strategy_id
        with self._lock:
            positions = dict(self._positions)
        if symbol is None:
            return positions
        return {symbol: positions[symbol]} if symbol in positions else {}

    def get_account(self) -> Account:
        with self._lock:
            return Account(**self._account.__dict__)

    def on_order_status(self, callback: Callable[[Order], None]) -> None:
        with self._lock:
            self._order_callbacks.append(callback)

    def on_trade(self, callback: Callable[[Trade], None]) -> None:
        with self._lock:
            self._trade_callbacks.append(callback)

    def _bind_callbacks(self) -> None:
        def _on_order(payload: dict[str, object]) -> None:
            now = _utc_now()
            order_id = str(payload.get("client_order_id", ""))
            status = _status_from_raw(payload.get("status"))
            with self._lock:
                previous = self._orders.get(order_id)
                order = Order(
                    order_id=order_id,
                    strategy_id=self._strategy_id,
                    symbol=str(payload.get("instrument_id", "")),
                    exchange=str(payload.get("exchange_id", "")),
                    direction=_direction_from_text(payload.get("side", "buy")),
                    offset=_offset_from_text(payload.get("offset", "open")),
                    order_type=OrderType.LIMIT,
                    price=Decimal(str(payload.get("avg_fill_price", "0"))),
                    quantity=_to_int(payload.get("total_volume", 0), 0),
                    filled_quantity=_to_int(payload.get("filled_volume", 0), 0),
                    avg_fill_price=Decimal(str(payload.get("avg_fill_price", "0"))),
                    status=status,
                    created_at=previous.created_at if previous else now,
                    updated_at=now,
                    message=str(payload.get("trace_id", "")),
                    account_id=str(payload.get("account_id", self._account.account_id)),
                )
                self._orders[order_id] = order
                order_callbacks = list(self._order_callbacks)
                trade_callbacks = list(self._trade_callbacks)
            for order_callback in order_callbacks:
                order_callback(order)

            if order.filled_quantity > 0 and status in {
                OrderStatus.FILLED,
                OrderStatus.PARTIAL_FILLED,
            }:
                trade = Trade(
                    trade_id=str(payload.get("trade_id", f"trade-{order_id}-{now.timestamp()}")),
                    order_id=order_id,
                    strategy_id=self._strategy_id,
                    symbol=order.symbol,
                    exchange=order.exchange,
                    direction=order.direction,
                    offset=order.offset,
                    price=order.avg_fill_price,
                    quantity=order.filled_quantity,
                    trade_time=now,
                    commission=Decimal("0"),
                    account_id=order.account_id,
                )
                with self._lock:
                    self._apply_trade_to_position_locked(trade)
                for trade_callback in trade_callbacks:
                    trade_callback(trade)

        self._trader.on_order_status(_on_order)

    def _place(
        self,
        direction: Direction,
        symbol: str,
        price: Decimal | None,
        quantity: int,
        offset: Offset,
        order_type: OrderType,
        kwargs: dict[str, object],
    ) -> Order:
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        order_id = str(kwargs.get("order_id", f"{self._strategy_id}-{uuid.uuid4().hex[:16]}"))
        trace_id = str(kwargs.get("trace_id", order_id))
        request = {
            "account_id": self._account.account_id,
            "client_order_id": order_id,
            "strategy_id": kwargs.get("strategy_id", self._strategy_id),
            "instrument_id": symbol,
            "side": direction.value,
            "offset": offset.value,
            "order_type": order_type.value,
            "volume": quantity,
            "price": float(price) if price is not None else 0.0,
            "trace_id": trace_id,
        }
        place = self._trader.place_order
        accepted = bool(place(request))
        now = _utc_now()
        order = Order(
            order_id=order_id,
            strategy_id=self._strategy_id,
            symbol=symbol,
            exchange="",
            direction=direction,
            offset=offset,
            order_type=order_type,
            price=price,
            quantity=quantity,
            status=OrderStatus.SUBMITTED if accepted else OrderStatus.REJECTED,
            created_at=now,
            updated_at=now,
            message=trace_id,
            account_id=self._account.account_id,
        )
        with self._lock:
            self._orders[order.order_id] = order
        if not accepted:
            raise RuntimeError("live broker place_order rejected")
        return order

    def _apply_trade_to_position_locked(self, trade: Trade) -> None:
        position = self._positions.get(
            trade.symbol,
            Position(
                symbol=trade.symbol,
                exchange=trade.exchange,
                strategy_id=trade.strategy_id,
                account_id=self._account.account_id,
            ),
        )
        qty = int(trade.quantity)
        if trade.offset == Offset.OPEN:
            if trade.direction == Direction.BUY:
                position.long_qty += qty
                position.long_today_qty += qty
            else:
                position.short_qty += qty
                position.short_today_qty += qty
        else:
            if trade.direction == Direction.BUY:
                position.short_qty = max(0, position.short_qty - qty)
            else:
                position.long_qty = max(0, position.long_qty - qty)
        position.update_time = _utc_now()
        self._positions[trade.symbol] = position
        self._account.update_time = _utc_now()
