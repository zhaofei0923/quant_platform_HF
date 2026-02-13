from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class TestTraderAdapter:
    connect_ok: bool = True
    confirm_ok: bool = True

    def __post_init__(self) -> None:
        self.connected = False
        self.orders: list[dict[str, object]] = []
        self.order_callback: Any | None = None

    def connect(self, config: dict[str, object]) -> bool:
        del config
        self.connected = self.connect_ok
        return self.connect_ok

    def disconnect(self) -> None:
        self.connected = False

    def confirm_settlement(self) -> bool:
        return self.confirm_ok

    def place_order(self, request: dict[str, object]) -> bool:
        self.orders.append(request)
        if self.order_callback is not None:
            self.order_callback(
                {
                    "account_id": str(request.get("account_id", "")),
                    "client_order_id": str(request.get("client_order_id", "")),
                    "instrument_id": str(request.get("instrument_id", "")),
                    "status": "ACCEPTED",
                    "total_volume": int(request.get("volume", 0)),
                    "filled_volume": int(request.get("volume", 0)),
                    "avg_fill_price": float(request.get("price", 0.0)),
                    "reason": "accepted",
                    "trace_id": str(request.get("trace_id", "")),
                    "ts_ns": int(request.get("ts_ns", 0) or 0),
                }
            )
        return True

    def cancel_order(self, client_order_id: str, trace_id: str) -> bool:
        del client_order_id
        del trace_id
        return True

    def on_order_status(self, callback: Any) -> None:
        self.order_callback = callback


@dataclass
class TestMdAdapter:
    connect_ok: bool = True
    subscribe_ok: bool = True

    def __post_init__(self) -> None:
        self.connected = False
        self.subscribed: list[str] = []
        self.tick_callback: Any | None = None

    def connect(self, config: dict[str, object]) -> bool:
        del config
        self.connected = self.connect_ok
        return self.connect_ok

    def disconnect(self) -> None:
        self.connected = False

    def subscribe(self, instruments: list[str]) -> bool:
        self.subscribed = list(instruments)
        return self.subscribe_ok

    def unsubscribe(self, instruments: list[str]) -> bool:
        del instruments
        self.subscribed = []
        return True

    def on_tick(self, callback: Any) -> None:
        self.tick_callback = callback

    def emit_tick(self, payload: dict[str, object]) -> None:
        if self.tick_callback is not None:
            self.tick_callback(payload)
