from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any


def _required(config: dict[str, object], key: str) -> str:
    value = str(config.get(key, "")).strip()
    if not value:
        raise ValueError(f"missing required config field: {key}")
    return value


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


def _to_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return default
    return default


class _FallbackCTPTraderAdapter:
    """Python fallback wrapper for environments without compiled pybind module."""

    def __init__(
        self,
        query_qps_limit: int = 10,
        dispatcher_workers: int = 1,
        python_queue_size: int = 5000,
    ) -> None:
        del query_qps_limit
        del dispatcher_workers
        del python_queue_size
        self._connected = False
        self._ready = False
        self._settlement_confirmed = False
        self._order_callback: Callable[[dict[str, object]], None] | None = None

    def connect(self, config: dict[str, object]) -> bool:
        _required(config, "market_front_address")
        _required(config, "trader_front_address")
        _required(config, "broker_id")
        _required(config, "user_id")
        _required(config, "investor_id")
        _required(config, "password")
        self._connected = True
        self._ready = False
        self._settlement_confirmed = False
        return True

    def disconnect(self) -> None:
        self._connected = False
        self._ready = False
        self._settlement_confirmed = False

    def confirm_settlement(self) -> bool:
        if not self._connected:
            return False
        self._settlement_confirmed = True
        self._ready = True
        return True

    def is_ready(self) -> bool:
        return self._ready

    def place_order(self, request: dict[str, object]) -> bool:
        if not self._ready or not self._settlement_confirmed:
            return False
        strategy_id = str(request.get("strategy_id", "")).strip()
        if not strategy_id:
            return False
        order = {
            "account_id": str(request.get("account_id", "")),
            "client_order_id": str(request.get("client_order_id", "")),
            "instrument_id": str(request.get("instrument_id", "")),
            "status": "ACCEPTED",
            "total_volume": _to_int(request.get("volume", 0)),
            "filled_volume": 0,
            "avg_fill_price": _to_float(request.get("price", request.get("limit_price", 0.0))),
            "reason": "accepted",
            "trace_id": str(request.get("trace_id", request.get("client_order_id", ""))),
            "ts_ns": time.time_ns(),
        }
        if self._order_callback is not None:
            self._order_callback(order)
        return True

    def cancel_order(self, client_order_id: str, trace_id: str) -> bool:
        if not self._ready:
            return False
        if self._order_callback is not None:
            self._order_callback(
                {
                    "account_id": "",
                    "client_order_id": client_order_id,
                    "instrument_id": "",
                    "status": "CANCELED",
                    "total_volume": 0,
                    "filled_volume": 0,
                    "avg_fill_price": 0.0,
                    "reason": "canceled",
                    "trace_id": trace_id,
                    "ts_ns": time.time_ns(),
                }
            )
        return True

    def on_order_status(self, callback: Callable[[dict[str, object]], None]) -> None:
        self._order_callback = callback


class _FallbackCTPMdAdapter:
    """Python fallback wrapper for environments without compiled pybind module."""

    def __init__(
        self,
        query_qps_limit: int = 10,
        dispatcher_workers: int = 1,
        python_queue_size: int = 5000,
    ) -> None:
        del query_qps_limit
        del dispatcher_workers
        del python_queue_size
        self._connected = False
        self._tick_callback: Callable[[dict[str, object]], None] | None = None
        self._subscribed: set[str] = set()
        self._tick_thread: threading.Thread | None = None
        self._stop = False

    def connect(self, config: dict[str, object]) -> bool:
        _required(config, "market_front_address")
        _required(config, "trader_front_address")
        _required(config, "broker_id")
        _required(config, "user_id")
        _required(config, "investor_id")
        _required(config, "password")
        self._connected = True
        return True

    def disconnect(self) -> None:
        self._connected = False
        self._stop = True
        if self._tick_thread is not None and self._tick_thread.is_alive():
            self._tick_thread.join(timeout=0.2)
        self._tick_thread = None
        self._subscribed.clear()

    def is_ready(self) -> bool:
        return self._connected

    def subscribe(self, instruments: list[str]) -> bool:
        if not self._connected:
            return False
        for instrument in instruments:
            if instrument:
                self._subscribed.add(instrument)
        if self._tick_callback is not None and self._tick_thread is None:
            self._start_tick_loop()
        return True

    def unsubscribe(self, instruments: list[str]) -> bool:
        if not self._connected:
            return False
        for instrument in instruments:
            self._subscribed.discard(instrument)
        return True

    def on_tick(self, callback: Callable[[dict[str, object]], None]) -> None:
        self._tick_callback = callback

    def _start_tick_loop(self) -> None:
        self._stop = False

        def _loop() -> None:
            while not self._stop:
                callback = self._tick_callback
                if callback is not None:
                    for instrument in sorted(self._subscribed):
                        callback(
                            {
                                "instrument_id": instrument,
                                "last_price": 0.0,
                                "bid_price_1": 0.0,
                                "ask_price_1": 0.0,
                                "bid_volume_1": 0,
                                "ask_volume_1": 0,
                                "volume": 0,
                                "ts_ns": time.time_ns(),
                            }
                        )
                time.sleep(0.05)

        self._tick_thread = threading.Thread(target=_loop, daemon=True)
        self._tick_thread.start()


# Prefer the compiled pybind module when available.
CTPTraderAdapter: type[Any]
CTPMdAdapter: type[Any]
try:
    from ._ctp_wrapper import CTPMdAdapter as _NativeCTPMdAdapter  # type: ignore[import-untyped]
    from ._ctp_wrapper import (
        CTPTraderAdapter as _NativeCTPTraderAdapter,
    )
except ImportError:
    CTPTraderAdapter = _FallbackCTPTraderAdapter
    CTPMdAdapter = _FallbackCTPMdAdapter
else:
    CTPTraderAdapter = _NativeCTPTraderAdapter
    CTPMdAdapter = _NativeCTPMdAdapter


def is_native_backend() -> bool:
    return CTPTraderAdapter is not _FallbackCTPTraderAdapter


def backend_name() -> str:
    return "pybind" if is_native_backend() else "python-fallback"


def create_connect_config(raw: dict[str, object]) -> dict[str, Any]:
    """Build a normalized connect config for both pybind and fallback adapters."""

    cfg: dict[str, Any] = {
        "market_front_address": str(raw.get("market_front_address", "")).strip(),
        "trader_front_address": str(raw.get("trader_front_address", "")).strip(),
        "flow_path": str(raw.get("flow_path", "./ctp_flow")).strip() or "./ctp_flow",
        "broker_id": str(raw.get("broker_id", "")).strip(),
        "user_id": str(raw.get("user_id", "")).strip(),
        "investor_id": str(raw.get("investor_id", "")).strip(),
        "password": str(raw.get("password", "")).strip(),
        "app_id": str(raw.get("app_id", "")).strip(),
        "auth_code": str(raw.get("auth_code", "")).strip(),
        "is_production_mode": bool(raw.get("is_production_mode", False)),
        "enable_real_api": bool(raw.get("enable_real_api", False)),
        "enable_terminal_auth": bool(raw.get("enable_terminal_auth", True)),
        "connect_timeout_ms": _to_int(raw.get("connect_timeout_ms", 10_000), 10_000),
        "reconnect_max_attempts": _to_int(raw.get("reconnect_max_attempts", 8), 8),
        "reconnect_initial_backoff_ms": _to_int(
            raw.get("reconnect_initial_backoff_ms", 500),
            500,
        ),
        "reconnect_max_backoff_ms": _to_int(raw.get("reconnect_max_backoff_ms", 8_000), 8_000),
        "query_retry_backoff_ms": _to_int(raw.get("query_retry_backoff_ms", 200), 200),
        "recovery_quiet_period_ms": _to_int(raw.get("recovery_quiet_period_ms", 3_000), 3_000),
        "settlement_confirm_required": bool(raw.get("settlement_confirm_required", True)),
    }
    if not cfg["investor_id"]:
        cfg["investor_id"] = cfg["user_id"]
    return cfg


__all__ = [
    "CTPTraderAdapter",
    "CTPMdAdapter",
    "backend_name",
    "create_connect_config",
    "is_native_backend",
]
