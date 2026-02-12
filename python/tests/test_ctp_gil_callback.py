from __future__ import annotations

import threading
import time

from quant_hft.ctp_wrapper import CTPMdAdapter, CTPTraderAdapter


def _connect_cfg() -> dict[str, object]:
    return {
        "market_front_address": "tcp://sim-md",
        "trader_front_address": "tcp://sim-td",
        "broker_id": "9999",
        "user_id": "191202",
        "investor_id": "191202",
        "password": "secret",
    }


def test_md_callback_thread_safety_under_background_tick_thread() -> None:
    md = CTPMdAdapter()
    assert md.connect(_connect_cfg()) is True

    callback_count = 0
    callback_errors: list[Exception] = []
    lock = threading.Lock()

    def on_tick(tick: dict[str, object]) -> None:
        nonlocal callback_count
        try:
            with lock:
                _ = tick["instrument_id"]
                callback_count += 1
        except Exception as exc:  # pragma: no cover - defensive guard for callback failures
            callback_errors.append(exc)

    md.on_tick(on_tick)
    assert md.subscribe(["SHFE.ag2406"]) is True

    deadline = time.time() + 0.8
    while time.time() < deadline:
        with lock:
            if callback_count >= 3:
                break
        time.sleep(0.02)

    md.disconnect()

    with lock:
        assert callback_count >= 1
    assert callback_errors == []


def test_trader_order_callback_thread_safety_under_concurrent_place_order() -> None:
    trader = CTPTraderAdapter()
    assert trader.connect(_connect_cfg()) is True
    assert trader.confirm_settlement() is True

    events: list[str] = []
    lock = threading.Lock()

    def on_order(order: dict[str, object]) -> None:
        with lock:
            events.append(str(order.get("trace_id", "")))

    trader.on_order_status(on_order)

    def worker(seq: int) -> None:
        trader.place_order(
            {
                "account_id": "191202",
                "client_order_id": f"ord-{seq}",
                "strategy_id": "demo",
                "instrument_id": "SHFE.ag2406",
                "volume": 1,
                "price": 100.0,
                "trace_id": f"trace-{seq}",
            }
        )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=1.0)

    trader.disconnect()

    with lock:
        assert len(events) == 12
        assert len(set(events)) == 12
