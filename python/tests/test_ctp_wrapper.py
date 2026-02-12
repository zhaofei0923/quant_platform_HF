from __future__ import annotations

import importlib
import sys
import time
import types


def _connect_cfg() -> dict[str, object]:
    return {
        "market_front_address": "tcp://sim-md",
        "trader_front_address": "tcp://sim-td",
        "broker_id": "9999",
        "user_id": "191202",
        "investor_id": "191202",
        "password": "secret",
    }


def _load_wrapper_fallback() -> types.ModuleType:
    sys.modules.pop("quant_hft._ctp_wrapper", None)
    module = importlib.import_module("quant_hft.ctp_wrapper")
    return importlib.reload(module)


def test_ctp_trader_fallback_requires_settlement_before_place_order() -> None:
    wrapper = _load_wrapper_fallback()
    trader = wrapper.CTPTraderAdapter()
    assert trader.connect(_connect_cfg()) is True

    accepted = trader.place_order(
        {
            "strategy_id": "demo",
            "client_order_id": "ord-1",
            "instrument_id": "SHFE.ag2406",
            "volume": 1,
            "price": 100.0,
            "trace_id": "trace-1",
        }
    )
    assert accepted is False

    assert trader.confirm_settlement() is True
    assert (
        trader.place_order(
            {
                "strategy_id": "demo",
                "client_order_id": "ord-2",
                "instrument_id": "SHFE.ag2406",
                "volume": 1,
                "price": 100.0,
                "trace_id": "trace-2",
            }
        )
        is True
    )


def test_ctp_md_fallback_subscribe_emits_ticks() -> None:
    wrapper = _load_wrapper_fallback()
    md = wrapper.CTPMdAdapter()
    assert md.connect(_connect_cfg()) is True

    ticks: list[dict[str, object]] = []

    def on_tick(tick: dict[str, object]) -> None:
        ticks.append(tick)

    md.on_tick(on_tick)
    assert md.subscribe(["SHFE.ag2406"]) is True

    deadline = time.time() + 0.6
    while time.time() < deadline and not ticks:
        time.sleep(0.02)

    md.disconnect()
    assert ticks
    assert ticks[0]["instrument_id"] == "SHFE.ag2406"


def test_ctp_wrapper_prefers_native_module_when_available() -> None:
    class FakeTrader:  # noqa: D401 - minimal fake for import selection test.
        pass

    class FakeMd:
        pass

    fake_module = types.SimpleNamespace(CTPTraderAdapter=FakeTrader, CTPMdAdapter=FakeMd)
    sys.modules["quant_hft._ctp_wrapper"] = fake_module

    module = importlib.import_module("quant_hft.ctp_wrapper")
    module = importlib.reload(module)

    try:
        assert module.CTPTraderAdapter is FakeTrader
        assert module.CTPMdAdapter is FakeMd
        assert module.is_native_backend() is True
    finally:
        sys.modules.pop("quant_hft._ctp_wrapper", None)
        importlib.reload(module)
