from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.runtime.ctp_direct_runner import CtpDirectRunner, load_ctp_direct_runner_config
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.strategy.base import StrategyBase


class _SignalStrategy(StrategyBase):
    def __init__(self, strategy_id: str) -> None:
        super().__init__(strategy_id)
        self.events: list[OrderEvent] = []

    def on_bar(
        self, ctx: dict[str, object], bar_batch: list[dict[str, object]]
    ) -> list[SignalIntent]:
        del ctx
        del bar_batch
        return []

    def on_state(
        self, ctx: dict[str, object], state_snapshot: StateSnapshot7D
    ) -> list[SignalIntent]:
        del ctx
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id=state_snapshot.instrument_id,
                side=Side.BUY,
                offset=OffsetFlag.OPEN,
                volume=1,
                limit_price=4500.0,
                ts_ns=state_snapshot.ts_ns,
                trace_id=f"{self.strategy_id}-trace-{state_snapshot.ts_ns}",
            )
        ]

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        del ctx
        self.events.append(order_event)


@dataclass
class _FakeTrader:
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
                    "filled_volume": 0,
                    "avg_fill_price": float(request.get("price", 0.0)),
                    "reason": "accepted",
                    "trace_id": str(request.get("trace_id", "")),
                    "ts_ns": (
                        int(request.get("ts_ns", request.get("trace_id", "0").split("-")[-1] or 0))
                        if str(request.get("trace_id", "")).startswith("demo-trace-")
                        else 0
                    ),
                }
            )
        return True

    def on_order_status(self, callback: Any) -> None:
        self.order_callback = callback


@dataclass
class _FakeMd:
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

    def emit_tick(self, instrument_id: str, ts_ns: int, last_price: float = 4500.0) -> None:
        if self.tick_callback is None:
            return
        self.tick_callback(
            {
                "instrument_id": instrument_id,
                "last_price": last_price,
                "bid_price_1": last_price - 1.0,
                "ask_price_1": last_price + 1.0,
                "bid_volume_1": 1,
                "ask_volume_1": 1,
                "volume": 1,
                "ts_ns": ts_ns,
            }
        )


def _write_config(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "environment: sim",
                "is_production_mode: false",
                "market_front: tcp://sim-md",
                "trader_front: tcp://sim-td",
                "broker_id: 9999",
                "user_id: 191202",
                "investor_id: 191202",
                "password: secret",
                "strategy_ids: demo",
                "instruments: SHFE.ag2406",
                "strategy_poll_interval_ms: 50",
                "query_rate_per_sec: 5",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_load_ctp_direct_runner_config_defaults(tmp_path: Path) -> None:
    config_file = tmp_path / "ctp.yaml"
    _write_config(config_file)

    cfg = load_ctp_direct_runner_config(str(config_file))
    assert cfg.strategy_id == "demo"
    assert cfg.account_id == "191202"
    assert cfg.instruments == ["SHFE.ag2406"]
    assert cfg.query_qps_limit == 5
    assert cfg.settlement_confirm_required is True
    assert cfg.connect_config["market_front_address"] == "tcp://sim-md"


def test_ctp_direct_runner_start_and_run_once_places_order(tmp_path: Path) -> None:
    config_file = tmp_path / "ctp.yaml"
    _write_config(config_file)
    cfg = load_ctp_direct_runner_config(str(config_file))

    runtime = StrategyRuntime()
    strategy = _SignalStrategy("demo")
    runtime.add_strategy(strategy)

    fake_trader = _FakeTrader()
    fake_md = _FakeMd()
    runner = CtpDirectRunner(
        runtime,
        cfg,
        trader_factory=lambda qps, workers: fake_trader,
        md_factory=lambda qps, workers: fake_md,
    )

    assert runner.start() is True
    fake_md.emit_tick("SHFE.ag2406", ts_ns=123456789, last_price=4501.0)
    placed = runner.run_once()

    runner.stop()
    assert placed == 1
    assert len(fake_trader.orders) == 1
    assert fake_trader.orders[0]["strategy_id"] == "demo"
    assert strategy.events
    assert strategy.events[0].status == "ACCEPTED"


def test_ctp_direct_runner_start_fails_when_settlement_confirmation_fails(tmp_path: Path) -> None:
    config_file = tmp_path / "ctp.yaml"
    _write_config(config_file)
    cfg = load_ctp_direct_runner_config(str(config_file))

    runtime = StrategyRuntime()
    runtime.add_strategy(_SignalStrategy("demo"))

    fake_trader = _FakeTrader(confirm_ok=False)
    fake_md = _FakeMd()
    runner = CtpDirectRunner(
        runtime,
        cfg,
        trader_factory=lambda qps, workers: fake_trader,
        md_factory=lambda qps, workers: fake_md,
    )

    assert runner.start() is False
    assert fake_md.connected is False
