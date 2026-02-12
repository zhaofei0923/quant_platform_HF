from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from quant_hft.contracts import (
    HedgeFlag,
    OffsetFlag,
    OrderEvent,
    Side,
    SignalIntent,
    StateSnapshot7D,
    TimeCondition,
    VolumeCondition,
)
from quant_hft.ctp_wrapper import CTPMdAdapter, CTPTraderAdapter, create_connect_config
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.runtime.unified import (
    ClickHouseHistoryReader,
    LiveBroker,
    LiveDataFeed,
    StrategyEngine,
)
from quant_hft.runtime.unified import (
    Strategy as UnifiedStrategy,
)


class _TraderAdapter(Protocol):
    def connect(self, config: dict[str, object]) -> bool: ...

    def disconnect(self) -> None: ...

    def confirm_settlement(self) -> bool: ...

    def place_order(self, request: dict[str, object]) -> bool: ...

    def cancel_order(self, client_order_id: str, trace_id: str) -> bool: ...

    def on_order_status(self, callback: Callable[[dict[str, object]], None]) -> None: ...


class _MdAdapter(Protocol):
    def connect(self, config: dict[str, object]) -> bool: ...

    def disconnect(self) -> None: ...

    def subscribe(self, instruments: list[str]) -> bool: ...

    def unsubscribe(self, instruments: list[str]) -> bool: ...

    def on_tick(self, callback: Callable[[dict[str, object]], None]) -> None: ...


@dataclass(frozen=True)
class CtpDirectRunnerConfig:
    strategy_id: str
    account_id: str
    instruments: list[str]
    poll_interval_ms: int
    settlement_confirm_required: bool
    query_qps_limit: int
    dispatcher_workers: int
    connect_config: dict[str, object]


def _trim(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return text[1:-1]
    return text


def _parse_bool(raw: str, default: bool) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    return default


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


def _load_simple_yaml_kv(config_path: str) -> dict[str, str]:
    kv: dict[str, str] = {}
    for raw_line in Path(config_path).read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", maxsplit=1)[0].strip()
        if not line or line == "ctp:":
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", maxsplit=1)
        key = _trim(key)
        value = _trim(value)
        if key:
            kv[key] = value
    return kv


def _split_csv(raw: str) -> list[str]:
    values: list[str] = []
    for item in raw.split(","):
        parsed = _trim(item)
        if parsed:
            values.append(parsed)
    return values


def _load_password(kv: dict[str, str]) -> str:
    inline = kv.get("password", "")
    if inline:
        return inline
    env_key = kv.get("password_env", "CTP_SIM_PASSWORD")
    return os.getenv(env_key, "")


def load_ctp_direct_runner_config(
    config_path: str,
    strategy_id_override: str = "",
) -> CtpDirectRunnerConfig:
    kv = _load_simple_yaml_kv(config_path)

    instruments = _split_csv(kv.get("instruments", ""))
    if not instruments:
        instruments = ["SHFE.ag2406"]

    strategy_ids = _split_csv(kv.get("strategy_ids", "demo"))
    if not strategy_ids:
        strategy_ids = ["demo"]
    strategy_id = strategy_id_override or strategy_ids[0]

    poll_interval_ms = 200
    if kv.get("strategy_poll_interval_ms", ""):
        parsed_interval = int(kv["strategy_poll_interval_ms"])
        if parsed_interval > 0:
            poll_interval_ms = parsed_interval

    query_qps_limit = 10
    if kv.get("query_rate_per_sec", ""):
        parsed_query_qps = int(kv["query_rate_per_sec"])
        if parsed_query_qps > 0:
            query_qps_limit = parsed_query_qps
    elif kv.get("query_rate_limit_qps", ""):
        parsed_query_qps = int(kv["query_rate_limit_qps"])
        if parsed_query_qps > 0:
            query_qps_limit = parsed_query_qps

    dispatcher_workers = 1
    if kv.get("dispatcher_workers", ""):
        parsed_workers = int(kv["dispatcher_workers"])
        if parsed_workers > 0:
            dispatcher_workers = parsed_workers

    user_id = kv.get("user_id", "")
    investor_id = kv.get("investor_id", user_id)
    connect_cfg = create_connect_config(
        {
            "market_front_address": kv.get("market_front", kv.get("md_front", "")),
            "trader_front_address": kv.get("trader_front", kv.get("td_front", "")),
            "flow_path": kv.get("flow_path", "./ctp_flow"),
            "broker_id": kv.get("broker_id", ""),
            "user_id": user_id,
            "investor_id": investor_id,
            "password": _load_password(kv),
            "app_id": kv.get("app_id", ""),
            "auth_code": kv.get("auth_code", ""),
            "is_production_mode": _parse_bool(kv.get("is_production_mode", "false"), False),
            "enable_real_api": _parse_bool(kv.get("enable_real_api", "false"), False),
            "enable_terminal_auth": _parse_bool(kv.get("enable_terminal_auth", "true"), True),
            "connect_timeout_ms": int(kv.get("connect_timeout_ms", "10000")),
            "reconnect_max_attempts": int(kv.get("reconnect_max_attempts", "8")),
            "reconnect_initial_backoff_ms": int(kv.get("reconnect_initial_backoff_ms", "500")),
            "reconnect_max_backoff_ms": int(kv.get("reconnect_max_backoff_ms", "8000")),
            "query_retry_backoff_ms": int(kv.get("query_retry_backoff_ms", "200")),
        }
    )

    account_id = kv.get("account_id", user_id)
    settlement_confirm_required = _parse_bool(
        kv.get("settlement_confirm_required", "true"),
        True,
    )

    return CtpDirectRunnerConfig(
        strategy_id=strategy_id,
        account_id=account_id,
        instruments=instruments,
        poll_interval_ms=max(1, poll_interval_ms),
        settlement_confirm_required=settlement_confirm_required,
        query_qps_limit=max(1, query_qps_limit),
        dispatcher_workers=max(1, dispatcher_workers),
        connect_config=connect_cfg,
    )


class CtpDirectRunner:
    def __init__(
        self,
        runtime: StrategyRuntime,
        config: CtpDirectRunnerConfig,
        *,
        trader_factory: Callable[[int, int], _TraderAdapter] | None = None,
        md_factory: Callable[[int, int], _MdAdapter] | None = None,
        ctx: dict[str, object] | None = None,
    ) -> None:
        self._runtime = runtime
        self._config = config
        self._ctx = {} if ctx is None else ctx

        trader_ctor = trader_factory or (lambda qps, workers: CTPTraderAdapter(qps, workers))
        md_ctor = md_factory or (lambda qps, workers: CTPMdAdapter(qps, workers))
        self._trader = trader_ctor(config.query_qps_limit, config.dispatcher_workers)
        self._md = md_ctor(config.query_qps_limit, config.dispatcher_workers)

        self._state_lock = threading.Lock()
        self._latest_state_by_instrument: dict[str, StateSnapshot7D] = {}
        self._last_processed_ts_ns: dict[str, int] = {}
        self._last_price_by_instrument: dict[str, float] = {}
        self._order_seq = 0
        self._started = False

        self._trader.on_order_status(self._on_order_status)
        self._md.on_tick(self._on_tick)

    @property
    def ctx(self) -> dict[str, object]:
        return self._ctx

    def start(self) -> bool:
        if self._started:
            return True
        if not self._trader.connect(dict(self._config.connect_config)):
            return False
        if self._config.settlement_confirm_required and not self._trader.confirm_settlement():
            self._trader.disconnect()
            return False
        if not self._md.connect(dict(self._config.connect_config)):
            self._trader.disconnect()
            return False
        if not self._md.subscribe(list(self._config.instruments)):
            self._md.disconnect()
            self._trader.disconnect()
            return False
        self._started = True
        return True

    def stop(self) -> None:
        if not self._started:
            return
        self._md.unsubscribe(list(self._config.instruments))
        self._md.disconnect()
        self._trader.disconnect()
        self._started = False

    def run_once(self) -> int:
        pending_states: list[StateSnapshot7D] = []
        with self._state_lock:
            for instrument_id in self._config.instruments:
                snapshot = self._latest_state_by_instrument.get(instrument_id)
                if snapshot is None:
                    continue
                last_ts = self._last_processed_ts_ns.get(instrument_id, -1)
                if snapshot.ts_ns <= last_ts:
                    continue
                self._last_processed_ts_ns[instrument_id] = snapshot.ts_ns
                pending_states.append(snapshot)

        placed = 0
        for state in pending_states:
            intents = self._runtime.on_state(self._ctx, state)
            for intent in intents:
                if self._submit_intent(intent):
                    placed += 1
        return placed

    def run_forever(self, run_seconds: int | None = None) -> None:
        started_ns = time.monotonic_ns()
        while True:
            self.run_once()
            if run_seconds is not None:
                elapsed_ns = time.monotonic_ns() - started_ns
                if elapsed_ns >= run_seconds * 1_000_000_000:
                    return
            time.sleep(self._config.poll_interval_ms / 1000.0)

    def _submit_intent(self, intent: SignalIntent) -> bool:
        self._order_seq += 1
        client_order_id = (
            f"{intent.strategy_id}_{intent.ts_ns}_{self._order_seq}"
            if intent.strategy_id
            else f"{self._config.strategy_id}_{intent.ts_ns}_{self._order_seq}"
        )
        request: dict[str, object] = {
            "account_id": self._config.account_id,
            "client_order_id": client_order_id,
            "strategy_id": intent.strategy_id or self._config.strategy_id,
            "instrument_id": intent.instrument_id,
            "side": intent.side.value if isinstance(intent.side, Side) else str(intent.side),
            "offset": (
                intent.offset.value if isinstance(intent.offset, OffsetFlag) else str(intent.offset)
            ),
            "volume": intent.volume,
            "price": intent.limit_price,
            "trace_id": intent.trace_id,
            "hedge_flag": HedgeFlag.SPECULATION.value,
            "time_condition": TimeCondition.GFD.value,
            "volume_condition": VolumeCondition.AV.value,
        }
        return self._trader.place_order(request)

    def _on_tick(self, payload: dict[str, object]) -> None:
        instrument_id = str(payload.get("instrument_id", "")).strip()
        if not instrument_id:
            return
        ts_ns = _to_int(payload.get("ts_ns", time.time_ns()), time.time_ns())
        last_price = _to_float(payload.get("last_price", 0.0), 0.0)

        with self._state_lock:
            prev_price = self._last_price_by_instrument.get(instrument_id)
            trend_score = 0.0
            if prev_price is not None:
                if last_price > prev_price:
                    trend_score = 1.0
                elif last_price < prev_price:
                    trend_score = -1.0
            self._last_price_by_instrument[instrument_id] = last_price

            state = StateSnapshot7D(
                instrument_id=instrument_id,
                trend={"score": trend_score, "confidence": 1.0},
                volatility={"score": 0.0, "confidence": 0.0},
                liquidity={"score": 0.0, "confidence": 0.0},
                sentiment={"score": 0.0, "confidence": 0.0},
                seasonality={"score": 0.0, "confidence": 0.0},
                pattern={"score": 0.0, "confidence": 0.0},
                event_drive={"score": 0.0, "confidence": 0.0},
                ts_ns=ts_ns,
            )
            self._latest_state_by_instrument[instrument_id] = state

    def _on_order_status(self, payload: dict[str, object]) -> None:
        event = OrderEvent(
            account_id=str(payload.get("account_id", self._config.account_id)),
            client_order_id=str(payload.get("client_order_id", "")),
            instrument_id=str(payload.get("instrument_id", "")),
            status=self._normalize_status(payload.get("status")),
            total_volume=_to_int(payload.get("total_volume", 0), 0),
            filled_volume=_to_int(payload.get("filled_volume", 0), 0),
            avg_fill_price=_to_float(payload.get("avg_fill_price", 0.0), 0.0),
            reason=str(payload.get("reason", "")),
            ts_ns=_to_int(payload.get("ts_ns", time.time_ns()), time.time_ns()),
            trace_id=str(payload.get("trace_id", "")),
        )
        self._runtime.on_order_event(self._ctx, event)

    @staticmethod
    def _normalize_status(raw_status: object) -> str:
        if isinstance(raw_status, str):
            normalized = raw_status.strip().upper()
            return normalized if normalized else "UNKNOWN"
        if isinstance(raw_status, int):
            # Keep compatibility with a few common legacy numeric statuses.
            mapping = {
                0: "NEW",
                1: "ACCEPTED",
                2: "PARTIALLY_FILLED",
                3: "FILLED",
                4: "CANCELED",
                5: "REJECTED",
            }
            return mapping.get(raw_status, "UNKNOWN")
        return "UNKNOWN"


class UnifiedCtpDirectRunner:
    def __init__(
        self,
        strategy: UnifiedStrategy,
        config: CtpDirectRunnerConfig,
        *,
        clickhouse_dsn: str = "http://clickhouse:8123",
        trader_factory: Callable[[int, int], _TraderAdapter] | None = None,
        md_factory: Callable[[int, int], _MdAdapter] | None = None,
    ) -> None:
        self._config = config
        self._order_events = 0
        self._started = False

        trader_ctor = trader_factory or (lambda qps, workers: CTPTraderAdapter(qps, workers))
        md_ctor = md_factory or (lambda qps, workers: CTPMdAdapter(qps, workers))
        self._trader = trader_ctor(config.query_qps_limit, config.dispatcher_workers)
        self._md = md_ctor(config.query_qps_limit, config.dispatcher_workers)

        self._history_reader = ClickHouseHistoryReader(dsn=clickhouse_dsn)
        self._broker = LiveBroker(
            trader_adapter=self._trader,
            account_id=config.account_id,
            strategy_id=config.strategy_id,
        )
        self._datafeed = LiveDataFeed(
            md_adapter=self._md,
            history_reader=self._history_reader,
            connect_config=dict(config.connect_config),
        )
        self._engine = StrategyEngine(mode="live", datafeed=self._datafeed, broker=self._broker)
        self._engine.add_strategy(strategy)
        self._broker.on_order_status(lambda _order: self._mark_order_event())

    def start(self) -> bool:
        if self._started:
            return True
        if not self._broker.connect(
            dict(self._config.connect_config),
            settlement_confirm_required=self._config.settlement_confirm_required,
        ):
            return False
        self._started = True
        return True

    def stop(self) -> None:
        if not self._started:
            return
        self._engine.stop()
        self._broker.disconnect()
        self._started = False

    def run_once(self) -> int:
        if not self._started:
            raise RuntimeError("runner is not started")
        before = self._order_events
        self._engine.run(run_seconds=1)
        return self._order_events - before

    def run_forever(self, run_seconds: int | None = None) -> None:
        if not self._started:
            raise RuntimeError("runner is not started")
        if run_seconds is None or run_seconds <= 0:
            self._engine.run()
        else:
            self._engine.run(run_seconds=run_seconds)

    def _mark_order_event(self) -> None:
        self._order_events += 1


__all__ = [
    "CtpDirectRunner",
    "CtpDirectRunnerConfig",
    "UnifiedCtpDirectRunner",
    "load_ctp_direct_runner_config",
]
