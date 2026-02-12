from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from quant_hft.contracts import SignalIntent
from quant_hft.runtime.datafeed import DataFeed, RedisLiveDataFeed
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.runtime.redis_hash import RedisHashClient


@dataclass(frozen=True)
class RunnerConfig:
    instruments: list[str]
    strategy_id: str
    poll_interval_ms: int


def _trim(value: str) -> str:
    trimmed = value.strip()
    if len(trimmed) >= 2 and trimmed[0] == '"' and trimmed[-1] == '"':
        return trimmed[1:-1]
    return trimmed


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


def load_runner_config(config_path: str, strategy_id_override: str = "") -> RunnerConfig:
    kv = _load_simple_yaml_kv(config_path)
    instruments = _split_csv(kv.get("instruments", ""))
    if not instruments:
        instruments = ["SHFE.ag2406"]

    strategy_ids = _split_csv(kv.get("strategy_ids", "demo"))
    if not strategy_ids:
        strategy_ids = ["demo"]

    poll_interval_ms = 200
    raw_interval = kv.get("strategy_poll_interval_ms")
    if raw_interval:
        parsed = int(raw_interval)
        if parsed > 0:
            poll_interval_ms = parsed

    strategy_id = strategy_id_override or strategy_ids[0]
    return RunnerConfig(
        instruments=instruments,
        strategy_id=strategy_id,
        poll_interval_ms=poll_interval_ms,
    )


def load_redis_client_from_env(
    factory: Callable[..., RedisHashClient],
) -> RedisHashClient:
    host = os.getenv("QUANT_HFT_REDIS_HOST", "127.0.0.1")
    port = int(os.getenv("QUANT_HFT_REDIS_PORT", "6379"))
    username = os.getenv("QUANT_HFT_REDIS_USER", "")
    password = os.getenv("QUANT_HFT_REDIS_PASSWORD", "")
    connect_timeout_ms = int(os.getenv("QUANT_HFT_REDIS_CONNECT_TIMEOUT_MS", "1000"))
    read_timeout_ms = int(os.getenv("QUANT_HFT_REDIS_READ_TIMEOUT_MS", "1000"))
    return factory(
        host=host,
        port=port,
        username=username,
        password=password,
        connect_timeout_s=max(0.001, connect_timeout_ms / 1000.0),
        read_timeout_s=max(0.001, read_timeout_ms / 1000.0),
    )


class StrategyRunner:
    def __init__(
        self,
        runtime: StrategyRuntime,
        strategy_id: str,
        instruments: list[str],
        datafeed: DataFeed | None = None,
        redis_client: RedisHashClient | None = None,
        poll_interval_ms: int = 200,
        ctx: dict[str, object] | None = None,
    ) -> None:
        self._runtime = runtime
        self._datafeed: DataFeed
        if datafeed is None:
            if redis_client is None:
                raise ValueError("datafeed or redis_client is required")
            self._datafeed = RedisLiveDataFeed(redis_client)
        else:
            self._datafeed = datafeed
        self._strategy_id = strategy_id
        self._instruments = instruments
        self._poll_interval_ms = max(1, poll_interval_ms)
        self._ctx: dict[str, object] = {} if ctx is None else ctx
        self._last_state_ts_ns: dict[str, int] = {}
        self._seq = 0
        self._active_trace_ids: set[str] = set()
        self._last_order_fingerprint: dict[str, tuple[str, int, int]] = {}

    @property
    def ctx(self) -> dict[str, object]:
        return self._ctx

    def run_once(self) -> int:
        intents = self._collect_signal_intents()
        if intents:
            self._seq += 1
            self._datafeed.publish_intent_batch(self._strategy_id, self._seq, intents)
            for intent in intents:
                self._active_trace_ids.add(intent.trace_id)
        self._poll_order_events()
        return len(intents)

    def run_forever(self, run_seconds: int | None = None) -> None:
        start_ns = time.monotonic_ns()
        while True:
            self.run_once()
            if run_seconds is not None:
                elapsed_ns = time.monotonic_ns() - start_ns
                if elapsed_ns >= run_seconds * 1_000_000_000:
                    break
            time.sleep(self._poll_interval_ms / 1000.0)

    def _collect_signal_intents(self) -> list[SignalIntent]:
        intents: list[SignalIntent] = []
        for instrument_id in self._instruments:
            snapshot = self._datafeed.get_latest_state_snapshot(instrument_id)
            if snapshot is None:
                continue

            last_ts = self._last_state_ts_ns.get(instrument_id, -1)
            if snapshot.ts_ns <= last_ts:
                continue
            self._last_state_ts_ns[instrument_id] = snapshot.ts_ns

            produced = self._runtime.on_state(self._ctx, snapshot)
            for intent in produced:
                if intent.strategy_id == self._strategy_id:
                    intents.append(intent)
                    continue
                intents.append(
                    SignalIntent(
                        strategy_id=self._strategy_id,
                        instrument_id=intent.instrument_id,
                        side=intent.side,
                        offset=intent.offset,
                        volume=intent.volume,
                        limit_price=intent.limit_price,
                        ts_ns=intent.ts_ns,
                        trace_id=intent.trace_id,
                    )
                )
        return intents

    def _poll_order_events(self) -> None:
        terminal = {"FILLED", "CANCELED", "REJECTED"}
        for trace_id in list(self._active_trace_ids):
            event = self._datafeed.get_order_event(trace_id)
            if event is None:
                continue
            fingerprint = (event.status, event.filled_volume, event.ts_ns)
            if self._last_order_fingerprint.get(trace_id) == fingerprint:
                continue
            self._last_order_fingerprint[trace_id] = fingerprint
            self._runtime.on_order_event(self._ctx, event)
            if event.status in terminal:
                self._active_trace_ids.discard(trace_id)


__all__ = [
    "RunnerConfig",
    "StrategyRunner",
    "load_runner_config",
    "load_redis_client_from_env",
]
