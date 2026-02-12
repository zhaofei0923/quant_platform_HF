from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Protocol

from quant_hft.contracts import OrderEvent, SignalIntent, StateSnapshot7D
from quant_hft.runtime.redis_hash import RedisHashClient
from quant_hft.runtime.redis_schema import (
    build_intent_batch_fields,
    order_event_key,
    parse_order_event,
    parse_state_snapshot,
    state_snapshot_key,
    strategy_intent_key,
)


class DataFeed(Protocol):
    def get_latest_state_snapshot(self, instrument_id: str) -> StateSnapshot7D | None: ...

    def publish_intent_batch(
        self, strategy_id: str, seq: int, intents: list[SignalIntent]
    ) -> None: ...

    def get_order_event(self, trace_id: str) -> OrderEvent | None: ...


@dataclass
class RedisLiveDataFeed(DataFeed):
    redis_client: RedisHashClient

    def get_latest_state_snapshot(self, instrument_id: str) -> StateSnapshot7D | None:
        fields = self.redis_client.hgetall(state_snapshot_key(instrument_id))
        if not fields:
            return None
        return parse_state_snapshot(fields)

    def publish_intent_batch(self, strategy_id: str, seq: int, intents: list[SignalIntent]) -> None:
        fields = build_intent_batch_fields(seq, intents, time.time_ns())
        self.redis_client.hset(strategy_intent_key(strategy_id), fields)

    def get_order_event(self, trace_id: str) -> OrderEvent | None:
        fields = self.redis_client.hgetall(order_event_key(trace_id))
        if not fields:
            return None
        return parse_order_event(fields)


@dataclass
class BacktestReplayDataFeed(DataFeed):
    state_by_instrument: dict[str, StateSnapshot7D] = field(default_factory=dict)
    order_event_by_trace: dict[str, OrderEvent] = field(default_factory=dict)
    published_intents: dict[str, dict[str, str]] = field(default_factory=dict)

    def get_latest_state_snapshot(self, instrument_id: str) -> StateSnapshot7D | None:
        return self.state_by_instrument.get(instrument_id)

    def publish_intent_batch(self, strategy_id: str, seq: int, intents: list[SignalIntent]) -> None:
        self.published_intents[strategy_id] = build_intent_batch_fields(
            seq, intents, time.time_ns()
        )

    def get_order_event(self, trace_id: str) -> OrderEvent | None:
        return self.order_event_by_trace.get(trace_id)

    def set_state_snapshot(self, snapshot: StateSnapshot7D) -> None:
        self.state_by_instrument[snapshot.instrument_id] = snapshot

    def set_order_event(self, trace_id: str, event: OrderEvent) -> None:
        self.order_event_by_trace[trace_id] = event
