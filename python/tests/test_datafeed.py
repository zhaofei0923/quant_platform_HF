from __future__ import annotations

from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.runtime.datafeed import BacktestReplayDataFeed, RedisLiveDataFeed
from quant_hft.runtime.redis_hash import InMemoryRedisHashClient
from quant_hft.runtime.redis_schema import order_event_key, state_snapshot_key, strategy_intent_key


def _seed_state(redis: InMemoryRedisHashClient, instrument_id: str, ts_ns: int) -> None:
    redis.hset(
        state_snapshot_key(instrument_id),
        {
            "instrument_id": instrument_id,
            "trend_score": "0.1",
            "trend_confidence": "0.9",
            "volatility_score": "0.2",
            "volatility_confidence": "0.8",
            "liquidity_score": "0.3",
            "liquidity_confidence": "0.7",
            "sentiment_score": "0.4",
            "sentiment_confidence": "0.6",
            "seasonality_score": "0.0",
            "seasonality_confidence": "0.2",
            "pattern_score": "0.1",
            "pattern_confidence": "0.3",
            "event_drive_score": "0.0",
            "event_drive_confidence": "0.2",
            "ts_ns": str(ts_ns),
        },
    )


def test_redis_live_datafeed_round_trip() -> None:
    redis = InMemoryRedisHashClient()
    feed = RedisLiveDataFeed(redis)
    _seed_state(redis, "SHFE.ag2406", 100)

    intent = SignalIntent(
        strategy_id="demo",
        instrument_id="SHFE.ag2406",
        side=Side.BUY,
        offset=OffsetFlag.OPEN,
        volume=1,
        limit_price=4500.0,
        ts_ns=101,
        trace_id="trace-1",
    )
    feed.publish_intent_batch("demo", 1, [intent])

    redis.hset(
        order_event_key("trace-1"),
        {
            "account_id": "acc-1",
            "client_order_id": "ord-1",
            "instrument_id": "SHFE.ag2406",
            "status": "ACCEPTED",
            "total_volume": "1",
            "filled_volume": "0",
            "avg_fill_price": "0.0",
            "reason": "",
            "ts_ns": "102",
            "trace_id": "trace-1",
            "exchange_ts_ns": "102",
            "recv_ts_ns": "103",
        },
    )

    snapshot = feed.get_latest_state_snapshot("SHFE.ag2406")
    assert snapshot is not None
    assert snapshot.instrument_id == "SHFE.ag2406"
    assert snapshot.ts_ns == 100

    batch = redis.hgetall(strategy_intent_key("demo"))
    assert batch["seq"] == "1"
    assert batch["count"] == "1"

    event = feed.get_order_event("trace-1")
    assert event is not None
    assert event.exchange_ts_ns == 102
    assert event.recv_ts_ns == 103


def test_backtest_replay_datafeed_round_trip() -> None:
    feed = BacktestReplayDataFeed()
    snapshot = StateSnapshot7D(
        instrument_id="SHFE.ag2406",
        trend={"score": 0.1, "confidence": 0.9},
        volatility={"score": 0.2, "confidence": 0.8},
        liquidity={"score": 0.3, "confidence": 0.7},
        sentiment={"score": 0.4, "confidence": 0.6},
        seasonality={"score": 0.0, "confidence": 0.2},
        pattern={"score": 0.1, "confidence": 0.3},
        event_drive={"score": 0.0, "confidence": 0.2},
        ts_ns=200,
    )
    feed.set_state_snapshot(snapshot)

    event = OrderEvent(
        account_id="acc-1",
        client_order_id="ord-2",
        instrument_id="SHFE.ag2406",
        status="FILLED",
        total_volume=1,
        filled_volume=1,
        avg_fill_price=4501.0,
        reason="ok",
        ts_ns=201,
        trace_id="trace-2",
    )
    feed.set_order_event("trace-2", event)

    intent = SignalIntent(
        strategy_id="demo",
        instrument_id="SHFE.ag2406",
        side=Side.BUY,
        offset=OffsetFlag.OPEN,
        volume=1,
        limit_price=4501.0,
        ts_ns=202,
        trace_id="trace-3",
    )
    feed.publish_intent_batch("demo", 1, [intent])

    assert feed.get_latest_state_snapshot("SHFE.ag2406") == snapshot
    assert feed.get_order_event("trace-2") == event
    assert "demo" in feed.published_intents
