from __future__ import annotations

from collections.abc import Mapping

from quant_hft.contracts import OrderEvent, SignalIntent, StateSnapshot7D


def state_snapshot_key(instrument_id: str) -> str:
    return f"market:state7d:{instrument_id}:latest"


def strategy_intent_key(strategy_id: str) -> str:
    return f"strategy:intent:{strategy_id}:latest"


def order_event_key(trace_id: str) -> str:
    return f"trade:order:{trace_id}:info"


def strategy_bar_key(strategy_id: str, instrument_id: str) -> str:
    return f"strategy:bar:{strategy_id}:{instrument_id}:latest"


def encode_signal_intent(intent: SignalIntent) -> str:
    parts = [
        intent.instrument_id,
        intent.side.value,
        intent.offset.value,
        str(intent.volume),
        f"{intent.limit_price}",
        str(intent.ts_ns),
        intent.trace_id,
    ]
    for part in parts:
        if "|" in part:
            raise ValueError("redis intent encoding does not allow '|' in segment values")
    return "|".join(parts)


def build_intent_batch_fields(seq: int, intents: list[SignalIntent], ts_ns: int) -> dict[str, str]:
    fields: dict[str, str] = {
        "seq": str(seq),
        "count": str(len(intents)),
        "ts_ns": str(ts_ns),
    }
    for index, intent in enumerate(intents):
        fields[f"intent_{index}"] = encode_signal_intent(intent)
    return fields


def parse_state_snapshot(fields: Mapping[str, str]) -> StateSnapshot7D | None:
    instrument_id = fields.get("instrument_id", "")
    if not instrument_id:
        return None

    def parse_dimension(name: str) -> dict[str, float] | None:
        score_raw = fields.get(f"{name}_score")
        confidence_raw = fields.get(f"{name}_confidence")
        if score_raw is None or confidence_raw is None:
            return None
        try:
            return {"score": float(score_raw), "confidence": float(confidence_raw)}
        except ValueError:
            return None

    trend = parse_dimension("trend")
    volatility = parse_dimension("volatility")
    liquidity = parse_dimension("liquidity")
    sentiment = parse_dimension("sentiment")
    seasonality = parse_dimension("seasonality")
    pattern = parse_dimension("pattern")
    event_drive = parse_dimension("event_drive")
    if (
        trend is None
        or volatility is None
        or liquidity is None
        or sentiment is None
        or seasonality is None
        or pattern is None
        or event_drive is None
    ):
        return None

    ts_raw = fields.get("ts_ns")
    if ts_raw is None:
        return None
    try:
        ts_ns = int(ts_raw)
    except ValueError:
        return None

    return StateSnapshot7D(
        instrument_id=instrument_id,
        trend=trend,
        volatility=volatility,
        liquidity=liquidity,
        sentiment=sentiment,
        seasonality=seasonality,
        pattern=pattern,
        event_drive=event_drive,
        ts_ns=ts_ns,
    )


def parse_order_event(fields: Mapping[str, str]) -> OrderEvent | None:
    required = (
        "account_id",
        "client_order_id",
        "instrument_id",
        "status",
        "total_volume",
        "filled_volume",
        "avg_fill_price",
        "reason",
        "ts_ns",
    )
    if any(fields.get(key) is None for key in required):
        return None
    trace_id = fields.get("trace_id", fields.get("client_order_id", ""))
    execution_algo_id = fields.get("execution_algo_id", "")
    raw_slice_index = fields.get("slice_index", "0")
    raw_slice_total = fields.get("slice_total", "0")
    raw_throttle_applied = fields.get("throttle_applied", "0")
    raw_slippage_bps = fields.get("slippage_bps", "0")
    raw_impact_cost = fields.get("impact_cost", "0")
    raw_exchange_ts_ns = fields.get("exchange_ts_ns", "0")
    raw_recv_ts_ns = fields.get("recv_ts_ns", "0")
    try:
        slice_index = int(raw_slice_index)
        slice_total = int(raw_slice_total)
        throttle_applied = raw_throttle_applied.strip().lower() in {"1", "true", "yes"}
        slippage_bps = float(raw_slippage_bps)
        impact_cost = float(raw_impact_cost)
        exchange_ts_ns = int(raw_exchange_ts_ns)
        recv_ts_ns = int(raw_recv_ts_ns)
        return OrderEvent(
            account_id=fields["account_id"],
            client_order_id=fields["client_order_id"],
            instrument_id=fields["instrument_id"],
            status=fields["status"],
            total_volume=int(fields["total_volume"]),
            filled_volume=int(fields["filled_volume"]),
            avg_fill_price=float(fields["avg_fill_price"]),
            reason=fields["reason"],
            ts_ns=int(fields["ts_ns"]),
            trace_id=trace_id,
            exchange_order_id=fields.get("exchange_order_id", ""),
            exchange_id=fields.get("exchange_id", ""),
            status_msg=fields.get("status_msg", ""),
            order_submit_status=fields.get("order_submit_status", ""),
            order_ref=fields.get("order_ref", ""),
            front_id=int(fields.get("front_id", "0")),
            session_id=int(fields.get("session_id", "0")),
            trade_id=fields.get("trade_id", ""),
            event_source=fields.get("event_source", ""),
            exchange_ts_ns=exchange_ts_ns,
            recv_ts_ns=recv_ts_ns,
            execution_algo_id=execution_algo_id,
            slice_index=slice_index,
            slice_total=slice_total,
            throttle_applied=throttle_applied,
            venue=fields.get("venue", ""),
            route_id=fields.get("route_id", ""),
            slippage_bps=slippage_bps,
            impact_cost=impact_cost,
        )
    except ValueError:
        return None


def parse_strategy_bar(fields: Mapping[str, str]) -> dict[str, object] | None:
    required = (
        "instrument_id",
        "exchange",
        "timeframe",
        "ts_ns",
        "open",
        "high",
        "low",
        "close",
        "volume",
    )
    if any(fields.get(key) is None for key in required):
        return None
    try:
        return {
            "instrument_id": fields["instrument_id"],
            "exchange": fields["exchange"],
            "timeframe": fields["timeframe"],
            "ts_ns": int(fields["ts_ns"]),
            "open": float(fields["open"]),
            "high": float(fields["high"]),
            "low": float(fields["low"]),
            "close": float(fields["close"]),
            "volume": int(fields["volume"]),
            "turnover": float(fields.get("turnover", "0")),
            "open_interest": int(fields.get("open_interest", "0")),
        }
    except ValueError:
        return None
