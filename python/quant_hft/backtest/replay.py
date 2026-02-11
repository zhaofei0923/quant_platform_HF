from __future__ import annotations

import csv
import json
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO, cast

from quant_hft.contracts import OrderEvent, Side
from quant_hft.runtime.engine import StrategyRuntime


@dataclass(frozen=True)
class CsvTick:
    trading_day: str
    instrument_id: str
    update_time: str
    update_millisec: int
    last_price: float
    volume: int
    bid_price_1: float
    bid_volume_1: int
    ask_price_1: float
    ask_volume_1: int
    average_price: float
    turnover: float
    open_interest: float


@dataclass(frozen=True)
class ReplayReport:
    ticks_read: int
    bars_emitted: int
    intents_emitted: int
    first_instrument: str
    last_instrument: str


@dataclass(frozen=True)
class InstrumentPnlSnapshot:
    instrument_id: str
    net_position: int
    avg_open_price: float
    realized_pnl: float
    unrealized_pnl: float
    last_price: float


@dataclass(frozen=True)
class DeterministicReplayReport:
    replay: ReplayReport
    intents_processed: int
    order_events_emitted: int
    wal_records: int
    instrument_bars: dict[str, int]
    instrument_pnl: dict[str, InstrumentPnlSnapshot]
    total_realized_pnl: float
    total_unrealized_pnl: float
    invariant_violations: tuple[str, ...]


@dataclass
class _InstrumentPnlState:
    net_position: int = 0
    avg_open_price: float = 0.0
    realized_pnl: float = 0.0


def _parse_int(raw: str) -> int:
    return int(raw) if raw else 0


def _parse_float(raw: str) -> float:
    return float(raw) if raw else 0.0


def _minute_key(tick: CsvTick) -> str:
    return f"{tick.trading_day} {tick.update_time[:5]} {tick.instrument_id}"


def _to_ts_ns(trading_day: str, update_time: str, update_millisec: int) -> int:
    dt = datetime.strptime(f"{trading_day} {update_time}", "%Y%m%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000) + update_millisec * 1_000_000


def _build_bar(bucket: list[CsvTick]) -> dict[str, object]:
    first = bucket[0]
    last = bucket[-1]
    high = max(t.last_price for t in bucket)
    low = min(t.last_price for t in bucket)
    volume_delta = max(0, last.volume - first.volume)
    return {
        "instrument_id": last.instrument_id,
        "minute": f"{last.trading_day} {last.update_time[:5]}",
        "open": first.last_price,
        "high": high,
        "low": low,
        "close": last.last_price,
        "volume": volume_delta,
        "bid_price_1": last.bid_price_1,
        "ask_price_1": last.ask_price_1,
        "bid_volume_1": last.bid_volume_1,
        "ask_volume_1": last.ask_volume_1,
        "ts_ns": _to_ts_ns(last.trading_day, last.update_time, last.update_millisec),
    }


def _status_to_code(status: str) -> int:
    mapping = {
        "NEW": 0,
        "ACCEPTED": 1,
        "PARTIALLY_FILLED": 2,
        "FILLED": 3,
        "CANCELED": 4,
        "REJECTED": 5,
    }
    return mapping.get(status, 5)


def _emit_wal_line(
    fp: TextIO,
    *,
    seq: int,
    kind: str,
    event: OrderEvent,
) -> None:
    record = {
        "seq": seq,
        "kind": kind,
        "ts_ns": event.ts_ns,
        "account_id": event.account_id,
        "client_order_id": event.client_order_id,
        "exchange_order_id": "",
        "instrument_id": event.instrument_id,
        "status": _status_to_code(event.status),
        "total_volume": event.total_volume,
        "filled_volume": event.filled_volume,
        "avg_fill_price": event.avg_fill_price,
        "reason": event.reason,
        "trace_id": event.trace_id,
    }
    fp.write(json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n")


def _apply_trade(
    state: _InstrumentPnlState,
    side: Side,
    volume: int,
    fill_price: float,
) -> None:
    if volume <= 0:
        return

    signed_qty = volume if side == Side.BUY else -volume

    if state.net_position == 0 or (state.net_position > 0) == (signed_qty > 0):
        current_abs = abs(state.net_position)
        next_abs = current_abs + abs(signed_qty)
        if next_abs > 0:
            state.avg_open_price = (
                state.avg_open_price * float(current_abs) + fill_price * float(abs(signed_qty))
            ) / float(next_abs)
        state.net_position += signed_qty
        return

    remaining = abs(signed_qty)
    if state.net_position > 0:
        close_qty = min(state.net_position, remaining)
        state.realized_pnl += (fill_price - state.avg_open_price) * float(close_qty)
        state.net_position -= close_qty
        remaining -= close_qty
    else:
        short_abs = abs(state.net_position)
        close_qty = min(short_abs, remaining)
        state.realized_pnl += (state.avg_open_price - fill_price) * float(close_qty)
        state.net_position += close_qty
        remaining -= close_qty

    if state.net_position == 0:
        state.avg_open_price = 0.0

    if remaining > 0:
        state.net_position = remaining if signed_qty > 0 else -remaining
        state.avg_open_price = fill_price


def _compute_unrealized(net_position: int, avg_open_price: float, last_price: float) -> float:
    if net_position > 0:
        return (last_price - avg_open_price) * float(net_position)
    if net_position < 0:
        return (avg_open_price - last_price) * float(abs(net_position))
    return 0.0


def _validate_invariants(
    instrument_pnl: dict[str, InstrumentPnlSnapshot],
) -> tuple[str, ...]:
    violations: list[str] = []
    for instrument_id in sorted(instrument_pnl):
        snapshot = instrument_pnl[instrument_id]
        if snapshot.net_position == 0 and abs(snapshot.avg_open_price) > 1e-9:
            violations.append(
                f"{instrument_id}: flat position must have zero avg_open_price "
                f"(got {snapshot.avg_open_price:.8f})"
            )
        if snapshot.net_position != 0 and snapshot.avg_open_price <= 0.0:
            violations.append(
                f"{instrument_id}: non-flat position must have positive avg_open_price "
                f"(got {snapshot.avg_open_price:.8f})"
            )
        if snapshot.net_position == 0 and abs(snapshot.unrealized_pnl) > 1e-9:
            violations.append(
                f"{instrument_id}: flat position must have zero unrealized_pnl "
                f"(got {snapshot.unrealized_pnl:.8f})"
            )
    return tuple(violations)


def iter_csv_ticks(csv_path: Path | str, max_ticks: int | None = None) -> Iterator[CsvTick]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        for idx, row in enumerate(reader):
            if max_ticks is not None and idx >= max_ticks:
                break

            yield CsvTick(
                trading_day=row.get("TradingDay", ""),
                instrument_id=row.get("InstrumentID", ""),
                update_time=row.get("UpdateTime", ""),
                update_millisec=_parse_int(row.get("UpdateMillisec", "")),
                last_price=_parse_float(row.get("LastPrice", "")),
                volume=_parse_int(row.get("Volume", "")),
                bid_price_1=_parse_float(row.get("BidPrice1", "")),
                bid_volume_1=_parse_int(row.get("BidVolume1", "")),
                ask_price_1=_parse_float(row.get("AskPrice1", "")),
                ask_volume_1=_parse_int(row.get("AskVolume1", "")),
                average_price=_parse_float(row.get("AveragePrice", "")),
                turnover=_parse_float(row.get("Turnover", "")),
                open_interest=_parse_float(row.get("OpenInterest", "")),
            )


def replay_csv_minute_bars(
    csv_path: Path | str,
    runtime: StrategyRuntime,
    ctx: dict[str, object],
    *,
    max_ticks: int | None = None,
) -> ReplayReport:
    ticks_read = 0
    bars_emitted = 0
    intents_emitted = 0
    first_instrument = ""
    last_instrument = ""

    bucket: list[CsvTick] = []
    current_key = ""

    for tick in iter_csv_ticks(csv_path, max_ticks=max_ticks):
        ticks_read += 1
        if not first_instrument:
            first_instrument = tick.instrument_id
        last_instrument = tick.instrument_id

        key = _minute_key(tick)
        if not bucket:
            current_key = key
            bucket.append(tick)
            continue

        if key == current_key:
            bucket.append(tick)
            continue

        bar = _build_bar(bucket)
        intents_emitted += len(runtime.on_bar(ctx, [bar]))
        bars_emitted += 1

        bucket = [tick]
        current_key = key

    if bucket:
        bar = _build_bar(bucket)
        intents_emitted += len(runtime.on_bar(ctx, [bar]))
        bars_emitted += 1

    return ReplayReport(
        ticks_read=ticks_read,
        bars_emitted=bars_emitted,
        intents_emitted=intents_emitted,
        first_instrument=first_instrument,
        last_instrument=last_instrument,
    )


def replay_csv_with_deterministic_fills(
    csv_path: Path | str,
    runtime: StrategyRuntime,
    ctx: dict[str, object],
    *,
    max_ticks: int | None = None,
    wal_path: Path | str | None = None,
    account_id: str = "sim-account",
) -> DeterministicReplayReport:
    ticks_read = 0
    bars_emitted = 0
    intents_processed = 0
    order_events_emitted = 0
    wal_records = 0
    first_instrument = ""
    last_instrument = ""
    next_order_id = 1
    next_wal_seq = 1
    instrument_bars: defaultdict[str, int] = defaultdict(int)
    last_close_price: dict[str, float] = {}
    instrument_state: defaultdict[str, _InstrumentPnlState] = defaultdict(_InstrumentPnlState)

    bucket: list[CsvTick] = []
    current_key = ""
    wal_fp = None
    if wal_path is not None:
        wal_fp = Path(wal_path).open("w", encoding="utf-8")

    try:
        for tick in iter_csv_ticks(csv_path, max_ticks=max_ticks):
            ticks_read += 1
            if not first_instrument:
                first_instrument = tick.instrument_id
            last_instrument = tick.instrument_id

            key = _minute_key(tick)
            if not bucket:
                current_key = key
                bucket.append(tick)
                continue

            if key == current_key:
                bucket.append(tick)
                continue

            bar = _build_bar(bucket)
            bars_emitted += 1
            instrument_id = str(bar["instrument_id"])
            instrument_bars[instrument_id] += 1
            fill_price = cast(float, bar["close"])
            last_close_price[instrument_id] = fill_price

            intents = runtime.on_bar(ctx, [bar])
            intents_processed += len(intents)
            for intent in intents:
                client_order_id = f"bt-{next_order_id:09d}"
                next_order_id += 1

                accepted = OrderEvent(
                    account_id=account_id,
                    client_order_id=client_order_id,
                    instrument_id=intent.instrument_id,
                    status="ACCEPTED",
                    total_volume=intent.volume,
                    filled_volume=0,
                    avg_fill_price=0.0,
                    reason="deterministic_accept",
                    ts_ns=int(intent.ts_ns),
                    trace_id=intent.trace_id,
                )
                filled = OrderEvent(
                    account_id=account_id,
                    client_order_id=client_order_id,
                    instrument_id=intent.instrument_id,
                    status="FILLED",
                    total_volume=intent.volume,
                    filled_volume=intent.volume,
                    avg_fill_price=fill_price,
                    reason="deterministic_fill",
                    ts_ns=int(intent.ts_ns),
                    trace_id=intent.trace_id,
                )

                runtime.on_order_event(ctx, accepted)
                runtime.on_order_event(ctx, filled)
                order_events_emitted += 2

                state = instrument_state[intent.instrument_id]
                _apply_trade(state, intent.side, intent.volume, fill_price)

                if wal_fp is not None:
                    _emit_wal_line(wal_fp, seq=next_wal_seq, kind="order", event=accepted)
                    next_wal_seq += 1
                    _emit_wal_line(wal_fp, seq=next_wal_seq, kind="trade", event=filled)
                    next_wal_seq += 1
                    wal_records += 2

            bucket = [tick]
            current_key = key

        if bucket:
            bar = _build_bar(bucket)
            bars_emitted += 1
            instrument_id = str(bar["instrument_id"])
            instrument_bars[instrument_id] += 1
            fill_price = cast(float, bar["close"])
            last_close_price[instrument_id] = fill_price

            intents = runtime.on_bar(ctx, [bar])
            intents_processed += len(intents)
            for intent in intents:
                client_order_id = f"bt-{next_order_id:09d}"
                next_order_id += 1

                accepted = OrderEvent(
                    account_id=account_id,
                    client_order_id=client_order_id,
                    instrument_id=intent.instrument_id,
                    status="ACCEPTED",
                    total_volume=intent.volume,
                    filled_volume=0,
                    avg_fill_price=0.0,
                    reason="deterministic_accept",
                    ts_ns=int(intent.ts_ns),
                    trace_id=intent.trace_id,
                )
                filled = OrderEvent(
                    account_id=account_id,
                    client_order_id=client_order_id,
                    instrument_id=intent.instrument_id,
                    status="FILLED",
                    total_volume=intent.volume,
                    filled_volume=intent.volume,
                    avg_fill_price=fill_price,
                    reason="deterministic_fill",
                    ts_ns=int(intent.ts_ns),
                    trace_id=intent.trace_id,
                )

                runtime.on_order_event(ctx, accepted)
                runtime.on_order_event(ctx, filled)
                order_events_emitted += 2

                state = instrument_state[intent.instrument_id]
                _apply_trade(state, intent.side, intent.volume, fill_price)

                if wal_fp is not None:
                    _emit_wal_line(wal_fp, seq=next_wal_seq, kind="order", event=accepted)
                    next_wal_seq += 1
                    _emit_wal_line(wal_fp, seq=next_wal_seq, kind="trade", event=filled)
                    next_wal_seq += 1
                    wal_records += 2
    finally:
        if wal_fp is not None:
            wal_fp.close()

    instrument_pnl: dict[str, InstrumentPnlSnapshot] = {}
    total_realized_pnl = 0.0
    total_unrealized_pnl = 0.0

    for instrument_id in sorted(instrument_state):
        state = instrument_state[instrument_id]
        mark_price = last_close_price.get(instrument_id, state.avg_open_price)
        unrealized = _compute_unrealized(state.net_position, state.avg_open_price, mark_price)
        snapshot = InstrumentPnlSnapshot(
            instrument_id=instrument_id,
            net_position=state.net_position,
            avg_open_price=state.avg_open_price,
            realized_pnl=state.realized_pnl,
            unrealized_pnl=unrealized,
            last_price=mark_price,
        )
        instrument_pnl[instrument_id] = snapshot
        total_realized_pnl += snapshot.realized_pnl
        total_unrealized_pnl += snapshot.unrealized_pnl

    replay_report = ReplayReport(
        ticks_read=ticks_read,
        bars_emitted=bars_emitted,
        intents_emitted=intents_processed,
        first_instrument=first_instrument,
        last_instrument=last_instrument,
    )

    return DeterministicReplayReport(
        replay=replay_report,
        intents_processed=intents_processed,
        order_events_emitted=order_events_emitted,
        wal_records=wal_records,
        instrument_bars=dict(instrument_bars),
        instrument_pnl=instrument_pnl,
        total_realized_pnl=total_realized_pnl,
        total_unrealized_pnl=total_unrealized_pnl,
        invariant_violations=_validate_invariants(instrument_pnl),
    )
