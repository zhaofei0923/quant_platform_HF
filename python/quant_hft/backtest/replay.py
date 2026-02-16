from __future__ import annotations

import csv
import heapq
import hashlib
import importlib
import json
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO, cast

from quant_hft.contracts import OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.research.metric_dictionary import metric_keys
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.runtime.trade_recorder import InMemoryTradeRecorder, TradeRecorder
from quant_hft.strategy.base import ensure_backtest_ctx


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
    instrument_count: int
    instrument_universe: tuple[str, ...]
    first_ts_ns: int
    last_ts_ns: int


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
    performance: BacktestPerformanceSummary
    invariant_violations: tuple[str, ...]
    rollover_events: tuple[dict[str, Any], ...] = ()
    rollover_actions: tuple[dict[str, Any], ...] = ()
    rollover_slippage_cost: float = 0.0
    rollover_canceled_orders: int = 0


@dataclass(frozen=True)
class BacktestPerformanceSummary:
    total_realized_pnl: float
    total_unrealized_pnl: float
    total_pnl: float
    max_equity: float
    min_equity: float
    max_drawdown: float
    order_status_counts: dict[str, int]


@dataclass(frozen=True)
class BacktestRunSpec:
    csv_path: str
    dataset_root: str = ""
    engine_mode: str = "csv"
    rollover_mode: str = "strict"
    rollover_price_mode: str = "bbo"
    rollover_slippage_bps: float = 0.0
    start_date: str = ""
    end_date: str = ""
    max_ticks: int | None = None
    deterministic_fills: bool = True
    wal_path: str | None = None
    account_id: str = "sim-account"
    run_id: str = "run-default"
    emit_state_snapshots: bool = False
    dataset_version: str = "local"
    factor_set_version: str = "default"
    experiment_id: str = ""

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> BacktestRunSpec:
        max_ticks_raw = raw.get("max_ticks")
        max_ticks = int(max_ticks_raw) if max_ticks_raw is not None else None
        wal_path_raw = raw.get("wal_path")
        wal_path = str(wal_path_raw) if wal_path_raw is not None else None
        return BacktestRunSpec(
            csv_path=str(raw["csv_path"]),
            dataset_root=str(raw.get("dataset_root", "")),
            engine_mode=str(raw.get("engine_mode", "csv")),
            rollover_mode=str(raw.get("rollover_mode", "strict")),
            rollover_price_mode=str(raw.get("rollover_price_mode", "bbo")),
            rollover_slippage_bps=float(raw.get("rollover_slippage_bps", 0.0)),
            start_date=str(raw.get("start_date", "")),
            end_date=str(raw.get("end_date", "")),
            max_ticks=max_ticks,
            deterministic_fills=bool(raw.get("deterministic_fills", True)),
            wal_path=wal_path,
            account_id=str(raw.get("account_id", "sim-account")),
            run_id=str(raw.get("run_id", "run-default")),
            emit_state_snapshots=bool(raw.get("emit_state_snapshots", False)),
            dataset_version=str(raw.get("dataset_version", "local")),
            factor_set_version=str(raw.get("factor_set_version", "default")),
            experiment_id=str(raw.get("experiment_id", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "csv_path": self.csv_path,
            "dataset_root": self.dataset_root,
            "engine_mode": self.engine_mode,
            "rollover_mode": self.rollover_mode,
            "rollover_price_mode": self.rollover_price_mode,
            "rollover_slippage_bps": self.rollover_slippage_bps,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "max_ticks": self.max_ticks,
            "deterministic_fills": self.deterministic_fills,
            "wal_path": self.wal_path,
            "account_id": self.account_id,
            "run_id": self.run_id,
            "emit_state_snapshots": self.emit_state_snapshots,
            "dataset_version": self.dataset_version,
            "factor_set_version": self.factor_set_version,
            "experiment_id": self.experiment_id,
        }


_SCENARIO_TEMPLATE_DEFAULTS: dict[str, dict[str, Any]] = {
    "smoke_fast": {
        "max_ticks": 2000,
        "deterministic_fills": False,
        "account_id": "sim-account",
    },
    "baseline_replay": {
        "max_ticks": 10000,
        "deterministic_fills": False,
        "account_id": "sim-account",
    },
    "deterministic_regression": {
        "max_ticks": 20000,
        "deterministic_fills": True,
        "account_id": "sim-account",
    },
}


def build_backtest_spec_from_template(
    template_name: str,
    *,
    csv_path: str,
    run_id: str | None = None,
    account_id: str | None = None,
    wal_dir: Path | str | None = None,
) -> BacktestRunSpec:
    normalized = template_name.strip().lower()
    defaults = _SCENARIO_TEMPLATE_DEFAULTS.get(normalized)
    if defaults is None:
        allowed = ", ".join(sorted(_SCENARIO_TEMPLATE_DEFAULTS))
        raise ValueError(f"unknown scenario template: {template_name}; allowed: {allowed}")

    resolved_run_id = run_id or f"{normalized}-run"
    resolved_account = account_id or str(defaults.get("account_id", "sim-account"))
    deterministic = bool(defaults.get("deterministic_fills", True))

    wal_path: str | None = None
    if deterministic and wal_dir is not None:
        wal_root = Path(wal_dir)
        wal_root.mkdir(parents=True, exist_ok=True)
        wal_path = str((wal_root / f"{resolved_run_id}.wal").resolve())

    return BacktestRunSpec(
        csv_path=csv_path,
        max_ticks=int(defaults.get("max_ticks", 10000)),
        deterministic_fills=deterministic,
        wal_path=wal_path,
        account_id=resolved_account,
        run_id=resolved_run_id,
    )


@dataclass(frozen=True)
class BacktestRunResult:
    run_id: str
    mode: str
    spec: BacktestRunSpec
    replay: ReplayReport
    deterministic: DeterministicReplayReport | None
    input_signature: str
    data_signature: str
    data_source: str = "csv"
    engine_mode: str = "csv"
    rollover_mode: str = "strict"
    attribution: dict[str, float] = field(default_factory=dict)
    risk_decomposition: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "run_id": self.run_id,
            "mode": self.mode,
            "data_source": self.data_source,
            "engine_mode": self.engine_mode,
            "rollover_mode": self.rollover_mode,
            "metric_keys": list(metric_keys()),
            "spec": self.spec.to_dict(),
            "input_signature": self.input_signature,
            "data_signature": self.data_signature,
            "attribution": dict(self.attribution),
            "risk_decomposition": dict(self.risk_decomposition),
            "replay": {
                "ticks_read": self.replay.ticks_read,
                "bars_emitted": self.replay.bars_emitted,
                "intents_emitted": self.replay.intents_emitted,
                "first_instrument": self.replay.first_instrument,
                "last_instrument": self.replay.last_instrument,
                "instrument_count": self.replay.instrument_count,
                "instrument_universe": list(self.replay.instrument_universe),
                "first_ts_ns": self.replay.first_ts_ns,
                "last_ts_ns": self.replay.last_ts_ns,
            },
        }
        if self.deterministic is not None:
            payload["deterministic"] = {
                "intents_processed": self.deterministic.intents_processed,
                "order_events_emitted": self.deterministic.order_events_emitted,
                "wal_records": self.deterministic.wal_records,
                "instrument_bars": self.deterministic.instrument_bars,
                "instrument_pnl": {
                    instrument_id: {
                        "net_position": item.net_position,
                        "avg_open_price": item.avg_open_price,
                        "realized_pnl": item.realized_pnl,
                        "unrealized_pnl": item.unrealized_pnl,
                        "last_price": item.last_price,
                    }
                    for instrument_id, item in self.deterministic.instrument_pnl.items()
                },
                "total_realized_pnl": self.deterministic.total_realized_pnl,
                "total_unrealized_pnl": self.deterministic.total_unrealized_pnl,
                "performance": {
                    "total_realized_pnl": self.deterministic.performance.total_realized_pnl,
                    "total_unrealized_pnl": self.deterministic.performance.total_unrealized_pnl,
                    "total_pnl": self.deterministic.performance.total_pnl,
                    "max_equity": self.deterministic.performance.max_equity,
                    "min_equity": self.deterministic.performance.min_equity,
                    "max_drawdown": self.deterministic.performance.max_drawdown,
                    "order_status_counts": self.deterministic.performance.order_status_counts,
                },
                "invariant_violations": list(self.deterministic.invariant_violations),
                "rollover_events": list(self.deterministic.rollover_events),
                "rollover_actions": list(self.deterministic.rollover_actions),
                "rollover_slippage_cost": self.deterministic.rollover_slippage_cost,
                "rollover_canceled_orders": self.deterministic.rollover_canceled_orders,
            }
        return payload


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


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _build_state_snapshot_from_bar(bar: dict[str, object]) -> StateSnapshot7D:
    instrument_id = str(bar["instrument_id"])
    open_price = cast(float, bar["open"])
    high_price = cast(float, bar["high"])
    low_price = cast(float, bar["low"])
    close_price = cast(float, bar["close"])
    bid_volume = cast(float, bar["bid_volume_1"])
    ask_volume = cast(float, bar["ask_volume_1"])
    ts_ns = cast(int, bar["ts_ns"])

    if abs(open_price) > 1e-9:
        trend_score = (close_price - open_price) / abs(open_price)
    else:
        trend_score = 0.0
    trend_confidence = _clamp01(abs(trend_score) * 10.0)

    volatility_score = (
        (high_price - low_price) / abs(close_price) if abs(close_price) > 1e-9 else 0.0
    )
    volatility_confidence = _clamp01(volatility_score * 5.0)

    liquidity_depth = max(0.0, bid_volume + ask_volume)
    liquidity_balance = min(bid_volume, ask_volume)
    liquidity_score = _clamp01(liquidity_depth / 1000.0)
    liquidity_confidence = _clamp01(liquidity_balance / 500.0)

    pattern_score = 1.0 if close_price > open_price else (-1.0 if close_price < open_price else 0.0)
    pattern_confidence = 0.7 if pattern_score != 0.0 else 0.2

    return StateSnapshot7D(
        instrument_id=instrument_id,
        trend={"score": trend_score, "confidence": trend_confidence},
        volatility={"score": volatility_score, "confidence": volatility_confidence},
        liquidity={"score": liquidity_score, "confidence": liquidity_confidence},
        sentiment={"score": 0.0, "confidence": 0.1},
        seasonality={"score": 0.0, "confidence": 0.1},
        pattern={"score": pattern_score, "confidence": pattern_confidence},
        event_drive={"score": 0.0, "confidence": 0.1},
        ts_ns=ts_ns,
    )


def _collect_strategy_intents(
    runtime: StrategyRuntime,
    ctx: dict[str, object],
    bar: dict[str, object],
    *,
    emit_state_snapshots: bool,
) -> list[SignalIntent]:
    intents: list[SignalIntent] = []
    if emit_state_snapshots:
        intents.extend(runtime.on_state(ctx, _build_state_snapshot_from_bar(bar)))
    intents.extend(runtime.on_bar(ctx, [bar]))
    return intents


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
        "exchange_ts_ns": event.exchange_ts_ns,
        "recv_ts_ns": event.recv_ts_ns,
        "ts_ns": event.ts_ns,
        "account_id": event.account_id,
        "client_order_id": event.client_order_id,
        "exchange_order_id": event.exchange_order_id,
        "instrument_id": event.instrument_id,
        "trade_id": event.trade_id,
        "event_source": event.event_source,
        "status": _status_to_code(event.status),
        "total_volume": event.total_volume,
        "filled_volume": event.filled_volume,
        "avg_fill_price": event.avg_fill_price,
        "reason": event.reason,
        "trace_id": event.trace_id,
    }
    fp.write(json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n")


def _emit_wal_rollover_line(
    fp: TextIO,
    *,
    seq: int,
    action: dict[str, Any],
) -> None:
    record = {
        "seq": seq,
        "kind": "rollover",
        "ts_ns": int(action.get("ts_ns", 0)),
        "symbol": str(action.get("symbol", "")),
        "action": str(action.get("action", "")),
        "from_instrument": str(action.get("from_instrument", "")),
        "to_instrument": str(action.get("to_instrument", "")),
        "position": int(action.get("position", 0)),
        "side": str(action.get("side", "")),
        "price": float(action.get("price", 0.0)),
        "mode": str(action.get("mode", "")),
        "price_mode": str(action.get("price_mode", "")),
        "slippage_bps": float(action.get("slippage_bps", 0.0)),
        "canceled_orders": int(action.get("canceled_orders", 0)),
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


def _instrument_symbol(instrument_id: str) -> str:
    symbol = []
    for ch in instrument_id:
        if ch.isalpha():
            symbol.append(ch)
            continue
        break
    return "".join(symbol).lower()


def _rollover_price(
    side: Side,
    *,
    last_price: float,
    bid_price: float,
    ask_price: float,
    price_mode: str,
    slippage_bps: float,
) -> tuple[float, float]:
    normalized_mode = price_mode.strip().lower()
    if normalized_mode == "last":
        base_price = last_price
    elif normalized_mode == "mid":
        if bid_price > 0.0 and ask_price > 0.0:
            base_price = (bid_price + ask_price) * 0.5
        else:
            base_price = last_price
    else:
        if side == Side.BUY:
            base_price = ask_price if ask_price > 0.0 else last_price
        else:
            base_price = bid_price if bid_price > 0.0 else last_price

    slip = max(0.0, slippage_bps) * 0.0001 * max(0.0, base_price)
    if side == Side.BUY:
        return max(0.0, base_price + slip), slip
    return max(0.0, base_price - slip), slip


def _compute_total_equity(
    instrument_state: dict[str, _InstrumentPnlState],
    mark_prices: dict[str, float],
) -> float:
    total = 0.0
    for instrument_id in instrument_state:
        state = instrument_state[instrument_id]
        mark_price = mark_prices.get(instrument_id, state.avg_open_price)
        total += state.realized_pnl + _compute_unrealized(
            state.net_position, state.avg_open_price, mark_price
        )
    return total


def _compute_equity_profile(equity_points: list[float]) -> tuple[float, float, float]:
    if not equity_points:
        return 0.0, 0.0, 0.0
    max_equity = equity_points[0]
    min_equity = equity_points[0]
    running_peak = equity_points[0]
    max_drawdown = 0.0
    for equity in equity_points:
        if equity > max_equity:
            max_equity = equity
        if equity < min_equity:
            min_equity = equity
        if equity > running_peak:
            running_peak = equity
        drawdown = running_peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return max_equity, min_equity, max_drawdown


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


def _build_input_signature(spec: BacktestRunSpec) -> str:
    canonical = json.dumps(spec.to_dict(), ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _build_data_signature(csv_path: Path | str) -> str:
    hasher = hashlib.sha256()
    with Path(csv_path).open("rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _build_parquet_dataset_signature(
    dataset_root: Path | str,
    *,
    start_date: str = "",
    end_date: str = "",
) -> str:
    root = Path(dataset_root)
    hasher = hashlib.sha256()
    hasher.update(str(root.resolve()).encode("utf-8"))
    hasher.update(start_date.encode("utf-8"))
    hasher.update(end_date.encode("utf-8"))

    parquet_files = _discover_parquet_files(root, start_date=start_date, end_date=end_date)
    for path in parquet_files:
        relative = path.relative_to(root).as_posix()
        trading_day = _extract_trading_day_from_parts(path.relative_to(root).parts)
        if start_date and trading_day and trading_day < start_date:
            continue
        if end_date and trading_day and trading_day > end_date:
            continue
        stat = path.stat()
        hasher.update(relative.encode("utf-8"))
        hasher.update(str(stat.st_size).encode("utf-8"))
        hasher.update(str(stat.st_mtime_ns).encode("utf-8"))
    return hasher.hexdigest()


def _resolve_csv_path_for_spec(spec: BacktestRunSpec) -> str:
    return spec.csv_path


def _extract_trading_day_from_parts(parts: tuple[str, ...]) -> str:
    for part in parts:
        if part.startswith("trading_day="):
            return part.split("=", 1)[1]
    return ""


def _discover_parquet_files(
    root: Path,
    *,
    start_date: str = "",
    end_date: str = "",
) -> list[Path]:
    day_partitions: list[Path] = []
    for entry in root.iterdir() if root.exists() else ():
        if not entry.is_dir() or not entry.name.startswith("trading_day="):
            continue
        trading_day = entry.name.split("=", 1)[1]
        if start_date and trading_day < start_date:
            continue
        if end_date and trading_day > end_date:
            continue
        day_partitions.append(entry)

    if day_partitions:
        parquet_files: list[Path] = []
        for partition_dir in sorted(day_partitions):
            parquet_files.extend(sorted(partition_dir.rglob("*.parquet")))
        return parquet_files

    parquet_files = sorted(root.rglob("*.parquet")) if root.exists() else []
    filtered_files: list[Path] = []
    for file_path in parquet_files:
        trading_day = _extract_trading_day_from_parts(file_path.relative_to(root).parts)
        if start_date and trading_day and trading_day < start_date:
            continue
        if end_date and trading_day and trading_day > end_date:
            continue
        filtered_files.append(file_path)
    return filtered_files


def _parse_tick_row(row: dict[str, Any]) -> CsvTick:
    return CsvTick(
        trading_day=str(row.get("TradingDay", "") or ""),
        instrument_id=str(row.get("InstrumentID", "") or ""),
        update_time=str(row.get("UpdateTime", "") or ""),
        update_millisec=_parse_int(str(row.get("UpdateMillisec", "") or "")),
        last_price=_parse_float(str(row.get("LastPrice", "") or "")),
        volume=_parse_int(str(row.get("Volume", "") or "")),
        bid_price_1=_parse_float(str(row.get("BidPrice1", "") or "")),
        bid_volume_1=_parse_int(str(row.get("BidVolume1", "") or "")),
        ask_price_1=_parse_float(str(row.get("AskPrice1", "") or "")),
        ask_volume_1=_parse_int(str(row.get("AskVolume1", "") or "")),
        average_price=_parse_float(str(row.get("AveragePrice", "") or "")),
        turnover=_parse_float(str(row.get("Turnover", "") or "")),
        open_interest=_parse_float(str(row.get("OpenInterest", "") or "")),
    )


def _iter_parquet_file_ticks(file_path: Path, pq_module: Any) -> Iterator[CsvTick]:
    parquet_file = pq_module.ParquetFile(str(file_path))
    columns = [
        "TradingDay",
        "InstrumentID",
        "UpdateTime",
        "UpdateMillisec",
        "LastPrice",
        "Volume",
        "BidPrice1",
        "BidVolume1",
        "AskPrice1",
        "AskVolume1",
        "AveragePrice",
        "Turnover",
        "OpenInterest",
    ]
    column_index = {name: idx for idx, name in enumerate(columns)}

    def cell(data: dict[str, list[Any]], name: str, row_idx: int) -> Any:
        values = data.get(name)
        if values is None or row_idx >= len(values):
            return ""
        value = values[row_idx]
        return "" if value is None else value

    for batch in parquet_file.iter_batches(columns=columns):
        data = {name: batch.column(index).to_pylist() for name, index in column_index.items()}
        for row_idx in range(batch.num_rows):
            yield CsvTick(
                trading_day=str(cell(data, "TradingDay", row_idx)),
                instrument_id=str(cell(data, "InstrumentID", row_idx)),
                update_time=str(cell(data, "UpdateTime", row_idx)),
                update_millisec=_parse_int(str(cell(data, "UpdateMillisec", row_idx))),
                last_price=_parse_float(str(cell(data, "LastPrice", row_idx))),
                volume=_parse_int(str(cell(data, "Volume", row_idx))),
                bid_price_1=_parse_float(str(cell(data, "BidPrice1", row_idx))),
                bid_volume_1=_parse_int(str(cell(data, "BidVolume1", row_idx))),
                ask_price_1=_parse_float(str(cell(data, "AskPrice1", row_idx))),
                ask_volume_1=_parse_int(str(cell(data, "AskVolume1", row_idx))),
                average_price=_parse_float(str(cell(data, "AveragePrice", row_idx))),
                turnover=_parse_float(str(cell(data, "Turnover", row_idx))),
                open_interest=_parse_float(str(cell(data, "OpenInterest", row_idx))),
            )


def iter_parquet_ticks(
    dataset_root: Path | str,
    *,
    max_ticks: int | None = None,
    start_date: str = "",
    end_date: str = "",
) -> Iterator[CsvTick]:
    root = Path(dataset_root)
    if not root.exists():
        raise ValueError(f"dataset_root does not exist: {root}")

    try:
        pq = importlib.import_module("pyarrow.parquet")
    except ModuleNotFoundError as exc:
        raise RuntimeError("pyarrow is required for parquet replay mode") from exc

    filtered_files = _discover_parquet_files(root, start_date=start_date, end_date=end_date)

    heap: list[tuple[int, int, CsvTick, Iterator[CsvTick]]] = []
    sequence = 0
    for file_path in filtered_files:
        tick_iter = _iter_parquet_file_ticks(file_path, pq)
        try:
            tick = next(tick_iter)
        except StopIteration:
            continue
        ts_ns = _to_ts_ns(tick.trading_day, tick.update_time, tick.update_millisec)
        heapq.heappush(heap, (ts_ns, sequence, tick, tick_iter))
        sequence += 1

    emitted = 0
    while heap:
        _, _, tick, tick_iter = heapq.heappop(heap)
        yield tick
        emitted += 1
        if max_ticks is not None and emitted >= max_ticks:
            return

        try:
            next_tick = next(tick_iter)
        except StopIteration:
            continue
        next_ts_ns = _to_ts_ns(next_tick.trading_day, next_tick.update_time, next_tick.update_millisec)
        heapq.heappush(heap, (next_ts_ns, sequence, next_tick, tick_iter))
        sequence += 1


def _replay_minute_bars_from_ticks(
    ticks: Iterator[CsvTick],
    runtime: StrategyRuntime,
    ctx: dict[str, object],
    *,
    emit_state_snapshots: bool = False,
) -> ReplayReport:
    ticks_read = 0
    bars_emitted = 0
    intents_emitted = 0
    first_instrument = ""
    last_instrument = ""
    instrument_ids: set[str] = set()
    first_ts_ns = 0
    last_ts_ns = 0

    bucket: list[CsvTick] = []
    current_key = ""

    for tick in ticks:
        ticks_read += 1
        instrument_ids.add(tick.instrument_id)
        tick_ts_ns = _to_ts_ns(tick.trading_day, tick.update_time, tick.update_millisec)
        if first_ts_ns == 0:
            first_ts_ns = tick_ts_ns
        last_ts_ns = tick_ts_ns
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
        intents_emitted += len(
            _collect_strategy_intents(
                runtime,
                ctx,
                bar,
                emit_state_snapshots=emit_state_snapshots,
            )
        )
        bars_emitted += 1

        bucket = [tick]
        current_key = key

    if bucket:
        bar = _build_bar(bucket)
        intents_emitted += len(
            _collect_strategy_intents(
                runtime,
                ctx,
                bar,
                emit_state_snapshots=emit_state_snapshots,
            )
        )
        bars_emitted += 1

    return ReplayReport(
        ticks_read=ticks_read,
        bars_emitted=bars_emitted,
        intents_emitted=intents_emitted,
        first_instrument=first_instrument,
        last_instrument=last_instrument,
        instrument_count=len(instrument_ids),
        instrument_universe=tuple(sorted(instrument_ids)),
        first_ts_ns=first_ts_ns,
        last_ts_ns=last_ts_ns,
    )


def _replay_deterministic_from_ticks(
    ticks: Iterator[CsvTick],
    runtime: StrategyRuntime,
    ctx: dict[str, object],
    *,
    wal_path: Path | str | None = None,
    account_id: str = "sim-account",
    emit_state_snapshots: bool = False,
    trade_recorder: TradeRecorder | None = None,
    enable_rollover: bool = False,
    rollover_mode: str = "strict",
    rollover_price_mode: str = "bbo",
    rollover_slippage_bps: float = 0.0,
) -> DeterministicReplayReport:
    ticks_read = 0
    bars_emitted = 0
    intents_processed = 0
    order_events_emitted = 0
    wal_records = 0
    first_instrument = ""
    last_instrument = ""
    instrument_ids: set[str] = set()
    first_ts_ns = 0
    last_ts_ns = 0
    next_order_id = 1
    next_wal_seq = 1
    instrument_bars: defaultdict[str, int] = defaultdict(int)
    last_close_price: dict[str, float] = {}
    instrument_state: defaultdict[str, _InstrumentPnlState] = defaultdict(_InstrumentPnlState)
    order_status_counts: defaultdict[str, int] = defaultdict(int)
    equity_points: list[float] = []
    rollover_events: list[dict[str, Any]] = []
    rollover_actions: list[dict[str, Any]] = []
    rollover_slippage_cost = 0.0
    rollover_canceled_orders = 0
    symbol_active_contract: dict[str, str] = {}
    pending_orders: defaultdict[str, list[SignalIntent]] = defaultdict(list)

    bucket: list[CsvTick] = []
    current_key = ""
    wal_fp = None
    recorder = trade_recorder or InMemoryTradeRecorder()
    ctx["trade_recorder"] = recorder
    if wal_path is not None:
        wal_fp = Path(wal_path).open("w", encoding="utf-8")

    normalized_rollover_mode = rollover_mode.strip().lower()
    if normalized_rollover_mode not in {"strict", "carry"}:
        normalized_rollover_mode = "strict"

    normalized_rollover_price_mode = rollover_price_mode.strip().lower()
    if normalized_rollover_price_mode not in {"bbo", "mid", "last"}:
        normalized_rollover_price_mode = "bbo"

    normalized_rollover_slippage_bps = max(0.0, float(rollover_slippage_bps))

    def _emit_deterministic_fill(intent: SignalIntent, fill_price: float) -> None:
        nonlocal next_order_id, next_wal_seq, wal_records, order_events_emitted

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
            event_source="deterministic_accept",
            exchange_ts_ns=int(intent.ts_ns),
            recv_ts_ns=int(intent.ts_ns),
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
            trade_id=f"{client_order_id}-fill",
            event_source="deterministic_fill",
            exchange_ts_ns=int(intent.ts_ns),
            recv_ts_ns=int(intent.ts_ns),
        )

        runtime.on_order_event(ctx, accepted)
        runtime.on_order_event(ctx, filled)
        recorder.record_order_event(accepted)
        recorder.record_order_event(filled)
        order_events_emitted += 2
        order_status_counts[accepted.status] += 1
        order_status_counts[filled.status] += 1

        state = instrument_state[intent.instrument_id]
        _apply_trade(state, intent.side, intent.volume, fill_price)

        if wal_fp is not None:
            _emit_wal_line(wal_fp, seq=next_wal_seq, kind="order", event=accepted)
            next_wal_seq += 1
            _emit_wal_line(wal_fp, seq=next_wal_seq, kind="trade", event=filled)
            next_wal_seq += 1
            wal_records += 2

    def _process_bar(bar: dict[str, object]) -> None:
        nonlocal bars_emitted, intents_processed
        bars_emitted += 1
        instrument_id = str(bar["instrument_id"])
        instrument_bars[instrument_id] += 1
        fill_price = cast(float, bar["close"])
        last_close_price[instrument_id] = fill_price

        intents = _collect_strategy_intents(
            runtime,
            ctx,
            bar,
            emit_state_snapshots=emit_state_snapshots,
        )
        intents_processed += len(intents)
        for intent in intents:
            pending_orders[intent.instrument_id].append(intent)
            _emit_deterministic_fill(intent, fill_price)
            pending_orders[intent.instrument_id].clear()

        equity_points.append(_compute_total_equity(instrument_state, last_close_price))

    def _handle_rollover(tick: CsvTick) -> None:
        nonlocal rollover_slippage_cost, rollover_canceled_orders, next_wal_seq, wal_records

        symbol = _instrument_symbol(tick.instrument_id)
        if not symbol:
            return
        current_contract = tick.instrument_id
        previous_contract = symbol_active_contract.get(symbol)
        if not previous_contract or previous_contract == current_contract:
            symbol_active_contract[symbol] = current_contract
            return

        previous_state = instrument_state[previous_contract]
        previous_position = int(abs(previous_state.net_position))
        if previous_position == 0:
            symbol_active_contract[symbol] = current_contract
            return

        canceled = len(pending_orders[previous_contract])
        if canceled:
            pending_orders[previous_contract].clear()
            rollover_canceled_orders += canceled

        tick_ts_ns = _to_ts_ns(tick.trading_day, tick.update_time, tick.update_millisec)

        direction = "long" if previous_state.net_position > 0 else "short"

        applied_mode = normalized_rollover_mode
        next_state = instrument_state[current_contract]
        if normalized_rollover_mode == "carry" and next_state.net_position != 0:
            applied_mode = "strict"

        if applied_mode == "strict":
            close_side = Side.SELL if previous_state.net_position > 0 else Side.BUY
            open_side = Side.BUY if previous_state.net_position > 0 else Side.SELL
            close_price, close_slip = _rollover_price(
                close_side,
                last_price=tick.last_price,
                bid_price=tick.bid_price_1,
                ask_price=tick.ask_price_1,
                price_mode=normalized_rollover_price_mode,
                slippage_bps=normalized_rollover_slippage_bps,
            )
            open_price, open_slip = _rollover_price(
                open_side,
                last_price=tick.last_price,
                bid_price=tick.bid_price_1,
                ask_price=tick.ask_price_1,
                price_mode=normalized_rollover_price_mode,
                slippage_bps=normalized_rollover_slippage_bps,
            )
            _apply_trade(previous_state, close_side, previous_position, close_price)
            _apply_trade(next_state, open_side, previous_position, open_price)
            rollover_slippage_cost += (close_slip + open_slip) * float(previous_position)

            if canceled:
                action = {
                    "symbol": symbol,
                    "action": "cancel",
                    "from_instrument": previous_contract,
                    "to_instrument": current_contract,
                    "position": previous_position,
                    "side": "",
                    "price": 0.0,
                    "mode": applied_mode,
                    "price_mode": normalized_rollover_price_mode,
                    "slippage_bps": normalized_rollover_slippage_bps,
                    "canceled_orders": canceled,
                    "ts_ns": tick_ts_ns,
                }
                rollover_actions.append(action)
                if wal_fp is not None:
                    _emit_wal_rollover_line(wal_fp, seq=next_wal_seq, action=action)
                    next_wal_seq += 1
                    wal_records += 1

            close_action = {
                "symbol": symbol,
                "action": "close",
                "from_instrument": previous_contract,
                "to_instrument": current_contract,
                "position": previous_position,
                "side": close_side.value,
                "price": close_price,
                "mode": applied_mode,
                "price_mode": normalized_rollover_price_mode,
                "slippage_bps": normalized_rollover_slippage_bps,
                "canceled_orders": 0,
                "ts_ns": tick_ts_ns,
            }
            open_action = {
                "symbol": symbol,
                "action": "open",
                "from_instrument": previous_contract,
                "to_instrument": current_contract,
                "position": previous_position,
                "side": open_side.value,
                "price": open_price,
                "mode": applied_mode,
                "price_mode": normalized_rollover_price_mode,
                "slippage_bps": normalized_rollover_slippage_bps,
                "canceled_orders": 0,
                "ts_ns": tick_ts_ns,
            }
            rollover_actions.append(close_action)
            rollover_actions.append(open_action)
            if wal_fp is not None:
                _emit_wal_rollover_line(wal_fp, seq=next_wal_seq, action=close_action)
                next_wal_seq += 1
                wal_records += 1
                _emit_wal_rollover_line(wal_fp, seq=next_wal_seq, action=open_action)
                next_wal_seq += 1
                wal_records += 1
        else:
            close_price = last_close_price.get(previous_contract, tick.last_price)
            open_price = close_price
            next_state.net_position = previous_state.net_position
            next_state.avg_open_price = previous_state.avg_open_price
            next_state.realized_pnl += previous_state.realized_pnl
            previous_state.realized_pnl = 0.0
            previous_state.avg_open_price = 0.0
            previous_state.net_position = 0

            if canceled:
                action = {
                    "symbol": symbol,
                    "action": "cancel",
                    "from_instrument": previous_contract,
                    "to_instrument": current_contract,
                    "position": previous_position,
                    "side": "",
                    "price": 0.0,
                    "mode": applied_mode,
                    "price_mode": normalized_rollover_price_mode,
                    "slippage_bps": normalized_rollover_slippage_bps,
                    "canceled_orders": canceled,
                    "ts_ns": tick_ts_ns,
                }
                rollover_actions.append(action)
                if wal_fp is not None:
                    _emit_wal_rollover_line(wal_fp, seq=next_wal_seq, action=action)
                    next_wal_seq += 1
                    wal_records += 1

            carry_action = {
                "symbol": symbol,
                "action": "carry",
                "from_instrument": previous_contract,
                "to_instrument": current_contract,
                "position": previous_position,
                "side": "",
                "price": open_price,
                "mode": applied_mode,
                "price_mode": normalized_rollover_price_mode,
                "slippage_bps": normalized_rollover_slippage_bps,
                "canceled_orders": 0,
                "ts_ns": tick_ts_ns,
            }
            rollover_actions.append(carry_action)
            if wal_fp is not None:
                _emit_wal_rollover_line(wal_fp, seq=next_wal_seq, action=carry_action)
                next_wal_seq += 1
                wal_records += 1

        rollover_events.append(
            {
                "symbol": symbol,
                "from_instrument": previous_contract,
                "to_instrument": current_contract,
                "mode": applied_mode,
                "position": previous_position,
                "direction": direction,
                "from_price": close_price,
                "to_price": open_price,
                "canceled_orders": canceled,
                "price_mode": normalized_rollover_price_mode,
                "slippage_bps": normalized_rollover_slippage_bps,
                "ts_ns": tick_ts_ns,
            }
        )

        symbol_active_contract[symbol] = current_contract

    try:
        for tick in ticks:
            ticks_read += 1
            instrument_ids.add(tick.instrument_id)
            tick_ts_ns = _to_ts_ns(tick.trading_day, tick.update_time, tick.update_millisec)
            if first_ts_ns == 0:
                first_ts_ns = tick_ts_ns
            last_ts_ns = tick_ts_ns
            if not first_instrument:
                first_instrument = tick.instrument_id
            last_instrument = tick.instrument_id

            if enable_rollover:
                _handle_rollover(tick)

            key = _minute_key(tick)
            if not bucket:
                current_key = key
                bucket.append(tick)
                continue

            if key == current_key:
                bucket.append(tick)
                continue

            bar = _build_bar(bucket)
            _process_bar(bar)

            bucket = [tick]
            current_key = key

        if bucket:
            bar = _build_bar(bucket)
            _process_bar(bar)
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
        instrument_count=len(instrument_ids),
        instrument_universe=tuple(sorted(instrument_ids)),
        first_ts_ns=first_ts_ns,
        last_ts_ns=last_ts_ns,
    )

    max_equity, min_equity, max_drawdown = _compute_equity_profile(equity_points)
    performance = BacktestPerformanceSummary(
        total_realized_pnl=total_realized_pnl,
        total_unrealized_pnl=total_unrealized_pnl,
        total_pnl=total_realized_pnl + total_unrealized_pnl,
        max_equity=max_equity,
        min_equity=min_equity,
        max_drawdown=max_drawdown,
        order_status_counts=dict(order_status_counts),
    )

    return DeterministicReplayReport(
        replay=replay_report,
        intents_processed=intents_processed,
        order_events_emitted=order_events_emitted,
        wal_records=wal_records,
        rollover_events=tuple(rollover_events),
        rollover_actions=tuple(rollover_actions),
        rollover_slippage_cost=rollover_slippage_cost,
        rollover_canceled_orders=rollover_canceled_orders,
        instrument_bars=dict(instrument_bars),
        instrument_pnl=instrument_pnl,
        total_realized_pnl=total_realized_pnl,
        total_unrealized_pnl=total_unrealized_pnl,
        performance=performance,
        invariant_violations=_validate_invariants(instrument_pnl),
    )


def load_backtest_run_spec(spec_file: Path | str) -> BacktestRunSpec:
    raw = json.loads(Path(spec_file).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("backtest spec must be a JSON object")
    return BacktestRunSpec.from_dict(raw)


def run_backtest_spec(
    spec: BacktestRunSpec,
    runtime: StrategyRuntime,
    *,
    ctx: dict[str, object] | None = None,
    trade_recorder: TradeRecorder | None = None,
) -> BacktestRunResult:
    run_ctx: dict[str, object] = {} if ctx is None else ctx
    mode = "deterministic" if spec.deterministic_fills else "bar_replay"
    run_ctx.setdefault("metric_keys", list(metric_keys()))
    ensure_backtest_ctx(run_ctx, run_id=spec.run_id, mode=mode, clock_ns=0)
    resolved_csv_path = _resolve_csv_path_for_spec(spec)
    engine_mode = spec.engine_mode.strip().lower()
    rollover_mode = spec.rollover_mode.strip().lower()
    rollover_price_mode = spec.rollover_price_mode.strip().lower()
    rollover_slippage_bps = float(spec.rollover_slippage_bps)

    if engine_mode not in {"csv", "parquet", "core_sim"}:
        raise ValueError(f"unsupported engine_mode: {spec.engine_mode}")

    if rollover_mode not in {"strict", "carry"}:
        raise ValueError(f"unsupported rollover_mode: {spec.rollover_mode}")

    if rollover_price_mode not in {"bbo", "mid", "last"}:
        raise ValueError(f"unsupported rollover_price_mode: {spec.rollover_price_mode}")

    if rollover_slippage_bps < 0.0:
        raise ValueError("rollover_slippage_bps must be non-negative")

    if engine_mode == "parquet":
        ticks = iter_parquet_ticks(
            spec.dataset_root,
            max_ticks=spec.max_ticks,
            start_date=spec.start_date,
            end_date=spec.end_date,
        )
        data_source = "parquet"
    elif engine_mode == "core_sim":
        if spec.dataset_root:
            ticks = iter_parquet_ticks(
                spec.dataset_root,
                max_ticks=spec.max_ticks,
                start_date=spec.start_date,
                end_date=spec.end_date,
            )
            data_source = "parquet"
        else:
            if not resolved_csv_path:
                raise ValueError("csv_path is required for core_sim when dataset_root is empty")
            ticks = iter_csv_ticks(resolved_csv_path, max_ticks=spec.max_ticks)
            data_source = "csv"
    else:
        ticks = iter_csv_ticks(resolved_csv_path, max_ticks=spec.max_ticks)
        data_source = "csv"

    if spec.deterministic_fills:
        deterministic = _replay_deterministic_from_ticks(
            ticks,
            runtime,
            run_ctx,
            wal_path=spec.wal_path,
            account_id=spec.account_id,
            emit_state_snapshots=spec.emit_state_snapshots,
            trade_recorder=trade_recorder,
            enable_rollover=(engine_mode == "core_sim"),
            rollover_mode=rollover_mode,
            rollover_price_mode=rollover_price_mode,
            rollover_slippage_bps=rollover_slippage_bps,
        )
        replay = deterministic.replay
    else:
        deterministic = None
        replay = _replay_minute_bars_from_ticks(
            ticks,
            runtime,
            run_ctx,
            emit_state_snapshots=spec.emit_state_snapshots,
        )

    data_signature = (
        _build_parquet_dataset_signature(
            spec.dataset_root,
            start_date=spec.start_date,
            end_date=spec.end_date,
        )
        if data_source == "parquet"
        else _build_data_signature(resolved_csv_path)
    )

    return BacktestRunResult(
        run_id=spec.run_id,
        mode=mode,
        spec=spec,
        replay=replay,
        deterministic=deterministic,
        input_signature=_build_input_signature(spec),
        data_signature=data_signature,
        data_source=data_source,
        engine_mode=spec.engine_mode,
        rollover_mode=spec.rollover_mode,
        attribution={},
        risk_decomposition={},
    )


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
    emit_state_snapshots: bool = False,
) -> ReplayReport:
    return _replay_minute_bars_from_ticks(
        iter_csv_ticks(csv_path, max_ticks=max_ticks),
        runtime,
        ctx,
        emit_state_snapshots=emit_state_snapshots,
    )


def replay_csv_with_deterministic_fills(
    csv_path: Path | str,
    runtime: StrategyRuntime,
    ctx: dict[str, object],
    *,
    max_ticks: int | None = None,
    wal_path: Path | str | None = None,
    account_id: str = "sim-account",
    emit_state_snapshots: bool = False,
    trade_recorder: TradeRecorder | None = None,
) -> DeterministicReplayReport:
    return _replay_deterministic_from_ticks(
        iter_csv_ticks(csv_path, max_ticks=max_ticks),
        runtime,
        ctx,
        wal_path=wal_path,
        account_id=account_id,
        emit_state_snapshots=emit_state_snapshots,
        trade_recorder=trade_recorder,
    )
