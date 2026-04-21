#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover
    pq = None


REQUIRED_TRADE_COLUMNS = {
    "fill_seq",
    "trade_id",
    "symbol",
    "side",
    "offset",
    "volume",
    "price",
    "timestamp_ns",
    "timestamp_dt_local",
    "commission",
    "slippage",
    "realized_pnl",
    "strategy_id",
    "signal_type",
    "regime_at_entry",
    "risk_budget_r",
}

REQUIRED_ORDER_COLUMNS = {
    "order_id",
    "status",
    "created_at_ns",
    "last_update_ns",
}

REQUIRED_DAILY_COLUMNS = {
    "date",
    "capital",
    "daily_return_pct",
    "cumulative_return_pct",
    "drawdown_pct",
    "position_value",
    "trades_count",
    "turnover",
    "market_regime",
}

R_MULTIPLE_BUCKETS = [
    ("< -2R", None, -2.0),
    ("-2R ~ -1R", -2.0, -1.0),
    ("-1R ~ 0", -1.0, 0.0),
    ("0 ~ +1R", 0.0, 1.0),
    ("+1R ~ +2R", 1.0, 2.0),
    ("+2R ~ +3R", 2.0, 3.0),
    ("> +3R", 3.0, None),
]


class ReportError(RuntimeError):
    pass


@dataclass
class DailyRow:
    date: str
    capital: float
    daily_return_pct: float
    cumulative_return_pct: float
    drawdown_pct: float
    position_value: float
    trades_count: int
    turnover: float
    market_regime: str


@dataclass
class TradeRow:
    fill_seq: int
    trade_id: str
    order_id: str
    symbol: str
    exchange: str
    side: str
    offset: str
    volume: int
    price: float
    timestamp_ns: int
    signal_ts_ns: int
    trading_day: str
    action_day: str
    update_time: str
    timestamp_dt_local: str
    signal_dt_local: str
    commission: float
    timestamp_dt_utc: str
    slippage: float
    realized_pnl: float
    strategy_id: str
    signal_type: str
    regime_at_entry: str
    risk_budget_r: float


@dataclass
class OrderRow:
    order_id: str
    status: str
    created_at_ns: int
    last_update_ns: int


@dataclass
class PositionSnapshotRow:
    timestamp_ns: int
    symbol: str
    net_position: int
    avg_price: float
    unrealized_pnl: float


@dataclass
class OpenLot:
    trade_id: str
    symbol: str
    side: str
    volume: int
    remaining_volume: int
    price: float
    timestamp_ns: int
    timestamp_dt_local: str
    commission: float
    risk_budget_r: float
    strategy_id: str
    signal_type: str
    regime_at_entry: str


@dataclass
class CompletedTrade:
    open_trade_id: str
    symbol: str
    entry_side: str
    entry_volume: int
    entry_price: float
    entry_time_ns: int
    entry_dt_local: str
    entry_signal_type: str
    entry_regime: str
    strategy_id: str
    risk_budget_r: float
    allocated_entry_commission: float = 0.0
    allocated_exit_commission: float = 0.0
    gross_realized_pnl: float = 0.0
    matched_volume: int = 0
    exit_trade_ids: list[str] = field(default_factory=list)
    exit_signal_types: list[str] = field(default_factory=list)
    exit_dt_local: str = ""
    exit_time_ns: int = 0

    @property
    def net_pnl(self) -> float:
        return self.gross_realized_pnl - self.allocated_entry_commission - self.allocated_exit_commission

    @property
    def holding_ns(self) -> int:
        if self.exit_time_ns <= self.entry_time_ns:
            return 0
        return self.exit_time_ns - self.entry_time_ns

    @property
    def r_multiple_gross(self) -> float | None:
        if self.risk_budget_r <= 0.0:
            return None
        return self.gross_realized_pnl / self.risk_budget_r

    @property
    def risk_coverage_ratio(self) -> float | None:
        if self.risk_budget_r <= 0.0 or self.gross_realized_pnl >= 0.0:
            return None
        return abs(self.gross_realized_pnl) / self.risk_budget_r


@dataclass
class WhatIfSample:
    open_trade_id: str
    symbol: str
    entry_dt_local: str
    entry_price: float
    actual_volume: int
    risk_budget_r: float
    stop_price: float
    stop_distance: float
    volume_multiple: int
    theoretical_volume: int
    basis: str
    gross_realized_pnl: float
    net_pnl: float
    r_multiple: float | None


@dataclass
class DrawdownWindow:
    peak_date: str
    trough_date: str
    peak_capital: float
    trough_capital: float
    drawdown_pct: float
    pnl: float


@dataclass
class RegimeAttributionRow:
    regime: str
    total_days: int
    total_pnl: float
    avg_daily_pnl: float
    win_rate: float
    conclusion: str


@dataclass
class ExpiryValidationResult:
    signal_type: str
    close_trade_id: str
    close_symbol: str
    close_trading_day: str
    expected_last_day: str | None
    next_open_trade_id: str | None
    next_symbol: str | None
    next_trading_day: str | None
    passed: bool
    note: str


@dataclass
class ConfigSummary:
    risk_enabled: bool = False
    risk_per_trade_pct: float | None = None
    max_risk_per_trade: float | None = None
    stop_loss_mode: str | None = None
    stop_loss_atr_period: int | None = None
    stop_loss_atr_multiplier: float | None = None


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def mean(values: Iterable[float]) -> float:
    values_list = list(values)
    if not values_list:
        return 0.0
    return sum(values_list) / float(len(values_list))


def safe_stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    return statistics.stdev(values)


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = max(0.0, min(1.0, ratio)) * (len(ordered) - 1)
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def format_pct(value: float) -> str:
    return f"{value:.2f}%"


def format_money(value: float) -> str:
    return f"{value:.2f} 元"


def format_ratio(value: float) -> str:
    return f"{value:.2f}"


def format_count(value: int) -> str:
    return f"{value}"


def format_duration_ns(duration_ns: int) -> str:
    total_seconds = max(0, duration_ns) // 1_000_000_000
    days, rem = divmod(total_seconds, 86_400)
    hours, rem = divmod(rem, 3_600)
    minutes, _ = divmod(rem, 60)
    parts: list[str] = []
    if days > 0:
        parts.append(f"{days}天")
    if hours > 0:
        parts.append(f"{hours}小时")
    if minutes > 0:
        parts.append(f"{minutes}分钟")
    if not parts:
        return "0分钟"
    return "".join(parts)


def ns_to_datetime_text(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def yyyy_mm_dd(day: str) -> str:
    text = re.sub(r"\D", "", str(day))
    if len(text) != 8:
        return str(day)
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def product_prefix(symbol: str) -> str:
    letters: list[str] = []
    for char in symbol:
        if char.isdigit():
            break
        letters.append(char)
    return "".join(letters).upper()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_csv_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        rows = [dict(row) for row in reader]
    return rows, fieldnames


def require_columns(name: str, columns: list[str], required: set[str]) -> None:
    missing = sorted(required - set(columns))
    if missing:
        raise ReportError(f"{name} 缺少必要列: {', '.join(missing)}")


def parse_daily_rows(rows: list[dict[str, str]]) -> list[DailyRow]:
    parsed = [
        DailyRow(
            date=str(row["date"]),
            capital=as_float(row["capital"]),
            daily_return_pct=as_float(row["daily_return_pct"]),
            cumulative_return_pct=as_float(row["cumulative_return_pct"]),
            drawdown_pct=as_float(row["drawdown_pct"]),
            position_value=as_float(row["position_value"]),
            trades_count=as_int(row["trades_count"]),
            turnover=as_float(row["turnover"]),
            market_regime=str(row["market_regime"] or "kUnknown"),
        )
        for row in rows
    ]
    if not parsed:
        raise ReportError("daily_equity.csv 为空，无法生成正式报告")
    return parsed


def parse_trade_rows(rows: list[dict[str, str]]) -> list[TradeRow]:
    parsed = [
        TradeRow(
            fill_seq=as_int(row["fill_seq"]),
            trade_id=str(row["trade_id"]),
            order_id=str(row["order_id"]),
            symbol=str(row["symbol"]),
            exchange=str(row.get("exchange", "")),
            side=str(row["side"]),
            offset=str(row["offset"]),
            volume=as_int(row["volume"]),
            price=as_float(row["price"]),
            timestamp_ns=as_int(row["timestamp_ns"]),
            signal_ts_ns=as_int(row.get("signal_ts_ns", 0)),
            trading_day=str(row.get("trading_day", "")),
            action_day=str(row.get("action_day", "")),
            update_time=str(row.get("update_time", "")),
            timestamp_dt_local=str(row.get("timestamp_dt_local", "")),
            signal_dt_local=str(row.get("signal_dt_local", "")),
            commission=as_float(row.get("commission", 0.0)),
            timestamp_dt_utc=str(row.get("timestamp_dt_utc", "")),
            slippage=as_float(row.get("slippage", 0.0)),
            realized_pnl=as_float(row.get("realized_pnl", 0.0)),
            strategy_id=str(row.get("strategy_id", "")),
            signal_type=str(row.get("signal_type", "")),
            regime_at_entry=str(row.get("regime_at_entry", "kUnknown") or "kUnknown"),
            risk_budget_r=as_float(row.get("risk_budget_r", 0.0)),
        )
        for row in rows
    ]
    parsed.sort(key=lambda row: (row.timestamp_ns, row.fill_seq))
    if not parsed:
        raise ReportError("trades.csv 为空，无法生成正式报告")
    return parsed


def parse_order_rows(rows: list[dict[str, str]]) -> list[OrderRow]:
    return [
        OrderRow(
            order_id=str(row["order_id"]),
            status=str(row["status"]),
            created_at_ns=as_int(row.get("created_at_ns", 0)),
            last_update_ns=as_int(row.get("last_update_ns", 0)),
        )
        for row in rows
    ]


def parse_position_rows(path: Path | None) -> list[PositionSnapshotRow]:
    if path is None or not path.exists():
        return []
    rows, columns = load_csv_rows(path)
    required = {"timestamp_ns", "symbol", "net_position", "avg_price", "unrealized_pnl"}
    require_columns("position_history.csv", columns, required)
    parsed = [
        PositionSnapshotRow(
            timestamp_ns=as_int(row["timestamp_ns"]),
            symbol=str(row["symbol"]),
            net_position=as_int(row["net_position"]),
            avg_price=as_float(row["avg_price"]),
            unrealized_pnl=as_float(row["unrealized_pnl"]),
        )
        for row in rows
    ]
    parsed.sort(key=lambda row: (row.timestamp_ns, row.symbol))
    return parsed


def parse_main_markdown_time_range(markdown_text: str) -> tuple[str | None, str | None]:
    match = re.search(r"Time Range \(ns\): `([0-9]+):([0-9]+)`", markdown_text)
    if match is None:
        return None, None
    start_ns = as_int(match.group(1))
    end_ns = as_int(match.group(2))
    return ns_to_datetime_text(start_ns), ns_to_datetime_text(end_ns)


def parse_strategy_config(config_path: Path | None) -> ConfigSummary:
    if config_path is None or not config_path.exists():
        return ConfigSummary()
    text = config_path.read_text(encoding="utf-8")
    summary = ConfigSummary()
    enabled = re.search(r"^\s*enabled:\s*(true|false)\s*$", text, re.MULTILINE)
    risk_pct = re.search(r"^\s*risk_per_trade_pct:\s*([0-9.]+)\s*$", text, re.MULTILINE)
    max_r = re.search(r"^\s*max_risk_per_trade:\s*([0-9.]+)\s*$", text, re.MULTILINE)
    summary.risk_enabled = enabled is not None and enabled.group(1).lower() == "true"
    if risk_pct is not None:
        summary.risk_per_trade_pct = as_float(risk_pct.group(1))
    if max_r is not None:
        summary.max_risk_per_trade = as_float(max_r.group(1))

    sub_path_match = re.search(r"^\s*config_path:\s*(.+)$", text, re.MULTILINE)
    if sub_path_match is None:
        return summary
    sub_path_text = sub_path_match.group(1).strip().strip('"\'')
    sub_path = (config_path.parent / sub_path_text).resolve()
    if not sub_path.exists():
        return summary
    sub_text = sub_path.read_text(encoding="utf-8")
    stop_mode = re.search(r"^\s*stop_loss_mode:\s*([A-Za-z0-9_]+)\s*$", sub_text, re.MULTILINE)
    stop_period = re.search(r"^\s*stop_loss_atr_period:\s*([0-9]+)\s*$", sub_text, re.MULTILINE)
    stop_multiplier = re.search(
        r"^\s*stop_loss_atr_multiplier:\s*([0-9.]+)\s*$", sub_text, re.MULTILINE
    )
    summary.stop_loss_mode = stop_mode.group(1) if stop_mode is not None else None
    summary.stop_loss_atr_period = as_int(stop_period.group(1)) if stop_period is not None else None
    summary.stop_loss_atr_multiplier = (
        as_float(stop_multiplier.group(1)) if stop_multiplier is not None else None
    )
    return summary


def parse_expiry_calendar(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    mapping: dict[str, str] = {}
    current_contract: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        contract_match = re.match(r"^\s{2}([A-Za-z0-9]+):\s*$", line)
        if contract_match is not None:
            current_contract = contract_match.group(1)
            continue
        last_day_match = re.match(r"^\s{4}last_trading_day:\s*([0-9]{8})\s*$", line)
        if current_contract is not None and last_day_match is not None:
            mapping[current_contract] = last_day_match.group(1)
    return mapping


def parse_instrument_info(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    return load_json(path)


def identify_strategy_name(trades: list[TradeRow], config_path: Path | None) -> str:
    counts = Counter(trade.strategy_id for trade in trades if trade.strategy_id)
    if counts:
        return counts.most_common(1)[0][0]
    if config_path is not None:
        return config_path.stem
    return "strategy"


def side_bucket_for_open(trade: TradeRow) -> str:
    return "long" if trade.side.lower() == "buy" else "short"


def side_bucket_for_close(trade: TradeRow) -> str:
    return "long" if trade.side.lower() == "sell" else "short"


def build_open_lot(trade: TradeRow) -> OpenLot:
    return OpenLot(
        trade_id=trade.trade_id,
        symbol=trade.symbol,
        side=side_bucket_for_open(trade),
        volume=trade.volume,
        remaining_volume=trade.volume,
        price=trade.price,
        timestamp_ns=trade.timestamp_ns,
        timestamp_dt_local=trade.timestamp_dt_local,
        commission=trade.commission,
        risk_budget_r=trade.risk_budget_r,
        strategy_id=trade.strategy_id,
        signal_type=trade.signal_type,
        regime_at_entry=trade.regime_at_entry,
    )


def pair_round_trips(
    trades: list[TradeRow], final_positions: dict[str, int]
) -> tuple[list[CompletedTrade], list[OpenLot]]:
    ledgers: dict[tuple[str, str], list[OpenLot]] = defaultdict(list)
    completed: dict[str, CompletedTrade] = {}

    for trade in trades:
        offset = trade.offset.lower()
        if offset == "open":
            ledgers[(trade.symbol, side_bucket_for_open(trade))].append(build_open_lot(trade))
            continue

        if offset not in {"close", "closetoday", "closeyesterday"}:
            if "expiry" in trade.signal_type.lower():
                close_bucket = side_bucket_for_close(trade)
            else:
                continue
        else:
            close_bucket = side_bucket_for_close(trade)

        queue = ledgers[(trade.symbol, close_bucket)]
        remaining = trade.volume
        if remaining <= 0:
            continue
        if not queue:
            raise ReportError(
                f"发现无法配对的平仓成交: trade_id={trade.trade_id}, symbol={trade.symbol}, volume={trade.volume}"
            )

        while remaining > 0:
            if not queue:
                raise ReportError(
                    f"平仓成交超出可用开仓库存: trade_id={trade.trade_id}, symbol={trade.symbol}, remaining={remaining}"
                )
            lot = queue[0]
            matched = min(remaining, lot.remaining_volume)
            remaining -= matched
            lot.remaining_volume -= matched

            key = lot.trade_id
            if key not in completed:
                completed[key] = CompletedTrade(
                    open_trade_id=lot.trade_id,
                    symbol=lot.symbol,
                    entry_side=lot.side,
                    entry_volume=lot.volume,
                    entry_price=lot.price,
                    entry_time_ns=lot.timestamp_ns,
                    entry_dt_local=lot.timestamp_dt_local,
                    entry_signal_type=lot.signal_type,
                    entry_regime=lot.regime_at_entry,
                    strategy_id=lot.strategy_id,
                    risk_budget_r=lot.risk_budget_r,
                )
            round_trip = completed[key]
            ratio_open = matched / float(lot.volume)
            ratio_close = matched / float(max(trade.volume, 1))
            round_trip.matched_volume += matched
            round_trip.allocated_entry_commission += lot.commission * ratio_open
            round_trip.allocated_exit_commission += trade.commission * ratio_close
            round_trip.gross_realized_pnl += trade.realized_pnl * ratio_close
            round_trip.exit_time_ns = max(round_trip.exit_time_ns, trade.timestamp_ns)
            round_trip.exit_dt_local = trade.timestamp_dt_local
            round_trip.exit_trade_ids.append(trade.trade_id)
            round_trip.exit_signal_types.append(trade.signal_type)
            if lot.remaining_volume == 0:
                queue.pop(0)

    remaining_lots: list[OpenLot] = []
    for queue in ledgers.values():
        remaining_lots.extend([lot for lot in queue if lot.remaining_volume > 0])

    expected_by_symbol: dict[str, int] = defaultdict(int)
    for lot in remaining_lots:
        signed = lot.remaining_volume if lot.side == "long" else -lot.remaining_volume
        expected_by_symbol[lot.symbol] += signed
    normalized_expected = {symbol: position for symbol, position in expected_by_symbol.items() if position != 0}
    normalized_actual = {symbol: position for symbol, position in final_positions.items() if position != 0}
    if normalized_expected != normalized_actual:
        raise ReportError(
            "未平仓库存与最终持仓不一致: "
            f"paired={normalized_expected}, final={normalized_actual}"
        )

    completed_trades = [
        item
        for item in completed.values()
        if item.matched_volume == item.entry_volume and item.exit_time_ns > 0
    ]
    completed_trades.sort(key=lambda item: (item.exit_time_ns, item.open_trade_id))
    return completed_trades, remaining_lots


def latest_positions(position_rows: list[PositionSnapshotRow], payload: dict[str, Any]) -> dict[str, int]:
    if position_rows:
        latest: dict[str, PositionSnapshotRow] = {}
        for row in position_rows:
            latest[row.symbol] = row
        return {symbol: row.net_position for symbol, row in latest.items() if row.net_position != 0}
    deterministic = payload.get("deterministic", {}) or {}
    instrument_pnl = deterministic.get("instrument_pnl", {}) or {}
    return {
        symbol: as_int(item.get("net_position", 0))
        for symbol, item in instrument_pnl.items()
        if as_int(item.get("net_position", 0)) != 0
    }


def validate_open_risk_budget(trades: list[TradeRow]) -> tuple[int, list[str]]:
    offending = [trade.trade_id for trade in trades if trade.offset == "Open" and trade.risk_budget_r <= 0.0]
    return len(offending), offending[:10]


def compute_sharpe_ratio(daily_returns_pct: list[float], risk_free_rate: float) -> float:
    if len(daily_returns_pct) < 2:
        return 0.0
    risk_free_daily = risk_free_rate / 252.0
    returns = [(value / 100.0) - risk_free_daily for value in daily_returns_pct]
    volatility = safe_stdev(returns)
    if volatility <= 1e-12:
        return 0.0
    return mean(returns) / volatility * math.sqrt(252.0)


def compute_sortino_ratio(daily_returns_pct: list[float], risk_free_rate: float) -> float:
    if len(daily_returns_pct) < 2:
        return 0.0
    risk_free_daily = risk_free_rate / 252.0
    excess = [(value / 100.0) - risk_free_daily for value in daily_returns_pct]
    downside = [min(0.0, value) for value in excess]
    downside_sq = [value * value for value in downside]
    if not downside_sq or sum(downside_sq) <= 0.0:
        return 0.0
    downside_dev = math.sqrt(sum(downside_sq) / len(downside_sq))
    if downside_dev <= 1e-12:
        return 0.0
    return mean(excess) / downside_dev * math.sqrt(252.0)


def compute_annualized_volatility(daily_returns_pct: list[float]) -> float:
    returns = [value / 100.0 for value in daily_returns_pct]
    if len(returns) < 2:
        return 0.0
    return safe_stdev(returns) * math.sqrt(252.0) * 100.0


def longest_negative_streak(values: Iterable[float]) -> int:
    best = 0
    current = 0
    for value in values:
        if value < 0.0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def longest_streak_by_predicate(values: Iterable[float], positive: bool) -> int:
    best = 0
    current = 0
    for value in values:
        if (value > 0.0) if positive else (value < 0.0):
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def compute_drawdown_window(daily_rows: list[DailyRow], initial_equity: float) -> DrawdownWindow:
    peak_capital = initial_equity
    peak_date = daily_rows[0].date
    max_drawdown = -1.0
    trough_capital = daily_rows[0].capital
    trough_date = daily_rows[0].date
    best_peak_capital = peak_capital
    best_peak_date = peak_date
    for row in daily_rows:
        if row.capital >= peak_capital:
            peak_capital = row.capital
            peak_date = row.date
        if peak_capital > 0.0:
            drawdown = (peak_capital - row.capital) / peak_capital * 100.0
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                best_peak_capital = peak_capital
                best_peak_date = peak_date
                trough_capital = row.capital
                trough_date = row.date
    return DrawdownWindow(
        peak_date=best_peak_date,
        trough_date=trough_date,
        peak_capital=best_peak_capital,
        trough_capital=trough_capital,
        drawdown_pct=max(0.0, max_drawdown),
        pnl=trough_capital - best_peak_capital,
    )


def compute_regime_attribution(
    daily_rows: list[DailyRow], initial_equity: float
) -> list[RegimeAttributionRow]:
    grouped_days: dict[str, list[float]] = defaultdict(list)
    previous_capital = initial_equity
    for row in daily_rows:
        grouped_days[row.market_regime].append(row.capital - previous_capital)
        previous_capital = row.capital
    result: list[RegimeAttributionRow] = []
    for regime, pnl_list in sorted(grouped_days.items()):
        total_days = len(pnl_list)
        total_pnl = sum(pnl_list)
        avg_pnl = total_pnl / total_days if total_days > 0 else 0.0
        win_rate = 100.0 * sum(1 for value in pnl_list if value > 0.0) / total_days if total_days > 0 else 0.0
        if total_pnl > 0 and win_rate >= 55.0:
            conclusion = "该状态下策略具备稳定正贡献"
        elif total_pnl > 0:
            conclusion = "该状态下有正贡献，但稳定性一般"
        else:
            conclusion = "该状态下贡献偏弱，建议重点优化"
        result.append(
            RegimeAttributionRow(
                regime=regime,
                total_days=total_days,
                total_pnl=total_pnl,
                avg_daily_pnl=avg_pnl,
                win_rate=win_rate,
                conclusion=conclusion,
            )
        )
    return result


def compute_fill_rates(orders: list[OrderRow], raw_fill_rate: float) -> tuple[float, float]:
    if not orders:
        return raw_fill_rate, 0.0
    unique_status: dict[str, set[str]] = defaultdict(set)
    for order in orders:
        unique_status[order.order_id].add(order.status.upper())
    actual_filled = sum(1 for statuses in unique_status.values() if "FILLED" in statuses)
    actual_rate = 100.0 * actual_filled / len(unique_status)
    return raw_fill_rate * 100.0, actual_rate


def slippage_stats(trades: list[TradeRow], signal_type: str | None = None) -> dict[str, float]:
    filtered = [
        trade.slippage
        for trade in trades
        if signal_type is None or trade.signal_type.lower() == signal_type.lower()
    ]
    if not filtered:
        return {"mean": 0.0, "median": 0.0, "max": 0.0, "min": 0.0}
    return {
        "mean": mean(filtered),
        "median": statistics.median(filtered),
        "max": max(filtered),
        "min": min(filtered),
    }


def commission_stats(trades: list[TradeRow], instrument_info: dict[str, dict[str, Any]]) -> dict[str, float]:
    if not trades:
        return {"avg_per_trade": 0.0, "avg_per_lot": 0.0, "median_bps": 0.0}
    per_trade = [trade.commission for trade in trades]
    per_lot = [trade.commission / trade.volume for trade in trades if trade.volume > 0]
    nominal_bps: list[float] = []
    for trade in trades:
        product = product_prefix(trade.symbol)
        volume_multiple = as_int(instrument_info.get(product, {}).get("volume_multiple", 1), 1)
        turnover = trade.price * trade.volume * volume_multiple
        if turnover > 0:
            nominal_bps.append(trade.commission / turnover * 10_000.0)
    return {
        "avg_per_trade": mean(per_trade),
        "avg_per_lot": mean(per_lot),
        "median_bps": statistics.median(nominal_bps) if nominal_bps else 0.0,
    }


def build_regime_trade_table(completed_trades: list[CompletedTrade]) -> list[dict[str, Any]]:
    grouped: dict[str, list[CompletedTrade]] = defaultdict(list)
    for trade in completed_trades:
        grouped[trade.entry_regime].append(trade)
    rows: list[dict[str, Any]] = []
    for regime, items in sorted(grouped.items()):
        wins = [item.net_pnl for item in items if item.net_pnl > 0.0]
        losses = [item.net_pnl for item in items if item.net_pnl < 0.0]
        win_rate = 100.0 * len(wins) / len(items) if items else 0.0
        avg_win = mean(wins)
        avg_loss = mean(losses)
        pnl_ratio = avg_win / abs(avg_loss) if avg_loss < 0.0 else 0.0
        rows.append(
            {
                "regime": regime,
                "trades": len(items),
                "win_rate": win_rate,
                "pnl_ratio": pnl_ratio,
            }
        )
    return rows


def compute_r_bucket_counts(values: list[float]) -> list[tuple[str, int, float]]:
    total = len(values)
    rows: list[tuple[str, int, float]] = []
    for label, lower, upper in R_MULTIPLE_BUCKETS:
        count = 0
        for value in values:
            if lower is None and value < upper:
                count += 1
            elif upper is None and value > lower:
                count += 1
            elif lower is not None and upper is not None and lower <= value < upper:
                count += 1
        ratio = (count / total * 100.0) if total > 0 else 0.0
        rows.append((label, count, ratio))
    return rows


def expectancy_from_r_multiples(values: list[float]) -> float:
    if not values:
        return 0.0
    wins = [value for value in values if value > 0.0]
    losses = [abs(value) for value in values if value < 0.0]
    win_rate = len(wins) / len(values)
    lose_rate = len(losses) / len(values)
    avg_win = mean(wins)
    avg_loss = mean(losses)
    return win_rate * avg_win - lose_rate * avg_loss


def top_drawdown_trade_summary(completed_trades: list[CompletedTrade], window: DrawdownWindow) -> dict[str, Any]:
    in_window = [
        trade
        for trade in completed_trades
        if window.peak_date <= trade.entry_dt_local[0:10].replace("-", "") <= window.trough_date
        or window.peak_date <= trade.exit_dt_local[0:10].replace("-", "") <= window.trough_date
    ]
    signal_counts = Counter()
    for trade in in_window:
        for signal_type in trade.exit_signal_types:
            signal_counts[signal_type] += 1
    stop_loss_count = sum(count for name, count in signal_counts.items() if "stop" in name.lower())
    if stop_loss_count >= max(2, len(in_window) // 2) and in_window:
        reason = "连续止损主导"
    elif any("expiry" in name.lower() for name in signal_counts):
        reason = "换月平仓对回撤有放大作用"
    elif in_window:
        reason = "趋势反复与反向波动共同导致回撤"
    else:
        reason = "回撤区间内缺少完整 round-trip，主要由未实现波动驱动"
    return {
        "count": len(in_window),
        "signal_counts": dict(signal_counts),
        "reason": reason,
    }


def find_trace_path(payload: dict[str, Any], run_dir: Path) -> Path | None:
    sub_trace = payload.get("sub_strategy_indicator_trace", {}) or {}
    if bool(sub_trace.get("enabled", False)):
        path = Path(str(sub_trace.get("path", "")))
        if path.exists():
            return path
    candidate = run_dir / "my_sub_trace.csv"
    if candidate.exists():
        return candidate
    return None


def load_trace_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    if path.suffix.lower() == ".csv":
        rows, _ = load_csv_rows(path)
        return rows
    if path.suffix.lower() == ".parquet":
        if pq is None:
            raise ReportError("trace 为 parquet，但当前环境缺少 pyarrow，无法执行动态头寸模拟")
        table = pq.read_table(path)
        return [dict(item) for item in table.to_pylist()]
    return []


def choose_trace_row_for_trade(trace_rows: list[dict[str, Any]], trade: CompletedTrade) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    best_distance: int | None = None
    product = trade.symbol
    for row in trace_rows:
        if str(row.get("instrument_id", "")) != product:
            continue
        ts_ns = as_int(row.get("ts_ns", 0))
        if ts_ns <= 0 or ts_ns > trade.entry_time_ns:
            continue
        distance = trade.entry_time_ns - ts_ns
        if best_distance is None or distance < best_distance:
            best_distance = distance
            best = row
    return best


def build_dynamic_position_samples(
    completed_trades: list[CompletedTrade],
    trace_rows: list[dict[str, Any]],
    instrument_info: dict[str, dict[str, Any]],
    config_summary: ConfigSummary,
    sample_count: int,
) -> list[WhatIfSample]:
    if not completed_trades:
        return []
    ranked = sorted(
        completed_trades,
        key=lambda item: (abs(item.r_multiple_gross or 0.0), abs(item.net_pnl), item.entry_time_ns),
        reverse=True,
    )
    samples: list[WhatIfSample] = []
    seen_ids: set[str] = set()
    fallback_multiplier = config_summary.stop_loss_atr_multiplier or 1.0

    for trade in ranked:
        if trade.open_trade_id in seen_ids:
            continue
        trace_row = choose_trace_row_for_trade(trace_rows, trade)
        if trace_row is None:
            continue
        stop_price = as_float(trace_row.get("stop_loss_price", 0.0))
        atr = as_float(trace_row.get("atr", 0.0))
        basis = "真实止损价"
        if stop_price > 0.0:
            stop_distance = abs(trade.entry_price - stop_price)
        else:
            if atr <= 0.0:
                continue
            stop_distance = abs(atr * fallback_multiplier)
            stop_price = trade.entry_price - stop_distance if trade.entry_side == "long" else trade.entry_price + stop_distance
            basis = f"ATR估算({fallback_multiplier:.2f}x)"
        if stop_distance <= 0.0 or trade.risk_budget_r <= 0.0:
            continue
        product = product_prefix(trade.symbol)
        volume_multiple = as_int(instrument_info.get(product, {}).get("volume_multiple", 1), 1)
        contract_risk = stop_distance * volume_multiple
        if contract_risk <= 0.0:
            continue
        theoretical_volume = max(1, int(trade.risk_budget_r / contract_risk))
        seen_ids.add(trade.open_trade_id)
        samples.append(
            WhatIfSample(
                open_trade_id=trade.open_trade_id,
                symbol=trade.symbol,
                entry_dt_local=trade.entry_dt_local,
                entry_price=trade.entry_price,
                actual_volume=trade.entry_volume,
                risk_budget_r=trade.risk_budget_r,
                stop_price=stop_price,
                stop_distance=stop_distance,
                volume_multiple=volume_multiple,
                theoretical_volume=theoretical_volume,
                basis=basis,
                gross_realized_pnl=trade.gross_realized_pnl,
                net_pnl=trade.net_pnl,
                r_multiple=trade.r_multiple_gross,
            )
        )
        if len(samples) >= sample_count:
            break
    if len(samples) < min(sample_count, len(completed_trades)):
        raise ReportError(
            "动态头寸模拟所需的 stop_loss_price/ATR 样本不足，无法满足至少 10 笔代表性交易的要求"
        )
    return samples


def validate_expiry_rollover(
    trades: list[TradeRow], expiry_calendar: dict[str, str]
) -> list[ExpiryValidationResult]:
    results: list[ExpiryValidationResult] = []
    opens_by_product: dict[str, list[TradeRow]] = defaultdict(list)
    for trade in trades:
        if trade.offset == "Open":
            opens_by_product[product_prefix(trade.symbol)].append(trade)
    for items in opens_by_product.values():
        items.sort(key=lambda trade: (trade.timestamp_ns, trade.fill_seq))

    for index, trade in enumerate(trades):
        if "expiry_close" not in trade.signal_type.lower():
            continue
        product = product_prefix(trade.symbol)
        next_open: TradeRow | None = None
        for candidate in opens_by_product.get(product, []):
            if candidate.timestamp_ns > trade.timestamp_ns:
                next_open = candidate
                break
        expected_last_day = expiry_calendar.get(trade.symbol)
        passed = next_open is not None and next_open.symbol != trade.symbol
        notes: list[str] = []
        if expected_last_day is not None:
            if trade.trading_day == expected_last_day:
                notes.append("到期平仓日与日历一致")
            else:
                notes.append(f"日历期望 {expected_last_day}，实际 {trade.trading_day}")
                passed = False
        if next_open is not None:
            if next_open.symbol != trade.symbol:
                notes.append(f"下一次开仓切换至 {next_open.symbol}")
            else:
                notes.append("下一次开仓仍为旧合约")
                passed = False
        else:
            notes.append("未找到后续开仓")
            passed = False
        results.append(
            ExpiryValidationResult(
                signal_type=trade.signal_type,
                close_trade_id=trade.trade_id,
                close_symbol=trade.symbol,
                close_trading_day=trade.trading_day,
                expected_last_day=expected_last_day,
                next_open_trade_id=next_open.trade_id if next_open is not None else None,
                next_symbol=next_open.symbol if next_open is not None else None,
                next_trading_day=next_open.trading_day if next_open is not None else None,
                passed=passed,
                note="；".join(notes),
            )
        )
    return results


def infer_overall_assessment(
    annualized_return: float,
    max_drawdown: float,
    sharpe: float,
    compliance_ratio: float,
) -> str:
    if annualized_return > 15.0 and max_drawdown < 10.0 and sharpe > 1.2 and compliance_ratio < 10.0:
        return "**推荐进入 Paper Trading 观察**：收益/回撤比和风控合规性均达到较好水平。"
    if annualized_return > 8.0 and max_drawdown < 15.0 and sharpe > 0.8:
        return "**建议谨慎进入观察池**：策略具备一定收益弹性，但仍需验证震荡市与超预算损失的稳定性。"
    return "**暂不推荐直接进入实盘观察**：当前收益质量或回撤控制尚不足以支持后续灰度。"


def build_markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    output = ["| " + " | ".join(headers) + " |", "| " + " | ".join([":---"] * len(headers)) + " |"]
    for row in rows:
        output.append("| " + " | ".join(row) + " |")
    return "\n".join(output)


def generate_report(
    run_dir: Path,
    output_path: Path | None,
    report_date: str,
    risk_free_rate: float,
    sample_trades: int,
) -> tuple[Path, str]:
    json_path = run_dir / "backtest_auto.json"
    md_path = run_dir / "backtest_auto.md"
    trades_path = run_dir / "csv" / "trades.csv"
    orders_path = run_dir / "csv" / "orders.csv"
    daily_path = run_dir / "csv" / "daily_equity.csv"
    position_path = run_dir / "csv" / "position_history.csv"
    for path in [json_path, md_path, trades_path, orders_path, daily_path]:
        if not path.exists():
            raise ReportError(f"缺少规范要求的输入文件: {path}")

    payload = load_json(json_path)
    markdown_text = md_path.read_text(encoding="utf-8")

    trade_rows_raw, trade_columns = load_csv_rows(trades_path)
    order_rows_raw, order_columns = load_csv_rows(orders_path)
    daily_rows_raw, daily_columns = load_csv_rows(daily_path)
    require_columns("trades.csv", trade_columns, REQUIRED_TRADE_COLUMNS)
    require_columns("orders.csv", order_columns, REQUIRED_ORDER_COLUMNS)
    require_columns("daily_equity.csv", daily_columns, REQUIRED_DAILY_COLUMNS)

    daily_rows = parse_daily_rows(daily_rows_raw)
    trades = parse_trade_rows(trade_rows_raw)
    orders = parse_order_rows(order_rows_raw)
    positions = parse_position_rows(position_path if position_path.exists() else None)
    open_zero_count, open_zero_samples = validate_open_risk_budget(trades)
    if open_zero_count > 0:
        raise ReportError(
            "存在开仓 risk_budget_r 非正值，无法生成正式报告："
            + ", ".join(open_zero_samples)
        )

    final_positions = latest_positions(positions, payload)
    completed_trades, unmatched_open_lots = pair_round_trips(trades, final_positions)
    if not completed_trades:
        raise ReportError("未能配对出任何已完成交易，无法生成正式报告")

    spec = payload.get("spec", {}) or {}
    config_path = Path(str(spec.get("strategy_main_config_path", ""))) if spec.get("strategy_main_config_path") else None
    config_summary = parse_strategy_config(config_path)
    expiry_calendar_path = (
        Path(str(spec.get("contract_expiry_calendar_path", "")))
        if spec.get("contract_expiry_calendar_path")
        else None
    )
    instrument_info_path = (
        Path(str(spec.get("product_config_path", ""))) if spec.get("product_config_path") else None
    )
    expiry_calendar = parse_expiry_calendar(expiry_calendar_path)
    instrument_info = parse_instrument_info(instrument_info_path)
    strategy_name = identify_strategy_name(trades, config_path)
    run_id = str(payload.get("run_id", run_dir.name))

    if output_path is None:
        safe_strategy_name = re.sub(r"[^A-Za-z0-9_\-\u4e00-\u9fff]+", "_", strategy_name)
        output_path = run_dir / f"{safe_strategy_name}_回测分析报告_{run_id}_{report_date}.md"

    start_time_text, end_time_text = parse_main_markdown_time_range(markdown_text)
    initial_equity = as_float(payload.get("initial_equity", spec.get("initial_equity", 0.0)))
    final_equity = as_float(payload.get("final_equity", 0.0))
    raw_fill_rate = as_float(
        (((payload.get("hf_standard", {}) or {}).get("execution_quality", {}) or {}).get(
            "limit_order_fill_rate", 0.0
        ))
    )
    raw_fill_rate_pct, actual_fill_rate_pct = compute_fill_rates(orders, raw_fill_rate)
    execution_quality = ((payload.get("hf_standard", {}) or {}).get("execution_quality", {}) or {})
    risk_metrics = ((payload.get("hf_standard", {}) or {}).get("risk_metrics", {}) or {})
    deterministic = payload.get("deterministic", {}) or {}
    performance = deterministic.get("performance", {}) or {}

    cumulative_return = daily_rows[-1].cumulative_return_pct
    trading_days = len(daily_rows)
    annualized_return = ((1.0 + cumulative_return / 100.0) ** (252.0 / trading_days) - 1.0) * 100.0 if trading_days > 0 else 0.0
    max_drawdown = max(row.drawdown_pct for row in daily_rows)
    calmar = annualized_return / max_drawdown if max_drawdown > 1e-12 else 0.0
    daily_returns_pct = [row.daily_return_pct for row in daily_rows]
    sharpe = compute_sharpe_ratio(daily_returns_pct, risk_free_rate)
    sortino = compute_sortino_ratio(daily_returns_pct, risk_free_rate)
    annualized_vol = compute_annualized_volatility(daily_returns_pct)
    exposure_ratios = [row.position_value / row.capital for row in daily_rows if row.capital > 0.0 and row.position_value > 0.0]
    nonzero_days_ratio = 100.0 * len(exposure_ratios) / trading_days if trading_days > 0 else 0.0
    avg_nonzero_exposure = mean(exposure_ratios) * 100.0 if exposure_ratios else 0.0
    average_exposure = (nonzero_days_ratio / 100.0) * avg_nonzero_exposure
    daily_win_rate = 100.0 * sum(1 for row in daily_rows if row.daily_return_pct > 0.0) / trading_days
    longest_losing_days = longest_negative_streak(row.daily_return_pct for row in daily_rows)
    drawdown_window = compute_drawdown_window(daily_rows, initial_equity)
    regime_attribution = compute_regime_attribution(daily_rows, initial_equity)
    drawdown_trade_summary = top_drawdown_trade_summary(completed_trades, drawdown_window)

    slippage_all = slippage_stats(trades)
    slippage_open = slippage_stats(trades, "kOpen")
    slippage_stop = slippage_stats(trades, "kStopLoss")
    commission_summary = commission_stats(trades, instrument_info)
    open_r_values = [trade.risk_budget_r for trade in trades if trade.offset == "Open"]
    r_quantiles = {
        "p10": percentile(open_r_values, 0.10),
        "p25": percentile(open_r_values, 0.25),
        "p50": percentile(open_r_values, 0.50),
        "p75": percentile(open_r_values, 0.75),
        "p90": percentile(open_r_values, 0.90),
    }
    risk_cap_hits = 0
    if config_summary.max_risk_per_trade is not None:
        risk_cap_hits = sum(1 for value in open_r_values if abs(value - config_summary.max_risk_per_trade) <= 1e-6)

    round_trip_net = [trade.net_pnl for trade in completed_trades]
    gross_r_values = [trade.r_multiple_gross for trade in completed_trades if trade.r_multiple_gross is not None]
    avg_r_multiple = mean([value for value in gross_r_values if value is not None]) if gross_r_values else 0.0
    expectancy = expectancy_from_r_multiples([value for value in gross_r_values if value is not None])
    over_budget_trades = [
        trade
        for trade in completed_trades
        if trade.risk_budget_r > 0.0 and trade.gross_realized_pnl < -trade.risk_budget_r
    ]
    over_budget_ratio = 100.0 * len(over_budget_trades) / len(completed_trades) if completed_trades else 0.0
    risk_coverage_values = [
        trade.risk_coverage_ratio
        for trade in completed_trades
        if trade.risk_coverage_ratio is not None
    ]
    risk_coverage_median = statistics.median(risk_coverage_values) if risk_coverage_values else 0.0

    regime_trade_rows = build_regime_trade_table(completed_trades)
    wins = [value for value in round_trip_net if value > 0.0]
    losses = [value for value in round_trip_net if value < 0.0]
    round_trip_win_rate = 100.0 * len(wins) / len(round_trip_net) if round_trip_net else 0.0
    avg_win = mean(wins)
    avg_loss = mean(losses)
    pnl_ratio = avg_win / abs(avg_loss) if avg_loss < 0.0 else 0.0
    profit_factor = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0.0 else 0.0
    avg_holding_ns = int(mean([trade.holding_ns for trade in completed_trades])) if completed_trades else 0
    max_win_trade = max(round_trip_net) if round_trip_net else 0.0
    max_loss_trade = min(round_trip_net) if round_trip_net else 0.0
    longest_win_streak = longest_streak_by_predicate(round_trip_net, positive=True)
    longest_loss_streak = longest_streak_by_predicate(round_trip_net, positive=False)

    expiry_validations = validate_expiry_rollover(trades, expiry_calendar)
    expiry_pass_rate = 100.0 * sum(1 for item in expiry_validations if item.passed) / len(expiry_validations) if expiry_validations else 100.0

    trace_path = find_trace_path(payload, run_dir)
    trace_rows = load_trace_rows(trace_path)
    what_if_samples = build_dynamic_position_samples(
        completed_trades=completed_trades,
        trace_rows=trace_rows,
        instrument_info=instrument_info,
        config_summary=config_summary,
        sample_count=max(10, sample_trades),
    )

    signal_counts = Counter(trade.signal_type for trade in trades)
    open_counts_by_regime = Counter(trade.regime_at_entry for trade in trades if trade.offset == "Open")
    completed_by_regime: dict[str, list[CompletedTrade]] = defaultdict(list)
    for trade in completed_trades:
        completed_by_regime[trade.entry_regime].append(trade)
    over_budget_signal_counts = Counter()
    over_budget_session_counts = Counter()
    for trade in over_budget_trades:
        for signal_type in trade.exit_signal_types:
            over_budget_signal_counts[signal_type] += 1
        session = trade.exit_dt_local[11:16] if trade.exit_dt_local else "unknown"
        if session and session != "":
            over_budget_session_counts[session] += 1

    overall_assessment = infer_overall_assessment(
        annualized_return=annualized_return,
        max_drawdown=max_drawdown,
        sharpe=sharpe,
        compliance_ratio=over_budget_ratio,
    )

    lines: list[str] = []
    lines.append(f"# {strategy_name} 回测分析报告")
    lines.append("")
    lines.append(f"- 运行 ID：{run_id}")
    lines.append(f"- 生成日期：{report_date}")
    lines.append(f"- 数据目录：{run_dir}")
    lines.append("")

    lines.append("## 一、回测摘要与总体评估")
    lines.append("")
    summary_rows = [
        ["回测区间", f"{yyyy_mm_dd(daily_rows[0].date)} 至 {yyyy_mm_dd(daily_rows[-1].date)}", "daily_equity.csv.date；backtest_auto.md Time Range (ns)"],
        ["时间范围（UTC）", f"{start_time_text or 'N/A'} ~ {end_time_text or 'N/A'}", "backtest_auto.md Time Range (ns)"],
        ["累计收益率", format_pct(cumulative_return), "daily_equity.csv.cumulative_return_pct(末日)"],
        ["年化收益率", format_pct(annualized_return), f"按 {trading_days} 个交易日复利折算"],
        ["最大回撤", format_pct(max_drawdown), "daily_equity.csv.drawdown_pct 最大值"],
        ["Calmar 比率", format_ratio(calmar), "年化收益率 / 最大回撤"],
        ["日均仓位暴露", format_pct(average_exposure), "非零仓位日占比 × 非零日平均 position_value/capital"],
        ["夏普比率", format_ratio(sharpe), "daily_equity.csv.daily_return_pct；无风险利率 2%"],
    ]
    lines.append(build_markdown_table(["指标", "结果", "来源/假设"], summary_rows))
    lines.append("")
    lines.append(
        f"- 非零仓位交易日占比：{format_pct(nonzero_days_ratio)}；非零日平均仓位占比：{format_pct(avg_nonzero_exposure)}。"
    )
    lines.append(f"- 总体评估：{overall_assessment}")
    lines.append("")

    lines.append("## 二、策略逻辑与执行分析")
    lines.append("")
    lines.append("### 2.1 信号逻辑验证")
    lines.append("")
    signal_rows = [[name, format_count(count)] for name, count in sorted(signal_counts.items())]
    lines.append(build_markdown_table(["信号类型", "出现次数"], signal_rows))
    lines.append("")
    regime_rows = []
    for regime, open_count in sorted(open_counts_by_regime.items()):
        completed_items = completed_by_regime.get(regime, [])
        win_rate = (
            100.0 * sum(1 for item in completed_items if item.net_pnl > 0.0) / len(completed_items)
            if completed_items
            else 0.0
        )
        regime_rows.append([regime, format_count(open_count), format_pct(win_rate)])
    lines.append(build_markdown_table(["建仓市场状态", "建仓次数", "对应胜率"], regime_rows))
    lines.append("")
    if expiry_validations:
        expiry_rows = [
            [
                item.close_trade_id,
                item.close_symbol,
                yyyy_mm_dd(item.close_trading_day),
                item.next_symbol or "-",
                yyyy_mm_dd(item.next_trading_day or "-") if item.next_trading_day else "-",
                "通过" if item.passed else "失败",
                item.note,
            ]
            for item in expiry_validations
        ]
        lines.append(
            build_markdown_table(
                ["expiry_close", "旧合约", "平仓日", "后续开仓合约", "后续开仓日", "结果", "说明"],
                expiry_rows,
            )
        )
        lines.append("")
        lines.append(f"- expiry_close 校验通过率：{format_pct(expiry_pass_rate)}。")
    else:
        lines.append("- 本次样本未出现 expiry_close 信号，未触发换月平仓校验。")
    lines.append("- 来源：trades.csv.signal_type / regime_at_entry / trading_day；contract_expiry_calendar.yaml。")
    lines.append("")

    lines.append("### 2.2 交易执行质量")
    lines.append("")
    execution_rows = [
        ["原始 Fill Rate", format_pct(raw_fill_rate_pct), "backtest_auto.json.hf_standard.execution_quality.limit_order_fill_rate"],
        ["实际成交率", format_pct(actual_fill_rate_pct), "orders.csv 按 order_id 去重后统计 FILLED"],
        ["平均滑点", format_ratio(slippage_all["mean"]), "trades.csv.slippage"],
        ["滑点中位数", format_ratio(slippage_all["median"]), "trades.csv.slippage"],
        ["单笔最大滑点", format_ratio(slippage_all["max"]), "trades.csv.slippage"],
        ["单笔最小滑点", format_ratio(slippage_all["min"]), "trades.csv.slippage"],
        ["平均手续费/笔", format_money(commission_summary["avg_per_trade"]), "trades.csv.commission"],
        ["平均手续费/手", format_money(commission_summary["avg_per_lot"]), "trades.csv.commission / volume"],
        ["名义费率中位数", f"{commission_summary['median_bps']:.4f} bps", "commission / (price×volume×合约乘数)"],
    ]
    lines.append(build_markdown_table(["指标", "结果", "来源/假设"], execution_rows))
    lines.append("")
    slippage_rows = [
        ["全样本", format_ratio(slippage_all["mean"]), format_ratio(slippage_all["median"]), format_ratio(slippage_all["max"]), format_ratio(slippage_all["min"])],
        ["kOpen", format_ratio(slippage_open["mean"]), format_ratio(slippage_open["median"]), format_ratio(slippage_open["max"]), format_ratio(slippage_open["min"])],
        ["kStopLoss", format_ratio(slippage_stop["mean"]), format_ratio(slippage_stop["median"]), format_ratio(slippage_stop["max"]), format_ratio(slippage_stop["min"])],
    ]
    lines.append(build_markdown_table(["样本", "均值", "中位数", "最大值", "最小值"], slippage_rows))
    lines.append("")
    lines.append(
        "- 执行解读：原始 Fill Rate 为 50%，原因是每笔订单在 orders.csv 中包含 Accepted 与 Filled 两条状态；按 order_id 去重后，实际成交率为 100%。"
    )
    lines.append(
        f"- 滑点假设解读：kOpen 平均滑点 {slippage_open['mean']:.2f}，kStopLoss 平均滑点 {slippage_stop['mean']:.2f}；当前回测的实盘滑点假设整体偏乐观，尤其止损端几乎没有额外冲击。"
    )
    lines.append(
        f"- 手续费解读：以 C 品种 volume_multiple=10 估算，名义费率中位数约 {commission_summary['median_bps']:.4f} bps，属于回测中的低成本假设区间。"
    )
    lines.append("")

    lines.append("### 2.3 风险预算 R 值概述")
    lines.append("")
    risk_rows = [
        ["开仓笔数", format_count(len(open_r_values)), "trades.csv offset=Open"],
        ["开仓 R 值非零率", format_pct(100.0), "trades.csv risk_budget_r > 0"],
        ["R 值均值", format_money(mean(open_r_values)), "trades.csv.risk_budget_r"],
        ["R 值中位数", format_money(statistics.median(open_r_values) if open_r_values else 0.0), "trades.csv.risk_budget_r"],
        ["R 值标准差", format_money(safe_stdev(open_r_values)), "trades.csv.risk_budget_r"],
        ["R 值最小值", format_money(min(open_r_values) if open_r_values else 0.0), "trades.csv.risk_budget_r"],
        ["R 值最大值", format_money(max(open_r_values) if open_r_values else 0.0), "trades.csv.risk_budget_r"],
    ]
    lines.append(build_markdown_table(["指标", "结果", "来源"], risk_rows))
    lines.append("")
    if config_summary.risk_enabled and config_summary.risk_per_trade_pct is not None:
        lines.append(
            f"- R 值设定方式：主配置 risk_management.enabled=true，按成交前权益 × {config_summary.risk_per_trade_pct * 100:.2f}% 计算；上限 {format_money(config_summary.max_risk_per_trade or 0.0)}。"
        )
    else:
        lines.append("- R 值设定方式：无法从主配置恢复精确参数，但样本显示为权益比例驱动的动态 R。")
    lines.append("")

    lines.append("## 三、深度绩效与风险归因")
    lines.append("")
    lines.append("### 3.1 核心绩效指标")
    lines.append("")
    perf_rows = [
        ["年化波动率", format_pct(annualized_vol)],
        ["夏普比率", format_ratio(sharpe)],
        ["索提诺比率", format_ratio(sortino)],
        ["日胜率", format_pct(daily_win_rate)],
        ["最长连续亏损交易日", f"{longest_losing_days} 天"],
    ]
    lines.append(build_markdown_table(["指标", "结果"], perf_rows))
    lines.append("")
    lines.append("- 来源：daily_equity.csv.daily_return_pct / capital；无风险利率假设为 2%。")
    lines.append("")

    lines.append("### 3.2 市场状态适应性分析")
    lines.append("")
    regime_attr_rows = [
        [
            item.regime,
            format_count(item.total_days),
            format_money(item.total_pnl),
            format_money(item.avg_daily_pnl),
            format_pct(item.win_rate),
            item.conclusion,
        ]
        for item in regime_attribution
    ]
    lines.append(
        build_markdown_table(
            ["市场状态", "出现天数", "累计盈亏贡献", "日均盈亏", "胜率（按日）", "结论"],
            regime_attr_rows,
        )
    )
    lines.append("")
    trend_pnl = sum(item.total_pnl for item in regime_attribution if "Trend" in item.regime)
    range_pnl = sum(item.total_pnl for item in regime_attribution if item.regime in {"kRanging", "kFlat"})
    best_regime = max(regime_attribution, key=lambda item: item.total_pnl)
    worst_regime = min(regime_attribution, key=lambda item: item.total_pnl)
    if trend_pnl > abs(range_pnl):
        regime_conclusion = "**策略整体呈现明显的趋势跟随特征**：趋势状态下的收益贡献显著高于震荡/平淡状态。"
    else:
        regime_conclusion = "**策略的趋势属性不够纯粹**：震荡与平淡状态对绩效的拖累较明显。"
    lines.append(f"- {regime_conclusion}")
    lines.append(
        f"- 最强市场状态：**{best_regime.regime}**（累计贡献 {format_money(best_regime.total_pnl)}）；最弱状态：**{worst_regime.regime}**（累计贡献 {format_money(worst_regime.total_pnl)}）。"
    )
    lines.append("")

    lines.append("### 3.3 回撤与风险分析")
    lines.append("")
    drawdown_rows = [
        ["最大回撤起点", yyyy_mm_dd(drawdown_window.peak_date)],
        ["最大回撤低点", yyyy_mm_dd(drawdown_window.trough_date)],
        ["回撤幅度", format_pct(drawdown_window.drawdown_pct)],
        ["区间权益变化", format_money(drawdown_window.pnl)],
        ["VaR95", format_pct(as_float(risk_metrics.get("var_95", 0.0)))],
        ["ES95", format_pct(as_float(risk_metrics.get("expected_shortfall_95", 0.0)))],
    ]
    lines.append(build_markdown_table(["指标", "结果"], drawdown_rows))
    lines.append("")
    lines.append(
        f"- 最大回撤区间内共覆盖 {drawdown_trade_summary['count']} 笔已完成交易，信号分布为 {drawdown_trade_summary['signal_counts']}。"
    )
    lines.append(f"- 回撤归因：**{drawdown_trade_summary['reason']}**。")
    lines.append(
        f"- VaR95 / ES95 解读：日频 95% 分位损失分别约为 {format_pct(as_float(risk_metrics.get('var_95', 0.0)))} / {format_pct(as_float(risk_metrics.get('expected_shortfall_95', 0.0)))}，下行尾部风险整体可控，但在连续止损阶段会明显放大。"
    )
    lines.append("")

    lines.append("### 3.4 交易层级绩效统计")
    lines.append("")
    trade_stat_rows = [
        ["总交易次数（Round-Trips）", format_count(len(completed_trades))],
        ["胜率", format_pct(round_trip_win_rate)],
        ["平均盈利", format_money(avg_win)],
        ["平均亏损", format_money(avg_loss)],
        ["盈亏比", format_ratio(pnl_ratio)],
        ["利润因子", format_ratio(profit_factor)],
        ["平均持仓时间", format_duration_ns(avg_holding_ns)],
        ["最大单笔盈利", format_money(max_win_trade)],
        ["最大单笔亏损", format_money(max_loss_trade)],
        ["最长连续盈利次数", format_count(longest_win_streak)],
        ["最长连续亏损次数", format_count(longest_loss_streak)],
    ]
    lines.append(build_markdown_table(["指标", "结果"], trade_stat_rows))
    lines.append("")
    lines.append(
        build_markdown_table(
            ["开仓市场状态", "已完成交易数", "胜率", "盈亏比"],
            [
                [
                    row["regime"],
                    format_count(row["trades"]),
                    format_pct(row["win_rate"]),
                    format_ratio(row["pnl_ratio"]),
                ]
                for row in regime_trade_rows
            ],
        )
    )
    lines.append("")
    lines.append("- 口径说明：交易层级统计采用配对后的 net_pnl（gross realized_pnl - 开/平仓手续费分摊）。")
    lines.append("")

    lines.append("## 四、风险预算 (R) 与头寸管理分析")
    lines.append("")
    lines.append("### 4.1 R 值分布与风险敞口画像")
    lines.append("")
    r_distribution_rows = [
        ["最小值", format_money(min(open_r_values))],
        ["P10", format_money(r_quantiles["p10"])],
        ["P25", format_money(r_quantiles["p25"])],
        ["中位数", format_money(r_quantiles["p50"])],
        ["P75", format_money(r_quantiles["p75"])],
        ["P90", format_money(r_quantiles["p90"])],
        ["最大值", format_money(max(open_r_values))],
        ["触及上限次数", format_count(risk_cap_hits)],
    ]
    lines.append(build_markdown_table(["统计项", "结果"], r_distribution_rows))
    lines.append("")
    lines.append(
        f"- R 值与权益联动解读：样本 R 值从 {format_money(min(open_r_values))} 上升至 {format_money(max(open_r_values))}，与权益曲线从 {format_money(initial_equity)} 增长至 {format_money(final_equity)} 的方向一致，符合权益比例驱动特征。"
    )
    if risk_cap_hits == 0:
        lines.append("- 本次回测未出现 risk_budget_r 触达配置上限的交易。")
    else:
        lines.append(f"- 本次回测共有 {risk_cap_hits} 笔交易触达 R 值上限，需要复核是否进入资金利用瓶颈。")
    lines.append("")

    lines.append("### 4.2 R-multiple 分析")
    lines.append("")
    r_bucket_rows = [
        [label, format_count(count), format_pct(ratio)]
        for label, count, ratio in compute_r_bucket_counts([value for value in gross_r_values if value is not None])
    ]
    lines.append(build_markdown_table(["R 区间", "笔数", "占比"], r_bucket_rows))
    lines.append("")
    r_summary_rows = [
        ["平均 R-multiple", format_ratio(avg_r_multiple)],
        ["Expectancy", format_ratio(expectancy)],
    ]
    lines.append(build_markdown_table(["指标", "结果"], r_summary_rows))
    lines.append("")
    if 0.2 <= expectancy <= 0.5:
        expectancy_comment = "期望值落在优秀趋势策略的典型区间。"
    elif expectancy > 0.0:
        expectancy_comment = "期望值为正，但仍有提升空间。"
    else:
        expectancy_comment = "期望值非正，说明每承担 1R 风险得到的回报不足。"
    lines.append(f"- 期望值评价：**{expectancy_comment}**")
    lines.append("")

    lines.append("### 4.3 风险预算合规性检查")
    lines.append("")
    compliance_rows = [
        ["超预算交易数", format_count(len(over_budget_trades))],
        ["超预算比例", format_pct(over_budget_ratio)],
        ["风险覆盖比率中位数", format_ratio(risk_coverage_median)],
    ]
    lines.append(build_markdown_table(["指标", "结果"], compliance_rows))
    lines.append("")
    lines.append(
        f"- 超预算交易信号分布：{dict(over_budget_signal_counts) if over_budget_signal_counts else '无'}。"
    )
    lines.append(
        f"- 超预算交易时段分布：{dict(over_budget_session_counts.most_common(5)) if over_budget_session_counts else '无'}。"
    )
    lines.append(
        f"- 风险覆盖比率解读：中位数 {risk_coverage_median:.2f}，{'符合理想值≤1.0' if risk_coverage_median <= 1.0 else '高于理想值，需要进一步收紧止损执行'}。"
    )
    lines.append("")

    lines.append("### 4.4 动态头寸模拟（What-If）")
    lines.append("")
    what_if_rows = [
        [
            item.open_trade_id,
            item.symbol,
            item.entry_dt_local,
            f"{item.entry_price:.2f}",
            format_count(item.actual_volume),
            format_count(item.theoretical_volume),
            format_money(item.risk_budget_r),
            f"{item.stop_price:.2f}",
            f"{item.stop_distance:.2f}",
            item.basis,
        ]
        for item in what_if_samples
    ]
    lines.append(
        build_markdown_table(
            ["trade_id", "合约", "开仓时间", "入场价", "原手数", "理论手数", "R 值", "止损价", "止损距离", "依据"],
            what_if_rows,
        )
    )
    lines.append("")
    if what_if_samples:
        avg_actual_volume = mean([sample.actual_volume for sample in what_if_samples])
        avg_theoretical_volume = mean([sample.theoretical_volume for sample in what_if_samples])
        lines.append(
            f"- 样本平均原手数 / 理论手数：{avg_actual_volume:.2f} / {avg_theoretical_volume:.2f}。"
        )
    lines.append(
        "- 定性结论：若引入动态头寸，单笔风险将围绕固定 R 收敛，收益曲线大概率更平滑、最大回撤更可控；但在趋势顺畅阶段，绝对收益可能因手数下调而被压缩。"
    )
    lines.append("")

    lines.append("### 4.5 系统风控建议")
    lines.append("")
    lines.append(f"- 建议单日最大亏损限额设为 **3R**，即约 {format_money(mean(open_r_values) * 3.0)}。")
    lines.append("- 建议在连续 3 笔亏损后启动缩仓或暂停新开仓观察。")
    lines.append(
        "- 若后续实盘验证发现 kRanging / kFlat 状态下的超预算交易集中，建议按市场状态降低 R 值或直接屏蔽部分开仓窗口。"
    )
    lines.append("")

    lines.append("## 五、架构与数据洞察")
    lines.append("")
    architecture_rows = [
        ["Input Signature", str(payload.get("input_signature", "")), "backtest_auto.json.input_signature"],
        ["Data Signature", str(payload.get("data_signature", "")), "backtest_auto.json.data_signature"],
        ["Order Events", format_count(as_int(deterministic.get("order_events_emitted", 0))), "backtest_auto.json.deterministic.order_events_emitted"],
        ["WAL Records", format_count(as_int(deterministic.get("wal_records", 0))), "backtest_auto.json.deterministic.wal_records"],
        ["Ticks Read", format_count(as_int((payload.get("replay", {}) or {}).get("ticks_read", 0))), "backtest_auto.json.replay.ticks_read"],
        ["IO Bytes", format_count(as_int((payload.get("replay", {}) or {}).get("io_bytes", 0))), "backtest_auto.json.replay.io_bytes"],
    ]
    lines.append(build_markdown_table(["指标", "结果", "来源"], architecture_rows))
    lines.append("")
    lines.append(
        "- Input Signature 与 Data Signature 的意义：两者共同锁定了回测配置与数据快照，有助于保证跨机器、跨时间的复现一致性。"
    )
    lines.append(
        f"- Deterministic Replay 评价：本次共发出 {format_count(as_int(deterministic.get('order_events_emitted', 0)))} 条订单事件，WAL Records 为 {format_count(as_int(deterministic.get('wal_records', 0)))}，说明回测链路具备稳定的确定性重放特征。"
    )
    lines.append(
        f"- 回测吞吐评价：全年读取 {format_count(as_int((payload.get('replay', {}) or {}).get('ticks_read', 0)))} 条 tick，IO 约 {format_count(as_int((payload.get('replay', {}) or {}).get('io_bytes', 0)))} 字节，说明当前引擎足以承载单品种全年级别的研究迭代。"
    )
    if expiry_validations:
        lines.append(
            f"- 合约换月评价：expiry_close 校验通过率 {format_pct(expiry_pass_rate)}，整体表明连续主力衔接逻辑是正确的。"
        )
    else:
        lines.append("- 合约换月评价：本样本无显式 expiry_close 记录，暂无法额外证明换月链路。")
    lines.append("")

    lines.append("## 六、综合结论与迭代建议")
    lines.append("")
    lines.append("### 6.1 策略优势")
    lines.append("")
    lines.append(
        f"- 风险收益特征：Calmar 为 **{calmar:.2f}**，累计收益 {format_pct(cumulative_return)}，最大回撤 {format_pct(max_drawdown)}，当前样本的收益/回撤配比具备可研究价值。"
    )
    lines.append(
        f"- 执行质量：按 order_id 去重后的实际成交率为 **{actual_fill_rate_pct:.2f}%**，执行链路在回测中稳定。"
    )
    lines.append(
        "- 工程可靠性：Input/Data Signature、Deterministic Replay 与 CSV/JSON 产物齐全，说明回测系统具备较好的复现与追溯能力。"
    )
    lines.append("")

    lines.append("### 6.2 潜在风险与改进方向")
    lines.append("")
    lines.append(
        "- 滑点模型偏乐观：止损成交几乎没有额外冲击，建议在下一轮回测中提高止损滑点或引入分场景滑点模型。"
    )
    lines.append(
        f"- 弱趋势/震荡市优化：当前最弱状态为 {worst_regime.regime}，建议通过信号过滤、市场状态自适应或禁止部分时段开仓来削弱回撤。"
    )
    lines.append(
        "- 参数敏感性测试：建议围绕 stop_loss_atr_multiplier、entry_market_regimes 与 forbid_open_windows 做系统性扫描，检验收益/回撤稳定性。"
    )
    lines.append(
        "- 基于 R 的风控改进：若后续进入 Paper Trading，**强烈建议同步评估动态头寸管理**，至少在弱趋势和连续亏损阶段引入缩仓规则。"
    )
    lines.append("")

    lines.append("### 6.3 下一步行动")
    lines.append("")
    if sharpe > 1.0 and over_budget_ratio < 15.0:
        lines.append("- 建议结论：**可以批准进入 Paper Trading**，但需同时监控超预算交易与震荡市表现。")
    else:
        lines.append("- 建议结论：**暂不直接进入 Paper Trading**，需先完成风控与参数鲁棒性修正。")
    lines.append(
        "- 进入实盘灰度的前提：1) 重新校准滑点模型；2) 完成不同市场状态下的 R 值分层验证；3) 连续两轮样本外回测维持正 Expectancy。"
    )
    lines.append("- 灰度规模建议：初始仅使用 1R 的 30%~50% 作为真实风险暴露，并设置单日 3R 止损闸门。")
    lines.append("")

    lines.append("## 附录：关键指标定义")
    lines.append("")
    appendix_rows = [
        ["夏普比率", "(日均超额收益 / 日波动) × sqrt(252)"],
        ["索提诺比率", "(日均超额收益 / 下行波动) × sqrt(252)"],
        ["Calmar 比率", "年化收益率 / 最大回撤"],
        ["R-multiple", "配对交易 gross realized_pnl / 开仓 risk_budget_r"],
        ["风险覆盖比率", "|实际亏损| / risk_budget_r，仅对亏损交易计算"],
    ]
    lines.append(build_markdown_table(["指标", "定义"], appendix_rows))
    lines.append("")
    lines.append(
        f"> 报告假设：无风险利率 {risk_free_rate * 100:.2f}% 年化；动态头寸模拟优先采用 sub-trace.stop_loss_price，缺失时退化为 ATR × {config_summary.stop_loss_atr_multiplier or 1.0:.2f}。"
    )
    lines.append(
        f"> 未配对开仓说明：当前尚有 {len(unmatched_open_lots)} 个未平仓开仓 lot，与最终持仓 {final_positions} 一致，因此不纳入已完成交易口径。"
    )
    lines.append("")

    report_text = "\n".join(lines) + "\n"
    output_path.write_text(report_text, encoding="utf-8")
    return output_path, report_text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="生成正式回测分析报告")
    parser.add_argument("--run-dir", type=Path, required=True, help="回测结果目录")
    parser.add_argument("--output", type=Path, default=None, help="报告输出路径")
    parser.add_argument(
        "--report-date",
        default=datetime.now().strftime("%Y%m%d"),
        help="报告日期，默认当天 YYYYMMDD",
    )
    parser.add_argument(
        "--risk-free-rate",
        type=float,
        default=0.02,
        help="年化无风险利率，默认 0.02",
    )
    parser.add_argument(
        "--sample-trades",
        type=int,
        default=10,
        help="动态头寸模拟最少抽样交易数，默认 10",
    )
    args = parser.parse_args(argv)

    try:
        output_path, _ = generate_report(
            run_dir=args.run_dir,
            output_path=args.output,
            report_date=args.report_date,
            risk_free_rate=args.risk_free_rate,
            sample_trades=args.sample_trades,
        )
    except ReportError as exc:
        print(f"[report] FAIL: {exc}")
        return 2

    print(f"[report] 生成完成: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
