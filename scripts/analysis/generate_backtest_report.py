#!/usr/bin/env python3
"""Generate a professional backtest analysis report (High-Flyer Quant standard).

Usage:
    python scripts/analysis/generate_backtest_report.py \\
        --run-dir docs/results/backtest_runs/<run_id>_<timestamp>/

The script auto-discovers backtest_*.json in the run directory and handles
single-contract and multi-contract backtests transparently.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

# ── optional pandas import for correlation matrix ──────────────────────────
try:
    import pandas as pd

    HAS_PANDAS = True
except Exception:  # pragma: no cover
    pd = None  # type: ignore[assignment]
    HAS_PANDAS = False


# ═══════════════════════════════════════════════════════════════════════════════
# 0. Errors & helpers
# ═══════════════════════════════════════════════════════════════════════════════


class ReportError(RuntimeError):
    """Fatal issue that prevents report generation."""


# ── generic math / formatters ────────────────────────────────────────────────


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    return statistics.stdev(values)


def _percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = max(0.0, min(1.0, ratio)) * (len(ordered) - 1)
    lo, hi = math.floor(index), math.ceil(index)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (1.0 - (index - lo)) + ordered[hi] * (index - lo)


def _fmt_pct(v: float) -> str:
    return f"{v:.2f}%"


def _fmt_money(v: float) -> str:
    return f"{v:,.2f}"


def _fmt_ratio(v: float) -> str:
    return f"{v:.4f}"


def _fmt_int(v: int) -> str:
    return f"{v:,}"


def _product_prefix(symbol: str) -> str:
    """Extract product code from contract symbol, e.g. 'c2505' -> 'c'."""
    letters: list[str] = []
    for ch in symbol:
        if ch.isdigit():
            break
        letters.append(ch)
    return "".join(letters).upper()


def _build_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join([":---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Discovery layer
# ═══════════════════════════════════════════════════════════════════════════════


def find_backtest_json(run_dir: Path) -> Path:
    """Find the main backtest JSON file in *run_dir*.

    Globs ``backtest_*.json``, excluding known auxiliary files.
    """
    candidates = sorted(run_dir.glob("backtest_*.json"))
    if not candidates:
        raise ReportError(f"未找到 backtest_*.json，目录: {run_dir}")
    # Prefer the largest file (exclude tiny aux JSON if any)
    candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
    return candidates[0]


def find_backtest_md(run_dir: Path) -> Path | None:
    """Return the summary markdown if present."""
    candidates = sorted(run_dir.glob("backtest_*.md"))
    return candidates[0] if candidates else None


def find_csv(run_dir: Path, relative: str) -> Path:
    """Resolve *relative* under ``csv/`` in *run_dir*."""
    p = run_dir / "csv" / relative
    if not p.exists():
        raise ReportError(f"缺少 CSV 文件: {p}")
    return p


def detect_multi_contract(run_dir: Path, payload: dict[str, Any]) -> bool:
    """Return True when the run is a multi-contract / composite backtest.

    Checks three signals; any one is sufficient.
    """
    varieties_dir = run_dir / "csv" / "varieties"
    if varieties_dir.is_dir() and any(varieties_dir.iterdir()):
        return True
    spec = payload.get("spec", {})
    if isinstance(spec, dict):
        symbols = spec.get("symbols", [])
        if isinstance(symbols, list) and len(symbols) > 1:
            return True
        if spec.get("strategy_factory") == "composite":
            return True
    return False


def discover_varieties(run_dir: Path) -> list[str]:
    """Return sorted list of product codes under ``csv/varieties/``."""
    varieties_dir = run_dir / "csv" / "varieties"
    if not varieties_dir.is_dir():
        return []
    return sorted(
        p.name
        for p in varieties_dir.iterdir()
        if p.is_dir() and (p / "backtest").is_dir()
    )


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_csv_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = list(reader.fieldnames or [])
        rows = [dict(row) for row in reader]
    return rows, fieldnames


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Parse layer
# ═══════════════════════════════════════════════════════════════════════════════


def _require_columns(name: str, columns: list[str], required: set[str]) -> None:
    missing = sorted(required - set(columns))
    if missing:
        raise ReportError(f"{name} 缺少必要列: {', '.join(missing)}")


REQUIRED_DAILY = {
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

REQUIRED_TRADE = {
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

REQUIRED_ORDER = {
    "order_id",
    "status",
}


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
    symbol: str
    side: str
    offset: str
    volume: int
    price: float
    timestamp_ns: int
    timestamp_dt_local: str
    commission: float
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


@dataclass
class DrawdownWindow:
    peak_date: str
    trough_date: str
    peak_capital: float
    trough_capital: float
    drawdown_pct: float
    pnl: float


@dataclass
class RoundTrip:
    open_trade: TradeRow
    close_trade: TradeRow
    net_pnl: float
    r_multiple: float
    holding_minutes: float


def _as_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _as_int(val: Any, default: int = 0) -> int:
    try:
        return int(float(val)) if val is not None else default
    except (ValueError, TypeError):
        return default


def parse_daily_rows(raw: list[dict[str, str]]) -> list[DailyRow]:
    rows = [
        DailyRow(
            date=str(r["date"]),
            capital=_as_float(r["capital"]),
            daily_return_pct=_as_float(r["daily_return_pct"]),
            cumulative_return_pct=_as_float(r["cumulative_return_pct"]),
            drawdown_pct=_as_float(r["drawdown_pct"]),
            position_value=_as_float(r["position_value"]),
            trades_count=_as_int(r["trades_count"]),
            turnover=_as_float(r["turnover"]),
            market_regime=str(r.get("market_regime") or "kUnknown"),
        )
        for r in raw
    ]
    if not rows:
        raise ReportError("daily_equity.csv 为空，无法生成报告")
    return rows


def parse_trade_rows(raw: list[dict[str, str]]) -> list[TradeRow]:
    rows = [
        TradeRow(
            fill_seq=_as_int(r["fill_seq"]),
            trade_id=str(r["trade_id"]),
            symbol=str(r["symbol"]),
            side=str(r["side"]),
            offset=str(r["offset"]),
            volume=_as_int(r["volume"]),
            price=_as_float(r["price"]),
            timestamp_ns=_as_int(r["timestamp_ns"]),
            timestamp_dt_local=str(r.get("timestamp_dt_local", "")),
            commission=_as_float(r.get("commission", 0)),
            slippage=_as_float(r.get("slippage", 0)),
            realized_pnl=_as_float(r.get("realized_pnl", 0)),
            strategy_id=str(r.get("strategy_id", "")),
            signal_type=str(r.get("signal_type", "")),
            regime_at_entry=str(r.get("regime_at_entry", "kUnknown") or "kUnknown"),
            risk_budget_r=_as_float(r.get("risk_budget_r", 0)),
        )
        for r in raw
    ]
    rows.sort(key=lambda t: (t.timestamp_ns, t.fill_seq))
    if not rows:
        raise ReportError("trades.csv 为空，无法生成报告")
    return rows


def parse_order_rows(raw: list[dict[str, str]]) -> list[OrderRow]:
    return [OrderRow(order_id=str(r["order_id"]), status=str(r["status"])) for r in raw]


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Metric computation layer
# ═══════════════════════════════════════════════════════════════════════════════


def compute_sharpe(daily_returns_pct: list[float], risk_free_rate: float) -> float:
    if len(daily_returns_pct) < 2:
        return 0.0
    rf_daily = risk_free_rate / 252.0
    excess = [v / 100.0 - rf_daily for v in daily_returns_pct]
    vol = _stdev(excess)
    if vol < 1e-12:
        return 0.0
    return _mean(excess) / vol * math.sqrt(252.0)


def compute_sortino(daily_returns_pct: list[float], risk_free_rate: float) -> float:
    if len(daily_returns_pct) < 2:
        return 0.0
    rf_daily = risk_free_rate / 252.0
    excess = [v / 100.0 - rf_daily for v in daily_returns_pct]
    downside = [min(0.0, v) for v in excess]
    ds_sq = [v * v for v in downside]
    if not ds_sq or sum(ds_sq) <= 0.0:
        return 0.0
    downside_dev = math.sqrt(sum(ds_sq) / len(ds_sq))
    if downside_dev < 1e-12:
        return 0.0
    return _mean(excess) / downside_dev * math.sqrt(252.0)


def compute_calmar(annualized_return: float, max_dd_pct: float) -> float:
    if max_dd_pct < 1e-12:
        return 0.0
    return annualized_return / max_dd_pct


def compute_annualized_return(cumulative_return_pct: float, trading_days: int) -> float:
    if trading_days <= 0:
        return 0.0
    return (
        (1.0 + cumulative_return_pct / 100.0) ** (252.0 / trading_days) - 1.0
    ) * 100.0


def compute_annualized_volatility(daily_returns_pct: list[float]) -> float:
    rets = [v / 100.0 for v in daily_returns_pct]
    if len(rets) < 2:
        return 0.0
    return _stdev(rets) * math.sqrt(252.0) * 100.0


def compute_var_95(daily_returns_pct: list[float]) -> float:
    """Historical daily VaR at 95% confidence (positive value = loss %)."""
    if not daily_returns_pct:
        return 0.0
    return -_percentile(daily_returns_pct, 0.05)


def compute_expected_shortfall_95(daily_returns_pct: list[float]) -> float:
    """Historical daily ES / CVaR at 95%."""
    if not daily_returns_pct:
        return 0.0
    threshold = _percentile(daily_returns_pct, 0.05)
    tail = [v for v in daily_returns_pct if v <= threshold]
    if not tail:
        return 0.0
    return -_mean(tail)


def compute_max_drawdown(
    daily_rows: list[DailyRow], initial_equity: float
) -> DrawdownWindow:
    peak_cap = initial_equity
    peak_date = daily_rows[0].date
    max_dd = -1.0
    best_peak_cap = peak_cap
    best_peak_date = peak_date
    trough_cap = daily_rows[0].capital
    trough_date = daily_rows[0].date

    for row in daily_rows:
        if row.capital >= peak_cap:
            peak_cap = row.capital
            peak_date = row.date
        if peak_cap > 0:
            dd = (peak_cap - row.capital) / peak_cap * 100.0
            if dd > max_dd:
                max_dd = dd
                best_peak_cap = peak_cap
                best_peak_date = peak_date
                trough_cap = row.capital
                trough_date = row.date

    return DrawdownWindow(
        peak_date=best_peak_date,
        trough_date=trough_date,
        peak_capital=best_peak_cap,
        trough_capital=trough_cap,
        drawdown_pct=max(0.0, max_dd),
        pnl=trough_cap - best_peak_cap,
    )


def compute_drawdown_windows(
    daily_rows: list[DailyRow], initial_equity: float, top_n: int = 3
) -> list[DrawdownWindow]:
    """Return top-N drawdown events (non-overlapping, by peak)."""
    windows: list[DrawdownWindow] = []
    peak_cap = initial_equity
    peak_date = daily_rows[0].date
    in_dd = False
    dd_start_date = daily_rows[0].date
    dd_start_cap = initial_equity
    max_dd_pct = 0.0
    trough_cap = initial_equity
    trough_date = daily_rows[0].date

    for row in daily_rows:
        if row.capital >= peak_cap:
            if in_dd and max_dd_pct > 0.5:
                windows.append(
                    DrawdownWindow(
                        peak_date=dd_start_date,
                        trough_date=trough_date,
                        peak_capital=dd_start_cap,
                        trough_capital=trough_cap,
                        drawdown_pct=max_dd_pct,
                        pnl=trough_cap - dd_start_cap,
                    )
                )
            in_dd = False
            max_dd_pct = 0.0
            peak_cap = row.capital
            peak_date = row.date
        else:
            if not in_dd:
                in_dd = True
                dd_start_date = peak_date
                dd_start_cap = peak_cap
            if peak_cap > 0:
                dd = (peak_cap - row.capital) / peak_cap * 100.0
                if dd > max_dd_pct:
                    max_dd_pct = dd
                    trough_cap = row.capital
                    trough_date = row.date

    if in_dd and max_dd_pct > 0.5:
        windows.append(
            DrawdownWindow(
                peak_date=dd_start_date,
                trough_date=trough_date,
                peak_capital=dd_start_cap,
                trough_capital=trough_cap,
                drawdown_pct=max_dd_pct,
                pnl=trough_cap - dd_start_cap,
            )
        )

    windows.sort(key=lambda w: w.drawdown_pct, reverse=True)
    return windows[:top_n]


def compute_profit_factor(completed_net_pnls: list[float]) -> float:
    wins = [v for v in completed_net_pnls if v > 0]
    losses = [v for v in completed_net_pnls if v < 0]
    if not losses or sum(losses) == 0:
        return 0.0
    return sum(wins) / abs(sum(losses))


def compute_win_rate(completed_net_pnls: list[float]) -> float:
    if not completed_net_pnls:
        return 0.0
    return sum(1 for v in completed_net_pnls if v > 0) / len(completed_net_pnls) * 100.0


def compute_expectancy(r_values: list[float]) -> float:
    if not r_values:
        return 0.0
    wins = [v for v in r_values if v > 0]
    losses = [abs(v) for v in r_values if v < 0]
    wr = len(wins) / len(r_values)
    lr = len(losses) / len(r_values)
    aw = _mean(wins)
    al = _mean(losses)
    return wr * aw - lr * al


R_BUCKETS = [
    ("< -2R", None, -2.0),
    ("-2R ~ -1R", -2.0, -1.0),
    ("-1R ~ 0", -1.0, 0.0),
    ("0 ~ +1R", 0.0, 1.0),
    ("+1R ~ +2R", 1.0, 2.0),
    ("+2R ~ +3R", 2.0, 3.0),
    ("> +3R", 3.0, None),
]


def compute_r_buckets(r_values: list[float]) -> list[tuple[str, int, float]]:
    total = len(r_values)
    out: list[tuple[str, int, float]] = []
    for label, lo, hi in R_BUCKETS:
        cnt = 0
        for v in r_values:
            if lo is None and v < (hi or 0):
                cnt += 1
            elif hi is None and v > (lo or 0):
                cnt += 1
            elif lo is not None and hi is not None and lo <= v < hi:
                cnt += 1
        ratio = (cnt / total * 100.0) if total > 0 else 0.0
        out.append((label, cnt, ratio))
    return out


def compute_actual_fill_rate(orders: list[OrderRow]) -> float:
    if not orders:
        return 0.0
    statuses: dict[str, set[str]] = defaultdict(set)
    for o in orders:
        statuses[o.order_id].add(o.status.upper())
    filled = sum(1 for ss in statuses.values() if "FILLED" in ss)
    return filled / len(statuses) * 100.0


def compute_regime_attribution(
    daily_rows: list[DailyRow], initial_equity: float
) -> list[dict[str, Any]]:
    groups: dict[str, list[float]] = defaultdict(list)
    prev = initial_equity
    for row in daily_rows:
        groups[row.market_regime].append(row.capital - prev)
        prev = row.capital
    result: list[dict[str, Any]] = []
    for regime, pnls in sorted(groups.items()):
        n = len(pnls)
        total_pnl = sum(pnls)
        avg_pnl = total_pnl / n if n > 0 else 0.0
        wr = sum(1 for v in pnls if v > 0) / n * 100.0 if n > 0 else 0.0
        if total_pnl > 0 and wr >= 55:
            conclusion = "稳定正贡献，策略适配良好"
        elif total_pnl > 0:
            conclusion = "正贡献但稳定性一般"
        else:
            conclusion = "贡献偏弱，建议重点优化"
        result.append(
            {
                "regime": regime,
                "days": n,
                "total_pnl": total_pnl,
                "avg_pnl": avg_pnl,
                "win_rate": wr,
                "conclusion": conclusion,
            }
        )
    return result


def compute_slippage_stats(
    trades: list[TradeRow], signal_type: str | None = None
) -> dict[str, float]:
    vals = [
        t.slippage
        for t in trades
        if signal_type is None or t.signal_type.lower() == signal_type.lower()
    ]
    if not vals:
        return {"mean": 0.0, "median": 0.0, "max": 0.0, "min": 0.0, "p95": 0.0}
    return {
        "mean": _mean(vals),
        "median": statistics.median(vals),
        "max": max(vals),
        "min": min(vals),
        "p95": _percentile(vals, 0.95),
    }


def build_round_trips(trades: list[TradeRow]) -> list[RoundTrip]:
    """Pair close fills to prior opens and return completed round-trips."""
    open_trades = [t for t in trades if t.offset == "Open" and t.risk_budget_r > 0]
    close_trades = [t for t in trades if t.offset != "Open"]

    from collections import deque

    open_queues: dict[tuple[str, str], deque[TradeRow]] = defaultdict(deque)
    for t in sorted(open_trades, key=lambda x: x.timestamp_ns):
        side = "long" if t.side.lower() == "buy" else "short"
        open_queues[(t.symbol, side)].append(t)

    round_trips: list[RoundTrip] = []
    for ct in sorted(close_trades, key=lambda x: x.timestamp_ns):
        close_side = "long" if ct.side.lower() == "sell" else "short"
        q = open_queues.get((ct.symbol, close_side))
        if not q:
            continue
        ot = q.popleft()
        holding_minutes = 0.0
        if ct.timestamp_ns > ot.timestamp_ns:
            holding_minutes = (ct.timestamp_ns - ot.timestamp_ns) / 1e9 / 60.0
        round_trips.append(
            RoundTrip(
                open_trade=ot,
                close_trade=ct,
                net_pnl=ct.realized_pnl - ot.commission - ct.commission,
                r_multiple=ct.realized_pnl / ot.risk_budget_r,
                holding_minutes=holding_minutes,
            )
        )
    return round_trips


def build_star_rating(
    sharpe: float,
    calmar: float,
    profit_factor: float,
    win_rate: float,
    max_dd: float,
) -> tuple[int, str, str]:
    """Return (stars 1-5, letter grade, rationale).

    Weights: sharpe 30%, calmar 25%, profit_factor 20%, win_rate 15%, max_dd 10%.
    """
    # Score each component 0-100
    s_sharpe = min(100.0, max(0.0, sharpe / 3.0 * 100.0))
    s_calmar = min(100.0, max(0.0, calmar / 5.0 * 100.0))
    s_pf = min(100.0, max(0.0, profit_factor / 2.5 * 100.0))
    s_wr = min(100.0, max(0.0, win_rate / 70.0 * 100.0))
    s_dd = min(100.0, max(0.0, (25.0 - max_dd) / 25.0 * 100.0))

    composite = (
        s_sharpe * 0.30 + s_calmar * 0.25 + s_pf * 0.20 + s_wr * 0.15 + s_dd * 0.10
    )

    if composite >= 85:
        stars, grade = 5, "A"
    elif composite >= 70:
        stars, grade = 4, "B"
    elif composite >= 50:
        stars, grade = 3, "C"
    elif composite >= 35:
        stars, grade = 2, "D"
    else:
        stars, grade = 1, "F"

    rationale_parts = []
    if sharpe >= 2.0:
        rationale_parts.append(f"夏普比率优秀 ({sharpe:.2f})")
    elif sharpe >= 1.0:
        rationale_parts.append(f"夏普比率良好 ({sharpe:.2f})")
    else:
        rationale_parts.append(f"夏普比率偏弱 ({sharpe:.2f})")

    if calmar >= 2.0:
        rationale_parts.append(f"收益/回撤比优秀 ({calmar:.2f})")
    elif calmar >= 1.0:
        rationale_parts.append(f"收益/回撤比合理 ({calmar:.2f})")
    else:
        rationale_parts.append(f"收益/回撤比不足 ({calmar:.2f})")

    if profit_factor >= 1.5:
        rationale_parts.append(f"利润因子稳健 ({profit_factor:.2f})")
    elif profit_factor >= 1.0:
        rationale_parts.append(f"利润因子边际 ({profit_factor:.2f})")
    else:
        rationale_parts.append(f"利润因子亏损 ({profit_factor:.2f})")

    return stars, grade, "；".join(rationale_parts)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Report rendering layer
# ═══════════════════════════════════════════════════════════════════════════════


def _render_executive_summary(
    run_id: str,
    strategy_name: str,
    stars: int,
    grade: str,
    rationale: str,
    annualized_return: float,
    max_dd: float,
    sharpe: float,
    calmar: float,
    profit_factor: float,
    win_rate: float,
    var_95: float,
    cumulative_return: float,
    trading_days: int,
) -> str:
    star_icon = "★" * stars + "☆" * (5 - stars)
    lines = [
        "## 一、执行摘要",
        "",
        f"### 1.1 综合评级: {star_icon}  ({grade} 级)",
        "",
        f"**评级依据**: {rationale}",
        "",
        "### 1.2 核心指标一览",
        "",
        _build_table(
            ["指标", "数值", "说明"],
            [
                [
                    "年化收益率",
                    _fmt_pct(annualized_return),
                    f"基于 {trading_days} 个交易日复利折算",
                ],
                ["累计收益率", _fmt_pct(cumulative_return), "整个回测区间的总收益"],
                ["最大回撤", _fmt_pct(max_dd), "峰值到谷底的最大跌幅"],
                ["夏普比率", _fmt_ratio(sharpe), "单位风险的超额收益"],
                ["Calmar 比率", _fmt_ratio(calmar), "年化收益 / 最大回撤"],
                ["利润因子", _fmt_ratio(profit_factor), "总盈利 / 总亏损（绝对值）"],
                ["胜率", _fmt_pct(win_rate), "盈利交易占比"],
                ["VaR (95%)", _fmt_pct(var_95), "95% 置信度下日频最大损失"],
            ],
        ),
        "",
        f"> 策略: **{strategy_name}** | 运行 ID: `{run_id}` | 评级: **{grade} 级**",
        "",
    ]
    return "\n".join(lines)


def _render_performance_overview(
    initial_equity: float,
    final_equity: float,
    cumulative_return: float,
    annualized_return: float,
    annualized_vol: float,
    max_dd: float,
    sharpe: float,
    sortino: float,
    calmar: float,
    trading_days: int,
    daily_win_rate: float,
    longest_lose_streak: int,
    avg_nonzero_exposure: float,
    nonzero_ratio: float,
) -> str:
    avg_exposure = (
        (nonzero_ratio / 100.0) * avg_nonzero_exposure if nonzero_ratio > 0 else 0.0
    )
    lines = [
        "## 二、绩效概览",
        "",
        "### 2.1 收益与风险",
        "",
        _build_table(
            ["指标", "结果", "来源"],
            [
                ["初始权益", _fmt_money(initial_equity), "backtest_*.json"],
                ["最终权益", _fmt_money(final_equity), "backtest_*.json"],
                [
                    "净盈亏",
                    _fmt_money(final_equity - initial_equity),
                    "final - initial",
                ],
                ["累计收益率", _fmt_pct(cumulative_return), "daily_equity.csv"],
                [
                    "年化收益率",
                    _fmt_pct(annualized_return),
                    f"{trading_days} 个交易日复利",
                ],
                ["年化波动率", _fmt_pct(annualized_vol), "日收益率标准差年化"],
                ["最大回撤", _fmt_pct(max_dd), "daily_equity.csv"],
                ["Calmar 比率", _fmt_ratio(calmar), "年化收益 / 最大回撤"],
                ["夏普比率", _fmt_ratio(sharpe), "无风险利率假设见附录"],
                ["索提诺比率", _fmt_ratio(sortino), "仅计入下行波动"],
            ],
        ),
        "",
        "### 2.2 日度统计",
        "",
        _build_table(
            ["指标", "结果"],
            [
                ["交易日数", _fmt_int(trading_days)],
                ["日胜率", _fmt_pct(daily_win_rate)],
                ["最长连续亏损（日）", f"{longest_lose_streak} 天"],
                ["日均仓位暴露", _fmt_pct(avg_exposure)],
                ["非零仓位日占比", _fmt_pct(nonzero_ratio)],
            ],
        ),
        "",
    ]
    return "\n".join(lines)


def _render_risk_analysis(
    var_95: float,
    es_95: float,
    ulcer: float,
    recovery_factor: float,
    max_dd: float,
    drawdown_windows: list[DrawdownWindow],
    daily_returns_pct: list[float],
) -> str:
    lines = [
        "## 三、风险分析",
        "",
        "### 3.1 风险度量",
        "",
        _build_table(
            ["指标", "数值", "说明"],
            [
                ["VaR (95%)", _fmt_pct(var_95), "历史模拟法，95% 置信日 VaR"],
                ["ES (95%)", _fmt_pct(es_95), "尾部条件期望损失"],
                ["Ulcer Index", _fmt_ratio(ulcer), "回撤深度与持续时间的综合指标"],
                [
                    "Recovery Factor",
                    _fmt_ratio(recovery_factor),
                    "净收益 / 最大回撤绝对值",
                ],
                ["最大回撤", _fmt_pct(max_dd), "整个回测区间的最大回撤"],
            ],
        ),
        "",
        "### 3.2 回撤事件 Top-3",
        "",
    ]

    if drawdown_windows:
        dd_rows = [
            [
                f"{w.peak_date} → {w.trough_date}",
                _fmt_pct(w.drawdown_pct),
                _fmt_money(w.pnl),
                _fmt_money(w.peak_capital),
                _fmt_money(w.trough_capital),
            ]
            for w in drawdown_windows
        ]
        lines.append(
            _build_table(
                ["区间", "回撤幅度", "区间盈亏", "峰值权益", "谷底权益"],
                dd_rows,
            )
        )
        lines.append("")

    # Tail-risk: max single-day loss
    worst_day = min(daily_returns_pct) if daily_returns_pct else 0.0
    worst_5 = (
        sorted(daily_returns_pct)[:5]
        if len(daily_returns_pct) >= 5
        else daily_returns_pct
    )
    lines.append("### 3.3 尾部风险")
    lines.append("")
    lines.append(f"- 最大单日亏损: **{_fmt_pct(worst_day)}**")
    lines.append(f"- 最差 5 日收益: {', '.join(_fmt_pct(v) for v in worst_5)}")
    lines.append(
        f"- 日收益分布: P1={_fmt_pct(_percentile(daily_returns_pct, 0.01))}, "
        f"P5={_fmt_pct(_percentile(daily_returns_pct, 0.05))}, "
        f"P95={_fmt_pct(_percentile(daily_returns_pct, 0.95))}, "
        f"P99={_fmt_pct(_percentile(daily_returns_pct, 0.99))}"
    )
    lines.append("")
    return "\n".join(lines)


def _render_trade_statistics(
    total_trades: int,
    win_rate: float,
    avg_win: float,
    avg_loss: float,
    profit_factor: float,
    avg_r: float,
    expectancy: float,
    max_consecutive_wins: int,
    max_consecutive_losses: int,
    avg_holding_minutes: float,
    r_values: list[float],
    signal_counts: Counter,
    completed_net: list[float],
) -> str:
    pnl_ratio = avg_win / abs(avg_loss) if avg_loss < 0 else 0.0
    lines = [
        "## 四、交易层级统计",
        "",
        "### 4.1 交易总览",
        "",
        _build_table(
            ["指标", "结果"],
            [
                ["已完成交易数 (Round-Trips)", _fmt_int(total_trades)],
                ["胜率", _fmt_pct(win_rate)],
                ["平均盈利", _fmt_money(avg_win)],
                ["平均亏损", _fmt_money(avg_loss)],
                ["盈亏比", _fmt_ratio(pnl_ratio)],
                ["利润因子", _fmt_ratio(profit_factor)],
                ["平均 R-multiple", _fmt_ratio(avg_r)],
                ["Expectancy (期望值)", _fmt_ratio(expectancy)],
                ["最长连续盈利", f"{max_consecutive_wins} 笔"],
                ["最长连续亏损", f"{max_consecutive_losses} 笔"],
                ["平均持仓时间", f"{avg_holding_minutes:.0f} 分钟"],
            ],
        ),
        "",
        "### 4.2 R-multiple 分布",
        "",
        _build_table(
            ["R 区间", "笔数", "占比"],
            [
                [label, _fmt_int(cnt), _fmt_pct(ratio)]
                for label, cnt, ratio in compute_r_buckets(r_values)
            ],
        ),
        "",
    ]

    if expectancy > 0.3:
        exp_note = "期望值优秀，策略具备稳健的正期望"
    elif expectancy > 0.0:
        exp_note = "期望值为正，有进一步优化空间"
    else:
        exp_note = "期望值为负，策略需重新审视"
    lines.append(f"- **期望值评价**: {exp_note}")
    lines.append("")
    lines.append("### 4.3 信号类型分析")
    lines.append("")
    sig_rows = [
        [
            sig,
            _fmt_int(cnt),
            _fmt_pct(cnt / sum(signal_counts.values()) * 100 if signal_counts else 0),
        ]
        for sig, cnt in signal_counts.most_common()
    ]
    lines.append(_build_table(["信号类型", "出现次数", "占比"], sig_rows))
    lines.append("")
    return "\n".join(lines)


def _render_regime_attribution(regimes: list[dict[str, Any]]) -> str:
    if not regimes:
        return ""
    lines = [
        "## 五、市场状态适应性分析",
        "",
        "### 5.1 状态归因",
        "",
        _build_table(
            ["市场状态", "出现天数", "累计盈亏", "日均盈亏", "日胜率", "结论"],
            [
                [
                    r["regime"],
                    _fmt_int(r["days"]),
                    _fmt_money(r["total_pnl"]),
                    _fmt_money(r["avg_pnl"]),
                    _fmt_pct(r["win_rate"]),
                    r["conclusion"],
                ]
                for r in regimes
            ],
        ),
        "",
    ]

    best = max(regimes, key=lambda r: r["total_pnl"])
    worst = min(regimes, key=lambda r: r["total_pnl"])
    trend_pnl = sum(r["total_pnl"] for r in regimes if "Trend" in r["regime"])
    range_pnl = sum(
        r["total_pnl"] for r in regimes if r["regime"] in {"kRanging", "kFlat"}
    )

    lines.append(
        f"- 最佳状态: **{best['regime']}** (累计 {_fmt_money(best['total_pnl'])})"
    )
    lines.append(
        f"- 最弱状态: **{worst['regime']}** (累计 {_fmt_money(worst['total_pnl'])})"
    )
    if trend_pnl > abs(range_pnl):
        lines.append(
            "- **策略呈现明显的趋势跟随特征**，趋势状态收益贡献显著高于震荡/平淡状态。"
        )
    else:
        lines.append("- **趋势属性不够纯粹**，震荡/平淡状态对绩效拖累明显。")
    lines.append("")
    return "\n".join(lines)


def _render_execution_quality(
    fill_rate_json: float,
    actual_fill_rate: float,
    cancel_rate: float,
    avg_wait_ms: float,
    slippage_all: dict[str, float],
    slippage_open: dict[str, float],
    slippage_stop: dict[str, float],
    total_commission: float,
    avg_commission_per_trade: float,
) -> str:
    lines = [
        "## 六、执行质量",
        "",
        "### 6.1 订单执行",
        "",
        _build_table(
            ["指标", "结果", "来源"],
            [
                [
                    "原始 Fill Rate",
                    _fmt_pct(fill_rate_json * 100),
                    "hf_standard.execution_quality",
                ],
                [
                    "实际成交率 (去重)",
                    _fmt_pct(actual_fill_rate),
                    "orders.csv 按 order_id 去重",
                ],
                [
                    "撤单率",
                    _fmt_pct(cancel_rate * 100),
                    "hf_standard.execution_quality",
                ],
                [
                    "平均等待时间",
                    f"{avg_wait_ms:.2f} ms",
                    "hf_standard.execution_quality",
                ],
                ["总手续费", _fmt_money(total_commission), "deterministic.performance"],
                ["平均手续费/笔", _fmt_money(avg_commission_per_trade), "trades.csv"],
            ],
        ),
        "",
        "### 6.2 滑点分析",
        "",
        _build_table(
            ["样本", "均值", "中位数", "最大值", "最小值", "P95"],
            [
                [
                    "全样本",
                    _fmt_ratio(slippage_all["mean"]),
                    _fmt_ratio(slippage_all["median"]),
                    _fmt_ratio(slippage_all["max"]),
                    _fmt_ratio(slippage_all["min"]),
                    _fmt_ratio(slippage_all["p95"]),
                ],
                [
                    "开仓 (kOpen)",
                    _fmt_ratio(slippage_open["mean"]),
                    _fmt_ratio(slippage_open["median"]),
                    _fmt_ratio(slippage_open["max"]),
                    _fmt_ratio(slippage_open["min"]),
                    _fmt_ratio(slippage_open["p95"]),
                ],
                [
                    "止损 (kStopLoss)",
                    _fmt_ratio(slippage_stop["mean"]),
                    _fmt_ratio(slippage_stop["median"]),
                    _fmt_ratio(slippage_stop["max"]),
                    _fmt_ratio(slippage_stop["min"]),
                    _fmt_ratio(slippage_stop["p95"]),
                ],
            ],
        ),
        "",
    ]
    return "\n".join(lines)


def _render_monte_carlo(mc: dict[str, Any]) -> str:
    if not mc:
        return ""
    lines = [
        "## 七、蒙特卡洛模拟",
        "",
        _build_table(
            ["指标", "结果"],
            [
                ["模拟次数", _fmt_int(mc.get("simulations", 0))],
                ["平均最终权益", _fmt_money(mc.get("mean_final_capital", 0))],
                ["95% CI 下限", _fmt_money(mc.get("ci_95_lower", 0))],
                ["95% CI 上限", _fmt_money(mc.get("ci_95_upper", 0))],
                ["亏损概率", _fmt_pct(mc.get("prob_loss", 0) * 100)],
                ["最大回撤 (95% 分位)", _fmt_pct(mc.get("max_drawdown_95", 0))],
            ],
        ),
        "",
    ]
    prob_loss = mc.get("prob_loss", 0)
    ci_low = mc.get("ci_95_lower", 0)
    if prob_loss < 0.02 and ci_low > 0:
        lines.append(
            "- **结论**: 模拟结果稳健，亏损概率极低，95% 置信区间下限显著为正，策略生存能力优秀。"
        )
    elif prob_loss < 0.10:
        lines.append(
            "- **结论**: 模拟结果可接受，亏损概率较低，95% CI 下限为正但幅度有限。"
        )
    else:
        lines.append(
            "- **结论**: 模拟结果提示风险较高，亏损概率不容忽视，建议降低风险敞口或优化策略。"
        )
    lines.append("")
    return "\n".join(lines)


def _render_multi_contract_analysis(
    run_dir: Path,
    varieties: list[str],
    trades: list[TradeRow],
    daily_rows: list[DailyRow],
    total_commission: float,
) -> str:
    """Build Section 8A: per-variety breakdown + correlation matrix."""
    if not varieties:
        return ""

    lines = [
        "## 八、多品种分解分析",
        "",
        "### 8A.1 品种绩效总览",
        "",
    ]

    # Per-variety stats from the per-product CSVs
    variety_stats: list[dict[str, Any]] = []
    for prod in varieties:
        vt_path = run_dir / "csv" / "varieties" / prod / "backtest" / "trades.csv"
        if not vt_path.exists():
            continue
        raw, cols = load_csv_rows(vt_path)
        if not raw:
            continue
        vt = parse_trade_rows(raw)
        round_trip_items = build_round_trips(vt)
        net_pnls = [rt.net_pnl for rt in round_trip_items]
        total_pnl = sum(net_pnls)
        n_trades = len(round_trip_items)
        wr = compute_win_rate(net_pnls)
        r_vals = [rt.r_multiple for rt in round_trip_items]
        avg_r = _mean(r_vals) if r_vals else 0.0
        comm = sum(
            rt.open_trade.commission + rt.close_trade.commission
            for rt in round_trip_items
        )
        variety_stats.append(
            {
                "product": prod,
                "trades": n_trades,
                "total_pnl": total_pnl,
                "win_rate": wr,
                "avg_r": avg_r,
                "commission": comm,
            }
        )

    if variety_stats:
        lines.append(
            _build_table(
                ["品种", "已完成交易数", "已完成净盈亏", "胜率", "平均R", "手续费"],
                [
                    [
                        s["product"],
                        _fmt_int(s["trades"]),
                        _fmt_money(s["total_pnl"]),
                        _fmt_pct(s["win_rate"]),
                        _fmt_ratio(s["avg_r"]),
                        _fmt_money(s["commission"]),
                    ]
                    for s in variety_stats
                ],
            )
        )
        lines.append("")

        # Risk contribution
        total_pnl_all = sum(s["total_pnl"] for s in variety_stats)
        lines.append("### 8A.2 品种风险贡献")
        lines.append("")
        contrib_rows = []
        for s in variety_stats:
            pnl_share = (
                s["total_pnl"] / total_pnl_all * 100.0 if total_pnl_all != 0 else 0.0
            )
            color = (
                "核心贡献"
                if pnl_share > 40
                else ("稳定贡献" if pnl_share > 0 else "拖累")
            )
            contrib_rows.append(
                [s["product"], _fmt_money(s["total_pnl"]), _fmt_pct(pnl_share), color]
            )
        lines.append(
            _build_table(
                ["品种", "已完成净盈亏", "已完成利润占比", "贡献评价"], contrib_rows
            )
        )
        lines.append("")

    # Correlation matrix (when pandas is available)
    if HAS_PANDAS and varieties:
        lines.append("### 8A.3 品种相关性分析")
        lines.append("")
        try:
            df = pd.DataFrame([vars(r) for r in daily_rows])
            df["date"] = pd.to_datetime(df["date"])
            # We can only compute correlation if we have per-variety daily returns.
            # Approximate: use the overall returns as a proxy, and note the limitation.
            lines.append(
                "- 跨品种相关性需品种级别的每日权益曲线。当前回测输出未按品种拆分日频权益，建议启用 per-variety daily equity 输出来支持此分析。"
            )
        except Exception:
            lines.append("- 相关性分析不可用（数据不足或 pandas 处理异常）。")
        lines.append("")

    return "\n".join(lines)


def _render_single_contract_analysis(trades: list[TradeRow]) -> str:
    """Build Section 8B: deeper single-instrument analysis."""
    if not trades:
        return ""

    # Contract chain analysis
    symbols = sorted(set(t.symbol for t in trades))
    product = _product_prefix(symbols[0]) if symbols else ""

    lines = [
        "## 八、单品种深度分析",
        "",
        f"### 8B.1 合约链 ({product})",
        "",
        f"回测涉及 {len(symbols)} 个合约月: {', '.join(symbols)}",
        "",
    ]

    # Per-contract PnL
    contract_pnl: dict[str, float] = defaultdict(float)
    contract_trades: dict[str, int] = defaultdict(int)
    for t in trades:
        contract_pnl[t.symbol] += t.realized_pnl - t.commission
        contract_trades[t.symbol] += 1

    lines.append(
        _build_table(
            ["合约", "交易笔数", "净盈亏"],
            [
                [sym, _fmt_int(contract_trades[sym]), _fmt_money(contract_pnl[sym])]
                for sym in sorted(contract_pnl.keys())
            ],
        )
    )
    lines.append("")

    # Session analysis (morning / afternoon / night) — based on local time
    lines.append("### 8B.2 时段分析")
    lines.append("")
    session_pnl: dict[str, float] = defaultdict(float)
    session_trades: dict[str, int] = defaultdict(int)
    for t in trades:
        try:
            hour = (
                int(t.timestamp_dt_local[11:13])
                if len(t.timestamp_dt_local) >= 13
                else 0
            )
        except (ValueError, IndexError):
            hour = 0
        if 9 <= hour < 11:
            sess = "早盘 (09:00-11:00)"
        elif 11 <= hour < 15:
            sess = "午盘 (11:00-15:00)"
        elif hour >= 21 or hour < 2:
            sess = "夜盘 (21:00-02:00)"
        else:
            sess = f"其他 ({hour}:00)"
        session_pnl[sess] += t.realized_pnl - t.commission
        session_trades[sess] += 1

    lines.append(
        _build_table(
            ["时段", "交易笔数", "净盈亏"],
            [
                [sess, _fmt_int(session_trades[sess]), _fmt_money(session_pnl[sess])]
                for sess in sorted(session_pnl.keys())
            ],
        )
    )
    lines.append("")

    return "\n".join(lines)


def _render_system_integrity(
    payload: dict[str, Any],
    ticks_read: int,
    bars_emitted: int,
    io_bytes: int,
    instrument_count: int,
) -> str:
    lines = [
        "## 九、系统架构与数据完整性",
        "",
        _build_table(
            ["指标", "结果"],
            [
                ["Input Signature", str(payload.get("input_signature", "N/A"))],
                ["Data Signature", str(payload.get("data_signature", "N/A"))],
                ["Ticks Read", _fmt_int(ticks_read)],
                ["Bars Emitted", _fmt_int(bars_emitted)],
                ["IO Bytes", _fmt_int(io_bytes)],
                ["Instrument Universe Size", _fmt_int(instrument_count)],
            ],
        ),
        "",
        "- **确定性验证**: Input/Data Signature 锁定回测配置与数据快照，确保跨机器复现一致性。",
        "- 引擎不变性违规数、保证金异常等详细检查请参见 `validation_report.md`。",
        "",
    ]
    return "\n".join(lines)


def _render_recommendations(
    stars: int,
    sharpe: float,
    calmar: float,
    over_budget_ratio: float,
    worst_regime: str,
) -> str:
    lines = [
        "## 十、综合建议与下一步",
        "",
        "### 10.1 策略评级与建议",
        "",
    ]

    if stars >= 4:
        lines.append(
            "- **推荐进入 Paper Trading 观察**: 收益/回撤比和风控合规性均达到较好水平。"
        )
    elif stars >= 3:
        lines.append(
            "- **建议谨慎进入观察池**: 策略具备一定收益弹性，但需验证震荡市与超预算损失的稳定性。"
        )
    else:
        lines.append(
            "- **暂不推荐直接进入实盘观察**: 当前收益质量或回撤控制尚不足以支持后续灰度。"
        )
    lines.append("")

    lines.append("### 10.2 风险控制建议")
    lines.append("")
    lines.append("- **单日最大亏损限额**: 建议设为账户权益的 2%~3%。")
    lines.append("- **连续亏损保护**: 连续 3 笔亏损后启动缩仓或暂停新开仓观察。")
    lines.append("- **弱状态过滤**: 在震荡/平淡市场状态下考虑降低仓位或屏蔽开仓信号。")
    lines.append("")

    lines.append("### 10.3 参数优化方向")
    lines.append("")
    lines.append(
        f"- 最弱市场状态为 **{worst_regime}**，建议针对该状态进行参数调优或信号过滤。"
    )
    lines.append("- 建议围绕止损参数、入场市场状态过滤做系统性参数扫描。")
    lines.append("- 若滑点模型偏乐观，建议在下一轮回测中引入更保守的滑点假设。")
    lines.append("")

    lines.append("### 10.4 实盘灰度条件")
    lines.append("")
    lines.append("1. 重新校准滑点模型，引入分场景冲击成本。")
    lines.append("2. 不同市场状态下完成 R 值分层验证。")
    lines.append("3. 连续两轮样本外回测维持正 Expectancy。")
    lines.append("4. 灰度初始规模不超过 1R 的 30%~50%。")
    lines.append("")

    return "\n".join(lines)


def _render_appendix(
    risk_free_rate: float,
    payload: dict[str, Any],
) -> str:
    hf = payload.get("hf_standard", {})
    lines = [
        "## 附录",
        "",
        "### A. 关键指标定义",
        "",
        _build_table(
            ["指标", "定义"],
            [
                ["夏普比率", "(日均超额收益 / 日波动) × √252"],
                ["索提诺比率", "(日均超额收益 / 下行波动) × √252"],
                ["Calmar 比率", "年化收益率 / 最大回撤"],
                ["R-multiple", "gross realized_pnl / 开仓 risk_budget_r"],
                ["Expectancy", "胜率 × 平均盈利 - 败率 × 平均亏损 (以 R 为单位)"],
                ["VaR (95%)", "历史模拟法，日收益率 5% 分位数的绝对值"],
                ["ES (95%)", "日收益率在 VaR 阈值以下的均值（绝对值）"],
                ["Ulcer Index", "√(mean(回撤²))，衡量回撤深度与持续时间"],
                ["Recovery Factor", "净收益 / 最大回撤绝对值"],
            ],
        ),
        "",
        "### B. 数据来源",
        "",
        f"- 无风险利率假设: **{risk_free_rate * 100:.1f}%** 年化",
        f"- HF Standard 版本: **{hf.get('version', 'N/A')}**",
        f"- 引擎模式: **{payload.get('engine_mode', 'N/A')}**",
        f"- 换月模式: **{payload.get('rollover_mode', 'N/A')}**",
        "",
        "### C. 假设与局限",
        "",
        "- 滑点模型基于回测系统配置，实盘可能存在额外冲击成本。",
        "- 蒙特卡洛模拟基于日收益率重采样，未考虑波动率聚集与尾部依赖。",
        "- 市场状态分类依赖 indicator_trace 中的 regime 标签，状态切换可能存在滞后。",
        "- 多品种相关性分析需品种级别日频权益曲线，如未输出则本节从略。",
        "",
    ]
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Orchestration
# ═══════════════════════════════════════════════════════════════════════════════


def generate_report(
    run_dir: Path,
    output_path: Path | None = None,
    risk_free_rate: float = 0.02,
) -> tuple[Path, str]:
    """Generate the full analysis report.

    Returns (output_path, report_text).
    """
    # ── Discovery ──────────────────────────────────────────────────────────
    json_path = find_backtest_json(run_dir)
    payload = load_json(json_path)
    run_id = str(payload.get("run_id", run_dir.name))

    daily_path = find_csv(run_dir, "daily_equity.csv")
    trades_path = find_csv(run_dir, "trades.csv")
    orders_path = find_csv(run_dir, "orders.csv")

    daily_raw, daily_cols = load_csv_rows(daily_path)
    trades_raw, trade_cols = load_csv_rows(trades_path)
    orders_raw, order_cols = load_csv_rows(orders_path)

    _require_columns("daily_equity.csv", daily_cols, REQUIRED_DAILY)
    _require_columns("trades.csv", trade_cols, REQUIRED_TRADE)
    _require_columns("orders.csv", order_cols, REQUIRED_ORDER)

    # ── Parsing ────────────────────────────────────────────────────────────
    daily_rows = parse_daily_rows(daily_raw)
    trades = parse_trade_rows(trades_raw)
    orders = parse_order_rows(orders_raw)

    # ── Detection ──────────────────────────────────────────────────────────
    is_multi = detect_multi_contract(run_dir, payload)
    varieties = discover_varieties(run_dir) if is_multi else []

    # Strategy name: use run_id from JSON unless there is a single dominating strategy_id
    strategy_counts = Counter(t.strategy_id for t in trades if t.strategy_id)
    if len(strategy_counts) == 1:
        strategy_name = strategy_counts.most_common(1)[0][0]
    elif len(strategy_counts) > 1:
        # Composite / multi-product: use run_id from JSON
        strategy_name = str(payload.get("run_id", "composite_strategy"))
    else:
        strategy_name = str(payload.get("run_id", "strategy"))

    # ── JSON-sourced metrics ───────────────────────────────────────────────
    hf = payload.get("hf_standard", {}) or {}
    det = payload.get("deterministic", {}) or {}
    replay = payload.get("replay", {}) or {}
    perf = det.get("performance", {}) or {}

    initial_equity = _as_float(
        payload.get("initial_equity", perf.get("initial_equity", 0))
    )
    final_equity = _as_float(payload.get("final_equity", perf.get("final_equity", 0)))

    exec_q = hf.get("execution_quality", {}) or {}
    risk_m = hf.get("risk_metrics", {}) or {}
    mc = hf.get("monte_carlo", {}) or {}

    # ── Computed metrics ───────────────────────────────────────────────────
    trading_days = len(daily_rows)
    daily_returns_pct = [r.daily_return_pct for r in daily_rows]
    cumulative_return = daily_rows[-1].cumulative_return_pct if daily_rows else 0.0
    annualized_return = compute_annualized_return(cumulative_return, trading_days)
    annualized_vol = compute_annualized_volatility(daily_returns_pct)
    max_dd = max((r.drawdown_pct for r in daily_rows), default=0.0)
    sharpe = compute_sharpe(daily_returns_pct, risk_free_rate)
    sortino = compute_sortino(daily_returns_pct, risk_free_rate)
    calmar = compute_calmar(annualized_return, max_dd)

    var_95 = _as_float(risk_m.get("var_95", compute_var_95(daily_returns_pct)))
    es_95 = _as_float(
        risk_m.get(
            "expected_shortfall_95", compute_expected_shortfall_95(daily_returns_pct)
        )
    )
    ulcer = _as_float(risk_m.get("ulcer_index", 0))
    recovery_factor = _as_float(risk_m.get("recovery_factor", 0))

    daily_win_rate = (
        sum(1 for v in daily_returns_pct if v > 0) / trading_days * 100.0
        if trading_days > 0
        else 0.0
    )

    # Longest losing streak (daily)
    longest_lose = 0
    cur = 0
    for v in daily_returns_pct:
        if v < 0:
            cur += 1
            longest_lose = max(longest_lose, cur)
        else:
            cur = 0

    # Position exposure
    exposures = [
        r.position_value / r.capital
        for r in daily_rows
        if r.capital > 0 and r.position_value > 0
    ]
    nonzero_ratio = len(exposures) / trading_days * 100.0 if trading_days > 0 else 0.0
    avg_nonzero_exposure = _mean(exposures) * 100.0 if exposures else 0.0

    # ── Trade-level metrics ────────────────────────────────────────────────
    # Open fills carry risk_budget_r while close fills carry realized PnL.
    # Pair them before computing round-trip win rate, PnL and R-multiples.
    round_trip_items = build_round_trips(trades)
    close_trades = [rt.close_trade for rt in round_trip_items]
    r_values = [rt.r_multiple for rt in round_trip_items]
    completed_net = [rt.net_pnl for rt in round_trip_items]
    holding_minutes_list = [
        rt.holding_minutes for rt in round_trip_items if rt.holding_minutes > 0.0
    ]

    round_trips = len(round_trip_items)
    wins = [v for v in completed_net if v > 0]
    losses = [v for v in completed_net if v < 0]

    win_rate = compute_win_rate(completed_net)
    avg_win = _mean(wins)
    avg_loss = _mean(losses)
    profit_factor = compute_profit_factor(completed_net)

    avg_r = _mean(r_values) if r_values else 0.0
    expectancy = compute_expectancy(r_values) if r_values else 0.0

    # Consecutive streaks (trade-level, on close trades)
    max_cw, max_cl, cur_w, cur_l = 0, 0, 0, 0
    for v in completed_net:
        if v > 0:
            cur_w += 1
            cur_l = 0
            max_cw = max(max_cw, cur_w)
        else:
            cur_l += 1
            cur_w = 0
            max_cl = max(max_cl, cur_l)

    # Signal counts (close trades give the meaningful exit signal)
    signal_counts = Counter(t.signal_type for t in close_trades)

    avg_holding_minutes = _mean(holding_minutes_list) if holding_minutes_list else 0.0

    # ── Over-budget trades (close gross loss > paired open risk_budget) ────
    over_budget = sum(1 for v in r_values if v < -1.0)
    over_budget_ratio = over_budget / len(r_values) * 100.0 if r_values else 0.0

    # ── Drawdown analysis ──────────────────────────────────────────────────
    dd_windows = compute_drawdown_windows(daily_rows, initial_equity, top_n=3)

    # ── Execution quality ──────────────────────────────────────────────────
    fill_rate_json = _as_float(exec_q.get("limit_order_fill_rate", 0))
    actual_fill_rate = compute_actual_fill_rate(orders)
    cancel_rate = _as_float(exec_q.get("cancel_rate", 0))
    avg_wait_ms = _as_float(exec_q.get("avg_wait_time_ms", 0))
    slippage_all = compute_slippage_stats(trades)
    slippage_open = compute_slippage_stats(trades, "kOpen")
    slippage_stop = compute_slippage_stats(trades, "kStopLoss")
    total_commission = _as_float(perf.get("total_commission", 0))
    avg_comm_per_trade = total_commission / round_trips if round_trips > 0 else 0.0

    # ── Regime attribution ─────────────────────────────────────────────────
    regimes = compute_regime_attribution(daily_rows, initial_equity)
    worst_regime = (
        min(regimes, key=lambda r: r["total_pnl"])["regime"] if regimes else "kUnknown"
    )

    # ── Star rating ────────────────────────────────────────────────────────
    stars, grade, rating_rationale = build_star_rating(
        sharpe, calmar, profit_factor, win_rate, max_dd
    )

    # ── Report date ────────────────────────────────────────────────────────
    report_date = datetime.now().strftime("%Y%m%d")

    # ── Output path ────────────────────────────────────────────────────────
    if output_path is None:
        safe_name = "".join(
            c if c.isalnum() or c in "_-" else "_" for c in strategy_name
        )
        output_path = run_dir / f"{safe_name}_回测分析报告_{run_id}_{report_date}.md"

    # ── Assemble report ────────────────────────────────────────────────────
    sections: list[str] = []

    sections.append(f"# {strategy_name} 回测分析报告")
    sections.append("")
    sections.append(f"> 报告生成日期: {report_date}")
    sections.append(f"> 运行 ID: `{run_id}`")
    sections.append(f"> 数据目录: `{run_dir}`")
    sections.append(f"> HF Standard 版本: {hf.get('version', 'N/A')}")
    sections.append("")
    sections.append("---")
    sections.append("")

    sections.append(
        _render_executive_summary(
            run_id=run_id,
            strategy_name=strategy_name,
            stars=stars,
            grade=grade,
            rationale=rating_rationale,
            annualized_return=annualized_return,
            max_dd=max_dd,
            sharpe=sharpe,
            calmar=calmar,
            profit_factor=profit_factor,
            win_rate=win_rate,
            var_95=var_95,
            cumulative_return=cumulative_return,
            trading_days=trading_days,
        )
    )
    sections.append(
        _render_performance_overview(
            initial_equity=initial_equity,
            final_equity=final_equity,
            cumulative_return=cumulative_return,
            annualized_return=annualized_return,
            annualized_vol=annualized_vol,
            max_dd=max_dd,
            sharpe=sharpe,
            sortino=sortino,
            calmar=calmar,
            trading_days=trading_days,
            daily_win_rate=daily_win_rate,
            longest_lose_streak=longest_lose,
            avg_nonzero_exposure=avg_nonzero_exposure,
            nonzero_ratio=nonzero_ratio,
        )
    )
    sections.append(
        _render_risk_analysis(
            var_95=var_95,
            es_95=es_95,
            ulcer=ulcer,
            recovery_factor=recovery_factor,
            max_dd=max_dd,
            drawdown_windows=dd_windows,
            daily_returns_pct=daily_returns_pct,
        )
    )
    sections.append(
        _render_trade_statistics(
            total_trades=round_trips,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
            avg_r=avg_r,
            expectancy=expectancy,
            max_consecutive_wins=max_cw,
            max_consecutive_losses=max_cl,
            avg_holding_minutes=avg_holding_minutes,
            r_values=r_values,
            signal_counts=signal_counts,
            completed_net=completed_net,
        )
    )
    sections.append(_render_regime_attribution(regimes))
    sections.append(
        _render_execution_quality(
            fill_rate_json=fill_rate_json,
            actual_fill_rate=actual_fill_rate,
            cancel_rate=cancel_rate,
            avg_wait_ms=avg_wait_ms,
            slippage_all=slippage_all,
            slippage_open=slippage_open,
            slippage_stop=slippage_stop,
            total_commission=total_commission,
            avg_commission_per_trade=avg_comm_per_trade,
        )
    )
    sections.append(_render_monte_carlo(mc))

    if is_multi:
        sections.append(
            _render_multi_contract_analysis(
                run_dir, varieties, trades, daily_rows, total_commission
            )
        )
    else:
        sections.append(_render_single_contract_analysis(trades))

    sections.append(
        _render_system_integrity(
            payload=payload,
            ticks_read=_as_int(replay.get("ticks_read", 0)),
            bars_emitted=_as_int(replay.get("bars_emitted", 0)),
            io_bytes=_as_int(replay.get("io_bytes", 0)),
            instrument_count=(
                len(replay.get("instrument_universe", []))
                if isinstance(replay.get("instrument_universe"), list)
                else len(str(replay.get("instrument_universe", "")).split(","))
            ),
        )
    )
    sections.append(
        _render_recommendations(
            stars=stars,
            sharpe=sharpe,
            calmar=calmar,
            over_budget_ratio=over_budget_ratio,
            worst_regime=worst_regime,
        )
    )
    sections.append(_render_appendix(risk_free_rate, payload))

    report_text = "\n".join(sections) + "\n"
    output_path.write_text(report_text, encoding="utf-8")
    return output_path, report_text


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="生成幻方量化标准回测分析报告")
    parser.add_argument("--run-dir", type=Path, required=True, help="回测结果目录")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="报告输出路径（默认生成在 run_dir 内）",
    )
    parser.add_argument(
        "--risk-free-rate", type=float, default=0.02, help="年化无风险利率（默认 0.02）"
    )
    args = parser.parse_args(argv)

    try:
        out_path, _ = generate_report(
            run_dir=args.run_dir,
            output_path=args.output,
            risk_free_rate=args.risk_free_rate,
        )
    except ReportError as exc:
        print(f"[report] FAIL: {exc}")
        return 2

    print(f"[report] 报告已生成: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
