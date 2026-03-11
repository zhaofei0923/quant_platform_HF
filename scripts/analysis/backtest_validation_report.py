#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover
    pq = None


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return float(value)
        return float(value)
    except Exception:
        return default


def approx_equal(left: float, right: float, tol: float = 1e-6) -> bool:
    return abs(left - right) <= tol


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def find_latest_run(root_dir: Path) -> Path:
    runs = sorted([path for path in root_dir.glob("backtest-*") if path.is_dir()])
    if not runs:
        raise FileNotFoundError(f"未找到回测目录: {root_dir}")
    return runs[-1]


def summarize_signal_counts(trades: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for trade in trades:
        signal_type = str(trade.get("signal_type", "UNKNOWN"))
        counter[signal_type] += 1
    return dict(counter)


def extract_trade_list(payload: dict[str, Any], deterministic: dict[str, Any]) -> list[dict[str, Any]]:
    deterministic_trades = deterministic.get("trades")
    if isinstance(deterministic_trades, list) and deterministic_trades:
        return deterministic_trades

    hf_standard = payload.get("hf_standard", {}) or {}
    hf_trades = hf_standard.get("trades")
    if isinstance(hf_trades, list):
        return hf_trades

    return []


def extract_order_list(payload: dict[str, Any], deterministic: dict[str, Any]) -> list[dict[str, Any]]:
    deterministic_orders = deterministic.get("orders")
    if isinstance(deterministic_orders, list) and deterministic_orders:
        return deterministic_orders

    hf_standard = payload.get("hf_standard", {}) or {}
    hf_orders = hf_standard.get("orders")
    if isinstance(hf_orders, list):
        return hf_orders

    return []


def analyze_sub_trace_parquet(path: Path) -> dict[str, Any]:
    if pq is None:
        return {
            "enabled": False,
            "error": "pyarrow 不可用，无法读取 parquet",
        }

    table = pq.read_table(path)
    rows = table.num_rows
    columns = set(table.column_names)

    metrics: dict[str, Any] = {
        "enabled": True,
        "rows": rows,
        "columns": sorted(columns),
        "field_non_null": {},
        "by_strategy": {},
        "error": "",
    }

    tracked_fields = [
        "kama",
        "er",
        "atr",
        "adx",
        "stop_loss_price",
        "take_profit_price",
    ]

    for field in tracked_fields:
        if field in columns:
            arr = table.column(field)
            non_null = rows - arr.null_count
            metrics["field_non_null"][field] = {
                "non_null": non_null,
                "null": arr.null_count,
                "ratio": (non_null / rows) if rows > 0 else 0.0,
            }

    if "strategy_id" in columns:
        strategy_values = table.column("strategy_id").to_pylist()
        strategy_rows: dict[str, list[int]] = defaultdict(list)
        for index, sid in enumerate(strategy_values):
            key = str(sid) if sid is not None else "<NULL>"
            strategy_rows[key].append(index)

        by_strategy: dict[str, dict[str, Any]] = {}
        for sid, idxs in strategy_rows.items():
            row_count = len(idxs)
            item: dict[str, Any] = {"rows": row_count}
            for field in tracked_fields:
                if field not in columns:
                    continue
                arr = table.column(field).to_pylist()
                non_null = 0
                for i in idxs:
                    if arr[i] is not None:
                        non_null += 1
                item[field] = {
                    "non_null": non_null,
                    "ratio": (non_null / row_count) if row_count > 0 else 0.0,
                }
            by_strategy[sid] = item
        metrics["by_strategy"] = by_strategy

    return metrics


def analyze_sub_trace_csv(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    columns = set(fieldnames)
    row_count = len(rows)
    metrics: dict[str, Any] = {
        "enabled": True,
        "rows": row_count,
        "columns": sorted(columns),
        "field_non_null": {},
        "by_strategy": {},
        "error": "",
    }

    tracked_fields = [
        "kama",
        "er",
        "atr",
        "adx",
        "stop_loss_price",
        "take_profit_price",
    ]

    for field in tracked_fields:
        if field not in columns:
            continue
        non_null = sum(1 for row in rows if str(row.get(field, "")).strip() != "")
        metrics["field_non_null"][field] = {
            "non_null": non_null,
            "null": row_count - non_null,
            "ratio": (non_null / row_count) if row_count > 0 else 0.0,
        }

    if "strategy_id" in columns:
        grouped_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in rows:
            key = str(row.get("strategy_id", "") or "<NULL>")
            grouped_rows[key].append(row)

        by_strategy: dict[str, dict[str, Any]] = {}
        for sid, strategy_rows in grouped_rows.items():
            item: dict[str, Any] = {"rows": len(strategy_rows)}
            for field in tracked_fields:
                if field not in columns:
                    continue
                non_null = sum(1 for row in strategy_rows if str(row.get(field, "")).strip() != "")
                item[field] = {
                    "non_null": non_null,
                    "ratio": (non_null / len(strategy_rows)) if strategy_rows else 0.0,
                }
            by_strategy[sid] = item
        metrics["by_strategy"] = by_strategy

    return metrics


def analyze_sub_trace(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".csv":
        return analyze_sub_trace_csv(path)
    return analyze_sub_trace_parquet(path)


def make_report(run_dir: Path, strict: bool) -> tuple[str, bool]:
    json_path = run_dir / "backtest_auto.json"
    if not json_path.exists():
        raise FileNotFoundError(f"缺少回测结果文件: {json_path}")

    payload = load_json(json_path)
    deterministic = payload.get("deterministic", {}) or {}
    performance = deterministic.get("performance", {}) or {}
    replay = payload.get("replay", {}) or {}
    spec = payload.get("spec", {}) or {}

    initial_equity = as_float(payload.get("initial_equity"))
    final_equity = as_float(payload.get("final_equity"))
    total_commission = as_float(performance.get("total_commission"))
    total_realized = as_float(performance.get("total_realized_pnl"))
    total_unrealized = as_float(performance.get("total_unrealized_pnl"))
    total_pnl_after_cost = as_float(performance.get("total_pnl_after_cost"))

    trades = extract_trade_list(payload, deterministic)
    orders = extract_order_list(payload, deterministic)
    invariant_violations = deterministic.get("invariant_violations", []) or []

    checks: list[CheckResult] = []

    expected_final_equity = initial_equity + total_pnl_after_cost
    checks.append(
        CheckResult(
            "资金恒等式",
            approx_equal(final_equity, expected_final_equity),
            f"final_equity={final_equity:.6f}, initial+total_pnl_after_cost={expected_final_equity:.6f}",
        )
    )

    expected_after_cost = total_realized + total_unrealized - total_commission
    checks.append(
        CheckResult(
            "盈亏口径一致性",
            approx_equal(total_pnl_after_cost, expected_after_cost),
            "total_pnl_after_cost="
            f"{total_pnl_after_cost:.6f}, realized+unrealized-commission={expected_after_cost:.6f}",
        )
    )

    checks.append(
        CheckResult(
            "手续费非负",
            total_commission >= 0.0 and math.isfinite(total_commission),
            f"total_commission={total_commission:.6f}",
        )
    )

    trade_commission_sum = sum(as_float(item.get("commission")) for item in trades)
    checks.append(
        CheckResult(
            "成交手续费汇总一致",
            approx_equal(trade_commission_sum, total_commission),
            f"sum(trades.commission)={trade_commission_sum:.6f}, total_commission={total_commission:.6f}",
        )
    )

    intents_processed = int(deterministic.get("intents_processed", 0) or 0)
    checks.append(
        CheckResult(
            "意图与成交数量关系",
            intents_processed >= len(trades),
            f"intents_processed={intents_processed}, trades={len(trades)}",
        )
    )

    order_events_emitted = int(deterministic.get("order_events_emitted", 0) or 0)
    checks.append(
        CheckResult(
            "订单事件数量关系",
            order_events_emitted >= len(trades),
            f"order_events_emitted={order_events_emitted}, trades={len(trades)}",
        )
    )

    checks.append(
        CheckResult(
            "引擎不变量",
            len(invariant_violations) == 0,
            f"invariant_violations={len(invariant_violations)}",
        )
    )

    margin_clipped = int(performance.get("margin_clipped_orders", 0) or 0)
    margin_rejected = int(performance.get("margin_rejected_orders", 0) or 0)
    checks.append(
        CheckResult(
            "保证金约束异常统计",
            margin_clipped >= 0 and margin_rejected >= 0,
            f"margin_clipped_orders={margin_clipped}, margin_rejected_orders={margin_rejected}",
        )
    )

    sub_trace_cfg = payload.get("sub_strategy_indicator_trace", {}) or {}
    sub_trace_enabled = bool(sub_trace_cfg.get("enabled", False))
    sub_trace_path = Path(str(sub_trace_cfg.get("path", ""))) if sub_trace_enabled else None
    sub_trace_result: dict[str, Any] = {"enabled": False}
    if sub_trace_enabled and sub_trace_path is not None and sub_trace_path.exists():
        sub_trace_result = analyze_sub_trace(sub_trace_path)
        expected_rows = int(sub_trace_cfg.get("rows", 0) or 0)
        actual_rows = int(sub_trace_result.get("rows", 0) or 0)
        checks.append(
            CheckResult(
                "子策略追踪行数一致",
                expected_rows == actual_rows,
                f"expected_rows={expected_rows}, trace_rows={actual_rows}",
            )
        )

    passed_count = sum(1 for item in checks if item.passed)
    total_count = len(checks)
    overall_pass = passed_count == total_count

    signal_counts = summarize_signal_counts(trades)

    lines: list[str] = []
    lines.append("# 回测执行检测报告")
    lines.append("")
    lines.append("## 1) 运行概览")
    lines.append(f"- run_dir: {run_dir}")
    lines.append(f"- run_id: {payload.get('run_id', '')}")
    lines.append(f"- engine_mode: {payload.get('engine_mode', '')}")
    lines.append(f"- strategy_factory: {spec.get('strategy_factory', '')}")
    lines.append(f"- symbols: {spec.get('symbols', [])}")
    lines.append(
        f"- ticks_read / bars_emitted: {replay.get('ticks_read', 0)} / {replay.get('bars_emitted', 0)}"
    )
    lines.append("")

    lines.append("## 2) 核心一致性检查")
    lines.append(f"- 结果: {'PASS' if overall_pass else 'FAIL'} ({passed_count}/{total_count})")
    for item in checks:
        lines.append(
            f"- [{'PASS' if item.passed else 'FAIL'}] {item.name}: {item.detail}"
        )
    lines.append("")

    lines.append("## 3) 执行结果分析")
    lines.append(f"- final_equity: {final_equity:.6f}")
    lines.append(f"- total_pnl_after_cost: {total_pnl_after_cost:.6f}")
    lines.append(f"- total_realized_pnl: {total_realized:.6f}")
    lines.append(f"- total_unrealized_pnl: {total_unrealized:.6f}")
    lines.append(f"- total_commission: {total_commission:.6f}")
    lines.append(f"- trades: {len(trades)}; orders: {len(orders)}")
    lines.append(f"- signal_type_count: {signal_counts}")
    lines.append("")

    lines.append("## 4) 过程数据覆盖（sub-strategy trace）")
    if sub_trace_enabled and sub_trace_result.get("enabled"):
        lines.append(f"- path: {sub_trace_cfg.get('path', '')}")
        lines.append(f"- rows: {sub_trace_result.get('rows', 0)}")
        field_non_null = sub_trace_result.get("field_non_null", {})
        for field, values in field_non_null.items():
            lines.append(
                f"- {field}: non_null={values['non_null']}, null={values['null']}, ratio={values['ratio']:.4f}"
            )
        by_strategy = sub_trace_result.get("by_strategy", {})
        for sid, values in by_strategy.items():
            lines.append(f"- strategy={sid}: rows={values.get('rows', 0)}")
            for key in ["kama", "er", "atr", "adx", "stop_loss_price", "take_profit_price"]:
                if key in values:
                    lines.append(
                        f"  - {key}: non_null={values[key]['non_null']}, ratio={values[key]['ratio']:.4f}"
                    )
    else:
        lines.append("- 未启用或无法读取 sub-strategy trace。")
    lines.append("")

    lines.append("## 5) 结论与建议")
    if overall_pass:
        lines.append("- 本次回测的关键资金/盈亏/执行口径检查通过。")
    else:
        lines.append("- 检测存在失败项，请先修复 FAIL 项再比较策略收益表现。")
    lines.append("- 若要验证‘为何触发某次开平仓’，建议同时开启 emit_orders 与 emit_position_history，并与 sub-trace 按 ts_ns 联查。")
    lines.append("- 若要压测稳定性，保持本报告脚本不变，仅扩大日期和标的范围。")

    if strict and not overall_pass:
        lines.append("")
        lines.append("> strict 模式下存在 FAIL，脚本将返回非零退出码。")

    return "\n".join(lines) + "\n", overall_pass


def main() -> int:
    parser = argparse.ArgumentParser(description="生成回测执行检测报告")
    parser.add_argument("--run-dir", type=Path, default=None, help="回测结果目录（默认自动取最新）")
    parser.add_argument(
        "--runs-root",
        type=Path,
        default=Path("docs/results/backtest_runs"),
        help="回测结果根目录",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="报告输出路径（默认 <run_dir>/validation_report.md）",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="若检查失败则返回非零退出码",
    )
    args = parser.parse_args()

    run_dir = args.run_dir if args.run_dir is not None else find_latest_run(args.runs_root)
    output_path = args.output if args.output is not None else (run_dir / "validation_report.md")

    report_text, all_passed = make_report(run_dir=run_dir, strict=args.strict)
    output_path.write_text(report_text, encoding="utf-8")

    print(f"[report] 生成完成: {output_path}")
    print(f"[report] 检查结果: {'PASS' if all_passed else 'FAIL'}")

    return 0 if (all_passed or not args.strict) else 2


if __name__ == "__main__":
    raise SystemExit(main())
