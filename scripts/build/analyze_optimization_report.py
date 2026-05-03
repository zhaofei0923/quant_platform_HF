#!/usr/bin/env python3
"""
参数优化报告分析工具，提供三个子命令：
  analyze        解析单次优化报告，输出 TopN 分析和 refinement 建议
  rollover-audit 对 rolling 窗口执行合约换月审计
  composite-score 计算候选参数的综合筛选得分

用法：
  python3 scripts/build/analyze_optimization_report.py analyze --report-json <path> [options]
  python3 scripts/build/analyze_optimization_report.py rollover-audit --manifest <path> --symbol <s> [options]
  python3 scripts/build/analyze_optimization_report.py composite-score --rolling-results-dir <dir> [options]
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

# ─── 通用工具 ────────────────────────────────────────────────────────────────


def load_json(path: str) -> dict[str, Any]:
    """加载 JSON 文件，文件不存在时返回空 dict。"""
    p = Path(path)
    if not p.exists():
        print(f"[WARN] 文件不存在: {path}", file=sys.stderr)
        return {}
    with open(p, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_json(data: Any, path: str) -> None:
    """保存 JSON 到文件。"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False, default=str)


def save_csv(rows: list[dict[str, Any]], path: str) -> None:
    """保存 CSV 文件（从 dict 列表）。"""
    if not rows:
        print("[WARN] 无数据可写入 CSV", file=sys.stderr)
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with open(p, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def percentile(sorted_vals: list[float], pct: float) -> float:
    """计算百分位数（线性插值）。"""
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * pct / 100.0
    f = int(math.floor(k))
    c = int(math.ceil(k))
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


# ─── 子命令：analyze ─────────────────────────────────────────────────────────


def cmd_analyze(args: argparse.Namespace) -> None:
    """解析单次优化报告，输出 TopN 分析和 refinement 建议。"""
    report = load_json(args.report_json)
    if not report:
        print("ERROR: 无法加载优化报告", file=sys.stderr)
        sys.exit(1)

    # 提取 trial 列表（兼容多种 JSON 结构）
    trials = _extract_trials(report)
    if not trials:
        print("ERROR: 报告中无 trial 数据", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] 总 trial 数: {len(trials)}")

    # 分类统计
    completed = []
    failed = []
    constraint_violated = []
    for t in trials:
        status = t.get("status", "unknown")
        cv = t.get("constraint_violated", False)
        if status == "failed":
            failed.append(t)
        elif cv:
            constraint_violated.append(t)
        elif status == "completed":
            completed.append(t)

    print(f"  completed: {len(completed)}")
    print(f"  failed: {len(failed)}")
    print(f"  constraint_violated: {len(constraint_violated)}")

    if not completed:
        print("ERROR: 无 completed trial，无法分析", file=sys.stderr)
        sys.exit(1)

    # 按 objective 排序（假设越大越好）
    completed.sort(key=lambda t: t.get("objective", float("-inf")), reverse=True)
    top_n = min(args.top_n, len(completed))
    top_trials = completed[:top_n]

    _print_topn_summary(top_trials, top_n)

    # 生成 TopN CSV
    if args.output_table:
        rows = []
        for i, t in enumerate(top_trials):
            row = {"rank": i + 1, "objective": t.get("objective")}
            params = t.get("params", {})
            for k, v in params.items():
                row[f"param_{k}"] = v
            row["status"] = t.get("status", "")
            row["trial_id"] = t.get("trial_id", t.get("id", ""))
            rows.append(row)
        save_csv(rows, args.output_table)
        print(f"[INFO] TopN 分析表已保存: {args.output_table}")

    # 生成 refinement 建议
    if args.refinement_suggestion:
        suggestion = _compute_refinement(top_trials, trials)
        save_json(suggestion, args.refinement_suggestion)
        print(f"[INFO] Refinement 建议已保存: {args.refinement_suggestion}")

    # 检查低成交伪高分
    _check_low_quality(top_trials)

    # 热力图稳定性（如果可用）
    _check_heatmap_stability(report, top_trials)


def _extract_trials(report: dict[str, Any]) -> list[dict[str, Any]]:
    """从报告中提取 trial 列表，兼容多种 JSON 结构。"""
    # 直接有 trials 数组
    if "trials" in report and isinstance(report["trials"], list):
        return report["trials"]

    # 嵌套在 optimization_results 中
    if "optimization_results" in report:
        results = report["optimization_results"]
        if isinstance(results, dict) and "trials" in results:
            return results["trials"]
        if isinstance(results, list):
            return results

    # results.trials
    if "results" in report:
        results = report["results"]
        if isinstance(results, dict) and "trials" in results:
            return results["trials"]
        if isinstance(results, list):
            return results

    # 也许整个 report 就是一个 trial 数组
    if isinstance(report, list):
        return report

    return []


def _print_topn_summary(top_trials: list[dict[str, Any]], top_n: int) -> None:
    """打印 TopN 摘要。"""
    print(f"\n{'='*80}")
    print(f"Top {top_n} Trials (按 objective 降序)")
    print(f"{'='*80}")
    header = f"{'Rank':<6} {'Objective':<16}"
    if top_trials:
        params = top_trials[0].get("params", {})
        for k in params:
            header += f" {k:<16}"
    print(header)
    print("-" * 80)
    for i, t in enumerate(top_trials):
        obj = t.get("objective", "N/A")
        row = f"{i+1:<6} {str(obj):<16}"
        params = t.get("params", {})
        for k in params:
            row += f" {str(params[k]):<16}"
        print(row)
    print(f"{'='*80}\n")


def _compute_refinement(
    top_trials: list[dict[str, Any]], all_trials: list[dict[str, Any]]
) -> dict[str, Any]:
    """根据 TopN 计算 refinement 建议区间。"""
    if len(top_trials) < 3:
        return {"stop": True, "reason": "有效 TopN trial 不足 3 个，无法可靠计算 refinement"}

    # 收集所有参数名
    param_names: list[str] = []
    if top_trials:
        param_names = list(top_trials[0].get("params", {}).keys())

    suggestion: dict[str, Any] = {"stop": False, "parameters": {}}

    for pname in param_names:
        vals = sorted([t["params"][pname] for t in top_trials if pname in t.get("params", {})])
        if len(vals) < 3:
            suggestion["parameters"][pname] = {
                "action": "keep",
                "reason": "有效值不足，保持当前区间",
            }
            continue

        best_val = top_trials[0]["params"].get(pname)
        p20 = percentile(vals, 20)
        p80 = percentile(vals, 80)
        val_range = max(vals) - min(vals)

        # 步长估计
        if len(vals) >= 2:
            diffs = [abs(vals[i + 1] - vals[i]) for i in range(len(vals) - 1)]
            nonzero_diffs = [d for d in diffs if d > 0]
            typical_step = min(nonzero_diffs) if nonzero_diffs else val_range / 10.0
        else:
            typical_step = val_range / 10.0

        dispersion = val_range / max(typical_step * 10, 1e-9)

        param_info: dict[str, Any] = {
            "best_value": best_val,
            "TopN_range": [min(vals), max(vals)],
            "P20": p20,
            "P80": p80,
            "dispersion_ratio": round(dispersion, 2),
        }

        # 判断是否在搜索边界（需要从所有 trial 中获取搜索空间）
        all_vals_for_param = []
        for t in all_trials:
            if pname in t.get("params", {}):
                all_vals_for_param.append(t["params"][pname])
        search_min = min(all_vals_for_param) if all_vals_for_param else min(vals)
        search_max = max(all_vals_for_param) if all_vals_for_param else max(vals)

        at_boundary = False
        boundary_direction = None
        margin = typical_step * 1.5
        if best_val is not None:
            if abs(best_val - search_max) <= margin:
                at_boundary = True
                boundary_direction = "upper"
            elif abs(best_val - search_min) <= margin:
                at_boundary = True
                boundary_direction = "lower"

        # 统计边界附近的 TopN 比例
        boundary_count = 0
        for t in top_trials:
            v = t.get("params", {}).get(pname)
            if v is not None:
                if boundary_direction == "upper" and abs(v - search_max) <= margin:
                    boundary_count += 1
                elif boundary_direction == "lower" and abs(v - search_min) <= margin:
                    boundary_count += 1
        boundary_ratio = boundary_count / len(top_trials)

        param_info["at_boundary"] = at_boundary
        param_info["boundary_direction"] = boundary_direction
        param_info["boundary_ratio"] = round(boundary_ratio, 2)
        param_info["search_space_current"] = [search_min, search_max]

        if dispersion <= 3.0:
            if at_boundary and boundary_ratio >= 0.3:
                # 情况 B：边界最优，扩展
                if boundary_direction == "lower":
                    new_min = search_min * 0.7 if search_min > 1e-9 else search_min * 0.5
                    new_max = p80 + 0.15 * (p80 - p20) if p80 > p20 else p80 * 1.15
                else:
                    new_min = p20 - 0.15 * (p80 - p20) if p80 > p20 else p20 * 0.85
                    new_max = search_max * 1.3
                param_info["action"] = "expand"
                param_info["suggested_range"] = [round(new_min, 6), round(new_max, 6)]
                param_info["reason"] = f"边界最优（{boundary_direction}），{boundary_ratio:.0%} TopN 在边界，扩展一次"
            else:
                # 情况 A：聚集 + 内部最优，收窄
                margin15 = 0.15 * (p80 - p20) if p80 > p20 else typical_step
                new_min = max(search_min, p20 - margin15)
                new_max = min(search_max, p80 + margin15)
                param_info["action"] = "narrow"
                param_info["suggested_range"] = [round(new_min, 6), round(new_max, 6)]
                param_info["reason"] = "TopN 聚集，收窄搜索区间"
        else:
            # 情况 C：分散
            param_info["action"] = "stop"
            param_info["reason"] = f"TopN 分散（dispersion={dispersion:.1f}），建议停止 refinement"

        suggestion["parameters"][pname] = param_info

    # 如果有任一参数需要 stop，全局标记
    for pinfo in suggestion["parameters"].values():
        if pinfo.get("action") == "stop":
            suggestion["stop"] = True
            suggestion["reason"] = "存在参数 TopN 分散，refinement 无法收敛"
            break

    # 计算建议的 grid 点
    if not suggestion["stop"]:
        suggest_grid_points(suggestion, top_trials)

    return suggestion


def suggest_grid_points(suggestion: dict[str, Any], top_trials: list[dict[str, Any]]) -> None:
    """为 refinement 建议生成具体的 grid 取值点（4 个点）。"""
    for pname, pinfo in suggestion.get("parameters", {}).items():
        if pinfo.get("action") in ("narrow", "expand"):
            r = pinfo.get("suggested_range")
            if r and len(r) == 2:
                step = (r[1] - r[0]) / 3.0
                points = [round(r[0] + i * step, 6) for i in range(4)]
                pinfo["suggested_values"] = points


def _check_low_quality(top_trials: list[dict[str, Any]]) -> None:
    """检查低成交伪高分和其他质量问题。"""
    warnings = []
    for i, t in enumerate(top_trials):
        trades = _extract_trade_count(t)
        if trades is not None and trades < 10:
            warnings.append(f"Top{i+1} 成交数 = {trades}，可能为低成交伪高分")
        obj = t.get("objective")
        if obj is not None and obj <= 0:
            warnings.append(f"Top{i+1} objective = {obj}，非正目标值")
    if warnings:
        print("\n[QUALITY WARNINGS]")
        for w in warnings:
            print(f"  ⚠ {w}")


def _extract_trade_count(trial: dict[str, Any]) -> Optional[int]:
    """从 trial 中提取成交数（可能嵌套在多个路径下）。"""
    # 直接字段
    for key in ("total_trades", "trade_count", "num_trades"):
        if key in trial and trial[key] is not None:
            return int(trial[key])

    # 嵌套路径
    for path in [
        "hf_standard.trade_statistics.total_trades",
        "metrics.trade_statistics.total_trades",
        "result.total_trades",
    ]:
        val = trial
        for part in path.split("."):
            if isinstance(val, dict):
                val = val.get(part)
            else:
                val = None
                break
        if val is not None:
            return int(val)

    return None


def _check_heatmap_stability(report: dict[str, Any], top_trials: list[dict[str, Any]]) -> None:
    """检查热力图是否暗示周围参数崩塌。"""
    heatmaps = []
    for key in report:
        if "heatmap" in key.lower():
            heatmaps.append(key)

    if not heatmaps:
        # 检查独立 heatmap 文件
        heatmap_files = report.get("heatmap_files", [])
        if not heatmap_files:
            print("[INFO] 无 heatmap 数据，跳过局部稳定性检查")
            return

    # 如果有 heatmap 数据，检查最优参数周围的梯度
    print("[INFO] Heatmap 稳定性：请人工检查最优参数周围无崩塌")
    print("  判断标准：最优参数前后左右的目标值不应出现 >30% 的突然下降")


# ─── 子命令：rollover-audit ───────────────────────────────────────────────────


def cmd_rollover_audit(args: argparse.Namespace) -> None:
    """对 rolling 窗口执行合约换月审计。"""
    manifest = _load_manifest(args.manifest)
    if not manifest:
        print("ERROR: 无法加载 manifest", file=sys.stderr)
        sys.exit(1)

    # 构建日期 → instrument_id 的映射
    symbol = args.symbol
    date_to_instrument: dict[str, str] = {}
    for entry in manifest:
        entry_symbol = str(entry.get("symbol", entry.get("source", "")))
        if entry_symbol == symbol:
            date_str = str(entry.get("date", entry.get("trading_date", entry.get("trading_day", ""))))
            instrument_id = str(entry.get("instrument_id", ""))
            if date_str and instrument_id:
                date_to_instrument[date_str] = instrument_id

    if not date_to_instrument:
        print(f"ERROR: manifest 中无品种 {symbol} 的数据", file=sys.stderr)
        sys.exit(1)

    # 生成窗口
    windows = _generate_windows(
        args.window_start,
        args.window_end,
        args.window_step,
        args.test_length,
    )

    window_results = []
    contaminated_windows = []
    clean_windows = []

    for i, (train_start, train_end, test_start, test_end) in enumerate(windows):
        # 收集 test 窗口内的 instrument_id
        instruments = set()
        d = test_start
        while d <= test_end:
            d_str = d.strftime("%Y%m%d")
            if d_str in date_to_instrument:
                instruments.add(date_to_instrument[d_str])
            d += timedelta(days=1)

        inst_list = sorted(instruments)
        is_contaminated = len(inst_list) > 1
        window_info = {
            "window_index": i,
            "train_start": train_start.strftime("%Y-%m-%d"),
            "train_end": train_end.strftime("%Y-%m-%d"),
            "test_start": test_start.strftime("%Y-%m-%d"),
            "test_end": test_end.strftime("%Y-%m-%d"),
            "instruments": inst_list,
            "instrument_count": len(inst_list),
            "contaminated": is_contaminated,
        }

        if is_contaminated:
            contaminated_windows.append(i)
        elif len(inst_list) == 1:
            clean_windows.append(i)
        elif len(inst_list) == 0:
            window_info["note"] = "无数据"

        window_results.append(window_info)

    # 输出
    audit_result = {
        "symbol": symbol,
        "total_windows": len(window_results),
        "clean_windows": clean_windows,
        "contaminated_windows": contaminated_windows,
        "contaminated_count": len(contaminated_windows),
        "windows": window_results,
    }

    if args.output:
        save_json(audit_result, args.output)
        print(f"[INFO] 换月审计结果已保存: {args.output}")

    print(f"\n换月审计摘要 ({symbol}):")
    print(f"  总窗口数: {len(window_results)}")
    print(f"  有效窗口（单合约）: {len(clean_windows)}")
    print(f"  污染窗口（多合约）: {len(contaminated_windows)}")

    if contaminated_windows:
        print(f"\n  ⚠ 污染窗口列表: {contaminated_windows}")
        for wi in contaminated_windows:
            w = window_results[wi]
            print(f"    Window {wi}: {w['test_start']} → {w['test_end']}, 合约: {w['instruments']}")
        print("\n  这些窗口已从最终排名中排除。")


def _load_manifest(manifest_path: str) -> list[dict[str, Any]]:
    """加载 Parquet manifest（JSONL 格式）。"""
    p = Path(manifest_path)
    if not p.exists():
        print(f"[WARN] manifest 文件不存在: {manifest_path}", file=sys.stderr)
        return []
    entries = []
    with open(p, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return entries


def _generate_windows(
    start_str: str,
    end_str: str,
    step_days: int,
    test_length_days: int,
    train_length_days: int = 120,
) -> list[tuple[datetime, datetime, datetime, datetime]]:
    """生成 rolling 窗口 (train_start, train_end, test_start, test_end)。"""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    train_len = timedelta(days=train_length_days)
    test_len = timedelta(days=test_length_days)
    step = timedelta(days=step_days)

    windows = []
    cursor = start
    while cursor + train_len + test_len <= end:
        train_end = cursor + train_len - timedelta(days=1)
        test_start = train_end + timedelta(days=1)
        test_end = test_start + test_len - timedelta(days=1)
        windows.append((cursor, train_end, test_start, test_end))
        cursor += step
    return windows


# ─── 子命令：composite-score ─────────────────────────────────────────────────


def cmd_composite_score(args: argparse.Namespace) -> None:
    """计算候选参数的综合筛选得分（仅用于 TopN 筛选，不做最终决策）。"""
    results_dir = Path(args.rolling_results_dir)
    if not results_dir.exists():
        print(f"ERROR: 结果目录不存在: {results_dir}", file=sys.stderr)
        sys.exit(1)

    # 解析权重
    weights = {}
    for pair in args.weights.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, v = pair.split("=", 1)
            weights[k.strip()] = float(v.strip())

    if not weights:
        weights = {
            "profit_factor": 0.30,
            "calmar": 0.25,
            "pnl": 0.20,
            "stability": 0.15,
            "trade_quality": 0.10,
        }

    # 加载排除窗口列表
    exclude_windows: set[int] = set()
    if args.exclude_windows_file:
        audit = load_json(args.exclude_windows_file)
        if audit:
            exclude_windows = set(audit.get("contaminated_windows", []))

    # 收集每个窗口的 test 结果
    test_dir = results_dir / "test_results"
    if not test_dir.exists():
        print(f"ERROR: test_results 目录不存在: {test_dir}", file=sys.stderr)
        sys.exit(1)

    window_scores: dict[int, dict[str, Any]] = {}
    for wdir in sorted(test_dir.iterdir()):
        if not wdir.is_dir():
            continue
        # 提取窗口索引
        wname = wdir.name
        wi = -1
        for part in wname.split("_"):
            try:
                wi = int(part)
                break
            except ValueError:
                continue
        if wi < 0 or wi in exclude_windows:
            continue

        result_file = wdir / "result.json"
        if not result_file.exists():
            continue

        result = load_json(str(result_file))
        if not result:
            continue

        metrics = _extract_window_metrics(result)
        window_scores[wi] = metrics

    if not window_scores:
        print("ERROR: 未找到有效窗口结果", file=sys.stderr)
        sys.exit(1)

    # 计算每个指标在各窗口中的统计量
    all_metrics = list(window_scores.values())
    metric_keys = set()
    for m in all_metrics:
        metric_keys.update(m.keys())

    # 对每个指标做归一化
    normalized: dict[int, dict[str, float]] = defaultdict(dict)
    for key in metric_keys:
        vals = [m.get(key, 0) or 0 for m in all_metrics]
        min_v = min(vals)
        max_v = max(vals)
        rng = max_v - min_v if max_v > min_v else 1.0
        for wi, m in window_scores.items():
            v = m.get(key, 0) or 0
            normalized[wi][key] = (v - min_v) / rng

    # 计算综合得分
    composite: dict[int, float] = {}
    for wi in window_scores:
        score = 0.0
        for metric, weight in weights.items():
            metric_key_map = {
                "pf": "profit_factor",
                "calmar": "calmar_ratio",
                "pnl": "total_pnl",
                "stability": "pf_stability",
                "trade_quality": "trade_count",
            }
            mapped = metric_key_map.get(metric, metric)
            if mapped in normalized[wi]:
                score += weight * normalized[wi][mapped]
        composite[wi] = score

    # 输出
    rows = []
    for wi in sorted(composite.keys(), key=lambda w: composite[w], reverse=True):
        rows.append(
            {
                "window": wi,
                "composite_score": round(composite[wi], 4),
                **{
                    k: round(v, 4) if isinstance(v, float) else v
                    for k, v in window_scores[wi].items()
                },
            }
        )

    if args.output:
        save_csv(rows, args.output)
        print(f"[INFO] 综合得分表已保存: {args.output}")

    print(f"\n综合得分排名 (权重: {weights}):")
    print(f"{'Window':<10} {'Score':<12}")
    print("-" * 30)
    for row in rows[:15]:
        print(f"{row['window']:<10} {row['composite_score']:<12.4f}")


def _extract_window_metrics(result: dict[str, Any]) -> dict[str, Any]:
    """从窗口回测结果中提取关键指标。"""
    metrics: dict[str, Any] = {}

    def _nested_get(d: dict[str, Any], path: str) -> Any:
        for part in path.split("."):
            if isinstance(d, dict):
                d = d.get(part, {})
            else:
                return None
        return d if not isinstance(d, dict) or d else None

    # 收益指标
    metrics["profit_factor"] = _nested_get(result, "hf_standard.advanced_summary.profit_factor")
    metrics["total_pnl"] = _nested_get(result, "hf_standard.advanced_summary.total_pnl")
    metrics["calmar_ratio"] = _nested_get(result, "hf_standard.risk_metrics.calmar_ratio")
    metrics["max_drawdown_pct"] = _nested_get(result, "hf_standard.risk_metrics.max_drawdown_pct")
    metrics["trade_count"] = _nested_get(result, "hf_standard.trade_statistics.total_trades")

    # 尝试直接字段
    for key in ["profit_factor", "total_pnl", "calmar_ratio", "max_drawdown_pct",
                 "total_trades", "trade_count"]:
        if key in result and metrics.get(key) is None:
            metrics[key] = result[key]

    # 稳定性占位（实际需要窗口间比较，这里只存放窗口内的原始值）
    metrics["pf_stability"] = 1.0
    metrics["calmar_stability"] = 1.0

    return metrics


# ─── 主入口 ───────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="参数优化报告分析工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # analyze
    p_analyze = subparsers.add_parser("analyze", help="解析单次优化报告")
    p_analyze.add_argument("--report-json", required=True, help="parameter_optim_report.json 路径")
    p_analyze.add_argument("--top-n", type=int, default=10, help="TopN 数量 (默认 10)")
    p_analyze.add_argument("--output-table", help="TopN CSV 输出路径")
    p_analyze.add_argument("--refinement-suggestion", help="Refinement 建议 JSON 输出路径")

    # rollover-audit
    p_rollover = subparsers.add_parser("rollover-audit", help="合约换月审计")
    p_rollover.add_argument("--manifest", required=True, help="Parquet manifest JSONL 路径")
    p_rollover.add_argument("--symbol", required=True, help="品种代码，如 c, rb")
    p_rollover.add_argument("--calendar", help="合约到期日历 YAML 路径")
    p_rollover.add_argument("--window-start", required=True, help="Rolling 总区间起点 YYYY-MM-DD")
    p_rollover.add_argument("--window-end", required=True, help="Rolling 总区间终点 YYYY-MM-DD")
    p_rollover.add_argument("--window-step", type=int, default=30, help="窗口步长（天）")
    p_rollover.add_argument("--test-length", type=int, default=60, help="Test 窗口长度（天）")
    p_rollover.add_argument("--output", help="审计结果 JSON 输出路径")

    # composite-score
    p_composite = subparsers.add_parser("composite-score", help="计算综合筛选得分")
    p_composite.add_argument("--rolling-results-dir", required=True, help="Rolling 结果根目录")
    p_composite.add_argument(
        "--weights",
        default="pf=0.30,calmar=0.25,pnl=0.20,stability=0.15,trade_quality=0.10",
        help="指标权重 key=weight 逗号分隔",
    )
    p_composite.add_argument("--exclude-windows-file", help="换月审计 JSON（用于排除污染窗口）")
    p_composite.add_argument("--output", help="综合得分 CSV 输出路径")

    args = parser.parse_args()

    if args.command == "analyze":
        cmd_analyze(args)
    elif args.command == "rollover-audit":
        cmd_rollover_audit(args)
    elif args.command == "composite-score":
        cmd_composite_score(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
