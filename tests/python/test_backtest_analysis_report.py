from __future__ import annotations

import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "analysis" / "backtest_analysis_report.py"

TRADE_COLUMNS = [
    "fill_seq",
    "trade_id",
    "order_id",
    "symbol",
    "exchange",
    "side",
    "offset",
    "volume",
    "price",
    "timestamp_ns",
    "signal_ts_ns",
    "trading_day",
    "action_day",
    "update_time",
    "timestamp_dt_local",
    "signal_dt_local",
    "commission",
    "timestamp_dt_utc",
    "slippage",
    "realized_pnl",
    "strategy_id",
    "signal_type",
    "regime_at_entry",
    "risk_budget_r",
]

ORDER_COLUMNS = [
    "order_seq",
    "order_id",
    "client_order_id",
    "symbol",
    "type",
    "side",
    "offset",
    "price",
    "volume",
    "status",
    "filled_volume",
    "avg_fill_price",
    "created_at_ns",
    "created_at_dt_utc",
    "last_update_ns",
    "last_update_dt_utc",
    "trading_day",
    "action_day",
    "update_time",
    "created_at_dt_local",
    "last_update_dt_local",
    "strategy_id",
    "cancel_reason",
]

DAILY_COLUMNS = [
    "date",
    "capital",
    "daily_return_pct",
    "cumulative_return_pct",
    "drawdown_pct",
    "position_value",
    "trades_count",
    "turnover",
    "market_regime",
]

TRACE_COLUMNS = [
    "instrument_id",
    "ts_ns",
    "dt_utc",
    "trading_day",
    "action_day",
    "timeframe_minutes",
    "strategy_id",
    "strategy_type",
    "bar_open",
    "bar_high",
    "bar_low",
    "bar_close",
    "bar_volume",
    "analysis_bar_open",
    "analysis_bar_high",
    "analysis_bar_low",
    "analysis_bar_close",
    "analysis_price_offset",
    "kama",
    "atr",
    "adx",
    "er",
    "stop_loss_price",
    "take_profit_price",
    "market_regime",
]


def load_script_module():
    spec = importlib.util.spec_from_file_location("backtest_analysis_report", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load script module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def make_trade_row(
    fill_seq: int,
    trade_id: str,
    side: str,
    offset: str,
    *,
    price: str,
    timestamp_ns: str,
    signal_ts_ns: str,
    trading_day: str,
    action_day: str,
    timestamp_dt_local: str,
    signal_dt_local: str,
    commission: str,
    realized_pnl: str,
    signal_type: str,
    regime_at_entry: str,
    risk_budget_r: str,
) -> dict[str, str]:
    return {
        "fill_seq": str(fill_seq),
        "trade_id": trade_id,
        "order_id": f"order-{fill_seq}",
        "symbol": "c2405",
        "exchange": "DCE",
        "side": side,
        "offset": offset,
        "volume": "1",
        "price": price,
        "timestamp_ns": timestamp_ns,
        "signal_ts_ns": signal_ts_ns,
        "trading_day": trading_day,
        "action_day": action_day,
        "update_time": timestamp_dt_local.split(" ", maxsplit=1)[1],
        "timestamp_dt_local": timestamp_dt_local,
        "signal_dt_local": signal_dt_local,
        "commission": commission,
        "timestamp_dt_utc": timestamp_dt_local,
        "slippage": "0.00000000",
        "realized_pnl": realized_pnl,
        "strategy_id": "kama_trend_1",
        "signal_type": signal_type,
        "regime_at_entry": regime_at_entry,
        "risk_budget_r": risk_budget_r,
    }


def make_order_row(order_seq: int, order_id: str, ts_ns: str, trading_day: str) -> dict[str, str]:
    dt_text = "2024-01-02 14:00:00" if order_seq == 1 else "2024-01-03 14:00:00"
    return {
        "order_seq": str(order_seq),
        "order_id": order_id,
        "client_order_id": f"client-{order_id}",
        "symbol": "c2405",
        "type": "Market",
        "side": "Buy",
        "offset": "Open",
        "price": "100.00000000",
        "volume": "1",
        "status": "Filled",
        "filled_volume": "1",
        "avg_fill_price": "100.00000000",
        "created_at_ns": ts_ns,
        "created_at_dt_utc": dt_text,
        "last_update_ns": ts_ns,
        "last_update_dt_utc": dt_text,
        "trading_day": trading_day,
        "action_day": trading_day,
        "update_time": dt_text.split(" ", maxsplit=1)[1],
        "created_at_dt_local": dt_text,
        "last_update_dt_local": dt_text,
        "strategy_id": "kama_trend_1",
        "cancel_reason": "",
    }


def make_daily_row(
    date: str,
    capital: str,
    daily_return_pct: str,
    cumulative_return_pct: str,
    drawdown_pct: str,
    position_value: str,
    trades_count: str,
    turnover: str,
    regime: str,
) -> dict[str, str]:
    return {
        "date": date,
        "capital": capital,
        "daily_return_pct": daily_return_pct,
        "cumulative_return_pct": cumulative_return_pct,
        "drawdown_pct": drawdown_pct,
        "position_value": position_value,
        "trades_count": trades_count,
        "turnover": turnover,
        "market_regime": regime,
    }


def make_trace_row() -> dict[str, str]:
    return {
        "instrument_id": "c2405",
        "ts_ns": "1704203990000000000",
        "dt_utc": "2024-01-02 13:59",
        "trading_day": "20240102",
        "action_day": "20240102",
        "timeframe_minutes": "5",
        "strategy_id": "kama_trend_1",
        "strategy_type": "KamaTrendStrategy",
        "bar_open": "100",
        "bar_high": "101",
        "bar_low": "99",
        "bar_close": "100",
        "bar_volume": "1000",
        "analysis_bar_open": "100",
        "analysis_bar_high": "101",
        "analysis_bar_low": "99",
        "analysis_bar_close": "100",
        "analysis_price_offset": "0",
        "kama": "100.0",
        "atr": "2.0",
        "adx": "25.0",
        "er": "0.5",
        "stop_loss_price": "96.0",
        "take_profit_price": "112.0",
        "market_regime": "kWeakTrend",
    }


def build_run_dir(root: Path, include_risk_budget: bool = True) -> Path:
    run_dir = root / "backtest-run"
    csv_dir = run_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)

    instrument_info_path = root / "instrument_info.json"
    instrument_info_path.write_text(
        json.dumps(
            {
                "C": {
                    "volume_multiple": 10,
                    "commission": {
                        "open_ratio_by_money": 0.0,
                        "open_ratio_by_volume": 0.2,
                        "close_ratio_by_money": 0.0,
                        "close_ratio_by_volume": 0.2,
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    expiry_path = root / "contract_expiry_calendar.yaml"
    expiry_path.write_text(
        "contracts:\n  c2405:\n    last_trading_day: 20240131\n",
        encoding="utf-8",
    )

    sub_config_path = root / "kama_trend_1.yaml"
    sub_config_path.write_text(
        "params:\n  id: kama_trend_1\n  stop_loss_mode: trailing_atr\n  stop_loss_atr_period: 14\n  stop_loss_atr_multiplier: 4.0\n",
        encoding="utf-8",
    )

    main_config_path = root / "main_backtest_strategy.yaml"
    main_config_path.write_text(
        "risk_management:\n  enabled: true\n  risk_per_trade_pct: 0.005\n  max_risk_per_trade: 2000.0\ncomposite:\n  sub_strategies:\n    - id: kama_trend_1\n      config_path: ./kama_trend_1.yaml\n",
        encoding="utf-8",
    )

    trace_path = run_dir / "my_sub_trace.csv"
    write_csv(trace_path, TRACE_COLUMNS, [make_trace_row()])

    trade_rows = [
        make_trade_row(
            1,
            "trade-1",
            "Buy",
            "Open",
            price="100.00000000",
            timestamp_ns="1704204000000000000",
            signal_ts_ns="1704203999000000000",
            trading_day="20240102",
            action_day="20240102",
            timestamp_dt_local="2024-01-02 14:00:00",
            signal_dt_local="2024-01-02 13:59:59",
            commission="2.00000000",
            realized_pnl="0.00000000",
            signal_type="kOpen",
            regime_at_entry="kWeakTrend",
            risk_budget_r="1000.00" if include_risk_budget else "",
        ),
        make_trade_row(
            2,
            "trade-2",
            "Sell",
            "Close",
            price="110.00000000",
            timestamp_ns="1704290400000000000",
            signal_ts_ns="1704290400000000000",
            trading_day="20240103",
            action_day="20240103",
            timestamp_dt_local="2024-01-03 14:00:00",
            signal_dt_local="2024-01-03 14:00:00",
            commission="2.00000000",
            realized_pnl="100.00000000",
            signal_type="kStopLoss",
            regime_at_entry="kUnknown",
            risk_budget_r="0.00" if include_risk_budget else "",
        ),
    ]
    trade_columns = list(TRADE_COLUMNS)
    if not include_risk_budget:
        trade_columns.remove("risk_budget_r")
        trade_rows = [{key: value for key, value in row.items() if key != "risk_budget_r"} for row in trade_rows]
    write_csv(csv_dir / "trades.csv", trade_columns, trade_rows)

    write_csv(
        csv_dir / "orders.csv",
        ORDER_COLUMNS,
        [
            make_order_row(1, "order-1", "1704204000000000000", "20240102"),
            make_order_row(2, "order-2", "1704290400000000000", "20240103"),
        ],
    )
    write_csv(
        csv_dir / "daily_equity.csv",
        DAILY_COLUMNS,
        [
            make_daily_row("20240102", "100000.00000000", "0.00000000", "0.00000000", "0.00000000", "1000.00000000", "1", "1000.00000000", "kWeakTrend"),
            make_daily_row("20240103", "100096.00000000", "0.09600000", "0.09600000", "0.00000000", "0.00000000", "1", "1100.00000000", "kStrongTrend"),
        ],
    )

    (run_dir / "backtest_auto.md").write_text(
        "# Backtest Replay Result\n\n## Replay Overview\n- Time Range (ns): `1704204000000000000:1704290400000000000`\n",
        encoding="utf-8",
    )

    payload = {
        "run_id": "unit-run-001",
        "initial_equity": 100000,
        "final_equity": 100096,
        "input_signature": "abc123",
        "data_signature": "def456",
        "spec": {
            "initial_equity": 100000,
            "strategy_main_config_path": str(main_config_path),
            "contract_expiry_calendar_path": str(expiry_path),
            "product_config_path": str(instrument_info_path),
        },
        "replay": {
            "ticks_read": 20,
            "bars_emitted": 2,
            "io_bytes": 1024,
        },
        "sub_strategy_indicator_trace": {
            "enabled": True,
            "path": str(trace_path),
            "rows": 1,
        },
        "deterministic": {
            "order_events_emitted": 4,
            "wal_records": 0,
            "instrument_pnl": {},
            "performance": {
                "initial_equity": 100000,
                "final_equity": 100096,
                "total_commission": 4,
                "total_realized_pnl": 100,
                "total_unrealized_pnl": 0,
                "total_pnl_after_cost": 96,
                "total_pnl": 100,
                "max_drawdown": 0,
            },
        },
        "hf_standard": {
            "execution_quality": {
                "limit_order_fill_rate": 0.5,
            },
            "risk_metrics": {
                "var_95": 0.1,
                "expected_shortfall_95": 0.2,
            },
        },
    }
    (run_dir / "backtest_auto.json").write_text(json.dumps(payload), encoding="utf-8")
    return run_dir


class BacktestAnalysisReportTest(unittest.TestCase):
    def test_generate_report_success(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_dir = build_run_dir(root, include_risk_budget=True)
            rc = module.main(
                [
                    "--run-dir",
                    str(run_dir),
                    "--report-date",
                    "20260420",
                    "--sample-trades",
                    "1",
                ]
            )
            self.assertEqual(rc, 0)
            report_path = run_dir / "kama_trend_1_回测分析报告_unit-run-001_20260420.md"
            self.assertTrue(report_path.exists())
            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn("## 一、回测摘要与总体评估", report_text)
            self.assertIn("## 二、策略逻辑与执行分析", report_text)
            self.assertIn("## 三、深度绩效与风险归因", report_text)
            self.assertIn("## 四、风险预算 (R) 与头寸管理分析", report_text)
            self.assertIn("## 五、架构与数据洞察", report_text)
            self.assertIn("## 六、综合结论与迭代建议", report_text)
            self.assertIn("动态头寸模拟", report_text)

    def test_missing_risk_budget_column_fails_fast(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_dir = build_run_dir(root, include_risk_budget=False)
            output_path = run_dir / "should_not_exist.md"
            rc = module.main(
                [
                    "--run-dir",
                    str(run_dir),
                    "--output",
                    str(output_path),
                    "--sample-trades",
                    "1",
                ]
            )
            self.assertEqual(rc, 2)
            self.assertFalse(output_path.exists())


if __name__ == "__main__":
    unittest.main()