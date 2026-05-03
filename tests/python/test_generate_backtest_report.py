"""Tests for generate_backtest_report.py."""

from __future__ import annotations

import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "analysis" / "generate_backtest_report.py"


def _load_module():
    """Load the script as a module, registering it in sys.modules first."""
    spec = importlib.util.spec_from_file_location("generate_backtest_report", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load script module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mod = _load_module()

ReportError = mod.ReportError
load_json = mod.load_json
parse_daily_rows = mod.parse_daily_rows
parse_trade_rows = mod.parse_trade_rows
parse_order_rows = mod.parse_order_rows
detect_multi_contract = mod.detect_multi_contract
discover_varieties = mod.discover_varieties
compute_sharpe = mod.compute_sharpe
compute_sortino = mod.compute_sortino
compute_calmar = mod.compute_calmar
compute_annualized_return = mod.compute_annualized_return
compute_annualized_volatility = mod.compute_annualized_volatility
compute_var_95 = mod.compute_var_95
compute_expected_shortfall_95 = mod.compute_expected_shortfall_95
compute_max_drawdown = mod.compute_max_drawdown
compute_drawdown_windows = mod.compute_drawdown_windows
compute_profit_factor = mod.compute_profit_factor
compute_win_rate = mod.compute_win_rate
compute_expectancy = mod.compute_expectancy
compute_r_buckets = mod.compute_r_buckets
compute_actual_fill_rate = mod.compute_actual_fill_rate
compute_regime_attribution = mod.compute_regime_attribution
build_star_rating = mod.build_star_rating
generate_report = mod.generate_report

# ── helpers ──────────────────────────────────────────────────────────────────

TRADE_COLUMNS = [
    "fill_seq", "trade_id", "symbol", "side", "offset", "volume", "price",
    "timestamp_ns", "timestamp_dt_local", "commission", "slippage",
    "realized_pnl", "strategy_id", "signal_type", "regime_at_entry",
    "risk_budget_r",
]

ORDER_COLUMNS = [
    "order_seq", "order_id", "status", "symbol", "side",
]

DAILY_COLUMNS = [
    "date", "capital", "daily_return_pct", "cumulative_return_pct",
    "drawdown_pct", "position_value", "trades_count", "turnover", "market_regime",
]


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _make_trade(fill_seq, symbol, side, offset, volume, price, realized_pnl,
                risk_budget_r, signal_type="kOpen", commission=10.0,
                timestamp_ns=1000000000000000000, regime="kStrongTrend"):
    return {
        "fill_seq": str(fill_seq),
        "trade_id": f"t{fill_seq}",
        "symbol": symbol,
        "side": side,
        "offset": offset,
        "volume": str(volume),
        "price": str(price),
        "timestamp_ns": str(timestamp_ns + fill_seq * 6_000_000_000_000),
        "timestamp_dt_local": "2024-01-15 09:30:00",
        "commission": str(commission),
        "slippage": "0.0",
        "realized_pnl": str(realized_pnl),
        "strategy_id": "test_strategy",
        "signal_type": signal_type,
        "regime_at_entry": regime,
        "risk_budget_r": str(risk_budget_r),
    }


def _make_order(order_seq, order_id, status="FILLED"):
    return {
        "order_seq": str(order_seq),
        "order_id": order_id,
        "status": status,
        "symbol": "rb2505",
        "side": "buy",
    }


def _make_daily(date, capital, daily_return, cumulative_return,
                drawdown=0.0, position_value=0.0, regime="kStrongTrend"):
    return {
        "date": date,
        "capital": str(capital),
        "daily_return_pct": str(daily_return),
        "cumulative_return_pct": str(cumulative_return),
        "drawdown_pct": str(drawdown),
        "position_value": str(position_value),
        "trades_count": "2",
        "turnover": "0.1",
        "market_regime": regime,
    }


def _build_run_dir(tmp: Path, multi: bool = True) -> Path:
    """Create a minimal synthetic backtest run directory."""
    run_dir = tmp / "test_run_20260503T120000"
    run_dir.mkdir(parents=True)

    # JSON
    payload = {
        "run_id": "test_run",
        "mode": "deterministic",
        "engine_mode": "parquet",
        "rollover_mode": "expiry_close",
        "initial_equity": 100000,
        "final_equity": 115000,
        "input_signature": "abcd1234",
        "data_signature": "efgh5678",
        "hf_standard": {
            "version": "2.0",
            "parameters": {"initial_capital": 100000},
            "advanced_summary": {
                "rolling_sharpe_3m_last": 1.8,
                "profit_factor": 1.55,
            },
            "execution_quality": {
                "limit_order_fill_rate": 0.5,
                "avg_wait_time_ms": 12.0,
                "cancel_rate": 0.05,
                "slippage_mean": -0.1,
                "slippage_std": 2.0,
            },
            "risk_metrics": {
                "var_95": 1.05,
                "expected_shortfall_95": 1.33,
                "ulcer_index": 1.8,
                "recovery_factor": 8.3,
            },
            "monte_carlo": {
                "simulations": 1000,
                "mean_final_capital": 118000,
                "ci_95_lower": 90000,
                "ci_95_upper": 150000,
                "prob_loss": 0.01,
                "max_drawdown_95": 18.5,
            },
        },
        "deterministic": {
            "performance": {
                "initial_equity": 100000,
                "final_equity": 115000,
                "total_commission": 3500,
                "total_pnl_after_cost": 15000,
            },
        },
        "replay": {
            "ticks_read": 5000000,
            "bars_emitted": 50000,
            "io_bytes": 150000000,
            "instrument_universe": ["rb2505"],
        },
        "spec": {"symbols": ["rb"]},
    }
    with (run_dir / "backtest_test_run.json").open("w") as fh:
        json.dump(payload, fh)

    # Daily equity CSV
    daily_rows = []
    for i in range(252):
        capital = 100000 + i * 60
        daily_rows.append(_make_daily(
            f"2024010{i % 3 + 1}{(i+1):02d}" if i < 10 else f"2024{i // 100 + 1:02d}{(i % 100):02d}",
            capital, 0.06, (capital / 100000 - 1) * 100,
        ))
    # Fix date formatting
    for i, row in enumerate(daily_rows):
        if i < 10:
            row["date"] = f"2024-01-{(i+1):02d}"
        elif i < 41:
            row["date"] = f"2024-02-{(i-30):02d}"
        elif i < 72:
            row["date"] = f"2024-03-{(i-60):02d}"
        else:
            row["date"] = f"2024-04-{(i-91):02d}"
    _write_csv(run_dir / "csv" / "daily_equity.csv", DAILY_COLUMNS, daily_rows)

    # Trade CSV (paired open/close)
    trades = []
    for i in range(50):
        trades.append(_make_trade(i * 2, "rb2505", "buy", "Open", 2, 3800 + i, 0, 2000 + i * 50,
                                  signal_type="kOpen", timestamp_ns=1700000000000000000))
        pnl = 500 + i * 50  # mostly wins
        trades.append(_make_trade(i * 2 + 1, "rb2505", "sell", "Close", 2, 3900 + i, pnl, 0,
                                  signal_type="kStopLoss", timestamp_ns=1700000000000000000 + (i + 1) * 3_600_000_000_000))
    _write_csv(run_dir / "csv" / "trades.csv", TRADE_COLUMNS, trades)

    # Order CSV
    orders = [_make_order(i, f"order_{i}") for i in range(100)]
    _write_csv(run_dir / "csv" / "orders.csv", ORDER_COLUMNS, orders)

    if multi:
        varieties = ["rb", "hc", "c"]
        payload["spec"]["symbols"] = varieties
        payload["replay"]["instrument_universe"] = ["rb2505", "hc2505", "c2505"]
        with (run_dir / "backtest_test_run.json").open("w") as fh:
            json.dump(payload, fh)
        for prod in varieties:
            vdir = run_dir / "csv" / "varieties" / prod / "backtest"
            vdir.mkdir(parents=True)
            vt = [_make_trade(i, f"{prod}2505", "buy", "Open", 2, 4000, 0, 2000, timestamp_ns=1700000000000000000 + i * 1000) for i in range(10)]
            vt += [_make_trade(i + 10, f"{prod}2505", "sell", "Close", 2, 4100, 300 + i * 20, 0, signal_type="kStopLoss", timestamp_ns=1700000000000000000 + (i + 11) * 1000) for i in range(10)]
            _write_csv(vdir / "trades.csv", TRADE_COLUMNS, vt)
            _write_csv(vdir / "orders.csv", ORDER_COLUMNS, orders[:20])
            _write_csv(vdir / "position_history.csv", ["timestamp_ns", "symbol", "net_position", "avg_price", "unrealized_pnl"], [])

    return run_dir


# ═══════════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestMetricComputation(unittest.TestCase):
    """Unit tests for individual metric functions."""

    def test_sharpe_positive_returns(self):
        # Alternating positive returns to create volatility
        rets = [0.5, 0.2] * 126
        s = compute_sharpe(rets, 0.02)
        self.assertGreater(s, 2.0)

    def test_sharpe_needs_two_points(self):
        self.assertEqual(compute_sharpe([0.5], 0.02), 0.0)

    def test_sortino(self):
        rets = [0.5, -0.3, 0.4, -0.1, 0.6] * 50
        s = compute_sortino(rets, 0.02)
        self.assertGreater(s, 0.0)

    def test_calmar(self):
        self.assertAlmostEqual(compute_calmar(20.0, 10.0), 2.0)

    def test_calmar_zero_drawdown(self):
        self.assertEqual(compute_calmar(15.0, 0.0), 0.0)

    def test_annualized_return(self):
        # 10% cumulative over 126 days → roughly 21% annualized
        r = compute_annualized_return(10.0, 126)
        self.assertGreater(r, 19.0)
        self.assertLess(r, 23.0)

    def test_annualized_volatility(self):
        rets = [1.0, -1.0] * 126
        vol = compute_annualized_volatility(rets)
        self.assertGreater(vol, 10.0)

    def test_var_95(self):
        rets = list(range(-100, 101))
        v = compute_var_95([float(x) for x in rets])
        self.assertGreater(v, 85.0)

    def test_expected_shortfall(self):
        rets = list(range(-100, 101))
        es = compute_expected_shortfall_95([float(x) for x in rets])
        self.assertGreater(es, 90.0)

    def test_max_drawdown(self):
        rows = [
            mod.DailyRow("2024-01-01", 100, 0, 0, 0, 0, 0, 0, "kFlat"),
            mod.DailyRow("2024-01-02", 90, -10, -10, 10, 0, 0, 0, "kFlat"),
            mod.DailyRow("2024-01-03", 95, 5.56, -5, 5, 0, 0, 0, "kFlat"),
            mod.DailyRow("2024-01-04", 110, 15.8, 10, 0, 0, 0, 0, "kFlat"),
        ]
        w = compute_max_drawdown(rows, 100)
        self.assertAlmostEqual(w.drawdown_pct, 10.0, places=1)

    def test_drawdown_windows_multiple(self):
        rows = [
            mod.DailyRow("2024-01-01", 100, 0, 0, 0, 0, 0, 0, "kFlat"),
            mod.DailyRow("2024-01-02", 95, -5, -5, 5, 0, 0, 0, "kFlat"),
            mod.DailyRow("2024-01-03", 100, 5.26, 0, 0, 0, 0, 0, "kFlat"),
            mod.DailyRow("2024-01-04", 105, 5, 5, 0, 0, 0, 0, "kFlat"),
            mod.DailyRow("2024-01-05", 98, -6.67, -2, 6.67, 0, 0, 0, "kFlat"),
        ]
        windows = compute_drawdown_windows(rows, 100, top_n=3)
        self.assertGreaterEqual(len(windows), 1)

    def test_profit_factor(self):
        self.assertAlmostEqual(compute_profit_factor([100, -50, 200, -30]), 300 / 80)

    def test_win_rate(self):
        self.assertAlmostEqual(compute_win_rate([10, -5, 20, -3, 15]), 60.0)

    def test_expectancy(self):
        vals = [1.0, -0.5, 2.0, -0.3, 1.5]
        e = compute_expectancy(vals)
        self.assertGreater(e, 0.5)

    def test_r_buckets(self):
        vals = [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5]
        buckets = compute_r_buckets(vals)
        self.assertEqual(len(buckets), 7)
        self.assertEqual(buckets[0][1], 1)  # < -2R
        self.assertEqual(buckets[6][1], 1)  # > +3R

    def test_actual_fill_rate(self):
        orders = [
            mod.OrderRow("o1", "ACCEPTED"),
            mod.OrderRow("o1", "FILLED"),
            mod.OrderRow("o2", "ACCEPTED"),
            mod.OrderRow("o2", "CANCELLED"),
        ]
        self.assertAlmostEqual(compute_actual_fill_rate(orders), 50.0)

    def test_regime_attribution(self):
        rows = [
            mod.DailyRow("2024-01-01", 101, 1, 1, 0, 0, 0, 0, "kTrend"),
            mod.DailyRow("2024-01-02", 99, -1.98, -1, 1.98, 0, 0, 0, "kRanging"),
            mod.DailyRow("2024-01-03", 102, 3.03, 2, 0, 0, 0, 0, "kTrend"),
        ]
        regimes = compute_regime_attribution(rows, 100)
        self.assertEqual(len(regimes), 2)

    def test_star_rating_high(self):
        stars, grade, _ = build_star_rating(2.5, 3.0, 2.0, 65.0, 8.0)
        self.assertGreaterEqual(stars, 4)

    def test_star_rating_low(self):
        stars, grade, _ = build_star_rating(0.3, 0.2, 0.8, 35.0, 30.0)
        self.assertLessEqual(stars, 2)


class TestDiscoveryAndParsing(unittest.TestCase):
    """Tests for file discovery, parsing, and detection."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.run_dir = _build_run_dir(Path(self.tmp.name), multi=True)

    def tearDown(self):
        self.tmp.cleanup()

    def test_find_backtest_json(self):
        path = mod.find_backtest_json(self.run_dir)
        self.assertTrue(path.exists())
        self.assertIn("backtest_", path.name)

    def test_detect_multi_contract(self):
        payload = load_json(self.run_dir / "backtest_test_run.json")
        self.assertTrue(detect_multi_contract(self.run_dir, payload))

    def test_discover_varieties(self):
        varieties = discover_varieties(self.run_dir)
        self.assertIn("rb", varieties)
        self.assertIn("hc", varieties)
        self.assertIn("c", varieties)

    def test_parse_daily_rows(self):
        raw, cols = mod.load_csv_rows(self.run_dir / "csv" / "daily_equity.csv")
        rows = parse_daily_rows(raw)
        self.assertGreater(len(rows), 0)
        self.assertIsInstance(rows[0].capital, float)

    def test_parse_trade_rows(self):
        raw, cols = mod.load_csv_rows(self.run_dir / "csv" / "trades.csv")
        rows = parse_trade_rows(raw)
        self.assertGreater(len(rows), 0)
        self.assertEqual(rows[0].offset, "Open")  # sorted by timestamp

    def test_parse_order_rows(self):
        raw, cols = mod.load_csv_rows(self.run_dir / "csv" / "orders.csv")
        rows = parse_order_rows(raw)
        self.assertGreater(len(rows), 0)
        self.assertEqual(rows[0].status, "FILLED")


class TestSingleContractDetection(unittest.TestCase):
    """Verify single-contract detection when varieties/ is absent."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.run_dir = _build_run_dir(Path(self.tmp.name), multi=False)

    def tearDown(self):
        self.tmp.cleanup()

    def test_detect_single_contract(self):
        payload = load_json(self.run_dir / "backtest_test_run.json")
        self.assertFalse(detect_multi_contract(self.run_dir, payload))

    def test_discover_varieties_empty(self):
        varieties = discover_varieties(self.run_dir)
        self.assertEqual(varieties, [])


class TestReportGeneration(unittest.TestCase):
    """End-to-end report generation tests."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.run_dir = _build_run_dir(Path(self.tmp.name), multi=True)

    def tearDown(self):
        self.tmp.cleanup()

    def test_generate_report_multi_contract(self):
        out_path, text = generate_report(run_dir=self.run_dir, risk_free_rate=0.02)
        self.assertTrue(out_path.exists())
        self.assertIn("执行摘要", text)
        self.assertIn("绩效概览", text)
        self.assertIn("风险分析", text)
        self.assertIn("交易层级统计", text)
        self.assertIn("市场状态适应性分析", text)
        self.assertIn("执行质量", text)
        self.assertIn("蒙特卡洛模拟", text)
        self.assertIn("多品种分解分析", text)
        self.assertIn("系统架构与数据完整性", text)
        self.assertIn("综合建议与下一步", text)
        self.assertIn("附录", text)

    def test_generate_report_single_contract(self):
        run_dir = _build_run_dir(Path(self.tmp.name) / "single", multi=False)
        out_path, text = generate_report(run_dir=run_dir, risk_free_rate=0.02)
        self.assertIn("单品种深度分析", text)
        self.assertIn("合约链", text)
        self.assertNotIn("多品种分解分析", text)

    def test_generate_report_missing_json(self):
        empty = Path(self.tmp.name) / "empty"
        empty.mkdir()
        with self.assertRaises(ReportError):
            generate_report(run_dir=empty)

    def test_report_output_path_custom(self):
        custom = Path(self.tmp.name) / "custom_report.md"
        out_path, _ = generate_report(run_dir=self.run_dir, output_path=custom)
        self.assertEqual(out_path, custom)
        self.assertTrue(custom.exists())


class TestCLIBasic(unittest.TestCase):
    """Basic CLI argument parsing."""

    def test_cli_help(self):
        with self.assertRaises(SystemExit) as ctx:
            mod.main(["--help"])
        self.assertEqual(ctx.exception.code, 0)

    def test_cli_missing_required(self):
        with self.assertRaises(SystemExit) as ctx:
            mod.main([])
        self.assertNotEqual(ctx.exception.code, 0)


if __name__ == "__main__":
    unittest.main()
