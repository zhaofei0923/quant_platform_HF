from __future__ import annotations

import csv
from pathlib import Path

from scripts.perf.run_backtest_benchmark import run_backtest_benchmark


def _write_csv(path: Path, rows: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
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
            ],
        )
        writer.writeheader()
        for idx in range(rows):
            writer.writerow(
                {
                    "TradingDay": "20240101",
                    "InstrumentID": "rb2405",
                    "UpdateTime": f"09:{idx // 60:02d}:{idx % 60:02d}",
                    "UpdateMillisec": "0",
                    "LastPrice": f"{3500 + (idx % 10)}",
                    "Volume": f"{100 + idx}",
                    "BidPrice1": f"{3499 + (idx % 10)}",
                    "BidVolume1": "5",
                    "AskPrice1": f"{3501 + (idx % 10)}",
                    "AskVolume1": "5",
                    "AveragePrice": f"{3500 + (idx % 10)}",
                    "Turnover": f"{350000 + idx}",
                    "OpenInterest": "1200000",
                }
            )


def test_run_backtest_benchmark_returns_summary(tmp_path: Path) -> None:
    csv_path = tmp_path / "bench.csv"
    _write_csv(csv_path, rows=120)

    summary = run_backtest_benchmark(csv_path, max_ticks=100, runs=2, warmup_runs=0)

    assert summary.runs == 2
    assert summary.max_ticks == 100
    assert summary.mean_ms >= 0.0
    assert summary.p95_ms >= 0.0
    assert summary.min_ticks_read > 0
    assert summary.max_ticks_read >= summary.min_ticks_read
