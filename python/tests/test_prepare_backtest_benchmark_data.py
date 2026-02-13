from __future__ import annotations

import csv
from pathlib import Path

from scripts.perf.prepare_backtest_benchmark_data import prepare_benchmark_dataset


def _write_input_csv(path: Path) -> None:
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
        for idx in range(5):
            writer.writerow(
                {
                    "TradingDay": "20240101",
                    "InstrumentID": "rb2405",
                    "UpdateTime": f"09:00:0{idx}",
                    "UpdateMillisec": "0",
                    "LastPrice": f"{3500 + idx}",
                    "Volume": f"{100 + idx}",
                    "BidPrice1": f"{3499 + idx}",
                    "BidVolume1": "5",
                    "AskPrice1": f"{3501 + idx}",
                    "AskVolume1": "5",
                    "AveragePrice": f"{3500 + idx}",
                    "Turnover": f"{350000 + idx}",
                    "OpenInterest": "1200000",
                }
            )


def test_prepare_benchmark_dataset_limits_rows(tmp_path: Path) -> None:
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"
    _write_input_csv(input_csv)

    report = prepare_benchmark_dataset(input_csv, output_csv, max_ticks=3)

    assert report.rows_written == 3
    assert report.first_instrument == "rb2405"
    assert report.last_instrument == "rb2405"

    with output_csv.open("r", encoding="utf-8", newline="") as fp:
        rows = list(csv.DictReader(fp))
    assert len(rows) == 3
