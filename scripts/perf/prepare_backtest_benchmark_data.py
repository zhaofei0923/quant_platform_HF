#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BenchmarkDatasetReport:
    input_csv: str
    output_csv: str
    max_ticks: int
    rows_written: int
    first_instrument: str
    last_instrument: str

    def to_dict(self) -> dict[str, object]:
        return {
            "input_csv": self.input_csv,
            "output_csv": self.output_csv,
            "max_ticks": self.max_ticks,
            "rows_written": self.rows_written,
            "first_instrument": self.first_instrument,
            "last_instrument": self.last_instrument,
        }


def prepare_benchmark_dataset(input_csv: Path, output_csv: Path, *, max_ticks: int) -> BenchmarkDatasetReport:
    if max_ticks <= 0:
        raise ValueError("max_ticks must be positive")

    rows_written = 0
    first_instrument = ""
    last_instrument = ""

    with input_csv.open("r", encoding="utf-8", newline="") as src:
        reader = csv.DictReader(src)
        if reader.fieldnames is None:
            raise ValueError(f"missing csv header: {input_csv}")

        output_csv.parent.mkdir(parents=True, exist_ok=True)
        with output_csv.open("w", encoding="utf-8", newline="") as dst:
            writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
            writer.writeheader()
            for index, row in enumerate(reader):
                if index >= max_ticks:
                    break
                writer.writerow(row)
                rows_written += 1
                instrument = str(row.get("InstrumentID", ""))
                if not first_instrument:
                    first_instrument = instrument
                last_instrument = instrument

    if rows_written == 0:
        raise RuntimeError("no rows copied into benchmark dataset")

    return BenchmarkDatasetReport(
        input_csv=str(input_csv),
        output_csv=str(output_csv),
        max_ticks=max_ticks,
        rows_written=rows_written,
        first_instrument=first_instrument,
        last_instrument=last_instrument,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare compact backtest benchmark dataset for CI")
    parser.add_argument("--input-csv", default="backtest_data/rb.csv")
    parser.add_argument("--output-csv", default="runtime/benchmarks/backtest/rb_ci_sample.csv")
    parser.add_argument("--max-ticks", type=int, default=1500)
    parser.add_argument("--report-json", default="docs/results/backtest_benchmark_data_report.json")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    started = time.perf_counter_ns()
    report = prepare_benchmark_dataset(
        Path(args.input_csv),
        Path(args.output_csv),
        max_ticks=int(args.max_ticks),
    )
    payload = {
        "generated_ts_ns": time.time_ns(),
        "duration_ms": (time.perf_counter_ns() - started) / 1_000_000.0,
        "success": True,
        **report.to_dict(),
    }
    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(report_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
