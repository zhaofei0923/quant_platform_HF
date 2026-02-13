#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib
import json
import time
from collections import defaultdict
from pathlib import Path

_REQUIRED_COLUMNS = {
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
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert backtest CSV dataset into partitioned parquet artifacts"
    )
    parser.add_argument("--input", default="backtest_data")
    parser.add_argument("--output-dir", default="runtime/backtest/parquet")
    parser.add_argument(
        "--report-json",
        default="docs/results/backtest_parquet_conversion_report.json",
    )
    parser.add_argument("--compression", default="zstd")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.execute and args.dry_run:
        parser.error("use either --dry-run or --execute")
    if not args.execute and not args.dry_run:
        args.dry_run = True
    return args


def _resolve_inputs(input_path: Path) -> list[Path]:
    if input_path.is_file():
        if input_path.suffix.lower() != ".csv":
            raise ValueError(f"input file must be .csv: {input_path}")
        return [input_path]
    if not input_path.exists():
        raise ValueError(f"input path does not exist: {input_path}")
    files = sorted(path for path in input_path.rglob("*.csv") if path.is_file())
    if not files:
        raise ValueError(f"no csv files found under: {input_path}")
    return files


def _read_csv_rows(csv_path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"missing CSV header in {csv_path}")
        header = [name.strip() for name in reader.fieldnames if name is not None]
        missing = sorted(_REQUIRED_COLUMNS.difference(header))
        if missing:
            raise ValueError(f"missing required columns in {csv_path}: {', '.join(missing)}")
        rows = [
            {
                key: (value.strip() if isinstance(value, str) else "")
                for key, value in row.items()
                if key is not None
            }
            for row in reader
        ]
    return rows, header


def _write_parquet_partition(
    rows: list[dict[str, str]],
    output_path: Path,
    *,
    compression: str,
) -> str:
    try:
        pa = importlib.import_module("pyarrow")
        pq = importlib.import_module("pyarrow.parquet")
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("pyarrow is required for --execute mode") from exc

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, str(output_path), compression=compression)
    return "pyarrow"


def main() -> int:
    args = _build_parser()
    started_ns = time.time_ns()
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    input_root = Path(args.input)
    output_root = Path(args.output_dir) / run_id
    report_path = Path(args.report_json)

    success = True
    error = ""
    writer_backend = "none"
    inputs: list[str] = []
    output_files: list[str] = []
    total_rows = 0
    instruments: set[str] = set()
    trading_days: set[str] = set()
    source_reports: dict[str, dict[str, object]] = {}

    try:
        csv_files = _resolve_inputs(input_root)
        inputs = [str(path) for path in csv_files]

        for csv_file in csv_files:
            rows, _header = _read_csv_rows(csv_file)
            total_rows += len(rows)

            grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
            for row in rows:
                trading_day = row.get("TradingDay", "").strip() or "unknown"
                instrument = row.get("InstrumentID", "").strip() or "unknown"
                grouped[(trading_day, instrument)].append(row)
                instruments.add(instrument)
                trading_days.add(trading_day)

            partitions: list[dict[str, object]] = []
            for (trading_day, instrument), payload in sorted(grouped.items()):
                partition_rel = (
                    f"source={csv_file.stem}/trading_day={trading_day}/"
                    f"instrument_id={instrument}/part-0000.parquet"
                )
                partition_path = output_root / partition_rel
                if args.execute:
                    writer_backend = _write_parquet_partition(
                        payload,
                        partition_path,
                        compression=args.compression,
                    )
                    output_files.append(str(partition_path))
                partitions.append(
                    {
                        "trading_day": trading_day,
                        "instrument_id": instrument,
                        "row_count": len(payload),
                        "relative_path": partition_rel,
                    }
                )

            source_reports[str(csv_file)] = {
                "row_count": len(rows),
                "partition_count": len(partitions),
                "partitions": partitions,
            }

    except Exception as exc:  # pylint: disable=broad-except
        success = False
        error = str(exc)

    report = {
        "generated_ts_ns": time.time_ns(),
        "run_id": run_id,
        "duration_ms": (time.time_ns() - started_ns) / 1_000_000.0,
        "mode": "execute" if args.execute else "dry-run",
        "success": success,
        "error": error,
        "input": str(input_root),
        "inputs": inputs,
        "output_dir": str(output_root),
        "report_path": str(report_path),
        "writer_backend": writer_backend,
        "total_rows": total_rows,
        "output_file_count": len(output_files),
        "output_files": output_files,
        "instrument_universe": sorted(instruments),
        "trading_days": sorted(trading_days),
        "sources": source_reports,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(report_path))
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
