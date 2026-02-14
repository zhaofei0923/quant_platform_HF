#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
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
        "--no-run-id",
        action="store_true",
        help="Write parquet partitions directly under --output-dir without timestamp run_id",
    )
    parser.add_argument(
        "--report-json",
        default="docs/results/backtest_parquet_conversion_report.json",
    )
    parser.add_argument("--compression", default="zstd")
    parser.add_argument(
        "--filename-prefix-policy",
        choices=("error", "warn"),
        default="error",
        help="How to handle source file stem and InstrumentID prefix mismatch",
    )
    parser.add_argument(
        "--rollover-min-gap-ms",
        type=int,
        default=0,
        help="Minimum time gap (ms) between contract switches to count as rollover",
    )
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


def _extract_symbol_prefix(value: str) -> str:
    prefix = []
    for ch in value.strip():
        if ch.isalpha():
            prefix.append(ch)
            continue
        break
    return "".join(prefix).lower()


def _tick_ts_ms(trading_day: str, update_time: str, update_millisec: int) -> int:
    if not trading_day or not update_time:
        return -1
    dt = datetime.strptime(f"{trading_day} {update_time}", "%Y%m%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000) + max(0, update_millisec)


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
    output_root = Path(args.output_dir) if args.no_run_id else Path(args.output_dir) / run_id
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
    warnings: list[str] = []
    prefix_mismatch_samples: list[dict[str, object]] = []
    total_prefix_mismatch_count = 0
    total_rollover_events = 0

    try:
        csv_files = _resolve_inputs(input_root)
        inputs = [str(path) for path in csv_files]

        for csv_file in csv_files:
            rows, _header = _read_csv_rows(csv_file)
            total_rows += len(rows)
            source_symbol = csv_file.stem.strip().lower()

            source_prefix_mismatches: list[dict[str, object]] = []
            rollover_events: list[dict[str, object]] = []
            previous_instrument = ""
            previous_ts_ms = -1
            previous_price = 0.0

            grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
            for row_index, row in enumerate(rows, start=1):
                trading_day = row.get("TradingDay", "").strip() or "unknown"
                instrument = row.get("InstrumentID", "").strip() or "unknown"
                grouped[(trading_day, instrument)].append(row)
                instruments.add(instrument)
                trading_days.add(trading_day)

                instrument_prefix = _extract_symbol_prefix(instrument)
                if source_symbol and instrument_prefix and instrument_prefix != source_symbol:
                    mismatch = {
                        "source_file": str(csv_file),
                        "source_symbol": source_symbol,
                        "row_index": row_index,
                        "instrument_id": instrument,
                        "instrument_prefix": instrument_prefix,
                    }
                    source_prefix_mismatches.append(mismatch)

                current_ts_ms = _tick_ts_ms(
                    trading_day,
                    row.get("UpdateTime", "").strip(),
                    int(row.get("UpdateMillisec", "0") or "0"),
                )
                current_price = float(row.get("LastPrice", "0") or "0")
                if (
                    previous_instrument
                    and instrument != previous_instrument
                    and current_ts_ms >= 0
                    and previous_ts_ms >= 0
                ):
                    gap_ms = max(0, current_ts_ms - previous_ts_ms)
                    if gap_ms >= args.rollover_min_gap_ms:
                        price_jump = current_price - previous_price
                        price_jump_bps = (
                            (price_jump / previous_price) * 10_000.0
                            if previous_price > 0.0
                            else 0.0
                        )
                        rollover_events.append(
                            {
                                "from_instrument_id": previous_instrument,
                                "to_instrument_id": instrument,
                                "at_trading_day": trading_day,
                                "at_update_time": row.get("UpdateTime", "").strip(),
                                "at_update_millisec": int(row.get("UpdateMillisec", "0") or "0"),
                                "at_ts_ms": current_ts_ms,
                                "gap_ms": gap_ms,
                                "from_last_price": previous_price,
                                "to_last_price": current_price,
                                "price_jump": price_jump,
                                "price_jump_bps": price_jump_bps,
                                "candidate_slippage_basis": abs(price_jump),
                            }
                        )

                previous_instrument = instrument
                previous_ts_ms = current_ts_ms
                previous_price = current_price

            if source_prefix_mismatches:
                total_prefix_mismatch_count += len(source_prefix_mismatches)
                prefix_mismatch_samples.extend(source_prefix_mismatches[:20])
                msg = (
                    f"source={csv_file.stem} has {len(source_prefix_mismatches)} "
                    "InstrumentID prefix mismatches"
                )
                if args.filename_prefix_policy == "error":
                    raise ValueError(msg)
                warnings.append(msg)

            total_rollover_events += len(rollover_events)

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
                "source_symbol": source_symbol,
                "row_count": len(rows),
                "partition_count": len(partitions),
                "prefix_mismatch_count": len(source_prefix_mismatches),
                "rollover_min_gap_ms": args.rollover_min_gap_ms,
                "contract_switch_count": len(rollover_events),
                "contract_switch_events": rollover_events,
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
        "filename_prefix_policy": args.filename_prefix_policy,
        "rollover_min_gap_ms": args.rollover_min_gap_ms,
        "total_rows": total_rows,
        "prefix_mismatch_count": total_prefix_mismatch_count,
        "prefix_mismatch_samples": prefix_mismatch_samples,
        "rollover_event_count": total_rollover_events,
        "output_file_count": len(output_files),
        "output_files": output_files,
        "warnings": warnings,
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
