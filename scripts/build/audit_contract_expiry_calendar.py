#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq


@dataclass(frozen=True)
class ManifestRow:
    instrument_id: str
    canonical_instrument_id: str
    trading_day: str
    file_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit contract expiry calendar coverage against parquet_v2 manifest data."
    )
    parser.add_argument(
        "--dataset-root",
        default="backtest_data/parquet_v2",
        help="Parquet dataset root. Default: backtest_data/parquet_v2",
    )
    parser.add_argument(
        "--calendar",
        default="configs/strategies/contract_expiry_calendar.yaml",
        help="Contract expiry calendar path.",
    )
    return parser.parse_args()


def load_calendar(path: Path) -> dict[str, str]:
    if not path.exists():
        raise RuntimeError(f"calendar file does not exist: {path}")

    calendar: dict[str, str] = {}
    in_contracts = False
    current_contract = ""
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        text = line.strip()
        if indent == 0:
            if text != "contracts:":
                raise RuntimeError(
                    f"{path}:{line_no}: expected top-level `contracts:` entry, got `{text}`"
                )
            in_contracts = True
            current_contract = ""
            continue
        if not in_contracts:
            raise RuntimeError(f"{path}:{line_no}: unexpected content before `contracts:`")
        if indent == 2 and text.endswith(":"):
            current_contract = text[:-1].strip().lower()
            continue
        if indent == 4 and text.startswith("last_trading_day:"):
            if not current_contract:
                raise RuntimeError(
                    f"{path}:{line_no}: last_trading_day must belong to a contract section"
                )
            last_trading_day = text.split(":", 1)[1].strip()
            if len(last_trading_day) != 8 or not last_trading_day.isdigit():
                raise RuntimeError(
                    f"{path}:{line_no}: last_trading_day must be an 8-digit trading day"
                )
            calendar[current_contract] = last_trading_day
            continue
        raise RuntimeError(f"{path}:{line_no}: unsupported calendar line: `{text}`")

    if not calendar:
        raise RuntimeError(f"calendar file is empty: {path}")
    return calendar


def load_manifest_rows(dataset_root: Path) -> list[ManifestRow]:
    manifest_path = dataset_root / "_manifest" / "partitions.jsonl"
    if not manifest_path.exists():
        raise RuntimeError(f"manifest file does not exist: {manifest_path}")

    rows: list[ManifestRow] = []
    for line_no, line in enumerate(manifest_path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        instrument_id = payload["instrument_id"]
        rows.append(
            ManifestRow(
                instrument_id=instrument_id,
                canonical_instrument_id=instrument_id.lower(),
                trading_day=payload["trading_day"],
                file_path=dataset_root / payload["file_path"],
            )
        )
    if not rows:
        raise RuntimeError(f"manifest is empty: {manifest_path}")
    return rows


def has_day_open_tick(parquet_paths: list[Path]) -> bool:
    for parquet_path in parquet_paths:
        if not parquet_path.exists():
            continue
        table = pq.read_table(parquet_path, columns=["ts_ns"])
        for value in table.column("ts_ns").to_pylist():
            if value is None:
                continue
            dt = datetime.fromtimestamp(int(value) / 1_000_000_000, tz=timezone.utc)
            if dt.hour == 9 and 0 <= dt.minute < 5:
                return True
    return False


def main() -> int:
    args = parse_args()
    dataset_root = Path(args.dataset_root)
    calendar_path = Path(args.calendar)

    calendar = load_calendar(calendar_path)
    manifest_rows = load_manifest_rows(dataset_root)

    raw_instruments = sorted({row.instrument_id for row in manifest_rows})
    canonical_instruments = sorted({row.canonical_instrument_id for row in manifest_rows})

    raw_variants_by_canonical: dict[str, set[str]] = defaultdict(set)
    max_trading_day_by_canonical: dict[str, str] = {}
    parquet_paths_by_contract_day: dict[tuple[str, str], list[Path]] = defaultdict(list)
    for row in manifest_rows:
        raw_variants_by_canonical[row.canonical_instrument_id].add(row.instrument_id)
        current_max = max_trading_day_by_canonical.get(row.canonical_instrument_id, "")
        if row.trading_day > current_max:
            max_trading_day_by_canonical[row.canonical_instrument_id] = row.trading_day
        parquet_paths_by_contract_day[(row.canonical_instrument_id, row.trading_day)].append(
            row.file_path
        )

    errors: list[str] = []
    covered_raw_instruments = 0
    for instrument_id in raw_instruments:
        if instrument_id.lower() in calendar:
            covered_raw_instruments += 1
        else:
            errors.append(f"missing calendar entry for raw instrument `{instrument_id}`")

    expired_in_dataset: list[str] = []
    not_yet_expired_in_dataset: list[str] = []
    for canonical in canonical_instruments:
        last_trading_day = calendar.get(canonical)
        if last_trading_day is None:
            continue
        dataset_last_day = max_trading_day_by_canonical[canonical]
        if last_trading_day <= dataset_last_day:
            expired_in_dataset.append(canonical)
            parquet_paths = parquet_paths_by_contract_day.get((canonical, last_trading_day), [])
            if not parquet_paths:
                errors.append(
                    f"missing manifest partition for `{canonical}` on last_trading_day "
                    f"{last_trading_day}"
                )
                continue
            if not has_day_open_tick(parquet_paths):
                errors.append(
                    f"missing [09:00, 09:05) tick for `{canonical}` on last_trading_day "
                    f"{last_trading_day}"
                )
        else:
            not_yet_expired_in_dataset.append(canonical)

    case_collapsed_canonicals = {
        canonical: sorted(variants)
        for canonical, variants in sorted(raw_variants_by_canonical.items())
        if len(variants) > 1
    }
    extra_calendar_contracts = sorted(set(calendar) - set(canonical_instruments))

    summary = {
        "calendar_path": str(calendar_path),
        "dataset_root": str(dataset_root),
        "raw_instrument_count": len(raw_instruments),
        "canonical_contract_count": len(canonical_instruments),
        "covered_raw_instruments": covered_raw_instruments,
        "covered_canonical_contracts": len(expired_in_dataset) + len(not_yet_expired_in_dataset),
        "expired_in_dataset": expired_in_dataset,
        "not_yet_expired_in_dataset": not_yet_expired_in_dataset,
        "case_collapsed_canonicals": case_collapsed_canonicals,
        "extra_calendar_contracts": extra_calendar_contracts,
    }

    print(
        f"covered_raw_instruments: {covered_raw_instruments}/{len(raw_instruments)}",
        file=sys.stdout,
    )
    print(
        "covered_canonical_contracts: "
        f"{summary['covered_canonical_contracts']}/{len(canonical_instruments)}",
        file=sys.stdout,
    )
    print(f"expired_in_dataset: {len(expired_in_dataset)}", file=sys.stdout)
    print(
        f"not_yet_expired_in_dataset: {len(not_yet_expired_in_dataset)}",
        file=sys.stdout,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2), file=sys.stdout)

    if errors:
        for item in errors:
            print(item, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
