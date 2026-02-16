#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import tempfile
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Remove duplicate header rows from CSV files under a directory"
    )
    parser.add_argument("--input", default="backtest_data")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.execute and args.dry_run:
        parser.error("use either --dry-run or --execute")
    if not args.execute and not args.dry_run:
        args.dry_run = True
    return args


def _resolve_files(input_path: Path) -> list[Path]:
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


def _normalize_row(row: list[str]) -> tuple[str, ...]:
    return tuple(value.strip() for value in row)


def _clean_file(csv_path: Path, *, execute: bool) -> tuple[int, int]:
    with csv_path.open("r", encoding="utf-8", newline="") as source_file:
        reader = csv.reader(source_file)
        header = next(reader, None)
        if header is None:
            return 0, 0

        normalized_header = _normalize_row(header)
        total_rows = 1
        removed_count = 0

        temp_path: Path | None = None
        writer = None
        if execute:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="",
                delete=False,
                dir=csv_path.parent,
                prefix=f"{csv_path.stem}.",
                suffix=".tmp",
            ) as temp_file:
                temp_path = Path(temp_file.name)
                writer = csv.writer(temp_file)
                writer.writerow(header)

                for row in reader:
                    total_rows += 1
                    if _normalize_row(row) == normalized_header:
                        removed_count += 1
                        continue
                    writer.writerow(row)
        else:
            for row in reader:
                total_rows += 1
                if _normalize_row(row) == normalized_header:
                    removed_count += 1

    if not execute:
        return removed_count, total_rows

    if temp_path is None:
        return removed_count, total_rows

    if removed_count > 0:
        temp_path.replace(csv_path)
    else:
        temp_path.unlink(missing_ok=True)

    return removed_count, total_rows


def main() -> int:
    args = _build_parser()
    input_path = Path(args.input)
    files = _resolve_files(input_path)

    total_removed = 0
    touched_files = 0

    for csv_path in files:
        removed_count, original_rows = _clean_file(csv_path, execute=args.execute)
        if removed_count > 0:
            touched_files += 1
            total_removed += removed_count
            print(
                f"{csv_path}: removed={removed_count}, original_rows={original_rows}, "
                f"mode={'execute' if args.execute else 'dry-run'}"
            )

    print(
        f"summary: files_scanned={len(files)}, files_with_duplicates={touched_files}, "
        f"headers_removed={total_removed}, mode={'execute' if args.execute else 'dry-run'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
