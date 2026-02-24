#!/usr/bin/env bash
set -euo pipefail

build_dir="build-gcc"
csv_path="backtest_data"
source_symbol=""
output_root="backtest_data/parquet_v2"
results_dir="docs/results"

convert_report=""
validate_report=""
fingerprint_file=""
backtest_report=""
backtest_markdown=""

max_ticks="20000"
batch_rows="500000"
memory_budget_mb="1024"
row_group_mb="128"
compression="snappy"

skip_build="false"
clean_output="true"
generate_sample_if_missing="true"
require_match_existing="true"
existing_compare_diff=""
expected_fingerprint_file=""
fingerprint_compare_diff=""
fingerprint_scope="parquet"
default_expected_fingerprint_file="docs/ops/backtest_data_parquet_v2_expected.sha256"

usage() {
  cat <<'USAGE'
Usage: scripts/build/run_reproducible_parquet_convert_validate.sh [options]

One-click reproducible flow:
1) Build C++ CLIs with Arrow/Parquet enabled
2) Convert CSV -> Parquet with strict Arrow writer requirement (single file or directory batch)
3) Validate dataset structure and metadata consistency
4) Run strict parquet replay smoke read
5) Emit deterministic file checksum fingerprint for cross-machine comparison

Options:
  --build-dir PATH                   Build directory (default: build-gcc)
  --csv-path PATH                    Input CSV file or directory; directory mode excludes output_root and *.ticks.csv (default: backtest_data)
  --source SYMBOL                    Optional source symbol prefix filter (default: empty)
  --output-root PATH                 Parquet output root (default: backtest_data/parquet_v2)
  --results-dir PATH                 Reports directory (default: docs/results)
  --convert-report PATH              Conversion JSON output path
  --validate-report PATH             Validation JSON output path
  --fingerprint-file PATH            Deterministic sha256 fingerprint output path
  --backtest-report PATH             Backtest parquet smoke JSON output path
  --backtest-markdown PATH           Backtest parquet smoke markdown output path
  --max-ticks N                      Max ticks for strict parquet smoke read (default: 20000)
  --batch-rows N                     csv_to_parquet_cli batch_rows (default: 500000)
  --memory-budget-mb N               csv_to_parquet_cli memory_budget_mb (default: 1024)
  --row-group-mb N                   csv_to_parquet_cli row_group_mb (default: 128)
  --compression CODEC                csv_to_parquet_cli compression (default: snappy)
  --skip-build BOOL                  Skip cmake configure/build (default: false)
  --clean-output BOOL                Remove output root before conversion (default: true)
  --generate-sample-if-missing BOOL  Generate sample only when --csv-path points to missing *.csv file (default: true)
  --require-match-existing BOOL      Require converted output to match existing output_root snapshot (default: true)
  --existing-compare-diff PATH       Diff output path when existing snapshot comparison fails
  --expected-fingerprint-file PATH   Compare generated fingerprint against baseline fingerprint file (default dataset auto-loads docs/ops/backtest_data_parquet_v2_expected.sha256 when present)
  --fingerprint-compare-diff PATH    Diff output path when baseline fingerprint compare fails
  --fingerprint-scope SCOPE          Fingerprint scope: parquet|all (default: parquet)
  -h, --help                         Show help
USAGE
}

to_lower() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

is_true() {
  case "$(to_lower "$1")" in
    true|1|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

require_positive_int() {
  local value="$1"
  local name="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || [[ "${value}" == "0" ]]; then
    echo "error: ${name} must be a positive integer, got: ${value}" >&2
    exit 2
  fi
}

snapshot_tree_sha256() {
  local root="$1"
  local out_file="$2"
  local scope="$3"
  python3 - "${root}" "${out_file}" "${scope}" <<'PY'
import hashlib
import sys
from pathlib import Path

root = Path(sys.argv[1])
out_file = Path(sys.argv[2])
scope = sys.argv[3]

if scope not in {"parquet", "all"}:
    raise SystemExit(f"invalid fingerprint scope: {scope}")


def include_file(path: Path) -> bool:
    if scope == "all":
        return True
    return path.suffix == ".parquet"


lines: list[str] = []
if root.exists():
    for path in sorted(p for p in root.rglob("*") if p.is_file() and include_file(p)):
        rel = path.relative_to(root).as_posix()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {rel}")
out_file.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
PY
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      build_dir="$2"
      shift 2
      ;;
    --csv-path)
      csv_path="$2"
      shift 2
      ;;
    --source)
      source_symbol="$2"
      shift 2
      ;;
    --output-root)
      output_root="$2"
      shift 2
      ;;
    --results-dir)
      results_dir="$2"
      shift 2
      ;;
    --convert-report)
      convert_report="$2"
      shift 2
      ;;
    --validate-report)
      validate_report="$2"
      shift 2
      ;;
    --fingerprint-file)
      fingerprint_file="$2"
      shift 2
      ;;
    --backtest-report)
      backtest_report="$2"
      shift 2
      ;;
    --backtest-markdown)
      backtest_markdown="$2"
      shift 2
      ;;
    --max-ticks)
      max_ticks="$2"
      shift 2
      ;;
    --batch-rows)
      batch_rows="$2"
      shift 2
      ;;
    --memory-budget-mb)
      memory_budget_mb="$2"
      shift 2
      ;;
    --row-group-mb)
      row_group_mb="$2"
      shift 2
      ;;
    --compression)
      compression="$2"
      shift 2
      ;;
    --skip-build)
      skip_build="$2"
      shift 2
      ;;
    --clean-output)
      clean_output="$2"
      shift 2
      ;;
    --generate-sample-if-missing)
      generate_sample_if_missing="$2"
      shift 2
      ;;
    --require-match-existing)
      require_match_existing="$2"
      shift 2
      ;;
    --existing-compare-diff)
      existing_compare_diff="$2"
      shift 2
      ;;
    --expected-fingerprint-file)
      expected_fingerprint_file="$2"
      shift 2
      ;;
    --fingerprint-compare-diff)
      fingerprint_compare_diff="$2"
      shift 2
      ;;
    --fingerprint-scope)
      fingerprint_scope="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

require_positive_int "${max_ticks}" "max_ticks"
require_positive_int "${batch_rows}" "batch_rows"
require_positive_int "${memory_budget_mb}" "memory_budget_mb"
require_positive_int "${row_group_mb}" "row_group_mb"
fingerprint_scope="$(to_lower "${fingerprint_scope}")"
if [[ "${fingerprint_scope}" != "parquet" && "${fingerprint_scope}" != "all" ]]; then
  echo "error: fingerprint_scope must be one of: parquet, all (got: ${fingerprint_scope})" >&2
  exit 2
fi

mkdir -p "${results_dir}"

if [[ -z "${convert_report}" ]]; then
  convert_report="${results_dir}/parquet_repro_convert.json"
fi
if [[ -z "${validate_report}" ]]; then
  validate_report="${results_dir}/parquet_repro_validate.json"
fi
if [[ -z "${fingerprint_file}" ]]; then
  fingerprint_file="${results_dir}/parquet_repro_fingerprint.sha256"
fi
if [[ -z "${backtest_report}" ]]; then
  backtest_report="${results_dir}/parquet_repro_backtest_smoke.json"
fi
if [[ -z "${backtest_markdown}" ]]; then
  backtest_markdown="${results_dir}/parquet_repro_backtest_smoke.md"
fi
if [[ -z "${existing_compare_diff}" ]]; then
  existing_compare_diff="${results_dir}/parquet_repro_existing_compare.diff"
fi
if [[ -z "${fingerprint_compare_diff}" ]]; then
  fingerprint_compare_diff="${results_dir}/parquet_repro_fingerprint_compare.diff"
fi
if [[ -z "${expected_fingerprint_file}" && "${output_root}" == "backtest_data/parquet_v2" && -f "${default_expected_fingerprint_file}" ]]; then
  expected_fingerprint_file="${default_expected_fingerprint_file}"
fi

declare -a csv_inputs=()
input_mode=""
input_path="${csv_path}"

if [[ -d "${csv_path}" ]]; then
  input_mode="directory"
  csv_path_abs="$(realpath "${csv_path}")"
  output_root_abs="$(realpath -m "${output_root}")"
  while IFS= read -r -d '' file_path; do
    csv_inputs+=("${file_path}")
  done < <(find "${csv_path_abs}" -path "${output_root_abs}" -prune -o -type f -name '*.csv' ! -name '*.ticks.csv' -print0 | sort -z)
  if [[ ${#csv_inputs[@]} -eq 0 ]]; then
    echo "error: no csv files found under directory: ${csv_path}" >&2
    exit 2
  fi
elif [[ -f "${csv_path}" ]]; then
  input_mode="single_file"
  csv_inputs+=("${csv_path}")
elif is_true "${generate_sample_if_missing}" && [[ "${csv_path}" == *.csv ]]; then
  input_mode="single_file"
  mkdir -p "$(dirname "${csv_path}")"
  cat >"${csv_path}" <<'CSV'
symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,ask_volume1,volume,turnover,open_interest
rb2405,SHFE,1704067200000000000,100.0,1,99.9,5,100.1,5,10,1000,100
rb2405,SHFE,1704067201000000000,101.0,1,100.9,5,101.1,5,11,1111,100
rb2405,SHFE,1704067202000000000,99.0,1,98.9,5,99.1,5,12,1188,100
rb2405,SHFE,1704067203000000000,102.0,1,101.9,5,102.1,5,13,1326,100
CSV
  csv_inputs+=("${csv_path}")
else
  echo "error: csv path does not exist (expect file or directory): ${csv_path}" >&2
  exit 2
fi

if ! is_true "${skip_build}"; then
  configure_cmd=(
    cmake
    -S .
    -B "${build_dir}"
    -DQUANT_HFT_BUILD_TESTS=ON
    -DQUANT_HFT_ENABLE_ARROW_PARQUET=ON
  )
  if [[ "${build_dir}" == "build-gcc" ]]; then
    configure_cmd+=(-DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++)
  fi

  "${configure_cmd[@]}"

  jobs="4"
  if command -v nproc >/dev/null 2>&1; then
    jobs="$(nproc)"
  fi
  cmake --build "${build_dir}" --target csv_to_parquet_cli backtest_cli -j"${jobs}"
fi

if [[ ! -f "${build_dir}/CMakeCache.txt" ]]; then
  echo "error: missing CMake cache: ${build_dir}/CMakeCache.txt" >&2
  exit 2
fi
if ! grep -q '^QUANT_HFT_ENABLE_ARROW_PARQUET:BOOL=ON$' "${build_dir}/CMakeCache.txt"; then
  echo "error: ${build_dir} is not configured with QUANT_HFT_ENABLE_ARROW_PARQUET=ON" >&2
  exit 2
fi

if [[ ! -x "${build_dir}/csv_to_parquet_cli" ]]; then
  echo "error: missing executable: ${build_dir}/csv_to_parquet_cli" >&2
  exit 2
fi
if [[ ! -x "${build_dir}/backtest_cli" ]]; then
  echo "error: missing executable: ${build_dir}/backtest_cli" >&2
  exit 2
fi

mkdir -p "$(dirname "${convert_report}")"
mkdir -p "$(dirname "${validate_report}")"
mkdir -p "$(dirname "${fingerprint_file}")"
mkdir -p "$(dirname "${backtest_report}")"
mkdir -p "$(dirname "${backtest_markdown}")"
mkdir -p "$(dirname "${existing_compare_diff}")"
rm -f "${existing_compare_diff}"
mkdir -p "$(dirname "${fingerprint_compare_diff}")"
rm -f "${fingerprint_compare_diff}"

tmp_run_dir="$(mktemp -d "${TMPDIR:-/tmp}/parquet-repro-runs.XXXXXX")"
staged_output_root="$(mktemp -d "${TMPDIR:-/tmp}/parquet-repro-stage.XXXXXX")"
existing_snapshot_file=""
staged_snapshot_file=""
has_existing_snapshot="false"

cleanup_tmp_artifacts() {
  rm -rf "${tmp_run_dir}" "${staged_output_root}"
  if [[ -n "${existing_snapshot_file}" ]]; then
    rm -f "${existing_snapshot_file}"
  fi
  if [[ -n "${staged_snapshot_file}" ]]; then
    rm -f "${staged_snapshot_file}"
  fi
}
trap cleanup_tmp_artifacts EXIT

if is_true "${require_match_existing}" && [[ -d "${output_root}" ]]; then
  if find "${output_root}" -type f -print -quit | grep -q .; then
    has_existing_snapshot="true"
    existing_snapshot_file="$(mktemp "${TMPDIR:-/tmp}/parquet-repro-existing.XXXXXX")"
    snapshot_tree_sha256 "${output_root}" "${existing_snapshot_file}" "${fingerprint_scope}"
  fi
fi

convert_index=0
for input_csv in "${csv_inputs[@]}"; do
  run_report="${tmp_run_dir}/run-${convert_index}.json"
  convert_cmd=(
    "${build_dir}/csv_to_parquet_cli"
    --input_csv "${input_csv}"
    --output_root "${staged_output_root}"
    --resume true
    --overwrite false
    --require_arrow_writer true
    --batch_rows "${batch_rows}"
    --memory_budget_mb "${memory_budget_mb}"
    --row_group_mb "${row_group_mb}"
    --compression "${compression}"
  )
  if [[ -n "${source_symbol}" ]]; then
    convert_cmd+=(--source "${source_symbol}")
  fi
  "${convert_cmd[@]}" >"${run_report}"
  convert_index=$((convert_index + 1))
done

python3 - "${tmp_run_dir}" "${convert_report}" "${input_mode}" "${input_path}" "${source_symbol}" <<'PY'
import json
import sys
from pathlib import Path

run_dir = Path(sys.argv[1])
convert_report = Path(sys.argv[2])
input_mode = sys.argv[3]
input_path = sys.argv[4]
source_filter = sys.argv[5]

run_files = sorted(run_dir.glob("run-*.json"))
if not run_files:
    print("no conversion run reports found", file=sys.stderr)
    sys.exit(1)

runs: list[dict] = []
errors: list[str] = []
partitions_converted = 0
partitions_skipped = 0
partitions_written_with_arrow = 0
files_with_converted_partitions = 0
files_with_skipped_partitions = 0

for run_file in run_files:
    try:
        payload = json.loads(run_file.read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"invalid conversion run report ({run_file}): {exc}")
        continue

    runs.append(payload)
    if payload.get("status") != "ok":
        errors.append(
            f"conversion run status is not ok for {payload.get('input_csv', run_file.name)}: "
            f"{payload.get('status')}"
        )

    converted = int(payload.get("partitions_converted", 0) or 0)
    skipped = int(payload.get("partitions_skipped", 0) or 0)
    arrow_written = int(payload.get("partitions_written_with_arrow", 0) or 0)
    partitions_converted += converted
    partitions_skipped += skipped
    partitions_written_with_arrow += arrow_written
    if converted > 0:
        files_with_converted_partitions += 1
    if skipped > 0:
        files_with_skipped_partitions += 1

summary = {
    "status": "ok" if not errors else "fail",
    "mode": "batch_csv_to_parquet",
    "input_mode": input_mode,
    "input_path": input_path,
    "source_filter": source_filter,
    "files_total": len(run_files),
    "files_with_converted_partitions": files_with_converted_partitions,
    "files_with_skipped_partitions": files_with_skipped_partitions,
    "partitions_converted": partitions_converted,
    "partitions_skipped": partitions_skipped,
    "partitions_written_with_arrow": partitions_written_with_arrow,
    "runs": runs,
    "errors": errors,
}
convert_report.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

if errors:
    for err in errors:
        print(f"[ERROR] {err}", file=sys.stderr)
    sys.exit(1)
PY

if is_true "${require_match_existing}" && is_true "${has_existing_snapshot}"; then
  staged_snapshot_file="$(mktemp "${TMPDIR:-/tmp}/parquet-repro-staged.XXXXXX")"
  snapshot_tree_sha256 "${staged_output_root}" "${staged_snapshot_file}" "${fingerprint_scope}"
  if ! diff -u "${existing_snapshot_file}" "${staged_snapshot_file}" >"${existing_compare_diff}"; then
    echo "error: converted output does not match existing dataset snapshot: ${output_root}" >&2
    echo "diff report: ${existing_compare_diff}" >&2
    exit 1
  fi
  rm -f "${existing_compare_diff}"
fi

if is_true "${clean_output}"; then
  rm -rf "${output_root}"
fi

if command -v rsync >/dev/null 2>&1; then
  mkdir -p "${output_root}"
  if is_true "${clean_output}"; then
    rsync -a --delete "${staged_output_root}/" "${output_root}/"
  else
    rsync -a "${staged_output_root}/" "${output_root}/"
  fi
else
  rm -rf "${output_root}"
  mkdir -p "$(dirname "${output_root}")"
  mv "${staged_output_root}" "${output_root}"
  staged_output_root=""
fi

run_id="parquet-repro-$(date -u +%Y%m%dT%H%M%SZ)"
"${build_dir}/backtest_cli" \
  --engine_mode parquet \
  --dataset_root "${output_root}" \
  --strict_parquet true \
  --max_ticks "${max_ticks}" \
  --deterministic_fills true \
  --run_id "${run_id}" \
  --output_json "${backtest_report}" \
  --output_md "${backtest_markdown}" >/dev/null

python3 - "${convert_report}" "${output_root}" "${backtest_report}" "${validate_report}" "${fingerprint_file}" "${fingerprint_scope}" <<'PY'
import hashlib
import json
import sys
from pathlib import Path


def load_json(path: Path, name: str, errors: list[str]) -> dict:
    if not path.is_file():
        errors.append(f"missing {name}: {path}")
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"invalid {name} json ({path}): {exc}")
        return {}


def parse_meta(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


convert_report = Path(sys.argv[1])
output_root = Path(sys.argv[2])
backtest_report = Path(sys.argv[3])
validate_report = Path(sys.argv[4])
fingerprint_file = Path(sys.argv[5])
fingerprint_scope = sys.argv[6]

if fingerprint_scope not in {"parquet", "all"}:
    errors = [f"invalid fingerprint_scope: {fingerprint_scope}"]
    validate_report.write_text(
        json.dumps({"status": "fail", "errors": errors}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    for err in errors:
        print(f"[ERROR] {err}", file=sys.stderr)
    sys.exit(1)

errors: list[str] = []
warnings: list[str] = []

convert = load_json(convert_report, "conversion report", errors)
backtest = load_json(backtest_report, "backtest report", errors)

partitions_converted = int(convert.get("partitions_converted", 0) or 0)
partitions_skipped = int(convert.get("partitions_skipped", 0) or 0)
partitions_written_with_arrow = int(convert.get("partitions_written_with_arrow", 0) or 0)
files_total = int(convert.get("files_total", 0) or 0)

if convert.get("status") != "ok":
    errors.append(f"conversion status is not ok: {convert.get('status')}")
if partitions_converted <= 0:
    errors.append(f"partitions_converted must be > 0, got: {partitions_converted}")
if partitions_written_with_arrow != partitions_converted:
    errors.append(
        "all partitions must be written by Arrow writer, "
        f"got written_with_arrow={partitions_written_with_arrow}, converted={partitions_converted}"
    )
if partitions_skipped > 0:
    warnings.append(
        f"partitions_skipped={partitions_skipped}; this may indicate duplicate CSV coverage"
    )
if files_total <= 0:
    warnings.append("conversion report does not expose files_total > 0")

if not output_root.is_dir():
    errors.append(f"output_root does not exist: {output_root}")

manifest_path = output_root / "_manifest" / "partitions.jsonl"
manifest_entries: list[dict] = []
if not manifest_path.is_file():
    errors.append(f"missing manifest file: {manifest_path}")
else:
    for line_no, line in enumerate(manifest_path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            manifest_entries.append(json.loads(stripped))
        except Exception as exc:
            errors.append(f"invalid manifest json at line {line_no}: {exc}")

if partitions_converted > 0 and len(manifest_entries) == 0:
    errors.append("manifest has no entries after conversion")
if partitions_converted + partitions_skipped > 0 and len(manifest_entries) > (
    partitions_converted + partitions_skipped
):
    errors.append(
        "manifest entries exceed converted+skipped partitions "
        f"({len(manifest_entries)} > {partitions_converted + partitions_skipped})"
    )

parquet_count = 0
sidecar_count = 0
meta_count = 0

for index, entry in enumerate(manifest_entries):
    rel_path = str(entry.get("file_path", "")).strip()
    if not rel_path:
        errors.append(f"manifest entry #{index} missing file_path")
        continue

    parquet_path = output_root / rel_path
    if not parquet_path.is_file():
        errors.append(f"missing parquet file from manifest: {parquet_path}")
        continue
    parquet_count += 1

    blob = parquet_path.read_bytes()
    if len(blob) < 8:
        errors.append(f"parquet file too small: {rel_path}")
    else:
        if blob[:4] != b"PAR1" or blob[-4:] != b"PAR1":
            errors.append(f"invalid parquet magic bytes: {rel_path}")
        # Stub fallback file starts with plain-text metadata immediately after PAR1.
        if b"schema_version=" in blob[:128]:
            errors.append(f"parquet file appears to be stub output (Arrow disabled or fallback): {rel_path}")

    sidecar_path = Path(str(parquet_path) + ".ticks.csv")
    if not sidecar_path.is_file():
        errors.append(f"missing sidecar file: {sidecar_path}")
    else:
        sidecar_count += 1

    meta_path = Path(str(parquet_path) + ".meta")
    if not meta_path.is_file():
        errors.append(f"missing meta file: {meta_path}")
        continue
    meta_count += 1

    meta = parse_meta(meta_path)
    for required_key in (
        "min_ts_ns",
        "max_ts_ns",
        "row_count",
        "schema_version",
        "source_csv_fingerprint",
        "source",
    ):
        if required_key not in meta:
            errors.append(f"meta missing key '{required_key}': {meta_path}")

    for numeric_key in ("min_ts_ns", "max_ts_ns", "row_count"):
        if numeric_key not in meta:
            continue
        try:
            meta_value = int(meta[numeric_key])
        except ValueError:
            errors.append(f"meta key '{numeric_key}' is not int: {meta_path}")
            continue
        entry_value = entry.get(numeric_key)
        if entry_value is None:
            warnings.append(f"manifest entry missing numeric key '{numeric_key}' for {rel_path}")
            continue
        try:
            manifest_value = int(entry_value)
        except Exception:
            errors.append(f"manifest key '{numeric_key}' is not int for {rel_path}")
            continue
        if meta_value != manifest_value:
            errors.append(
                f"meta/manifest mismatch for {numeric_key} at {rel_path}: "
                f"meta={meta_value}, manifest={manifest_value}"
            )

replay = backtest.get("replay", {})
ticks_read = int(replay.get("ticks_read", 0) or 0)
scan_rows = int(replay.get("scan_rows", 0) or 0)

if backtest.get("data_source") != "parquet":
    errors.append(f"backtest data_source is not parquet: {backtest.get('data_source')}")
if ticks_read <= 0:
    errors.append(f"backtest ticks_read must be > 0, got: {ticks_read}")
if scan_rows <= 0:
    errors.append(f"backtest scan_rows must be > 0, got: {scan_rows}")

checksum_lines: list[str] = []
for path in sorted(p for p in output_root.rglob("*") if p.is_file()):
    if fingerprint_scope == "parquet" and path.suffix != ".parquet":
        continue
    rel = path.relative_to(output_root).as_posix()
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    checksum_lines.append(f"{digest}  {rel}")
fingerprint_file.write_text("\n".join(checksum_lines) + ("\n" if checksum_lines else ""), encoding="utf-8")
fingerprint_digest = hashlib.sha256("\n".join(checksum_lines).encode("utf-8")).hexdigest()

status = "ok" if not errors else "fail"
summary = {
    "status": status,
    "convert_report_path": str(convert_report),
    "backtest_report_path": str(backtest_report),
    "manifest_path": str(manifest_path),
    "output_root": str(output_root),
    "files_total": files_total,
    "partitions_converted": partitions_converted,
    "partitions_skipped": partitions_skipped,
    "partitions_written_with_arrow": partitions_written_with_arrow,
    "manifest_entry_count": len(manifest_entries),
    "parquet_count": parquet_count,
    "meta_count": meta_count,
    "sidecar_count": sidecar_count,
    "backtest_ticks_read": ticks_read,
    "backtest_scan_rows": scan_rows,
    "fingerprint_file": str(fingerprint_file),
    "fingerprint_scope": fingerprint_scope,
    "fingerprint_digest_sha256": fingerprint_digest,
    "warnings": warnings,
    "errors": errors,
}
validate_report.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

if errors:
    for err in errors:
        print(f"[ERROR] {err}", file=sys.stderr)
    print(f"[ERROR] validation report: {validate_report}", file=sys.stderr)
    sys.exit(1)

print(json.dumps(summary, ensure_ascii=False))
PY

if [[ -n "${expected_fingerprint_file}" ]]; then
  if [[ ! -f "${expected_fingerprint_file}" ]]; then
    echo "error: expected fingerprint file not found: ${expected_fingerprint_file}" >&2
    exit 1
  fi
  if ! diff -u "${expected_fingerprint_file}" "${fingerprint_file}" >"${fingerprint_compare_diff}"; then
    echo "error: generated fingerprint does not match expected baseline" >&2
    echo "expected: ${expected_fingerprint_file}" >&2
    echo "actual:   ${fingerprint_file}" >&2
    echo "diff report: ${fingerprint_compare_diff}" >&2
    exit 1
  fi
  rm -f "${fingerprint_compare_diff}"
fi

echo "reproducible conversion + validation passed"
echo "output root: ${output_root}"
echo "conversion report: ${convert_report}"
echo "validation report: ${validate_report}"
echo "fingerprint file: ${fingerprint_file}"
echo "fingerprint scope: ${fingerprint_scope}"
echo "backtest report: ${backtest_report}"
