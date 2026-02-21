#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
cd "${repo_root}"

# Prefer user-local Arrow/Parquet pkg-config path when available.
local_arrow_pkgconfig="${HOME}/.local/arrow-pkg/lib/pkgconfig"
if [[ -d "${local_arrow_pkgconfig}" ]]; then
  case ":${PKG_CONFIG_PATH:-}:" in
    *":${local_arrow_pkgconfig}:"*) ;;
    *)
      export PKG_CONFIG_PATH="${local_arrow_pkgconfig}${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
      ;;
  esac
fi

config_path="configs/ops/backtest_run.yaml"
dry_run=false
skip_build=false

usage() {
  cat <<'EOF'
Usage: scripts/build/run_backtest_from_config.sh [options]

Options:
  --config PATH   Backtest run config file (default: configs/ops/backtest_run.yaml)
  --dry-run       Print commands only, do not execute
  --skip-build    Skip cmake configure/build and run backtest directly
  -h, --help      Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      config_path="$2"
      shift 2
      ;;
    --dry-run)
      dry_run=true
      shift
      ;;
    --skip-build)
      skip_build=true
      shift
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

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

to_lower() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

to_upper() {
  printf '%s' "$1" | tr '[:lower:]' '[:upper:]'
}

unquote() {
  local value="$1"
  if [[ ${#value} -ge 2 ]]; then
    if [[ "${value:0:1}" == "\"" && "${value: -1}" == "\"" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
      value="${value:1:${#value}-2}"
    fi
  fi
  printf '%s' "${value}"
}

is_true() {
  case "$(to_lower "$1")" in
    true|1|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

to_cmake_bool() {
  if is_true "$1"; then
    printf 'ON'
  else
    printf 'OFF'
  fi
}

print_cmd() {
  local arg
  for arg in "$@"; do
    printf '%q ' "${arg}"
  done
  printf '\n'
}

run_cmd() {
  print_cmd "$@"
  if [[ "${dry_run}" == true ]]; then
    return 0
  fi
  "$@"
}

cmake_cache_value() {
  local key="$1"
  local cache_file="${build_dir}/CMakeCache.txt"
  if [[ ! -f "${cache_file}" ]]; then
    printf ''
    return 0
  fi
  local line=""
  line="$(grep -E "^${key}(:[A-Z]+)?=" "${cache_file}" | head -n 1 || true)"
  if [[ -z "${line}" ]]; then
    printf ''
    return 0
  fi
  printf '%s' "${line#*=}"
}

to_abs_path() {
  local path="$1"
  if [[ -z "${path}" ]]; then
    printf ''
    return 0
  fi
  if [[ "${path}" == /* ]]; then
    printf '%s' "${path}"
  else
    printf '%s' "${repo_root}/${path}"
  fi
}

declare -A cfg=()

load_config() {
  local file_path="$1"
  if [[ ! -f "${file_path}" ]]; then
    echo "error: config file not found: ${file_path}" >&2
    exit 2
  fi

  local line=""
  local trimmed=""
  local key=""
  local value=""
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%$'\r'}"
    trimmed="$(trim "${line}")"
    if [[ -z "${trimmed}" || "${trimmed:0:1}" == "#" ]]; then
      continue
    fi

    if [[ "${trimmed}" =~ ^([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*:[[:space:]]*(.*)$ ]]; then
      key="${BASH_REMATCH[1]}"
      value="${BASH_REMATCH[2]}"
      if [[ "${value}" == *"#"* ]]; then
        if [[ "${value:0:1}" != "\"" && "${value:0:1}" != "'" ]]; then
          value="${value%%#*}"
        fi
      fi
      value="$(trim "${value}")"
      value="$(unquote "${value}")"
      cfg["${key}"]="${value}"
      continue
    fi

    echo "error: unsupported config line (top-level 'key: value' only): ${line}" >&2
    exit 2
  done < "${file_path}"
}

cfg_get() {
  local key="$1"
  local default_value="$2"
  if [[ -v cfg["${key}"] ]]; then
    printf '%s' "${cfg["${key}"]}"
  else
    printf '%s' "${default_value}"
  fi
}

append_arg_if_set() {
  local -n target_ref="$1"
  local flag="$2"
  local value="$3"
  if [[ -n "${value}" ]]; then
    target_ref+=("${flag}" "${value}")
  fi
}

load_config "${config_path}"

build_dir="$(cfg_get "build_dir" "build-gcc")"
cmake_build_type="$(cfg_get "cmake_build_type" "Release")"
build_tests="$(cfg_get "build_tests" "false")"
enable_arrow_parquet="$(cfg_get "enable_arrow_parquet" "true")"
auto_install_arrow_parquet_deps="$(cfg_get "auto_install_arrow_parquet_deps" "true")"

engine_mode="$(to_lower "$(cfg_get "engine_mode" "parquet")")"
dataset_root_raw="$(cfg_get "dataset_root" "")"
strategy_main_config_path_raw="$(cfg_get "strategy_main_config_path" "")"
output_json_raw="$(cfg_get "output_json" "")"
output_md_raw="$(cfg_get "output_md" "")"

export_csv_dir_raw="$(cfg_get "export_csv_dir" "")"
run_id="$(cfg_get "run_id" "")"
max_ticks="$(cfg_get "max_ticks" "")"
start_date="$(cfg_get "start_date" "")"
end_date="$(cfg_get "end_date" "")"
deterministic_fills="$(cfg_get "deterministic_fills" "true")"
strict_parquet="$(cfg_get "strict_parquet" "true")"
rollover_mode="$(cfg_get "rollover_mode" "strict")"
rollover_price_mode="$(cfg_get "rollover_price_mode" "bbo")"
rollover_slippage_bps="$(cfg_get "rollover_slippage_bps" "0")"
emit_trades="$(cfg_get "emit_trades" "true")"
emit_orders="$(cfg_get "emit_orders" "true")"
emit_position_history="$(cfg_get "emit_position_history" "false")"

if [[ -z "${engine_mode}" || -z "${dataset_root_raw}" || -z "${strategy_main_config_path_raw}" ||
      -z "${output_json_raw}" || -z "${output_md_raw}" ]]; then
  echo "error: required config keys: engine_mode, dataset_root, strategy_main_config_path, output_json, output_md" >&2
  exit 2
fi

if [[ "${engine_mode}" != "parquet" ]]; then
  echo "error: parquet-only policy: engine_mode must be parquet" >&2
  exit 2
fi

dataset_root="$(to_abs_path "${dataset_root_raw}")"
strategy_main_config_path="$(to_abs_path "${strategy_main_config_path_raw}")"
output_json="$(to_abs_path "${output_json_raw}")"
output_md="$(to_abs_path "${output_md_raw}")"
export_csv_dir="$(to_abs_path "${export_csv_dir_raw}")"

if [[ ! -f "${strategy_main_config_path}" ]]; then
  echo "error: strategy_main_config_path does not exist: ${strategy_main_config_path}" >&2
  exit 2
fi
if [[ ! -d "${dataset_root}" ]]; then
  echo "error: dataset_root does not exist: ${dataset_root}" >&2
  exit 2
fi

mkdir -p "$(dirname "${output_json}")"
mkdir -p "$(dirname "${output_md}")"
if [[ -n "${export_csv_dir}" ]]; then
  mkdir -p "${export_csv_dir}"
fi

jobs="4"
if command -v nproc >/dev/null 2>&1; then
  jobs="$(nproc)"
fi

configure_cmd=(
  cmake
  -S .
  -B "${build_dir}"
  "-DCMAKE_BUILD_TYPE=${cmake_build_type}"
  "-DQUANT_HFT_BUILD_TESTS=$(to_cmake_bool "${build_tests}")"
  "-DQUANT_HFT_ENABLE_ARROW_PARQUET=$(to_cmake_bool "${enable_arrow_parquet}")"
)
if [[ "${build_dir}" == "build-gcc" ]]; then
  configure_cmd+=(-DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++)
fi

build_cmd=(
  cmake
  --build "${build_dir}"
  --target backtest_cli
  -j"${jobs}"
)

backtest_bin="${build_dir}/backtest_cli"

build_once() {
  local cache_file="${build_dir}/CMakeCache.txt"
  local needs_configure=false
  if [[ ! -f "${cache_file}" ]]; then
    needs_configure=true
  else
    local desired_arrow
    desired_arrow="$(to_cmake_bool "${enable_arrow_parquet}")"
    local current_arrow
    current_arrow="$(to_upper "$(cmake_cache_value "QUANT_HFT_ENABLE_ARROW_PARQUET")")"
    if [[ -z "${current_arrow}" || "${current_arrow}" != "${desired_arrow}" ]]; then
      needs_configure=true
    fi

    if [[ "${build_dir}" == "build-gcc" ]]; then
      local current_cc
      local current_cxx
      current_cc="$(cmake_cache_value "CMAKE_C_COMPILER")"
      current_cxx="$(cmake_cache_value "CMAKE_CXX_COMPILER")"
      if [[ "${current_cc}" != "/usr/bin/gcc" || "${current_cxx}" != "/usr/bin/g++" ]]; then
        needs_configure=true
      fi
    fi
  fi

  if [[ "${needs_configure}" == true ]]; then
    if ! run_cmd "${configure_cmd[@]}"; then
      return 1
    fi
  fi
  run_cmd "${build_cmd[@]}"
}

if [[ "${skip_build}" != true ]]; then
  if ! build_once; then
    if is_true "${auto_install_arrow_parquet_deps}"; then
      echo "build failed, trying Arrow/Parquet dependency installation and one retry..." >&2
      if ! run_cmd bash scripts/build/install_arrow_parquet_deps.sh; then
        echo "error: failed to install Arrow/Parquet dependencies" >&2
        exit 1
      fi
      if ! run_cmd "${configure_cmd[@]}"; then
        echo "error: cmake configure failed after dependency installation" >&2
        exit 1
      fi
      if ! run_cmd "${build_cmd[@]}"; then
        echo "error: build failed after dependency installation retry" >&2
        exit 1
      fi
    else
      echo "error: build failed and auto_install_arrow_parquet_deps=false" >&2
      exit 1
    fi
  fi
fi

if [[ "${skip_build}" == true && "${dry_run}" != true && ! -x "${backtest_bin}" ]]; then
  echo "error: missing executable: ${backtest_bin} (use --skip-build only when binary already exists)" >&2
  exit 2
fi

if [[ "${skip_build}" != true && "${dry_run}" != true && ! -x "${backtest_bin}" ]]; then
  echo "error: build succeeded but executable not found: ${backtest_bin}" >&2
  exit 1
fi

backtest_cmd=(
  "${backtest_bin}"
  --engine_mode "${engine_mode}"
  --dataset_root "${dataset_root}"
  --strategy_main_config_path "${strategy_main_config_path}"
  --output_json "${output_json}"
  --output_md "${output_md}"
)

append_arg_if_set backtest_cmd --export_csv_dir "${export_csv_dir}"
append_arg_if_set backtest_cmd --run_id "${run_id}"
append_arg_if_set backtest_cmd --max_ticks "${max_ticks}"
append_arg_if_set backtest_cmd --start_date "${start_date}"
append_arg_if_set backtest_cmd --end_date "${end_date}"
append_arg_if_set backtest_cmd --deterministic_fills "${deterministic_fills}"
append_arg_if_set backtest_cmd --strict_parquet "${strict_parquet}"
append_arg_if_set backtest_cmd --rollover_mode "${rollover_mode}"
append_arg_if_set backtest_cmd --rollover_price_mode "${rollover_price_mode}"
append_arg_if_set backtest_cmd --rollover_slippage_bps "${rollover_slippage_bps}"
append_arg_if_set backtest_cmd --emit_trades "${emit_trades}"
append_arg_if_set backtest_cmd --emit_orders "${emit_orders}"
append_arg_if_set backtest_cmd --emit_position_history "${emit_position_history}"

echo "=== backtest run summary ==="
echo "config_path=${config_path}"
echo "build_dir=${build_dir}"
echo "dataset_root=${dataset_root}"
echo "strategy_main_config_path=${strategy_main_config_path}"
echo "output_json=${output_json}"
echo "output_md=${output_md}"
if [[ -n "${export_csv_dir}" ]]; then
  echo "export_csv_dir=${export_csv_dir}"
fi
echo "dry_run=${dry_run}, skip_build=${skip_build}"

run_cmd "${backtest_cmd[@]}"
