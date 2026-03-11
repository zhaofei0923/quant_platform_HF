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
quiet_backtest_stdout=true
progress_only=true
show_steps=true

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

sanitize_for_path() {
  local value="$1"
  local sanitized=""
  sanitized="$(printf '%s' "${value}" | sed -E 's/[^A-Za-z0-9._-]+/_/g; s/^_+//; s/_+$//')"
  if [[ -z "${sanitized}" ]]; then
    sanitized="run"
  fi
  printf '%s' "${sanitized}"
}

timestamp_now() {
  local timezone_mode="$1"
  case "${timezone_mode}" in
    utc)
      date -u +%Y%m%dT%H%M%SZ
      ;;
    local)
      date +%Y%m%dT%H%M%S
      ;;
    *)
      echo "error: timestamp_timezone must be one of: local, utc (got: ${timezone_mode})" >&2
      exit 2
      ;;
  esac
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

normalize_trace_output_format() {
  local value
  value="$(to_lower "$(trim "$1")")"
  case "${value}" in
    ""|csv)
      printf 'csv'
      ;;
    parquet|both)
      printf '%s' "${value}"
      ;;
    *)
      echo "error: trace_output_format must be one of: csv, parquet, both (got: $1)" >&2
      exit 2
      ;;
  esac
}

trace_primary_extension() {
  case "$1" in
    parquet) printf 'parquet' ;;
    *) printf 'csv' ;;
  esac
}

trace_stem_from_path() {
  local value="$1"
  local filename
  filename="$(basename "${value}")"
  if [[ "${filename}" == .* && "${filename#*.}" != *.* ]]; then
    printf '%s' "${filename}"
    return 0
  fi
  if [[ "${filename}" == *.* ]]; then
    printf '%s' "${filename%.*}"
  else
    printf '%s' "${filename}"
  fi
}

resolve_trace_output_path() {
  local raw_path="$1"
  local default_stem="$2"
  local format="$3"
  local stem="${default_stem}"
  if [[ -n "${raw_path}" ]]; then
    stem="$(trace_stem_from_path "${raw_path}")"
  fi
  local ext
  ext="$(trace_primary_extension "${format}")"
  printf '%s' "${run_dir}/${stem}.${ext}"
}

print_cmd() {
  local arg
  for arg in "$@"; do
    printf '%q ' "${arg}"
  done
  printf '\n'
}

run_cmd() {
  if [[ "${dry_run}" == true ]]; then
    print_cmd "$@"
    return 0
  fi
  if is_true "${progress_only}"; then
    local log_file=""
    local rc=0
    log_file="$(mktemp)"
    if "$@" >"${log_file}" 2>&1; then
      rm -f "${log_file}"
      return 0
    else
      rc=$?
    fi
    cat "${log_file}" >&2
    rm -f "${log_file}"
    return "${rc}"
  fi
  print_cmd "$@"
  "$@"
}

render_progress_bar() {
  local percent="$1"
  if (( percent < 0 )); then
    percent=0
  fi
  if (( percent > 100 )); then
    percent=100
  fi
  printf '\r%3d%%' "${percent}"
}

run_backtest_cmd() {
  local -a cmd=("$@")
  local quiet_stdout=false
  local use_progress=false
  local log_file=""
  local cmd_pid=0
  local rc=0
  local percent=0

  if is_true "${quiet_backtest_stdout}"; then
    quiet_stdout=true
  fi
  if is_true "${progress_only}"; then
    use_progress=true
  fi

  if [[ "${dry_run}" == true ]]; then
    print_cmd "${cmd[@]}"
    return 0
  fi

  if [[ "${use_progress}" != true ]]; then
    print_cmd "${cmd[@]}"
    if [[ "${quiet_stdout}" != true ]]; then
      "${cmd[@]}"
      return $?
    fi
    log_file="$(mktemp)"
    if "${cmd[@]}" >"${log_file}" 2>&1; then
      rm -f "${log_file}"
      return 0
    fi
    rc=$?
    cat "${log_file}" >&2
    rm -f "${log_file}"
    return "${rc}"
  fi

  log_file="$(mktemp)"
  "${cmd[@]}" >"${log_file}" 2>&1 &
  cmd_pid=$!

  render_progress_bar 0
  while kill -0 "${cmd_pid}" >/dev/null 2>&1; do
    if (( percent < 95 )); then
      ((percent += 1))
    fi
    render_progress_bar "${percent}"
    sleep 0.2
  done
  if wait "${cmd_pid}"; then
    rc=0
  else
    rc=$?
  fi

  if [[ "${rc}" -eq 0 ]]; then
    render_progress_bar 100
    printf '\n'
    if [[ "${quiet_stdout}" != true ]]; then
      cat "${log_file}"
    fi
    rm -f "${log_file}"
    return 0
  fi

  printf '\n' >&2
  cat "${log_file}" >&2
  rm -f "${log_file}"
  return "${rc}"
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

emit_step() {
  local message="$1"
  if is_true "${show_steps}"; then
    printf '[step] %s\n' "${message}" >&2
  fi
}

load_config "${config_path}"

build_dir="$(cfg_get "build_dir" "build-gcc")"
cmake_build_type="$(cfg_get "cmake_build_type" "Release")"
build_tests="$(cfg_get "build_tests" "false")"
enable_arrow_parquet="$(cfg_get "enable_arrow_parquet" "true")"
auto_install_arrow_parquet_deps="$(cfg_get "auto_install_arrow_parquet_deps" "true")"
output_root_dir_raw="$(cfg_get "output_root_dir" "docs/results/backtest_runs")"
timestamp_timezone="$(to_lower "$(cfg_get "timestamp_timezone" "local")")"

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
emit_indicator_trace="$(cfg_get "emit_indicator_trace" "false")"
trace_output_format="$(normalize_trace_output_format "$(cfg_get "trace_output_format" "csv")")"
indicator_trace_path_raw="$(cfg_get "indicator_trace_path" "")"
emit_sub_strategy_indicator_trace="$(cfg_get "emit_sub_strategy_indicator_trace" "false")"
sub_strategy_indicator_trace_path_raw="$(cfg_get "sub_strategy_indicator_trace_path" "")"
quiet_backtest_stdout="$(cfg_get "quiet_backtest_stdout" "true")"
progress_only="$(cfg_get "progress_only" "true")"
show_steps="$(cfg_get "show_steps" "true")"

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
output_root_dir="$(to_abs_path "${output_root_dir_raw}")"

run_timestamp="$(timestamp_now "${timestamp_timezone}")"
effective_run_id="${run_id}"
if [[ -z "${effective_run_id}" ]]; then
  effective_run_id="backtest-${run_timestamp}"
fi

safe_run_id="$(sanitize_for_path "${effective_run_id}")"
run_dir="${output_root_dir}/${safe_run_id}_${run_timestamp}"
output_json_basename="$(basename "${output_json_raw}")"
output_md_basename="$(basename "${output_md_raw}")"
output_json="${run_dir}/${output_json_basename}"
output_md="${run_dir}/${output_md_basename}"

if [[ -z "${export_csv_dir_raw}" ]]; then
  export_csv_dir="${run_dir}/csv"
else
  export_csv_dir="${run_dir}/$(basename "${export_csv_dir_raw}")"
fi

if is_true "${emit_indicator_trace}"; then
  indicator_trace_path="$(resolve_trace_output_path "${indicator_trace_path_raw}" "indicator_trace" "${trace_output_format}")"
else
  indicator_trace_path=""
fi

if is_true "${emit_sub_strategy_indicator_trace}"; then
  sub_strategy_indicator_trace_path="$(resolve_trace_output_path "${sub_strategy_indicator_trace_path_raw}" "sub_strategy_indicator_trace" "${trace_output_format}")"
else
  sub_strategy_indicator_trace_path=""
fi

if [[ ! -f "${strategy_main_config_path}" ]]; then
  echo "error: strategy_main_config_path does not exist: ${strategy_main_config_path}" >&2
  exit 2
fi
if [[ ! -d "${dataset_root}" ]]; then
  echo "error: dataset_root does not exist: ${dataset_root}" >&2
  exit 2
fi

mkdir -p "${run_dir}"
mkdir -p "$(dirname "${output_json}")"
mkdir -p "$(dirname "${output_md}")"
mkdir -p "${export_csv_dir}"
emit_step "配置加载完成，输出目录已准备: ${run_dir}"

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
    emit_step "执行 CMake 配置"
    if ! run_cmd "${configure_cmd[@]}"; then
      return 1
    fi
  else
    emit_step "复用已有 CMake 配置"
  fi
  emit_step "编译 backtest_cli"
  run_cmd "${build_cmd[@]}"
}

if [[ "${skip_build}" != true ]]; then
  emit_step "开始构建阶段"
  if ! build_once; then
    if is_true "${auto_install_arrow_parquet_deps}"; then
      emit_step "首次构建失败，尝试安装 Arrow/Parquet 依赖后重试"
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

if [[ "${skip_build}" != true && "${dry_run}" != true ]]; then
  current_arrow="$(to_upper "$(cmake_cache_value "QUANT_HFT_ENABLE_ARROW_PARQUET")")"
  if [[ "${current_arrow}" != "ON" ]]; then
    echo "error: parquet-only policy requires QUANT_HFT_ENABLE_ARROW_PARQUET=ON in ${build_dir}/CMakeCache.txt (got: ${current_arrow:-<unset>})" >&2
    echo "hint: rerun configure with -DQUANT_HFT_ENABLE_ARROW_PARQUET=ON and fix any Arrow/Parquet compile errors first" >&2
    exit 2
  fi
fi

backtest_cmd=(
  "${backtest_bin}"
  --engine_mode "${engine_mode}"
  --dataset_root "${dataset_root}"
  --strategy_main_config_path "${strategy_main_config_path}"
  --output_json "${output_json}"
  --output_md "${output_md}"
  --run_id "${effective_run_id}"
)

append_arg_if_set backtest_cmd --export_csv_dir "${export_csv_dir}"
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
append_arg_if_set backtest_cmd --trace_output_format "${trace_output_format}"
if is_true "${emit_indicator_trace}"; then
  backtest_cmd+=(--emit_indicator_trace true --indicator_trace_path "${indicator_trace_path}")
fi
if is_true "${emit_sub_strategy_indicator_trace}"; then
  backtest_cmd+=(--emit_sub_strategy_indicator_trace true \
    --sub_strategy_indicator_trace_path "${sub_strategy_indicator_trace_path}")
fi

if [[ "${dry_run}" == true ]] || ! is_true "${progress_only}"; then
  echo "=== backtest run summary ==="
  echo "config_path=${config_path}"
  echo "build_dir=${build_dir}"
  echo "output_root_dir=${output_root_dir}"
  echo "timestamp_timezone=${timestamp_timezone}"
  echo "run_id=${effective_run_id}"
  echo "run_dir=${run_dir}"
  echo "trace_output_format=${trace_output_format}"
  echo "dataset_root=${dataset_root}"
  echo "strategy_main_config_path=${strategy_main_config_path}"
  echo "output_json=${output_json}"
  echo "output_md=${output_md}"
  echo "export_csv_dir=${export_csv_dir}"
  if [[ -n "${indicator_trace_path}" ]]; then
    echo "indicator_trace_path=${indicator_trace_path}"
  fi
  if [[ -n "${sub_strategy_indicator_trace_path}" ]]; then
    echo "sub_strategy_indicator_trace_path=${sub_strategy_indicator_trace_path}"
  fi
  echo "dry_run=${dry_run}, skip_build=${skip_build}"
  echo "quiet_backtest_stdout=${quiet_backtest_stdout}, progress_only=${progress_only}"
fi

emit_step "启动回测主程序"
run_backtest_cmd "${backtest_cmd[@]}"
emit_step "回测完成，结果文件: ${output_json} / ${output_md}"
