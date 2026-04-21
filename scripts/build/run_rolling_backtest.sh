#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="${QUANT_ROOT:-$(cd "${script_dir}/../.." && pwd)}"
cd "${repo_root}"

local_arrow_pkgconfig="${HOME}/.local/arrow-pkg/lib/pkgconfig"
if [[ -d "${local_arrow_pkgconfig}" ]]; then
  case ":${PKG_CONFIG_PATH:-}:" in
    *":${local_arrow_pkgconfig}:"*) ;;
    *)
      export PKG_CONFIG_PATH="${local_arrow_pkgconfig}${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
      ;;
  esac
fi

shopt -s nullglob
pyarrow_candidates=("${repo_root}"/.venv/lib/python*/site-packages/pyarrow)
shopt -u nullglob
if (( ${#pyarrow_candidates[@]} > 0 )); then
  pyarrow_lib_dir="${pyarrow_candidates[0]}"
  case ":${LD_LIBRARY_PATH:-}:" in
    *":${pyarrow_lib_dir}:"*) ;;
    *)
      export LD_LIBRARY_PATH="${pyarrow_lib_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
      ;;
  esac
fi

default_config_path="configs/ops/rolling_backtest.yaml"
config_path="${default_config_path}"
build_dir=""
dry_run=false
skip_build=false
show_steps=true

usage() {
  cat <<'EOF'
Usage: scripts/build/run_rolling_backtest.sh [options]

Options:
  --config PATH      Rolling backtest YAML config
                     Default: configs/ops/rolling_backtest.yaml
  --build-dir PATH   CMake build directory
                     Default: auto-detect build-gcc, then build
  --skip-build       Skip cmake build and run rolling_backtest_cli directly
  --dry-run          Print the build and run commands only
  -h, --help         Show help

Running with no arguments builds rolling_backtest_cli if needed and then runs
the checked-in rolling config.
EOF
}

emit_step() {
  local message="$1"
  if [[ "${show_steps}" == true ]]; then
    printf '[step] %s\n' "${message}" >&2
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
  if [[ "${dry_run}" == true ]]; then
    print_cmd "$@"
    return 0
  fi
  "$@"
}

require_option_value() {
  local option_name="$1"
  local remaining_args="$2"
  if (( remaining_args < 2 )); then
    echo "error: ${option_name} requires a value" >&2
    exit 2
  fi
}

require_file() {
  local file_path="$1"
  local label="$2"
  if [[ ! -f "${file_path}" ]]; then
    echo "error: missing ${label}: ${file_path}" >&2
    exit 2
  fi
}

require_executable() {
  local file_path="$1"
  local label="$2"
  if [[ ! -x "${file_path}" ]]; then
    echo "error: missing executable ${label}: ${file_path}" >&2
    exit 2
  fi
}

detect_default_build_dir() {
  local candidates=("build-gcc" "build")
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -d "${candidate}" ]]; then
      printf '%s' "${candidate}"
      return 0
    fi
  done
  printf 'build-gcc'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      require_option_value "$1" $#
      config_path="$2"
      shift 2
      ;;
    --build-dir)
      require_option_value "$1" $#
      build_dir="$2"
      shift 2
      ;;
    --skip-build)
      skip_build=true
      shift
      ;;
    --dry-run)
      dry_run=true
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

if [[ -z "${build_dir}" ]]; then
  build_dir="$(detect_default_build_dir)"
fi

require_file "${config_path}" "rolling config"

rolling_cli_path="${build_dir}/rolling_backtest_cli"
build_targets=("cmake" "--build" "${build_dir}" "--target" "rolling_backtest_cli")

if [[ "${skip_build}" == false ]]; then
  if [[ ! -f "${build_dir}/CMakeCache.txt" ]]; then
    echo "error: missing configured build directory: ${build_dir}" >&2
    echo "error: run cmake -S . -B ${build_dir} before invoking this wrapper" >&2
    exit 2
  fi
  emit_step "构建 rolling_backtest_cli"
  run_cmd "${build_targets[@]}"
else
  require_executable "${rolling_cli_path}" "rolling_backtest_cli"
fi

if [[ "${dry_run}" == false ]]; then
  require_executable "${rolling_cli_path}" "rolling_backtest_cli"
fi

rolling_cmd=("${rolling_cli_path}" "--config" "${config_path}")

emit_step "运行 rolling_backtest_cli"
run_cmd "${rolling_cmd[@]}"

if [[ "${dry_run}" == true ]]; then
  echo "dry-run complete: rolling commands printed above."
else
  echo "rolling_backtest_cli completed successfully."
fi

echo "Config: ${config_path}"
echo "Rolling CLI: ${rolling_cli_path}"
echo "Result locations are defined by output.report_json and output.report_md in the rolling config."