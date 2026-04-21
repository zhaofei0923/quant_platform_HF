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

default_config_path="configs/ops/parameter_optim.yaml"
config_path=""
backtest_cli_path=""
build_dir=""
dry_run=false
skip_build=false
show_steps=true

usage() {
  cat <<'EOF'
Usage: scripts/build/run_parameter_optim.sh [options]

Options:
  --config PATH              Parameter optimization YAML config
                             Default: configs/ops/parameter_optim.yaml
  --build-dir PATH           CMake build directory
                             Default: auto-detect build-gcc, then build
  --backtest-cli-path PATH   Explicit backtest_cli path
  --skip-build               Skip cmake build and run parameter_optim_cli directly
  --dry-run                  Print the build and run commands only
  -h, --help                 Show help

Notes:
  - parameter_optim_cli only accepts --config and --backtest-cli-path.
  - Report output paths are controlled by optimization.output_json,
    optimization.output_md, and best_params_yaml in the YAML config.
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
    --backtest-cli-path)
      require_option_value "$1" $#
      backtest_cli_path="$2"
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

if [[ -z "${config_path}" ]]; then
  config_path="${default_config_path}"
  if [[ ! -f "${config_path}" ]]; then
    echo "error: default optimization config does not exist: ${config_path}" >&2
    echo "error: pass --config PATH to a valid parameter optimization YAML" >&2
    exit 2
  fi
fi

require_file "${config_path}" "optimization config"

optim_cli_path="${build_dir}/parameter_optim_cli"
build_targets=("cmake" "--build" "${build_dir}" "--target" "parameter_optim_cli" "backtest_cli")

if [[ "${skip_build}" == false ]]; then
  if [[ ! -f "${build_dir}/CMakeCache.txt" ]]; then
    echo "error: missing configured build directory: ${build_dir}" >&2
    echo "error: run cmake -S . -B ${build_dir} before invoking this wrapper" >&2
    exit 2
  fi
  emit_step "构建 parameter_optim_cli 和 backtest_cli"
  run_cmd "${build_targets[@]}"
else
  require_executable "${optim_cli_path}" "parameter_optim_cli"
fi

if [[ -n "${backtest_cli_path}" ]]; then
  require_executable "${backtest_cli_path}" "backtest_cli"
elif [[ -x "${build_dir}/backtest_cli" ]]; then
  backtest_cli_path="${build_dir}/backtest_cli"
fi

if [[ "${dry_run}" == false ]]; then
  require_executable "${optim_cli_path}" "parameter_optim_cli"
fi

optim_cmd=("${optim_cli_path}" "--config" "${config_path}")
if [[ -n "${backtest_cli_path}" ]]; then
  optim_cmd+=("--backtest-cli-path" "${backtest_cli_path}")
fi

emit_step "运行 parameter_optim_cli"
run_cmd "${optim_cmd[@]}"

if [[ "${dry_run}" == true ]]; then
  echo "dry-run complete: optimization commands printed above."
else
  echo "parameter_optim_cli completed successfully."
fi

echo "Config: ${config_path}"
if [[ -n "${backtest_cli_path}" ]]; then
  echo "Backtest CLI: ${backtest_cli_path}"
else
  echo "Backtest CLI: auto-detect by parameter_optim_cli or YAML config"
fi
echo "Result locations are defined by optimization.output_json, optimization.output_md,"
echo "and best_params_yaml in the optimization config."