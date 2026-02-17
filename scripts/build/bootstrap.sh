#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-build}"
INSTALL_DEPS_MODE="auto"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --install-deps       Force install Ubuntu C++ dependencies via apt-get
  --skip-install-deps  Skip dependency installation (fail if prerequisites are missing)
  --build-dir PATH     CMake build directory (default: build)
  -h, --help           Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-deps)
      INSTALL_DEPS_MODE="always"
      shift
      ;;
    --skip-install-deps)
      INSTALL_DEPS_MODE="never"
      shift
      ;;
    --build-dir)
      BUILD_DIR="$2"
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

is_ubuntu() {
  [[ -f /etc/os-release ]] && grep -qi '^ID=ubuntu' /etc/os-release
}

collect_missing_core_commands() {
  local -a required_commands=(cmake ctest gcc g++ make)
  local -a missing=()
  local cmd
  for cmd in "${required_commands[@]}"; do
    if ! command -v "${cmd}" >/dev/null 2>&1; then
      missing+=("${cmd}")
    fi
  done
  printf '%s\n' "${missing[@]}"
}

install_ubuntu_deps() {
  sudo apt-get update
  sudo apt-get install -y build-essential cmake git pkg-config
}

install_ubuntu_deps_if_needed() {
  local -a missing=()
  mapfile -t missing < <(collect_missing_core_commands)
  local has_missing=0
  if [[ ${#missing[@]} -gt 0 ]]; then
    has_missing=1
  fi

  if [[ "${INSTALL_DEPS_MODE}" == "never" && ${has_missing} -eq 1 ]]; then
    echo "error: missing required commands: ${missing[*]}" >&2
    exit 2
  fi

  if [[ "${INSTALL_DEPS_MODE}" == "always" || (${INSTALL_DEPS_MODE} == "auto" && ${has_missing} -eq 1) ]]; then
    if ! is_ubuntu; then
      echo "error: automatic dependency installation is only supported on Ubuntu." >&2
      exit 2
    fi
    if ! command -v sudo >/dev/null 2>&1; then
      echo "error: dependency installation requires sudo." >&2
      exit 2
    fi
    install_ubuntu_deps
  fi
}

cd "${REPO_ROOT}"
install_ubuntu_deps_if_needed

cmake -S . -B "${BUILD_DIR}" -DQUANT_HFT_BUILD_TESTS=ON
cmake --build "${BUILD_DIR}" -j"$(nproc)"
ctest --test-dir "${BUILD_DIR}" --output-on-failure

bash scripts/build/dependency_audit.sh --build-dir "${BUILD_DIR}"

mkdir -p docs/results
"${BUILD_DIR}/backtest_benchmark_cli" --runs 5 --baseline_p95_ms 100 --result_json docs/results/backtest_benchmark_result.json
bash scripts/build/run_consistency_gates.sh --build-dir "${BUILD_DIR}" --results-dir docs/results
bash scripts/build/run_preprod_rehearsal_gate.sh --build-dir "${BUILD_DIR}" --results-dir docs/results
"${BUILD_DIR}/verify_contract_sync_cli"
"${BUILD_DIR}/verify_develop_requirements_cli"

bash scripts/build/repo_purity_check.sh --repo-root .

echo "bootstrap completed"
