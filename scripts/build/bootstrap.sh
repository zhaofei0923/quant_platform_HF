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
  --install-deps    Force install Ubuntu dependencies via apt-get
  --skip-install-deps  Skip dependency installation (fail if prerequisites are missing)
  --build-dir PATH  CMake build directory (default: build)
  -h, --help        Show this help
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

ensure_command() {
  local cmd="$1"
  local install_hint="$2"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "error: missing command '${cmd}'. ${install_hint}" >&2
    exit 2
  fi
}

is_ubuntu() {
  [[ -f /etc/os-release ]] && grep -qi '^ID=ubuntu' /etc/os-release
}

collect_missing_core_commands() {
  local -a required_commands=(cmake ctest gcc g++ make python3)
  local -a missing=()
  local cmd
  for cmd in "${required_commands[@]}"; do
    if ! command -v "${cmd}" >/dev/null 2>&1; then
      missing+=("${cmd}")
    fi
  done
  printf '%s\n' "${missing[@]}"
}

ensure_python_venv_module() {
  python3 - <<'PY'
import venv
print(venv.__name__)
PY
}

install_ubuntu_deps() {
  sudo apt-get update
  sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    pkg-config \
    python3 \
    python3-dev \
    python3-pip \
    python3-venv
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
    echo "hint: run scripts/build/bootstrap.sh or pass --install-deps" >&2
    exit 2
  fi

  if [[ "${INSTALL_DEPS_MODE}" == "never" ]]; then
    return
  fi

  if [[ "${INSTALL_DEPS_MODE}" == "always" || ${has_missing} -eq 1 ]]; then
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

  if ! ensure_python_venv_module >/dev/null 2>&1; then
    if [[ "${INSTALL_DEPS_MODE}" == "never" ]]; then
      echo "error: python3-venv is missing." >&2
      exit 2
    fi
    if ! is_ubuntu; then
      echo "error: python3-venv is missing and automatic install is only supported on Ubuntu." >&2
      exit 2
    fi
    install_ubuntu_deps
  fi
}

cd "${REPO_ROOT}"

install_ubuntu_deps_if_needed

ensure_command cmake "Ubuntu 可执行: sudo apt-get install -y cmake"
ensure_command ctest "Ubuntu 可执行: sudo apt-get install -y cmake"
ensure_command python3 "Ubuntu 可执行: sudo apt-get install -y python3 python3-venv python3-pip"

cmake -S . -B "${BUILD_DIR}" -DQUANT_HFT_BUILD_TESTS=ON
cmake --build "${BUILD_DIR}" -j"$(nproc)"
ctest --test-dir "${BUILD_DIR}" --output-on-failure
python3 scripts/perf/run_hotpath_bench.py \
  --benchmark-bin "${BUILD_DIR}/hotpath_benchmark" \
  --baseline configs/perf/baseline.json \
  --result-json docs/results/hotpath_bench_result.json
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -e ".[dev]"
.venv/bin/ruff check python scripts
.venv/bin/black --check python scripts
.venv/bin/mypy python/quant_hft
.venv/bin/python scripts/build/verify_contract_sync.py
.venv/bin/python scripts/build/verify_develop_requirements.py
.venv/bin/pytest python/tests -q
