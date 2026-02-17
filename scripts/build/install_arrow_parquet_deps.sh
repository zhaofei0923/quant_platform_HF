#!/usr/bin/env bash
set -euo pipefail

required_arrow_version="12"
required_parquet_version="12"

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --required-arrow-version V    Minimum arrow pkg-config version (default: 12)
  --required-parquet-version V  Minimum parquet pkg-config version (default: 12)
  -h, --help                    Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --required-arrow-version)
      required_arrow_version="$2"
      shift 2
      ;;
    --required-parquet-version)
      required_parquet_version="$2"
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

version_ge() {
  local actual="$1"
  local required="$2"
  [[ -n "${actual}" ]] || return 1
  [[ "$(printf '%s\n%s\n' "${required}" "${actual}" | sort -V | tail -n1)" == "${actual}" ]]
}

have_required_pkgs() {
  if ! command -v pkg-config >/dev/null 2>&1; then
    return 1
  fi

  local arrow_ver=""
  local parquet_ver=""
  arrow_ver="$(pkg-config --modversion arrow 2>/dev/null || true)"
  parquet_ver="$(pkg-config --modversion parquet 2>/dev/null || true)"

  if [[ -z "${arrow_ver}" || -z "${parquet_ver}" ]]; then
    return 1
  fi

  version_ge "${arrow_ver}" "${required_arrow_version}" && \
    version_ge "${parquet_ver}" "${required_parquet_version}"
}

if have_required_pkgs; then
  echo "arrow/parquet dependencies already satisfied"
  exit 0
fi

if [[ ! -f /etc/os-release ]] || ! grep -qi '^ID=ubuntu' /etc/os-release; then
  echo "error: automatic Arrow/Parquet dependency installation is supported on Ubuntu only" >&2
  exit 2
fi

if ! command -v sudo >/dev/null 2>&1; then
  echo "error: sudo is required to install Arrow/Parquet dependencies" >&2
  exit 2
fi

sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  gnupg \
  pkg-config \
  libarrow-dev \
  libparquet-dev

if ! have_required_pkgs; then
  arrow_ver="$(pkg-config --modversion arrow 2>/dev/null || echo missing)"
  parquet_ver="$(pkg-config --modversion parquet 2>/dev/null || echo missing)"
  echo "error: arrow/parquet dependency installation incomplete" >&2
  echo "  arrow version: ${arrow_ver}" >&2
  echo "  parquet version: ${parquet_ver}" >&2
  echo "  required: arrow>=${required_arrow_version}, parquet>=${required_parquet_version}" >&2
  exit 1
fi

echo "arrow/parquet dependencies installed and verified"
