#!/usr/bin/env bash
set -euo pipefail

required_arrow_version="12"
required_parquet_version="12"
check_only="false"

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --required-arrow-version V    Minimum arrow pkg-config version (default: 12)
  --required-parquet-version V  Minimum parquet pkg-config version (default: 12)
  --check-only BOOL             Only verify dependencies and versions, do not install (default: false)
  -h, --help                    Show help
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
    --check-only)
      check_only="$2"
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

arrow_ver="missing"
parquet_ver="missing"
version_source="unavailable"

refresh_versions() {
  arrow_ver="missing"
  parquet_ver="missing"
  version_source="unavailable"
  if command -v pkg-config >/dev/null 2>&1; then
    arrow_ver="$(pkg-config --modversion arrow 2>/dev/null || echo missing)"
    parquet_ver="$(pkg-config --modversion parquet 2>/dev/null || echo missing)"
    if [[ "${arrow_ver}" != "missing" && "${parquet_ver}" != "missing" ]]; then
      version_source="pkg-config"
      return
    fi
  fi

  if command -v python3 >/dev/null 2>&1; then
    local pyarrow_ver=""
    pyarrow_ver="$(python3 - <<'PY' 2>/dev/null || true
try:
    import pyarrow as pa
except Exception:
    print("missing")
else:
    print(pa.__version__)
PY
)"
    if [[ -n "${pyarrow_ver}" && "${pyarrow_ver}" != "missing" ]]; then
      arrow_ver="${pyarrow_ver}"
      parquet_ver="${pyarrow_ver}"
      version_source="pyarrow"
    fi
  fi
}

print_versions() {
  echo "version source: ${version_source}"
  echo "arrow version: ${arrow_ver}"
  echo "parquet version: ${parquet_ver}"
  echo "required: arrow>=${required_arrow_version}, parquet>=${required_parquet_version}"
}

have_required_pkgs() {
  refresh_versions
  if [[ "${arrow_ver}" == "missing" || "${parquet_ver}" == "missing" ]]; then
    return 1
  fi

  version_ge "${arrow_ver}" "${required_arrow_version}" && \
    version_ge "${parquet_ver}" "${required_parquet_version}"
}

if have_required_pkgs; then
  if is_true "${check_only}"; then
    echo "arrow/parquet dependency check passed"
    print_versions
  else
    echo "arrow/parquet dependencies already satisfied"
    print_versions
  fi
  exit 0
fi

if is_true "${check_only}"; then
  echo "error: arrow/parquet dependencies are missing or below required versions" >&2
  print_versions >&2
  echo "hint: run scripts/build/install_arrow_parquet_deps.sh without --check-only to install" >&2
  exit 1
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
  echo "error: arrow/parquet dependency installation incomplete" >&2
  print_versions >&2
  exit 1
fi

echo "arrow/parquet dependencies installed and verified"
print_versions
