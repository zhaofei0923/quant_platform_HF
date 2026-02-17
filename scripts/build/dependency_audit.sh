#!/usr/bin/env bash
set -euo pipefail

build_dir="build"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --build-dir PATH  CMake build directory containing CMakeCache.txt (default: build)
  -h, --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      build_dir="$2"
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

cache_file="${build_dir}/CMakeCache.txt"
if [[ ! -f "${cache_file}" ]]; then
  echo "error: missing CMake cache: ${cache_file}" >&2
  exit 2
fi

declare -a failures=()

version_ge() {
  local actual="$1"
  local required="$2"
  [[ -n "${actual}" ]] || return 1
  [[ "$(printf '%s\n%s\n' "${required}" "${actual}" | sort -V | tail -n1)" == "${actual}" ]]
}

record_version_check() {
  local name="$1"
  local actual="$2"
  local required="$3"
  if version_ge "${actual}" "${required}"; then
    echo "[ok] ${name} ${actual} >= ${required}"
  else
    failures+=("${name} ${actual:-unknown} < ${required}")
  fi
}

cache_value() {
  local key="$1"
  local line=""
  line="$(grep -E "^${key}:" "${cache_file}" | head -n1 || true)"
  if [[ -z "${line}" ]]; then
    printf ''
    return 0
  fi
  printf '%s' "${line#*=}"
}

extract_version() {
  local text="$1"
  grep -Eo '[0-9]+(\.[0-9]+){0,2}' <<<"${text}" | head -n1 || true
}

bool_on() {
  local value="$1"
  [[ "${value}" == "ON" || "${value}" == "1" || "${value}" == "TRUE" ]]
}

read_prometheus_version_from_header() {
  local include_dir="$1"
  local header_path=""
  local version=""

  if [[ -z "${include_dir}" || "${include_dir}" == *"-NOTFOUND" ]]; then
    printf ''
    return 0
  fi

  for header_path in \
    "${include_dir}/prometheus/version.h" \
    "${include_dir}/version.h" \
    "/usr/include/prometheus/version.h" \
    "/usr/local/include/prometheus/version.h"; do
    if [[ -f "${header_path}" ]]; then
      version="$(grep -Eo 'PROMETHEUS_CPP_VERSION[[:space:]]+\"[0-9]+(\.[0-9]+){0,2}\"' "${header_path}" \
        | grep -Eo '[0-9]+(\.[0-9]+){0,2}' | head -n1 || true)"
      if [[ -n "${version}" ]]; then
        printf '%s' "${version}"
        return 0
      fi
    fi
  done

  printf ''
}

require_pkg_version() {
  local package_name="$1"
  local min_version="$2"

  if ! command -v pkg-config >/dev/null 2>&1; then
    failures+=("pkg-config unavailable while checking ${package_name}")
    return
  fi

  local version=""
  version="$(pkg-config --modversion "${package_name}" 2>/dev/null || true)"
  if [[ -z "${version}" ]]; then
    failures+=("missing pkg-config package: ${package_name}")
    return
  fi

  record_version_check "${package_name}" "${version}" "${min_version}"
}

echo "== dependency_audit =="

cmake_version="$(cmake --version | awk 'NR==1{print $3}')"
record_version_check "cmake" "${cmake_version}" "3.20"

cxx_compiler="$(cache_value CMAKE_CXX_COMPILER)"
if [[ -z "${cxx_compiler}" ]]; then
  cxx_compiler="c++"
fi

if ! command -v "${cxx_compiler}" >/dev/null 2>&1; then
  failures+=("configured compiler not found: ${cxx_compiler}")
else
  compiler_resolved="$(readlink -f "${cxx_compiler}" 2>/dev/null || printf '%s' "${cxx_compiler}")"
  compiler_line="$("${cxx_compiler}" --version 2>/dev/null | head -n1 || true)"
  compiler_version="$(extract_version "${compiler_line}")"
  compiler_tag="$(printf '%s %s' "${compiler_resolved}" "${compiler_line}" | tr '[:upper:]' '[:lower:]')"

  if [[ "${compiler_tag}" == *clang* ]]; then
    record_version_check "clang" "${compiler_version}" "14"
  elif [[ "${compiler_tag}" == *gcc* || "${compiler_tag}" == *g++* ]]; then
    record_version_check "gcc" "${compiler_version}" "11"
  else
    failures+=("unknown C++ compiler family: ${compiler_line}")
  fi
fi

arrow_enabled="$(cache_value QUANT_HFT_ENABLE_ARROW_PARQUET)"
redis_external_enabled="$(cache_value QUANT_HFT_ENABLE_REDIS_EXTERNAL)"
timescale_external_enabled="$(cache_value QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL)"
metrics_enabled="$(cache_value QUANT_HFT_WITH_METRICS)"

if bool_on "${arrow_enabled}"; then
  require_pkg_version "arrow" "12"
  require_pkg_version "parquet" "12"
else
  echo "[skip] arrow/parquet checks (QUANT_HFT_ENABLE_ARROW_PARQUET=${arrow_enabled:-OFF})"
fi

if bool_on "${timescale_external_enabled}"; then
  require_pkg_version "libpq" "14"
else
  echo "[skip] libpq check (QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL=${timescale_external_enabled:-OFF})"
fi

if bool_on "${redis_external_enabled}"; then
  require_pkg_version "hiredis" "1.1"
else
  echo "[skip] hiredis check (QUANT_HFT_ENABLE_REDIS_EXTERNAL=${redis_external_enabled:-OFF})"
fi

if bool_on "${redis_external_enabled}" || bool_on "${timescale_external_enabled}"; then
  require_pkg_version "openssl" "1.1"
else
  echo "[skip] openssl check (no external Redis/Timescale enabled)"
fi

if bool_on "${metrics_enabled}"; then
  prometheus_include_dir="$(cache_value PrometheusCpp_INCLUDE_DIR)"
  prometheus_version="$(read_prometheus_version_from_header "${prometheus_include_dir}")"
  if [[ -z "${prometheus_version}" ]]; then
    failures+=("prometheus-cpp version unavailable (PrometheusCpp_INCLUDE_DIR=${prometheus_include_dir:-unknown})")
  else
    record_version_check "prometheus-cpp" "${prometheus_version}" "1.1"
  fi
else
  echo "[skip] prometheus-cpp check (QUANT_HFT_WITH_METRICS=${metrics_enabled:-OFF})"
fi

if [[ ${#failures[@]} -gt 0 ]]; then
  echo "dependency audit failed:" >&2
  for failure in "${failures[@]}"; do
    echo " - ${failure}" >&2
  done
  exit 1
fi

echo "dependency audit passed"
