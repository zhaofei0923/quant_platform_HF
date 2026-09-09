#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT
# shellcheck source=runtime_path_defaults.sh
source "${SCRIPT_DIR}/runtime_path_defaults.sh"

ENV_FILE="${ENV_FILE:-${QUANT_ROOT}/runtime/simnow.env}"
CONFIG_PATH="${QUANT_ROOT}/configs/sim/ctp_sim_trade_candidates.yaml"
BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build-gcc}"
CORE_ENGINE_BIN="${CORE_ENGINE_BIN:-${BUILD_DIR}/core_engine}"
SIMNOW_PROBE_BIN="${SIMNOW_PROBE_BIN:-${BUILD_DIR}/simnow_probe}"
RUN_ID="${SIMNOW_RUN_ID:-simnow-$(date +%Y%m%dT%H%M%S)}"
RUN_ROOT="${SIMNOW_RUN_ROOT:-}"
WAL_FILE="${SIMNOW_WAL_FILE:-${QUANT_HFT_WAL_FILE:-}}"
RUN_SECONDS="${SIMNOW_RUN_SECONDS:-0}"
PROBE_SECONDS="${SIMNOW_PROBE_SECONDS:-5}"
PROBE_TIMEOUT_SECONDS="${SIMNOW_PROBE_TIMEOUT_SECONDS:-120}"
HEALTH_INTERVAL_MS="${SIMNOW_HEALTH_INTERVAL_MS:-1000}"
INSTRUMENT_TIMEOUT_SECONDS="${SIMNOW_INSTRUMENT_TIMEOUT_SECONDS:-45}"
ALLOW_UNCONFIRMED_SETTLEMENT="${SIMNOW_ALLOW_UNCONFIRMED_SETTLEMENT:-0}"
FORCE_INSTRUMENT_REFRESH="${SIMNOW_FORCE_INSTRUMENT_REFRESH:-0}"
MIN_FREE_MB="${SIMNOW_MIN_FREE_MB:-2048}"
LOG_MAX_BYTES="${SIMNOW_LOG_MAX_BYTES:-104857600}"
LOG_RETENTION_DAYS="${SIMNOW_LOG_RETENTION_DAYS:-14}"
STARTUP_GRACE_SECONDS="${SIMNOW_STARTUP_GRACE_SECONDS:-3}"
EXPECTED_INITIAL_BALANCE="${SIMNOW_EXPECTED_INITIAL_BALANCE:-}"
EXPECTED_INITIAL_TRADING_DAY="${SIMNOW_EXPECTED_INITIAL_TRADING_DAY:-}"
INITIAL_BALANCE_TOLERANCE="${SIMNOW_INITIAL_BALANCE_TOLERANCE:-0.01}"
INITIAL_MARGIN_TOLERANCE="${SIMNOW_INITIAL_MARGIN_TOLERANCE:-0.01}"
EXPECTED_CONFIGURED_CONTRACT_COUNT="${SIMNOW_EXPECTED_CONTRACT_COUNT:-}"
BACKGROUND=1
SKIP_PROBE=0
PROBE_ONLY=0
DRY_RUN=0
ALLOW_EXISTING=0
STOP_EXISTING=0
RUN_ROOT_SET_BY_CLI=0
WAL_FILE_SET_BY_CLI=0
PROBE_SECONDS_SET_BY_CLI=0
PROBE_TIMEOUT_SECONDS_SET_BY_CLI=0
INSTRUMENT_TIMEOUT_SECONDS_SET_BY_CLI=0
FORCE_INSTRUMENT_REFRESH_SET_BY_CLI=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Start SimNow trading with production-oriented preflight checks.

Options:
  --env-file <path>              Environment file to source (default: ${ENV_FILE})
  --config <path>                CTP yaml config (default: ${CONFIG_PATH})
  --build-dir <path>             Build directory (default: ${BUILD_DIR})
  --core-engine-bin <path>       core_engine binary (default: ${CORE_ENGINE_BIN})
  --simnow-probe-bin <path>      simnow_probe binary (default: ${SIMNOW_PROBE_BIN})
  --run-id <value>               Run id for logs/PID (default: ${RUN_ID})
  --run-root <path>              Run output root (default: ${RUN_ROOT})
  --wal-file <path>              WAL output path (default: ${WAL_FILE})
  --run-seconds <int>            Stop core_engine after N seconds; 0 means unlimited (default: ${RUN_SECONDS})
  --probe-seconds <int>          simnow_probe monitor seconds before trading (default: ${PROBE_SECONDS})
  --probe-timeout-seconds <int>  Hard timeout for simnow_probe (default: ${PROBE_TIMEOUT_SECONDS})
  --health-interval-ms <int>     simnow_probe health interval (default: ${HEALTH_INTERVAL_MS})
  --instrument-timeout-seconds <int>
                                  simnow_probe instrument query timeout (default: ${INSTRUMENT_TIMEOUT_SECONDS})
  --force-instrument-refresh      Bypass the candidate cache in probe and core_engine
  --min-free-mb <int>            Required free disk space under run root (default: ${MIN_FREE_MB})
  --log-max-bytes <int>          Rotate log files above this size (default: ${LOG_MAX_BYTES})
  --log-retention-days <int>     Delete old rotated logs after N days (default: ${LOG_RETENTION_DAYS})
  --skip-probe                   Start core_engine without running simnow_probe first
  --probe-only                   Run the pre-trade probe and do not start core_engine
  --foreground                   Run core_engine in foreground and tee logs
  --background                   Run core_engine in background (default)
  --allow-existing               Exit successfully when an existing core_engine is already alive
  --stop-existing                Stop any existing core_engine before starting this run
  --dry-run                      Print resolved command/env summary and exit
  -h, --help                     Show this help

Alert hooks:
  SIMNOW_ALERT_WEBHOOK_URL       POST a generic JSON text alert to this URL
  SIMNOW_ALERT_EMAIL_TO          Send mail if the local mail command exists
  SIMNOW_ALERT_COMMAND           Run command with ALERT_SEVERITY and ALERT_MESSAGE in env

Optional new-instance account reset gate:
  SIMNOW_EXPECTED_INITIAL_BALANCE
                                Require the first successful probe under this identity-scoped
                                run root to match this account balance before core_engine starts
  SIMNOW_EXPECTED_INITIAL_TRADING_DAY
                                Optionally require that first broker snapshot to match this
                                YYYYMMDD trading day
  SIMNOW_INITIAL_BALANCE_TOLERANCE
                                Absolute balance tolerance (default: 0.01)
  SIMNOW_INITIAL_MARGIN_TOLERANCE
                                Absolute CurrMargin/FrozenMargin zero tolerance (default: 0.01)
  SIMNOW_EXPECTED_CONTRACT_COUNT
                                Require one final all-contract probe-ready marker with this count
USAGE
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

send_alert() {
  local severity="${1:-info}"
  local message="${2:-}"
  local payload
  local escaped_message

  if [[ -z "${message}" ]]; then
    return 0
  fi

  echo "[alert:${severity}] ${message}" >&2

  if [[ -n "${SIMNOW_ALERT_WEBHOOK_URL:-}" ]] && command -v curl >/dev/null 2>&1; then
    escaped_message="$(json_escape "[${severity}] ${message}")"
    payload="{\"msgtype\":\"text\",\"text\":{\"content\":\"${escaped_message}\"}}"
    curl -fsS -m 10 -H 'Content-Type: application/json' \
      -d "${payload}" "${SIMNOW_ALERT_WEBHOOK_URL}" >/dev/null 2>&1 || true
  fi

  if [[ -n "${SIMNOW_ALERT_EMAIL_TO:-}" ]] && command -v mail >/dev/null 2>&1; then
    printf '%s\n' "${message}" | mail -s "[quant-hft][${severity}] SimNow trading" \
      "${SIMNOW_ALERT_EMAIL_TO}" >/dev/null 2>&1 || true
  fi

  if [[ -n "${SIMNOW_ALERT_COMMAND:-}" ]]; then
    ALERT_SEVERITY="${severity}" ALERT_MESSAGE="${message}" \
      bash -lc "${SIMNOW_ALERT_COMMAND}" >/dev/null 2>&1 || true
  fi
}

die() {
  echo "error: $*" >&2
  send_alert "error" "$*"
  exit 1
}

require_value() {
  local option_name="$1"
  local option_value="${2:-}"
  [[ -n "${option_value}" ]] || die "${option_name} requires a value"
}

is_positive_int() {
  [[ "${1:-}" =~ ^[1-9][0-9]*$ ]]
}

is_non_negative_int() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

is_strict_decimal() {
  local value="${1:-}"
  local unsigned_value="${value#-}"
  local integer_part="${unsigned_value%%.*}"
  local fractional_part=""

  [[ "${value}" =~ ^-?[0-9]+([.][0-9]+)?$ ]] || return 1
  if [[ "${unsigned_value}" == *.* ]]; then
    fractional_part="${unsigned_value#*.}"
  fi
  # Keep awk comparisons inside a range with predictable decimal precision. The CTP
  # snapshot currently emits six fractional digits, so these limits are deliberately loose.
  (( ${#integer_part} <= 15 && ${#fractional_part} <= 9 ))
}

is_strict_non_negative_decimal() {
  [[ "${1:-}" != -* ]] && is_strict_decimal "${1:-}"
}

decimal_within_tolerance() {
  local actual="$1"
  local expected="$2"
  local tolerance="$3"
  awk -v actual="${actual}" -v expected="${expected}" -v tolerance="${tolerance}" '
    BEGIN {
      difference = actual - expected
      if (difference < 0) difference = -difference
      exit !(difference <= tolerance)
    }
  '
}

extract_quoted_log_field() {
  local line="$1"
  local key="$2"
  local prefix=" ${key}=\""
  local remainder

  remainder="${line#*${prefix}}"
  [[ "${remainder}" != "${line}" ]] || return 1
  [[ "${remainder}" == *\"* ]] || return 1
  printf '%s\n' "${remainder%%\"*}"
}

validate_configured_universe_probe() {
  local probe_log="$1"
  local expected_count="$2"
  local marker_line observed_count state
  local -a marker_lines=()

  [[ -n "${expected_count}" ]] || return 0
  is_positive_int "${expected_count}" ||
    die "SIMNOW_EXPECTED_CONTRACT_COUNT must be a positive integer"
  [[ -f "${probe_log}" && ! -L "${probe_log}" ]] ||
    die "configured-universe probe gate cannot read the safe probe log"
  mapfile -t marker_lines < <(
    grep -E '(^|[[:space:]])event=configured_universe_probe_complete([[:space:]]|$)' \
      "${probe_log}" || true
  )
  [[ ${#marker_lines[@]} -eq 1 ]] ||
    die "configured-universe probe gate requires exactly one final ready marker"
  marker_line="${marker_lines[0]}"
  observed_count="$(extract_quoted_log_field "${marker_line}" contract_count || true)"
  state="$(extract_quoted_log_field "${marker_line}" state || true)"
  is_positive_int "${observed_count}" ||
    die "configured-universe probe gate received an invalid contract count"
  [[ "${observed_count}" == "${expected_count}" ]] ||
    die "configured-universe probe gate contract count mismatch: expected=${expected_count} observed=${observed_count}"
  [[ "${state}" == "ready" ]] ||
    die "configured-universe probe gate did not receive ready state"
}

initial_balance_expectation_hash() {
  local canonical_expected
  canonical_expected="$(awk -v value="${EXPECTED_INITIAL_BALANCE}" \
    'BEGIN { printf "%.6f", value }')"
  printf '%s' "${canonical_expected}" | sha256sum | awk '{print $1}'
}

initial_identity_hash() {
  printf '%s\n%s' "${CTP_SIM_BROKER_ID:-}" "${CTP_SIM_INVESTOR_ID:-}" | \
    sha256sum | awk '{print $1}'
}

validate_initial_balance_marker() {
  local marker_file="$1"
  local expected_hash="$2"
  local expected_identity_hash="$3"
  local marker_hash marker_day marker_identity_hash

  [[ -f "${marker_file}" && ! -L "${marker_file}" ]] || return 1
  grep -qx 'schema_version=1' "${marker_file}" || return 1
  grep -qx 'status=verified' "${marker_file}" || return 1
  marker_hash="$(awk -F= '$1 == "expected_balance_sha256" {print $2}' "${marker_file}")"
  marker_day="$(awk -F= '$1 == "trading_day" {print $2}' "${marker_file}")"
  marker_identity_hash="$(awk -F= '$1 == "configured_identity_sha256" {print $2}' \
    "${marker_file}")"
  [[ "${marker_hash}" =~ ^[0-9a-f]{64}$ && "${marker_hash}" == "${expected_hash}" ]] || return 1
  [[ "${marker_day}" =~ ^[0-9]{8}$ ]] || return 1
  [[ "${marker_identity_hash}" =~ ^[0-9a-f]{64}$ &&
     "${marker_identity_hash}" == "${expected_identity_hash}" ]] || return 1
  if [[ -n "${EXPECTED_INITIAL_TRADING_DAY}" ]]; then
    [[ "${marker_day}" == "${EXPECTED_INITIAL_TRADING_DAY}" ]] || return 1
  fi
}

enforce_initial_balance_gate() {
  local probe_log="$1"
  local marker_file="$2"
  local expected_hash="$3"
  local expected_identity_hash="$4"
  local snapshot_line account_id investor_id balance curr_margin frozen_margin trading_day source
  local marker_dir marker_tmp probe_hash verified_at
  local -a snapshot_lines=()

  [[ -n "${EXPECTED_INITIAL_BALANCE}" ]] || return 0

  if [[ -e "${marker_file}" || -L "${marker_file}" ]]; then
    validate_initial_balance_marker "${marker_file}" "${expected_hash}" \
      "${expected_identity_hash}" ||
      die "initial account reset marker is invalid or belongs to a different expectation; use a new runtime instance"
    echo "[ok] initial account reset gate already verified for this runtime instance"
    return 0
  fi

  [[ ${SKIP_PROBE} -eq 0 ]] ||
    die "initial account reset gate requires a successful safe probe on this runtime instance"
  [[ -f "${probe_log}" && ! -L "${probe_log}" ]] ||
    die "initial account reset gate cannot read the safe probe log"
  mapfile -t snapshot_lines < <(
    grep -E '(^|[[:space:]])event=trading_account_snapshot([[:space:]]|$)' \
      "${probe_log}" || true
  )
  [[ ${#snapshot_lines[@]} -eq 1 ]] ||
    die "initial account reset gate requires exactly one trading account snapshot"
  snapshot_line="${snapshot_lines[0]}"

  account_id="$(extract_quoted_log_field "${snapshot_line}" account_id || true)"
  investor_id="$(extract_quoted_log_field "${snapshot_line}" investor_id || true)"
  balance="$(extract_quoted_log_field "${snapshot_line}" balance || true)"
  curr_margin="$(extract_quoted_log_field "${snapshot_line}" curr_margin || true)"
  frozen_margin="$(extract_quoted_log_field "${snapshot_line}" frozen_margin || true)"
  trading_day="$(extract_quoted_log_field "${snapshot_line}" trading_day || true)"
  source="$(extract_quoted_log_field "${snapshot_line}" source || true)"
  is_strict_non_negative_decimal "${balance}" ||
    die "initial account reset gate received an invalid balance value"
  is_strict_decimal "${curr_margin}" ||
    die "initial account reset gate received an invalid current margin value"
  is_strict_decimal "${frozen_margin}" ||
    die "initial account reset gate received an invalid frozen margin value"
  [[ "${trading_day}" =~ ^[0-9]{8}$ ]] ||
    die "initial account reset gate received an invalid broker trading day"
  [[ "${source}" == "ctp" ]] ||
    die "initial account reset gate requires a broker-originated CTP account snapshot"
  [[ -n "${CTP_SIM_INVESTOR_ID:-}" && "${investor_id}" == "${CTP_SIM_INVESTOR_ID}" &&
     "${account_id}" == "${CTP_SIM_INVESTOR_ID}" ]] ||
    die "initial account reset gate rejected the broker account identity"
  if [[ -n "${EXPECTED_INITIAL_TRADING_DAY}" ]]; then
    [[ "${trading_day}" == "${EXPECTED_INITIAL_TRADING_DAY}" ]] ||
      die "initial account reset gate rejected the broker trading day"
  fi

  decimal_within_tolerance "${balance}" "${EXPECTED_INITIAL_BALANCE}" \
    "${INITIAL_BALANCE_TOLERANCE}" ||
    die "initial account reset gate rejected the broker balance"
  decimal_within_tolerance "${curr_margin}" 0 "${INITIAL_MARGIN_TOLERANCE}" ||
    die "initial account reset gate rejected non-zero current margin"
  decimal_within_tolerance "${frozen_margin}" 0 "${INITIAL_MARGIN_TOLERANCE}" ||
    die "initial account reset gate rejected non-zero frozen margin"

  marker_dir="$(dirname "${marker_file}")"
  mkdir -p -m 700 "${marker_dir}"
  marker_tmp="$(mktemp "${marker_dir}/.initial_account_reset_verified.XXXXXX")"
  probe_hash="$(sha256sum "${probe_log}" | awk '{print $1}')"
  verified_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  {
    printf 'schema_version=1\n'
    printf 'status=verified\n'
    printf 'trading_day=%s\n' "${trading_day}"
    printf 'verified_at_utc=%s\n' "${verified_at}"
    printf 'expected_balance_sha256=%s\n' "${expected_hash}"
    printf 'configured_identity_sha256=%s\n' "${expected_identity_hash}"
    printf 'probe_log_sha256=%s\n' "${probe_hash}"
  } > "${marker_tmp}"
  chmod 600 "${marker_tmp}"
  mv -f "${marker_tmp}" "${marker_file}"
  echo "[ok] initial account reset gate verified trading_day=${trading_day}"
}

is_bool_flag() {
  [[ "${1:-}" == "0" || "${1:-}" == "1" ]]
}

is_true_text() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  [[ "${value}" == "1" || "${value}" == "true" || "${value}" == "yes" || "${value}" == "on" ]]
}

yaml_bool_value() {
  local key="$1"
  local file_path="$2"
  awk -F: -v key="${key}" '
    $1 ~ "^[[:space:]]*" key "[[:space:]]*$" {
      value = $2
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/[[:space:]\"'"'"']/, "", value)
      print tolower(value)
      exit
    }
  ' "${file_path}" 2>/dev/null
}

pid_is_alive() {
  local process_pid="${1:-}"
  [[ "${process_pid}" =~ ^[0-9]+$ ]] || return 1
  kill -0 "${process_pid}" 2>/dev/null
}

stop_pid() {
  local process_pid="$1"
  local reason="${2:-requested}"
  local waited_seconds=0

  pid_is_alive "${process_pid}" || return 0
  echo "[step] stopping existing core_engine pid=${process_pid} reason=${reason}"
  kill -TERM "${process_pid}" 2>/dev/null || true
  while pid_is_alive "${process_pid}" && [[ ${waited_seconds} -lt 20 ]]; do
    sleep 1
    waited_seconds=$((waited_seconds + 1))
  done
  if pid_is_alive "${process_pid}"; then
    echo "[warn] core_engine pid=${process_pid} did not stop after TERM; sending KILL"
    kill -KILL "${process_pid}" 2>/dev/null || true
  fi
}

find_existing_core_engine_pids() {
  # PID ownership follows this identity's run directory, never a shared config basename.
  local pid_file process_pid
  while IFS= read -r -d '' pid_file; do
    process_pid="$(tr -dc '0-9' < "${pid_file}")"
    pid_is_alive "${process_pid}" && printf '%s\n' "${process_pid}"
  done < <(find "${RUN_ROOT}" -maxdepth 2 -name core_engine.pid -type f -print0)
}

check_free_disk() {
  local path="$1"
  local min_free_mb="$2"
  local free_mb

  mkdir -p "${path}"
  free_mb="$(df -Pm "${path}" | awk 'NR == 2 {print $4}')"
  [[ "${free_mb}" =~ ^[0-9]+$ ]] || die "unable to determine free disk space for ${path}"
  if (( free_mb < min_free_mb )); then
    die "free disk space under ${path} is ${free_mb}MB, below required ${min_free_mb}MB"
  fi
}

rotate_file_if_needed() {
  local file_path="$1"
  local max_bytes="$2"
  local file_size
  local rotated_path

  [[ -f "${file_path}" ]] || return 0
  file_size="$(stat -c '%s' "${file_path}")"
  [[ "${file_size}" =~ ^[0-9]+$ ]] || return 0
  if (( file_size <= max_bytes )); then
    return 0
  fi

  rotated_path="${file_path}.$(date +%Y%m%dT%H%M%S)"
  mv "${file_path}" "${rotated_path}"
  if command -v gzip >/dev/null 2>&1; then
    gzip -f "${rotated_path}" || true
  fi
  echo "[info] rotated log: ${file_path}"
}

cleanup_old_logs() {
  local root_dir="$1"
  local retention_days="$2"
  [[ -d "${root_dir}" ]] || return 0
  find "${root_dir}" -type f \( -name '*.log.*' -o -name '*.log.*.gz' \) \
    -mtime "+${retention_days}" -delete 2>/dev/null || true
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) require_value "$1" "${2:-}"; ENV_FILE="$2"; shift 2 ;;
    --config|--ctp-config-path) require_value "$1" "${2:-}"; CONFIG_PATH="$2"; shift 2 ;;
    --build-dir)
      require_value "$1" "${2:-}"
      BUILD_DIR="$2"
      CORE_ENGINE_BIN="${BUILD_DIR}/core_engine"
      SIMNOW_PROBE_BIN="${BUILD_DIR}/simnow_probe"
      shift 2
      ;;
    --core-engine-bin|--core-bin) require_value "$1" "${2:-}"; CORE_ENGINE_BIN="$2"; shift 2 ;;
    --simnow-probe-bin|--probe-bin) require_value "$1" "${2:-}"; SIMNOW_PROBE_BIN="$2"; shift 2 ;;
    --run-id) require_value "$1" "${2:-}"; RUN_ID="$2"; shift 2 ;;
    --run-root) require_value "$1" "${2:-}"; RUN_ROOT="$2"; RUN_ROOT_SET_BY_CLI=1; shift 2 ;;
    --wal-file) require_value "$1" "${2:-}"; WAL_FILE="$2"; WAL_FILE_SET_BY_CLI=1; shift 2 ;;
    --run-seconds) require_value "$1" "${2:-}"; RUN_SECONDS="$2"; shift 2 ;;
    --probe-seconds) require_value "$1" "${2:-}"; PROBE_SECONDS="$2"; PROBE_SECONDS_SET_BY_CLI=1; shift 2 ;;
    --probe-timeout-seconds) require_value "$1" "${2:-}"; PROBE_TIMEOUT_SECONDS="$2"; PROBE_TIMEOUT_SECONDS_SET_BY_CLI=1; shift 2 ;;
    --health-interval-ms) require_value "$1" "${2:-}"; HEALTH_INTERVAL_MS="$2"; shift 2 ;;
    --instrument-timeout-seconds) require_value "$1" "${2:-}"; INSTRUMENT_TIMEOUT_SECONDS="$2"; INSTRUMENT_TIMEOUT_SECONDS_SET_BY_CLI=1; shift 2 ;;
    --force-instrument-refresh) FORCE_INSTRUMENT_REFRESH=1; FORCE_INSTRUMENT_REFRESH_SET_BY_CLI=1; shift ;;
    --min-free-mb) require_value "$1" "${2:-}"; MIN_FREE_MB="$2"; shift 2 ;;
    --log-max-bytes) require_value "$1" "${2:-}"; LOG_MAX_BYTES="$2"; shift 2 ;;
    --log-retention-days) require_value "$1" "${2:-}"; LOG_RETENTION_DAYS="$2"; shift 2 ;;
    --skip-probe) SKIP_PROBE=1; shift ;;
    --probe-only) PROBE_ONLY=1; shift ;;
    --foreground) BACKGROUND=0; shift ;;
    --background) BACKGROUND=1; shift ;;
    --allow-existing) ALLOW_EXISTING=1; shift ;;
    --stop-existing) STOP_EXISTING=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "${ENV_FILE}" ]] || die "--env-file is empty"
[[ -n "${CONFIG_PATH}" ]] || die "--config is empty"
[[ -n "${RUN_ID}" ]] || die "--run-id is empty"
is_non_negative_int "${RUN_SECONDS}" || die "--run-seconds must be a non-negative integer"
is_positive_int "${PROBE_SECONDS}" || die "--probe-seconds must be a positive integer"
is_positive_int "${PROBE_TIMEOUT_SECONDS}" || die "--probe-timeout-seconds must be a positive integer"
is_positive_int "${HEALTH_INTERVAL_MS}" || die "--health-interval-ms must be a positive integer"
is_positive_int "${INSTRUMENT_TIMEOUT_SECONDS}" || die "--instrument-timeout-seconds must be a positive integer"
is_positive_int "${MIN_FREE_MB}" || die "--min-free-mb must be a positive integer"
is_positive_int "${LOG_MAX_BYTES}" || die "--log-max-bytes must be a positive integer"
is_non_negative_int "${LOG_RETENTION_DAYS}" || die "--log-retention-days must be a non-negative integer"
is_non_negative_int "${STARTUP_GRACE_SECONDS}" || die "SIMNOW_STARTUP_GRACE_SECONDS must be a non-negative integer"

cd "${QUANT_ROOT}"

if [[ "${QUANT_HFT_SUPERVISOR_BOUND:-0}" != "1" ]]; then
  [[ -f "${ENV_FILE}" ]] || die "env file not found: ${ENV_FILE}"
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

if [[ ${PROBE_SECONDS_SET_BY_CLI} -eq 0 ]]; then
  PROBE_SECONDS="${SIMNOW_PROBE_SECONDS:-${PROBE_SECONDS}}"
fi
if [[ ${RUN_ROOT_SET_BY_CLI} -eq 0 ]]; then
  RUN_ROOT="${SIMNOW_RUN_ROOT:-${RUN_ROOT}}"
fi
if [[ ${WAL_FILE_SET_BY_CLI} -eq 0 ]]; then
  WAL_FILE="${SIMNOW_WAL_FILE:-${QUANT_HFT_WAL_FILE:-${WAL_FILE}}}"
fi
if [[ ${PROBE_TIMEOUT_SECONDS_SET_BY_CLI} -eq 0 ]]; then
  PROBE_TIMEOUT_SECONDS="${SIMNOW_PROBE_TIMEOUT_SECONDS:-${PROBE_TIMEOUT_SECONDS}}"
fi
if [[ ${INSTRUMENT_TIMEOUT_SECONDS_SET_BY_CLI} -eq 0 ]]; then
  INSTRUMENT_TIMEOUT_SECONDS="${SIMNOW_INSTRUMENT_TIMEOUT_SECONDS:-${INSTRUMENT_TIMEOUT_SECONDS}}"
fi
EXPECTED_INITIAL_BALANCE="${SIMNOW_EXPECTED_INITIAL_BALANCE:-${EXPECTED_INITIAL_BALANCE}}"
EXPECTED_INITIAL_TRADING_DAY="${SIMNOW_EXPECTED_INITIAL_TRADING_DAY:-${EXPECTED_INITIAL_TRADING_DAY}}"
INITIAL_BALANCE_TOLERANCE="${SIMNOW_INITIAL_BALANCE_TOLERANCE:-${INITIAL_BALANCE_TOLERANCE}}"
INITIAL_MARGIN_TOLERANCE="${SIMNOW_INITIAL_MARGIN_TOLERANCE:-${INITIAL_MARGIN_TOLERANCE}}"
EXPECTED_CONFIGURED_CONTRACT_COUNT="${SIMNOW_EXPECTED_CONTRACT_COUNT:-${EXPECTED_CONFIGURED_CONTRACT_COUNT}}"
ALLOW_UNCONFIRMED_SETTLEMENT="${SIMNOW_ALLOW_UNCONFIRMED_SETTLEMENT:-${ALLOW_UNCONFIRMED_SETTLEMENT}}"
if [[ ${FORCE_INSTRUMENT_REFRESH_SET_BY_CLI} -eq 0 ]]; then
  FORCE_INSTRUMENT_REFRESH="${SIMNOW_FORCE_INSTRUMENT_REFRESH:-${FORCE_INSTRUMENT_REFRESH}}"
fi
is_positive_int "${PROBE_SECONDS}" || die "SIMNOW_PROBE_SECONDS must be a positive integer"
is_positive_int "${PROBE_TIMEOUT_SECONDS}" || die "SIMNOW_PROBE_TIMEOUT_SECONDS must be a positive integer"
is_positive_int "${INSTRUMENT_TIMEOUT_SECONDS}" || die "SIMNOW_INSTRUMENT_TIMEOUT_SECONDS must be a positive integer"
is_bool_flag "${ALLOW_UNCONFIRMED_SETTLEMENT}" || die "SIMNOW_ALLOW_UNCONFIRMED_SETTLEMENT must be 0 or 1"
is_bool_flag "${FORCE_INSTRUMENT_REFRESH}" || die "SIMNOW_FORCE_INSTRUMENT_REFRESH must be 0 or 1"
if [[ -n "${EXPECTED_CONFIGURED_CONTRACT_COUNT}" ]]; then
  is_positive_int "${EXPECTED_CONFIGURED_CONTRACT_COUNT}" ||
    die "SIMNOW_EXPECTED_CONTRACT_COUNT must be a positive integer"
  [[ ${SKIP_PROBE} -eq 0 ]] ||
    die "configured-universe startup cannot skip the all-contract probe"
fi
if [[ -n "${EXPECTED_INITIAL_BALANCE}" ]]; then
  is_strict_non_negative_decimal "${EXPECTED_INITIAL_BALANCE}" ||
    die "SIMNOW_EXPECTED_INITIAL_BALANCE must be a non-negative decimal"
  is_strict_non_negative_decimal "${INITIAL_BALANCE_TOLERANCE}" ||
    die "SIMNOW_INITIAL_BALANCE_TOLERANCE must be a non-negative decimal"
  is_strict_non_negative_decimal "${INITIAL_MARGIN_TOLERANCE}" ||
    die "SIMNOW_INITIAL_MARGIN_TOLERANCE must be a non-negative decimal"
  command -v sha256sum >/dev/null 2>&1 ||
    die "sha256sum is required by the initial account reset gate"
  if [[ -n "${EXPECTED_INITIAL_TRADING_DAY}" ]]; then
    [[ "${EXPECTED_INITIAL_TRADING_DAY}" =~ ^[0-9]{8}$ ]] ||
      die "SIMNOW_EXPECTED_INITIAL_TRADING_DAY must use YYYYMMDD"
  fi
elif [[ -n "${EXPECTED_INITIAL_TRADING_DAY}" ]]; then
  die "SIMNOW_EXPECTED_INITIAL_TRADING_DAY requires SIMNOW_EXPECTED_INITIAL_BALANCE"
fi
# Resolve paths under exactly the environment that the engine will inherit.
export CTP_SIM_MARKET_FRONT="${CTP_SIM_MARKET_FRONT:-tcp://182.254.243.31:30011}"
export CTP_SIM_TRADER_FRONT="${CTP_SIM_TRADER_FRONT:-tcp://182.254.243.31:30001}"
export CTP_SIM_IS_PRODUCTION_MODE="${CTP_SIM_IS_PRODUCTION_MODE:-true}"
export CTP_SIM_ENABLE_REAL_API="${CTP_SIM_ENABLE_REAL_API:-true}"
if [[ ${WAL_FILE_SET_BY_CLI} -eq 0 && -n "${QUANT_HFT_WAL_FILE:-}" &&
      -n "${SIMNOW_WAL_FILE:-}" && "${QUANT_HFT_WAL_FILE}" != "${SIMNOW_WAL_FILE}" ]]; then
  die "QUANT_HFT_WAL_FILE conflicts with SIMNOW_WAL_FILE; select one explicit --wal-file"
fi
if [[ -z "${RUN_ROOT}" || -z "${WAL_FILE}" ]]; then
  load_runtime_path_defaults
  RUN_ROOT="${RUN_ROOT:-${RESOLVED_RUN_ROOT}}"
  WAL_FILE="${WAL_FILE:-${RESOLVED_WAL_FILE}}"
  export QUANT_HFT_MARKET_DATA_DIR="${QUANT_HFT_MARKET_DATA_DIR:-${RESOLVED_MARKET_DATA_DIR}}"
  export QUANT_HFT_READINESS_FILE="${QUANT_HFT_READINESS_FILE:-${RESOLVED_READINESS_FILE}}"
fi
[[ -n "${RUN_ROOT}" ]] || die "SIMNOW_RUN_ROOT must not be empty"
[[ -n "${WAL_FILE}" ]] || die "SIMNOW_WAL_FILE must not be empty"

export CTP_CONFIG_PATH="${CONFIG_PATH}"
export SIMNOW_RUN_ID="${RUN_ID}"
export SIMNOW_WAL_FILE="${WAL_FILE}"
export QUANT_HFT_WAL_FILE="${WAL_FILE}"
export CTP_SIM_MARKET_FRONT="${CTP_SIM_MARKET_FRONT:-tcp://182.254.243.31:30011}"
export CTP_SIM_TRADER_FRONT="${CTP_SIM_TRADER_FRONT:-tcp://182.254.243.31:30001}"
export CTP_SIM_IS_PRODUCTION_MODE="${CTP_SIM_IS_PRODUCTION_MODE:-true}"
export CTP_SIM_ENABLE_REAL_API="${CTP_SIM_ENABLE_REAL_API:-true}"

LOCK_DIR="${SIMNOW_LOCK_DIR:-${RUN_ROOT}/locks}"
LOCK_FILE="${LOCK_DIR}/core_engine_start.lock"
CURRENT_PID_FILE="${SIMNOW_CURRENT_PID_FILE:-${RUN_ROOT}/current_core_engine.pid}"
CURRENT_RUN_FILE="${SIMNOW_CURRENT_RUN_FILE:-${RUN_ROOT}/current_run_dir}"
CURRENT_LOG_FILE="${SIMNOW_CURRENT_LOG_FILE:-${RUN_ROOT}/current_core_engine_log}"
INITIAL_BALANCE_MARKER="${RUN_ROOT}/gates/initial_account_reset_verified.env"
mkdir -p "${RUN_ROOT}" "${LOCK_DIR}"

EXPECTED_INITIAL_BALANCE_HASH=""
EXPECTED_INITIAL_IDENTITY_HASH=""
if [[ -n "${EXPECTED_INITIAL_BALANCE}" ]]; then
  EXPECTED_INITIAL_BALANCE_HASH="$(initial_balance_expectation_hash)"
  EXPECTED_INITIAL_IDENTITY_HASH="$(initial_identity_hash)"
fi

if command -v flock >/dev/null 2>&1; then
  exec 9>"${LOCK_FILE}"
  flock -n 9 || die "another SimNow core_engine start is already in progress: ${LOCK_FILE}"
else
  LOCK_FALLBACK_DIR="${LOCK_FILE}.d"
  mkdir "${LOCK_FALLBACK_DIR}" 2>/dev/null || die "another SimNow core_engine start is already in progress: ${LOCK_FALLBACK_DIR}"
  trap 'rm -rf "${LOCK_FALLBACK_DIR}"' EXIT
fi

[[ -f "${CONFIG_PATH}" ]] || die "config file not found: ${CONFIG_PATH}"
[[ -x "${CORE_ENGINE_BIN}" ]] || die "core_engine binary is not executable: ${CORE_ENGINE_BIN}"
[[ -x "${SIMNOW_PROBE_BIN}" ]] || die "simnow_probe binary is not executable: ${SIMNOW_PROBE_BIN}"
[[ "${CTP_SIM_IS_PRODUCTION_MODE}" == "true" ]] || die "CTP_SIM_IS_PRODUCTION_MODE must be true for trading-hours SimNow fronts"
[[ "${CTP_SIM_ENABLE_REAL_API}" == "true" ]] || die "CTP_SIM_ENABLE_REAL_API must be true to start SimNow trading"
[[ -n "${CTP_SIM_BROKER_ID:-}" ]] || die "CTP_SIM_BROKER_ID is missing"
[[ -n "${CTP_SIM_USER_ID:-}" ]] || die "CTP_SIM_USER_ID is missing"
[[ -n "${CTP_SIM_INVESTOR_ID:-}" ]] || die "CTP_SIM_INVESTOR_ID is missing"
[[ -n "${CTP_SIM_PASSWORD:-}" ]] || die "CTP_SIM_PASSWORD is missing"
[[ -n "${CTP_SIM_AUTH_CODE:-}" ]] || die "CTP_SIM_AUTH_CODE is missing"
[[ -n "${CTP_SIM_APP_ID:-}" ]] || die "CTP_SIM_APP_ID is missing"

CONFIG_SETTLEMENT_CONFIRM_REQUIRED="$(yaml_bool_value settlement_confirm_required "${CONFIG_PATH}")"
if is_true_text "${CTP_SIM_ENABLE_REAL_API}"; then
  if [[ "${CONFIG_SETTLEMENT_CONFIRM_REQUIRED}" == "false" && "${ALLOW_UNCONFIRMED_SETTLEMENT}" != "1" ]]; then
    die "settlement_confirm_required=false is unsafe for real SimNow trading; set it true or export SIMNOW_ALLOW_UNCONFIRMED_SETTLEMENT=1 for diagnostics only"
  fi
  if [[ ${SKIP_PROBE} -eq 1 && "${ALLOW_UNCONFIRMED_SETTLEMENT}" != "1" ]]; then
    die "--skip-probe is unsafe for real SimNow trading because it can skip settlement confirmation; export SIMNOW_ALLOW_UNCONFIRMED_SETTLEMENT=1 for diagnostics only"
  fi
fi

case "${CTP_SIM_MARKET_FRONT}|${CTP_SIM_TRADER_FRONT}" in
  tcp://182.254.243.31:30011\|tcp://182.254.243.31:30001|\
  tcp://182.254.243.31:30012\|tcp://182.254.243.31:30002|\
  tcp://182.254.243.31:30013\|tcp://182.254.243.31:30003)
    ;;
  *)
    die "CTP_SIM_MARKET_FRONT/CTP_SIM_TRADER_FRONT must point to a SimNow trading-hours group (30011/30001, 30012/30002, or 30013/30003)"
    ;;
esac

check_free_disk "${RUN_ROOT}" "${MIN_FREE_MB}"
cleanup_old_logs "${RUN_ROOT}" "${LOG_RETENTION_DAYS}"

existing_pids=""
if [[ -f "${CURRENT_PID_FILE}" ]]; then
  existing_pid="$(tr -dc '0-9' < "${CURRENT_PID_FILE}" || true)"
  if pid_is_alive "${existing_pid}"; then
    existing_pids="${existing_pid}"
  fi
fi
detected_pids="$(find_existing_core_engine_pids | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//' || true)"
if [[ -n "${detected_pids}" ]]; then
  existing_pids="$(printf '%s %s\n' "${existing_pids}" "${detected_pids}" | tr ' ' '\n' | grep -E '^[0-9]+$' | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//' || true)"
fi

if [[ -n "${existing_pids}" ]]; then
  if [[ ${STOP_EXISTING} -eq 1 ]]; then
    for existing_pid in ${existing_pids}; do
      stop_pid "${existing_pid}" "stop-existing"
    done
    rm -f "${CURRENT_PID_FILE}"
  elif [[ ${ALLOW_EXISTING} -eq 1 ]]; then
    echo "[ok] existing core_engine already alive: ${existing_pids}"
    exit 0
  else
    die "core_engine is already running for this config: ${existing_pids}; use --allow-existing or --stop-existing"
  fi
fi

RUN_DIR="${RUN_ROOT}/${RUN_ID}"
mkdir -p "${RUN_DIR}"
PROBE_LOG="${RUN_DIR}/simnow_probe.log"
ENGINE_LOG="${RUN_DIR}/core_engine.log"
PID_FILE="${RUN_DIR}/core_engine.pid"
SUMMARY_FILE="${RUN_DIR}/run_summary.env"

rotate_file_if_needed "${PROBE_LOG}" "${LOG_MAX_BYTES}"
rotate_file_if_needed "${ENGINE_LOG}" "${LOG_MAX_BYTES}"

cat > "${SUMMARY_FILE}" <<EOF
run_id=${RUN_ID}
run_dir=${RUN_DIR}
config_path=${CONFIG_PATH}
wal_file=${WAL_FILE}
market_front=${CTP_SIM_MARKET_FRONT}
trader_front=${CTP_SIM_TRADER_FRONT}
production_mode=${CTP_SIM_IS_PRODUCTION_MODE}
real_api=${CTP_SIM_ENABLE_REAL_API}
run_seconds=${RUN_SECONDS}
probe_seconds=${PROBE_SECONDS}
probe_timeout_seconds=${PROBE_TIMEOUT_SECONDS}
health_interval_ms=${HEALTH_INTERVAL_MS}
instrument_timeout_seconds=${INSTRUMENT_TIMEOUT_SECONDS}
min_free_mb=${MIN_FREE_MB}
log_max_bytes=${LOG_MAX_BYTES}
engine_log=${ENGINE_LOG}
probe_log=${PROBE_LOG}
pid_file=${PID_FILE}
candidate_groups=group1(30011/30001),group2(30012/30002),group3(30013/30003)
EOF

core_cmd=("${CORE_ENGINE_BIN}" --config "${CONFIG_PATH}")
if [[ -n "${QUANT_HFT_DEPLOYMENT_FILE:-}" ]]; then
  [[ -n "${QUANT_HFT_DEPLOYMENT_ACCOUNT:-}" ]] || die "formal deployment account is missing"
  [[ -x "${BUILD_DIR}/quant_config_cli" ]] || die "formal deployment launcher is missing"
  [[ "${RUN_SECONDS}" == "0" && "${FORCE_INSTRUMENT_REFRESH}" == "0" ]] ||
    die "formal supervised launch does not accept run-seconds or forced instrument refresh"
  core_cmd=("${BUILD_DIR}/quant_config_cli" launch "${QUANT_HFT_DEPLOYMENT_FILE}"
            "${QUANT_HFT_DEPLOYMENT_ACCOUNT}")
fi
instrument_refresh_args=()
if [[ "${FORCE_INSTRUMENT_REFRESH}" == "1" ]]; then
  core_cmd+=(--force-instrument-refresh)
  instrument_refresh_args+=(--force-instrument-refresh)
fi
if [[ "${RUN_SECONDS}" != "0" ]]; then
  core_cmd+=(--run-seconds "${RUN_SECONDS}")
fi

echo "[info] SimNow trading run id: ${RUN_ID}"
echo "[info] runtime artifacts resolved under the identity-scoped reset epoch"
echo "[info] primary front: md=${CTP_SIM_MARKET_FRONT} td=${CTP_SIM_TRADER_FRONT}"
echo "[info] automatic candidates: group1 -> group2 -> group3"

if [[ ${DRY_RUN} -eq 1 ]]; then
  echo "[dry-run] disk check: ${RUN_ROOT} free >= ${MIN_FREE_MB}MB"
  echo "[dry-run] duplicate check: no live core_engine detected"
  echo "[dry-run] wal file: ${WAL_FILE}"
  echo "[dry-run] probe: timeout ${PROBE_TIMEOUT_SECONDS}s ${SIMNOW_PROBE_BIN} ${CONFIG_PATH} --monitor-seconds ${PROBE_SECONDS} --health-interval-ms ${HEALTH_INTERVAL_MS} --instrument-timeout-seconds ${INSTRUMENT_TIMEOUT_SECONDS} ${instrument_refresh_args[*]}"
  if [[ -n "${EXPECTED_CONFIGURED_CONTRACT_COUNT}" ]]; then
    echo "[dry-run] configured-universe probe gate: expected_contract_count=${EXPECTED_CONFIGURED_CONTRACT_COUNT}"
  fi
  printf '[dry-run] core_engine:'
  printf ' %q' "${core_cmd[@]}"
  printf '\n'
  if [[ -n "${EXPECTED_INITIAL_BALANCE}" ]]; then
    if [[ -e "${INITIAL_BALANCE_MARKER}" || -L "${INITIAL_BALANCE_MARKER}" ]]; then
      validate_initial_balance_marker "${INITIAL_BALANCE_MARKER}" \
        "${EXPECTED_INITIAL_BALANCE_HASH}" "${EXPECTED_INITIAL_IDENTITY_HASH}" ||
        die "initial account reset marker is invalid or belongs to a different expectation; use a new runtime instance"
      echo "[dry-run] initial account reset gate: already verified for this runtime instance"
    else
      echo "[dry-run] initial account reset gate: pending first successful broker snapshot"
    fi
  fi
  exit 0
fi

if [[ ${SKIP_PROBE} -eq 0 ]]; then
  if [[ "${QUANT_HFT_SUPERVISOR_BOUND:-0}" == "1" ]]; then
    export QUANT_HFT_PROBE_FLOW_PATH="${RUN_DIR}/probe_flow"
    mkdir -p "${QUANT_HFT_PROBE_FLOW_PATH}"
  fi
  echo "[step] running safe SimNow probe before trading"
  if ! timeout "${PROBE_TIMEOUT_SECONDS}s" "${SIMNOW_PROBE_BIN}" "${CONFIG_PATH}" \
      --monitor-seconds "${PROBE_SECONDS}" \
      --health-interval-ms "${HEALTH_INTERVAL_MS}" \
      --instrument-timeout-seconds "${INSTRUMENT_TIMEOUT_SECONDS}" \
      "${instrument_refresh_args[@]}" \
      > "${PROBE_LOG}" 2>&1; then
    send_alert "critical" "simnow_probe failed before starting core_engine; run_id=${RUN_ID}"
    echo "error: simnow_probe failed; redacted tail follows" >&2
    sed -E 's/((investor_id|account_id)=")[^"]*(")/\1<redacted>\3/g; s/((balance|available|curr_margin|frozen_margin|close_profit|position_profit)=")[-0-9.eE+]*(")/\1<redacted>\3/g' "${PROBE_LOG}" | tail -n 80 >&2
    exit 1
  fi
  validate_configured_universe_probe "${PROBE_LOG}" \
    "${EXPECTED_CONFIGURED_CONTRACT_COUNT}"
  echo "[ok] simnow_probe passed for run_id=${RUN_ID}"
else
  echo "[warn] skipping simnow_probe by request"
fi

enforce_initial_balance_gate "${PROBE_LOG}" "${INITIAL_BALANCE_MARKER}" \
  "${EXPECTED_INITIAL_BALANCE_HASH}" "${EXPECTED_INITIAL_IDENTITY_HASH}"

if [[ ${PROBE_ONLY} -eq 1 ]]; then
  echo "[ok] probe-only completed"
  exit 0
fi

printf '%s\n' "${RUN_DIR}" > "${CURRENT_RUN_FILE}"
printf '%s\n' "${ENGINE_LOG}" > "${CURRENT_LOG_FILE}"

echo "[step] starting core_engine"
if [[ ${BACKGROUND} -eq 1 ]]; then
  nohup "${core_cmd[@]}" > "${ENGINE_LOG}" 2>&1 &
  pid=$!
  printf '%s\n' "${pid}" > "${PID_FILE}"
  printf '%s\n' "${pid}" > "${CURRENT_PID_FILE}"

  if (( STARTUP_GRACE_SECONDS > 0 )); then
    sleep "${STARTUP_GRACE_SECONDS}"
  fi
  if ! pid_is_alive "${pid}"; then
    send_alert "critical" "core_engine exited during startup grace period; run_id=${RUN_ID}"
    echo "error: core_engine exited during startup; log tail follows" >&2
    tail -n 80 "${ENGINE_LOG}" >&2 || true
    exit 1
  fi

  echo "[ok] core_engine started in background"
  echo "[info] pid: ${pid}"
  echo "[info] pid file: ${PID_FILE}"
  echo "[info] current pid file: ${CURRENT_PID_FILE}"
  echo "[info] log: ${ENGINE_LOG}"
else
  echo "[info] running in foreground; log is also written to ${ENGINE_LOG}"
  "${core_cmd[@]}" 2>&1 | tee -a "${ENGINE_LOG}"
fi
