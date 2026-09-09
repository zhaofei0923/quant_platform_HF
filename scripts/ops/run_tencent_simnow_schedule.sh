#!/usr/bin/env bash
set -euo pipefail

# Tencent-hosted SimNow entrypoint.  Keep deployment-specific choices here instead of
# duplicating credentials or mutable trading settings in the systemd unit.
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
DISCOVERED_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"
QUANT_ROOT="${QUANT_ROOT:-${DISCOVERED_ROOT}}"
[[ -d "${QUANT_ROOT}" ]] || {
  echo "error: QUANT_ROOT is not a directory: ${QUANT_ROOT}" >&2
  exit 1
}
QUANT_ROOT="$(cd "${QUANT_ROOT}" && pwd -P)"

ENV_FILE="${QUANT_ROOT}/.env"
BUILD_DIR="${QUANT_ROOT}/build-real-server"
CONFIG_PATH="${QUANT_ROOT}/configs/sim/ctp_sim_trade_hc.yaml"
UNIVERSE_FILE="${QUANT_ROOT}/configs/sim/tencent_simnow_universe.csv"
TRADING_SESSIONS_CONFIG="${QUANT_ROOT}/configs/trading_sessions.yaml"
SUPERVISOR="${QUANT_ROOT}/scripts/ops/supervise_simnow_trading.sh"
RUNTIME_PATHS_BIN="${BUILD_DIR}/runtime_paths_cli"
ACCOUNTING_POLICY_CHECK_BIN="${BUILD_DIR}/simnow_accounting_policy_check_cli"
CORE_ENGINE_BIN="${BUILD_DIR}/core_engine"
SIMNOW_PROBE_BIN="${BUILD_DIR}/simnow_probe"
DAILY_SETTLEMENT_BIN="${BUILD_DIR}/daily_settlement"
POSITION_SNAPSHOT_BIN="${BUILD_DIR}/simnow_flatten_positions"
DAILY_SETTLEMENT_SCRIPT="${QUANT_ROOT}/scripts/ops/run_daily_settlement.sh"
EXPORT_SCRIPT="${QUANT_ROOT}/scripts/ops/export_simnow_trading_day.sh"

# The engine is deliberately alive before the exchange opens and briefly after it closes.
# core_engine's mandatory session gate blocks opens outside configured exchange sessions while
# still allowing close handling, recovery, reconciliation, and strategy warmup.
TENCENT_TRADING_WINDOWS="night=21:00-23:05,day_am=09:00-11:35,day_pm=13:30-15:20"
TENCENT_PREWARM_WINDOWS="night=20:45-21:00,day_am=08:45-09:00,day_pm=13:25-13:30"
readonly DEPLOY_ROOT="${QUANT_ROOT}"
readonly DEPLOY_ENV_FILE="${ENV_FILE}"
readonly DEPLOY_BUILD_DIR="${BUILD_DIR}"
readonly DEPLOY_CONFIG_PATH="${CONFIG_PATH}"
readonly DEPLOY_UNIVERSE_FILE="${UNIVERSE_FILE}"
readonly DEPLOY_TRADING_SESSIONS_CONFIG="${TRADING_SESSIONS_CONFIG}"
readonly DEPLOY_SUPERVISOR="${SUPERVISOR}"
readonly DEPLOY_RUNTIME_PATHS_BIN="${RUNTIME_PATHS_BIN}"
readonly DEPLOY_ACCOUNTING_POLICY_CHECK_BIN="${ACCOUNTING_POLICY_CHECK_BIN}"
readonly DEPLOY_CORE_ENGINE_BIN="${CORE_ENGINE_BIN}"
readonly DEPLOY_SIMNOW_PROBE_BIN="${SIMNOW_PROBE_BIN}"
readonly DEPLOY_DAILY_SETTLEMENT_BIN="${DAILY_SETTLEMENT_BIN}"
readonly DEPLOY_POSITION_SNAPSHOT_BIN="${POSITION_SNAPSHOT_BIN}"
readonly DEPLOY_DAILY_SETTLEMENT_SCRIPT="${DAILY_SETTLEMENT_SCRIPT}"
readonly DEPLOY_EXPORT_SCRIPT="${EXPORT_SCRIPT}"
readonly DEPLOY_CHILD_ENV_FILE="${QUANT_ROOT}/configs/sim/empty_delegated_runtime.env"
readonly DEPLOY_INSTANCE="${QUANT_HFT_INSTANCE:-}"
readonly DEPLOY_EXPECTED_INITIAL_BALANCE="${SIMNOW_EXPECTED_INITIAL_BALANCE:-}"
readonly DEPLOY_EXPECTED_INITIAL_TRADING_DAY="${SIMNOW_EXPECTED_INITIAL_TRADING_DAY:-}"
CHECK_ONLY=0

usage() {
  cat <<USAGE
Usage: $0 [--check-only]

Run the Tencent Cloud configured-universe SimNow schedule after a strict local preflight.

  --check-only  Validate the private env, configured universe, binaries, and linked libraries,
                then exit without connecting to SimNow.
  -h, --help    Show this help.

Fixed deployment inputs:
  env:       ${ENV_FILE}
  config:    ${CONFIG_PATH}
  universe:  ${UNIVERSE_FILE}
  build dir: ${BUILD_DIR}
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

info() {
  echo "[info] $*"
}

yaml_scalar() {
  local key="$1"
  local file_path="$2"
  awk -F: -v key="${key}" '
    $1 ~ "^[[:space:]]*" key "[[:space:]]*$" {
      value = substr($0, index($0, ":") + 1)
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      gsub(/^["'"'"']|["'"'"']$/, "", value)
      print value
      exit
    }
  ' "${file_path}"
}

yaml_mapping_scalar() {
  local key="$1"
  local file_path="$2"
  awk -F: -v key="${key}" '
    $1 ~ "^[[:space:]]+" key "[[:space:]]*$" {
      value = substr($0, index($0, ":") + 1)
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      gsub(/^["'"'"']|["'"'"']$/, "", value)
      print value
      exit
    }
  ' "${file_path}"
}

yaml_composite_product_id() {
  local file_path="$1"
  awk '
    function clean(value) {
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      gsub(/^["'"'"']|["'"'"']$/, "", value)
      return value
    }
    function indentation(value, first_non_space) {
      first_non_space = match(value, /[^ ]/)
      return first_non_space == 0 ? length(value) : first_non_space - 1
    }
    /^[[:space:]]*($|#)/ { next }
    {
      if (index($0, "\t") != 0) invalid = 1
      indent = indentation($0)
      if (indent == 0 && $0 ~ /^composite[[:space:]]*:[[:space:]]*(#.*)?$/) {
        composite_count++
        in_composite = 1
        child_indent = -1
        next
      }
      if (in_composite && indent == 0) in_composite = 0
      if (!in_composite) next
      if (child_indent < 0 && $0 ~ /^[ ]+[A-Za-z0-9_-]+[[:space:]]*:/) {
        child_indent = indent
      }
      if (indent == child_indent && $0 ~ /^[ ]*product_id[[:space:]]*:/) {
        value = substr($0, index($0, ":") + 1)
        product = clean(value)
        product_count++
      }
    }
    END {
      if (invalid || composite_count != 1 || product_count != 1 || product == "") exit 2
      print product
    }
  ' "${file_path}"
}

normalize_product_key() {
  printf '%s\n' "$1" | tr '[:upper:]' '[:lower:]'
}

trim_field() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s\n' "${value}"
}

require_controlled_file() {
  local file_path="$1"
  local label="$2"
  local mode
  local mode_value
  local owner_uid

  [[ -f "${file_path}" && -r "${file_path}" ]] ||
    die "${label} is missing or unreadable: ${file_path}"
  [[ ! -L "${file_path}" ]] || die "${label} must not be a symbolic link: ${file_path}"
  mode="$(stat -c '%a' "${file_path}")" || die "unable to inspect ${label} permissions"
  [[ "${mode}" =~ ^[0-7]{3,4}$ ]] || die "unexpected ${label} mode: ${mode}"
  mode_value=$((8#${mode}))
  (( (mode_value & 022) == 0 )) || die "${label} must not be group/world writable"
  owner_uid="$(stat -c '%u' "${file_path}")" || die "unable to inspect ${label} owner"
  [[ "${owner_uid}" == "$(id -u)" ]] || die "${label} must be owned by the service user"
}

load_universe() {
  local file_path="$1"
  local header=""
  local row=""
  local instrument=""
  local product=""
  local exchange=""
  local strategy_id=""
  local strategy_config=""
  local instrument_product=""
  local absolute_strategy_config=""
  local scope_member=""
  local product_key=""
  local line_number=1
  local -A seen_instruments=()
  local -A seen_scopes=()
  local -A seen_strategies=()
  local -A strategy_products=()
  local -A seen_generic_close_exchanges=()

  require_controlled_file "${file_path}" "universe file"
  IFS= read -r header < "${file_path}" || die "universe file is empty"
  header="${header%$'\r'}"
  header="${header#$'\xef\xbb\xbf'}"
  [[ "${header}" == "instrument,product,exchange,strategy_id,strategy_config" ]] ||
    die "universe file has an invalid header"

  UNIVERSE_INSTRUMENTS=""
  UNIVERSE_PRODUCT_SCOPE=""
  UNIVERSE_STRATEGY_IDS=""
  UNIVERSE_CONTRACT_COUNT=0
  UNIVERSE_REQUIRES_GENERIC_CLOSE_POLICY=0
  UNIVERSE_GENERIC_CLOSE_EXCHANGES=""
  while IFS= read -r row || [[ -n "${row}" ]]; do
    line_number=$((line_number + 1))
    row="${row%$'\r'}"
    [[ -n "${row}" && "${row}" != \#* ]] ||
      die "universe file contains an empty/comment row at line ${line_number}"
    [[ "${row}" =~ ^[^,]*,[^,]*,[^,]*,[^,]*,[^,]*$ ]] ||
      die "universe file has an invalid column count at line ${line_number}"
    IFS=',' read -r instrument product exchange strategy_id strategy_config <<< "${row}"
    instrument="$(trim_field "${instrument}")"
    product="$(trim_field "${product}")"
    exchange="$(trim_field "${exchange}")"
    strategy_id="$(trim_field "${strategy_id}")"
    strategy_config="$(trim_field "${strategy_config}")"

    [[ "${instrument}" =~ ^[A-Za-z][A-Za-z0-9]*[0-9]{3,4}$ ]] ||
      die "invalid instrument in universe file at line ${line_number}"
    [[ "${product}" =~ ^[A-Za-z][A-Za-z0-9]*$ ]] ||
      die "invalid product in universe file at line ${line_number}"
    [[ "${exchange}" =~ ^[A-Z][A-Z0-9]*$ ]] ||
      die "invalid exchange in universe file at line ${line_number}"
    case "${exchange}" in
      SHFE|INE|DCE|CZCE|GFEX) ;;
      *) die "exchange ${exchange} is outside the supported commodity-exchange set" ;;
    esac
    case "${exchange}" in
      DCE|CZCE|GFEX)
        UNIVERSE_REQUIRES_GENERIC_CLOSE_POLICY=1
        if [[ -z "${seen_generic_close_exchanges[${exchange}]+x}" ]]; then
          seen_generic_close_exchanges["${exchange}"]=1
          UNIVERSE_GENERIC_CLOSE_EXCHANGES="${UNIVERSE_GENERIC_CLOSE_EXCHANGES:+${UNIVERSE_GENERIC_CLOSE_EXCHANGES},}${exchange}"
        fi
        ;;
    esac
    [[ "${strategy_id}" =~ ^[A-Za-z0-9_.-]+$ ]] ||
      die "invalid strategy id in universe file at line ${line_number}"
    [[ "${strategy_config}" =~ ^configs/strategies/[A-Za-z0-9_./-]+\.yaml$ &&
       "${strategy_config}" != *".."* ]] ||
      die "invalid strategy config path in universe file at line ${line_number}"
    instrument_product="${instrument%%[0-9]*}"
    [[ "${instrument_product}" == "${product}" ]] ||
      die "instrument/product mismatch in universe file at line ${line_number}"
    [[ -z "${seen_instruments[${instrument}]+x}" ]] ||
      die "duplicate instrument in universe file at line ${line_number}"
    seen_instruments["${instrument}"]=1
    UNIVERSE_CONTRACT_COUNT=$((UNIVERSE_CONTRACT_COUNT + 1))

    product_key="$(normalize_product_key "${product}")"
    if [[ -n "${strategy_products[${strategy_id}]+x}" && \
          "${strategy_products[${strategy_id}]}" != "${product_key}" ]]; then
      die "strategy id ${strategy_id} cannot be reused across products in the universe file"
    fi
    strategy_products["${strategy_id}"]="${product_key}"

    absolute_strategy_config="$(readlink -f "${QUANT_ROOT}/${strategy_config}" 2>/dev/null || true)"
    [[ -f "${absolute_strategy_config}" &&
       "${absolute_strategy_config}" == "${QUANT_ROOT}/configs/strategies/"* ]] ||
      die "strategy config is missing or escapes the repo at line ${line_number}"

    UNIVERSE_INSTRUMENTS="${UNIVERSE_INSTRUMENTS:+${UNIVERSE_INSTRUMENTS},}${instrument}"
    scope_member="${product}:${exchange}"
    if [[ -z "${seen_scopes[${scope_member}]+x}" ]]; then
      seen_scopes["${scope_member}"]=1
      UNIVERSE_PRODUCT_SCOPE="${UNIVERSE_PRODUCT_SCOPE:+${UNIVERSE_PRODUCT_SCOPE},}${scope_member}"
    fi
    if [[ -z "${seen_strategies[${strategy_id}]+x}" ]]; then
      seen_strategies["${strategy_id}"]=1
      UNIVERSE_STRATEGY_IDS="${UNIVERSE_STRATEGY_IDS:+${UNIVERSE_STRATEGY_IDS},}${strategy_id}"
    fi
  done < <(tail -n +2 "${file_path}")

  [[ -n "${UNIVERSE_INSTRUMENTS}" && -n "${UNIVERSE_PRODUCT_SCOPE}" &&
     -n "${UNIVERSE_STRATEGY_IDS}" ]] || die "universe file contains no instruments"
}

time_to_minutes() {
  local time_text="$1"
  local hour_text="${time_text%:*}"
  local minute_text="${time_text#*:}"
  local hour
  local minute

  [[ "${hour_text}" =~ ^[0-9][0-9]$ && "${minute_text}" =~ ^[0-9][0-9]$ ]] || return 1
  hour=$((10#${hour_text}))
  minute=$((10#${minute_text}))
  (( hour <= 23 && minute <= 59 )) || return 1
  printf '%d\n' $((hour * 60 + minute))
}

minutes_to_time() {
  local minutes="$1"
  minutes=$((minutes % 1440))
  printf '%02d:%02d\n' $((minutes / 60)) $((minutes % 60))
}

trading_session_value() {
  local file_path="$1"
  local target_product="$2"
  local target_exchange="$3"
  local target_key="$4"

  awk -F: -v product="${target_product}" -v exchange="${target_exchange}" \
    -v target_key="${target_key}" '
    function clean(value) {
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      gsub(/^["'"'"']|["'"'"']$/, "", value)
      return value
    }
    /^[[:space:]]*-[[:space:]]*exchange[[:space:]]*:/ {
      current_exchange = clean(substr($0, index($0, ":") + 1))
      current_product = ""
      next
    }
    /^[[:space:]]*instrument_prefix[[:space:]]*:/ {
      current_product = clean(substr($0, index($0, ":") + 1))
      next
    }
    /^[[:space:]]*(day|night)[[:space:]]*:/ {
      key = $1
      gsub(/[[:space:]]/, "", key)
      if (current_exchange == exchange && current_product == product && key == target_key) {
        print clean(substr($0, index($0, ":") + 1))
        found = 1
        exit
      }
    }
    END { exit(found ? 0 : 1) }
  ' "${file_path}"
}

derive_deployment_windows() {
  local universe_file="$1"
  local sessions_config="$2"
  local row=""
  local instrument=""
  local product=""
  local exchange=""
  local strategy_id=""
  local strategy_config=""
  local day_value=""
  local night_value=""
  local night_start=""
  local night_end=""
  local start_minutes
  local end_minutes
  local end_timeline
  local earliest_start=1440
  local latest_end=0
  local night_count=0
  local scope_member=""
  local -A seen_scopes=()

  UNIVERSE_NIGHT_SCOPE=""
  while IFS= read -r row || [[ -n "${row}" ]]; do
    IFS=',' read -r instrument product exchange strategy_id strategy_config <<< "${row%$'\r'}"
    product="$(trim_field "${product}")"
    exchange="$(trim_field "${exchange}")"
    scope_member="${product}:${exchange}"
    [[ -z "${seen_scopes[${scope_member}]+x}" ]] || continue
    seen_scopes["${scope_member}"]=1

    day_value="$(trading_session_value "${sessions_config}" "${product}" "${exchange}" day || true)"
    [[ -n "${day_value}" && "${day_value}" != "null" ]] ||
      die "trading session config has no exact day rule for ${scope_member}"
    night_value="$(trading_session_value "${sessions_config}" "${product}" "${exchange}" night || true)"
    [[ -n "${night_value}" ]] ||
      die "trading session config has no exact night rule for ${scope_member}"
    [[ "${night_value}" != "null" ]] || continue
    [[ "${night_value}" =~ ^([0-9]{2}:[0-9]{2})-([0-9]{2}:[0-9]{2})$ ]] ||
      die "trading session config has an invalid night rule for ${scope_member}"
    night_start="${BASH_REMATCH[1]}"
    night_end="${BASH_REMATCH[2]}"
    start_minutes="$(time_to_minutes "${night_start}")" ||
      die "trading session config has an invalid night start for ${scope_member}"
    end_minutes="$(time_to_minutes "${night_end}")" ||
      die "trading session config has an invalid night end for ${scope_member}"
    [[ "${start_minutes}" != "${end_minutes}" ]] ||
      die "trading session config has a zero/full-day night rule for ${scope_member}"
    end_timeline="${end_minutes}"
    if (( end_minutes <= start_minutes )); then
      end_timeline=$((end_timeline + 1440))
    fi
    (( start_minutes < earliest_start )) && earliest_start="${start_minutes}"
    (( end_timeline > latest_end )) && latest_end="${end_timeline}"
    night_count=$((night_count + 1))
    UNIVERSE_NIGHT_SCOPE="${UNIVERSE_NIGHT_SCOPE:+${UNIVERSE_NIGHT_SCOPE},}${scope_member}"
  done < <(tail -n +2 "${universe_file}")

  if (( night_count > 0 )); then
    local aggregate_start
    local aggregate_end
    local prewarm_start
    aggregate_start="$(minutes_to_time "${earliest_start}")"
    aggregate_end="$(minutes_to_time "$((latest_end + 5))")"
    prewarm_start="$(minutes_to_time "$((earliest_start + 1440 - 15))")"
    TENCENT_TRADING_WINDOWS="night=${aggregate_start}-${aggregate_end},day_am=09:00-11:35,day_pm=13:30-15:20"
    TENCENT_PREWARM_WINDOWS="night=${prewarm_start}-${aggregate_start},day_am=08:45-09:00,day_pm=13:25-13:30"
  else
    TENCENT_TRADING_WINDOWS="day_am=09:00-11:35,day_pm=13:30-15:20"
    TENCENT_PREWARM_WINDOWS="day_am=08:45-09:00,day_pm=13:25-13:30"
  fi
}

trading_session_has_product() {
  local file_path="$1"
  local target_product="$2"
  local target_exchange="$3"
  awk -F: -v product="${target_product}" -v exchange="${target_exchange}" '
    /^[[:space:]]*-[[:space:]]*exchange[[:space:]]*:/ {
      current = $2
      gsub(/[[:space:]"'"'"']/, "", current)
      next
    }
    /^[[:space:]]*instrument_prefix[[:space:]]*:/ {
      prefix = $2
      sub(/[[:space:]]*#.*/, "", prefix)
      gsub(/[[:space:]"'"'"']/, "", prefix)
      if (current == exchange && prefix == product) found = 1
    }
    END { exit(found ? 0 : 1) }
  ' "${file_path}"
}

validate_universe_bindings() {
  local universe_file="$1"
  local runtime_config="$2"
  local sessions_config="$3"
  local row=""
  local instrument=""
  local product=""
  local exchange=""
  local strategy_id=""
  local strategy_config=""
  local configured_strategy_path=""
  local configured_product=""
  local product_key=""
  local -A validated_strategy_products=()

  while IFS= read -r row || [[ -n "${row}" ]]; do
    row="${row%$'\r'}"
    IFS=',' read -r instrument product exchange strategy_id strategy_config <<< "${row}"
    product="$(trim_field "${product}")"
    exchange="$(trim_field "${exchange}")"
    strategy_id="$(trim_field "${strategy_id}")"
    strategy_config="$(trim_field "${strategy_config}")"
    configured_strategy_path="$(yaml_mapping_scalar "${strategy_id}" "${runtime_config}")"
    [[ "${configured_strategy_path}" == "${strategy_config}" ]] ||
      die "strategy mapping for ${strategy_id} does not match the universe file"
    product_key="$(normalize_product_key "${product}")"
    if [[ -z "${validated_strategy_products[${strategy_id}]+x}" ]]; then
      if ! configured_product="$(yaml_composite_product_id "${QUANT_ROOT}/${strategy_config}")"; then
        die "strategy config ${strategy_config} must contain exactly one direct composite.product_id"
      fi
      [[ "${configured_product}" =~ ^[A-Za-z][A-Za-z0-9]*$ ]] ||
        die "strategy config ${strategy_config} has an invalid composite.product_id"
      configured_product="$(normalize_product_key "${configured_product}")"
      [[ "${configured_product}" == "${product_key}" ]] ||
        die "strategy config ${strategy_config} composite.product_id does not match universe product ${product}"
      validated_strategy_products["${strategy_id}"]="${configured_product}"
    elif [[ "${validated_strategy_products[${strategy_id}]}" != "${product_key}" ]]; then
      die "strategy id ${strategy_id} cannot be reused across products in the universe file"
    fi
    trading_session_has_product "${sessions_config}" "${product}" "${exchange}" ||
      die "trading session config has no exact ${product}:${exchange} rule"
  done < <(tail -n +2 "${universe_file}")
}

require_private_env() {
  local file_path="$1"
  local mode
  local mode_value
  local owner_uid

  [[ -f "${file_path}" ]] || die "repo-root .env not found: ${file_path}"
  [[ ! -L "${file_path}" ]] || die "repo-root .env must not be a symbolic link: ${file_path}"
  [[ -r "${file_path}" ]] || die "repo-root .env is not readable by the service user"

  mode="$(stat -c '%a' "${file_path}")" || die "unable to inspect .env permissions"
  [[ "${mode}" =~ ^[0-7]{3,4}$ ]] || die "unexpected .env permission mode: ${mode}"
  mode_value=$((8#${mode}))
  (( (mode_value & 077) == 0 )) ||
    die "repo-root .env must not grant group/other permissions (expected 0600 or stricter)"

  owner_uid="$(stat -c '%u' "${file_path}")" || die "unable to inspect .env owner"
  [[ "${owner_uid}" == "$(id -u)" ]] || die "repo-root .env must be owned by the service user"
}

require_controlled_calendar() {
  local file_path="$1"
  local expected_scope="$2"
  local expected_night_scope="$3"
  local header
  local configured_night_scope=""
  local configured_night_scope_rows=0

  require_controlled_file "${file_path}" "session calendar"
  IFS= read -r header < "${file_path}" || die "session calendar is empty"
  header="${header%$'\r'}"
  header="${header#$'\xef\xbb\xbf'}"
  [[ "${header}" == "natural_date,session,trading_day,exchange,product" ]] ||
    die "session calendar has an invalid header"
  [[ "$(grep -Fxc "# product_scope=${expected_scope}" "${file_path}" || true)" == "1" ]] ||
    die "session calendar scope must exactly match configured universe: ${expected_scope}"
  configured_night_scope_rows="$(grep -c '^# session_scope\.night=' "${file_path}" || true)"
  if (( configured_night_scope_rows > 1 )); then
    die "session calendar contains duplicate night session scope metadata"
  fi
  if (( configured_night_scope_rows == 1 )); then
    configured_night_scope="$(sed -n 's/^# session_scope\.night=//p' "${file_path}")"
    [[ "${configured_night_scope}" == "${expected_night_scope}" ]] ||
      die "session calendar night scope must exactly match configured night products: ${expected_night_scope}"
  elif [[ "${expected_night_scope}" != "${expected_scope}" ]]; then
    die "session calendar must declare exact night scope for mixed/no-night products: ${expected_night_scope}"
  fi
}

require_value() {
  local name="$1"
  [[ -n "${!name:-}" ]] || die "${name} is missing from repo-root .env"
}

require_non_placeholder() {
  local name="$1"
  local value="${!name:-}"
  require_value "${name}"
  case "${value}" in
    your_*|YOUR_*|replace_*|REPLACE_*|changeme|CHANGE_ME)
      die "${name} still contains a placeholder"
      ;;
  esac
}

require_executable() {
  local file_path="$1"
  local label="$2"
  [[ -f "${file_path}" && -x "${file_path}" ]] ||
    die "${label} is missing or not executable: ${file_path}"
}

check_linked_libraries() {
  local binary="$1"
  local label="$2"
  local output

  command -v ldd >/dev/null 2>&1 || die "ldd is required for the real-API preflight"
  if ! output="$(ldd "${binary}" 2>&1)"; then
    die "ldd failed for ${label}"
  fi
  if grep -Fq 'not found' <<< "${output}"; then
    echo "${output}" >&2
    die "${label} has unresolved shared-library dependencies"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$(uname -s)" == "Linux" ]] || die "Tencent SimNow runtime requires Linux"
[[ "$(uname -m)" == "x86_64" ]] || die "CTP 6.7.11 runtime requires Linux x86_64"
require_private_env "${ENV_FILE}"
load_universe "${UNIVERSE_FILE}"
readonly DEPLOY_UNIVERSE_INSTRUMENTS="${UNIVERSE_INSTRUMENTS}"
readonly DEPLOY_UNIVERSE_PRODUCT_SCOPE="${UNIVERSE_PRODUCT_SCOPE}"
readonly DEPLOY_UNIVERSE_STRATEGY_IDS="${UNIVERSE_STRATEGY_IDS}"
readonly DEPLOY_UNIVERSE_CONTRACT_COUNT="${UNIVERSE_CONTRACT_COUNT}"
readonly DEPLOY_UNIVERSE_REQUIRES_GENERIC_CLOSE_POLICY="${UNIVERSE_REQUIRES_GENERIC_CLOSE_POLICY}"
readonly DEPLOY_UNIVERSE_GENERIC_CLOSE_EXCHANGES="${UNIVERSE_GENERIC_CLOSE_EXCHANGES}"
[[ -n "${DEPLOY_INSTANCE}" ]] ||
  die "QUANT_HFT_INSTANCE must identify the new funds/runtime epoch"
[[ "${DEPLOY_INSTANCE}" =~ ^[A-Za-z0-9_.-]+$ && "${DEPLOY_INSTANCE}" != "." &&
   "${DEPLOY_INSTANCE}" != ".." ]] || die "QUANT_HFT_INSTANCE is not a safe path component"

# Credentials and the authoritative calendar must actually come from the repo-root .env,
# not from a stale systemd-manager or interactive-shell environment.
unset CTP_SIM_IS_PRODUCTION_MODE CTP_SIM_ENABLE_REAL_API CTP_SIM_BROKER_ID \
  CTP_SIM_USER_ID CTP_SIM_INVESTOR_ID CTP_SIM_PASSWORD CTP_SIM_AUTH_CODE \
  CTP_SIM_APP_ID CTP_SIM_MARKET_FRONT CTP_SIM_TRADER_FRONT CTP_SIM_INSTRUMENT \
  CTP_SIM_INSTRUMENTS SIMNOW_PRODUCT_SCOPE SIMNOW_SESSION_CALENDAR_FILE \
  QUANT_HFT_INSTANCE QUANT_HFT_ACCOUNTING_POLICY_FILE \
  QUANT_HFT_SIMNOW_BROKER_OBSERVED_ACCOUNTING \
  QUANT_HFT_SIMNOW_ALLOW_ASSUMED_ZERO_ORDER_FEES \
  QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE \
  SIMNOW_EXPECTED_INITIAL_BALANCE SIMNOW_EXPECTED_INITIAL_TRADING_DAY \
  SIMNOW_EXPECTED_CONTRACT_COUNT
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a
set -euo pipefail

# This deployment always derives runtime locations from the account identity plus the new
# instance.  Remove legacy path/program overrides before lower layers are started.
unset SIMNOW_RUN_ROOT SIMNOW_MARKET_DATA_DIR QUANT_HFT_MARKET_DATA_DIR \
  SIMNOW_WAL_FILE QUANT_HFT_WAL_FILE QUANT_HFT_READINESS_FILE \
  QUANT_HFT_PENDING_EXIT_WAL QUANT_HFT_RUNTIME_ROOT SIMNOW_REPORT_ROOT \
  SIMNOW_EXPORT_ROOT SIMNOW_RECONCILE_ROOT SIMNOW_LOCK_DIR \
  SIMNOW_CURRENT_PID_FILE SIMNOW_CURRENT_RUN_FILE SIMNOW_CURRENT_LOG_FILE \
  SIMNOW_START_SCRIPT SIMNOW_DAILY_SETTLEMENT_SCRIPT SIMNOW_POSITION_SNAPSHOT_BIN \
  SIMNOW_EXPORT_SCRIPT SIMNOW_SIGNAL_MONITOR_SCRIPT SIMNOW_SIGNAL_MONITOR_ROOT \
  SIMNOW_SIGNAL_MONITOR_HEARTBEAT_FILE OPS_HEALTH_BIN OPS_ALERT_BIN \
  SIMNOW_FAKE_NOW SIMNOW_ANALYSIS_COMMAND

# A private .env may contain generic defaults.  Reassert every deployment boundary after
# sourcing it so that systemd and an old env file cannot widen the controlled universe.
QUANT_ROOT="${DEPLOY_ROOT}"
ENV_FILE="${DEPLOY_ENV_FILE}"
BUILD_DIR="${DEPLOY_BUILD_DIR}"
CONFIG_PATH="${DEPLOY_CONFIG_PATH}"
UNIVERSE_FILE="${DEPLOY_UNIVERSE_FILE}"
TRADING_SESSIONS_CONFIG="${DEPLOY_TRADING_SESSIONS_CONFIG}"
SUPERVISOR="${DEPLOY_SUPERVISOR}"
RUNTIME_PATHS_BIN="${DEPLOY_RUNTIME_PATHS_BIN}"
ACCOUNTING_POLICY_CHECK_BIN="${DEPLOY_ACCOUNTING_POLICY_CHECK_BIN}"
CORE_ENGINE_BIN="${DEPLOY_CORE_ENGINE_BIN}"
SIMNOW_PROBE_BIN="${DEPLOY_SIMNOW_PROBE_BIN}"
DAILY_SETTLEMENT_BIN="${DEPLOY_DAILY_SETTLEMENT_BIN}"
POSITION_SNAPSHOT_BIN="${DEPLOY_POSITION_SNAPSHOT_BIN}"
DAILY_SETTLEMENT_SCRIPT="${DEPLOY_DAILY_SETTLEMENT_SCRIPT}"
EXPORT_SCRIPT="${DEPLOY_EXPORT_SCRIPT}"
UNIVERSE_INSTRUMENTS="${DEPLOY_UNIVERSE_INSTRUMENTS}"
UNIVERSE_PRODUCT_SCOPE="${DEPLOY_UNIVERSE_PRODUCT_SCOPE}"
UNIVERSE_STRATEGY_IDS="${DEPLOY_UNIVERSE_STRATEGY_IDS}"
UNIVERSE_CONTRACT_COUNT="${DEPLOY_UNIVERSE_CONTRACT_COUNT}"
UNIVERSE_REQUIRES_GENERIC_CLOSE_POLICY="${DEPLOY_UNIVERSE_REQUIRES_GENERIC_CLOSE_POLICY}"
UNIVERSE_GENERIC_CLOSE_EXCHANGES="${DEPLOY_UNIVERSE_GENERIC_CLOSE_EXCHANGES}"
TENCENT_TRADING_WINDOWS="night=21:00-23:05,day_am=09:00-11:35,day_pm=13:30-15:20"
TENCENT_PREWARM_WINDOWS="night=20:45-21:00,day_am=08:45-09:00,day_pm=13:25-13:30"
export TZ=Asia/Shanghai
export QUANT_HFT_ENV=simnow
export QUANT_ROOT
export ENV_FILE
export BUILD_DIR
export QUANT_HFT_INSTANCE="${DEPLOY_INSTANCE}"
export CTP_SIM_INSTRUMENTS="${UNIVERSE_INSTRUMENTS}"
export SIMNOW_PRODUCT_SCOPE="${UNIVERSE_PRODUCT_SCOPE}"
export SIMNOW_EXPECTED_CONTRACT_COUNT="${UNIVERSE_CONTRACT_COUNT}"
export CTP_CONFIG_PATH="${CONFIG_PATH}"
export SIMNOW_STRICT_RECONCILE=1
export SIMNOW_ALLOW_UNCONFIRMED_SETTLEMENT=0
export SIMNOW_FORCE_INSTRUMENT_REFRESH=1
export SIMNOW_PROBE_SECONDS=5
export SIMNOW_INSTRUMENT_TIMEOUT_SECONDS=45
# Probe queries are serialized: one account query plus metadata, commission, and order-fee
# queries for every configured contract.  Size the outer timeout for that worst case so a valid
# multi-contract probe is not killed by the old fixed 120-second envelope.
SIMNOW_PROBE_TIMEOUT_SECONDS=$((
  60 + SIMNOW_PROBE_SECONDS +
  (1 + 3 * UNIVERSE_CONTRACT_COUNT) * SIMNOW_INSTRUMENT_TIMEOUT_SECONDS))
if (( SIMNOW_PROBE_TIMEOUT_SECONDS < 120 )); then
  SIMNOW_PROBE_TIMEOUT_SECONDS=120
fi
export SIMNOW_PROBE_TIMEOUT_SECONDS
export SIMNOW_EOD_TIME=15:25
export SIMNOW_EOD_EXECUTE=1
export SIMNOW_EOD_RETRY_INTERVAL_SECONDS=300
export SIMNOW_EOD_PROJECT_DB=0
export SIMNOW_EOD_QUERY_DB=1
export SIMNOW_EOD_CONVERT_MARKET_PARQUET=0
export SIMNOW_START_SCRIPT="${QUANT_ROOT}/scripts/ops/start_simnow_trading.sh"
export SIMNOW_DAILY_SETTLEMENT_SCRIPT="${DAILY_SETTLEMENT_SCRIPT}"
export SIMNOW_POSITION_SNAPSHOT_BIN="${POSITION_SNAPSHOT_BIN}"
export SIMNOW_EXPORT_SCRIPT="${EXPORT_SCRIPT}"
export QUANT_HFT_RUNTIME_ROOT="${QUANT_ROOT}/runtime"
export SIMNOW_EXPECTED_INITIAL_BALANCE="${DEPLOY_EXPECTED_INITIAL_BALANCE}"
export SIMNOW_EXPECTED_INITIAL_TRADING_DAY="${DEPLOY_EXPECTED_INITIAL_TRADING_DAY}"
export SIMNOW_INITIAL_BALANCE_TOLERANCE=0.01
export SIMNOW_INITIAL_MARGIN_TOLERANCE=0.01
export SIMNOW_EPOCH_FIRST_TRADING_DAY="${DEPLOY_EXPECTED_INITIAL_TRADING_DAY}"
# The controlled list is plural.  A stale singular override would make simnow_probe inspect
# a different contract than the one admitted by the deployment universe.
unset CTP_SIM_INSTRUMENT QUANT_HFT_ACCOUNTING_POLICY_FILE

if (( UNIVERSE_REQUIRES_GENERIC_CLOSE_POLICY == 1 )); then
  require_value QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE
  if [[ "${QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE}" != /* ]]; then
    QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE="${QUANT_ROOT}/${QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE}"
  fi
  require_controlled_file "${QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE}" \
    "SimNow generic-close accounting policy"
  QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE="$(
    readlink -f "${QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE}")"
  [[ "${QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE}" == \
       "${QUANT_ROOT}/configs/sim/accounting/"*.json ]] ||
    die "SimNow generic-close accounting policy must be a .json file under configs/sim/accounting"
  require_executable "${ACCOUNTING_POLICY_CHECK_BIN}" simnow_accounting_policy_check_cli
  "${ACCOUNTING_POLICY_CHECK_BIN}" \
    --file "${QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE}" \
    --required-exchanges "${UNIVERSE_GENERIC_CLOSE_EXCHANGES}" >/dev/null ||
    die "SimNow generic-close accounting policy failed semantic or exchange-coverage validation"
  export QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE
else
  unset QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE
fi

[[ "${CTP_SIM_IS_PRODUCTION_MODE:-}" == "true" ]] ||
  die "CTP_SIM_IS_PRODUCTION_MODE must be true"
[[ "${CTP_SIM_ENABLE_REAL_API:-}" == "true" ]] ||
  die "CTP_SIM_ENABLE_REAL_API must be true"
for key in CTP_SIM_BROKER_ID CTP_SIM_USER_ID CTP_SIM_INVESTOR_ID CTP_SIM_PASSWORD \
  CTP_SIM_AUTH_CODE CTP_SIM_APP_ID; do
  require_non_placeholder "${key}"
done
[[ "${QUANT_HFT_SIMNOW_BROKER_OBSERVED_ACCOUNTING:-}" == "1" ]] ||
  die "QUANT_HFT_SIMNOW_BROKER_OBSERVED_ACCOUNTING must be 1 for this SimNow canary"
[[ "${QUANT_HFT_SIMNOW_ALLOW_ASSUMED_ZERO_ORDER_FEES:-}" == "1" ]] ||
  die "QUANT_HFT_SIMNOW_ALLOW_ASSUMED_ZERO_ORDER_FEES must be 1 for this SimNow canary"
[[ "${SIMNOW_EXPECTED_INITIAL_BALANCE}" == "200000" ]] ||
  die "SIMNOW_EXPECTED_INITIAL_BALANCE must be 200000 for this reset epoch"
[[ "${SIMNOW_EXPECTED_INITIAL_TRADING_DAY}" == "20260908" ]] ||
  die "SIMNOW_EXPECTED_INITIAL_TRADING_DAY must be 20260908 for this reset epoch"
require_value SIMNOW_SESSION_CALENDAR_FILE
if [[ "${SIMNOW_SESSION_CALENDAR_FILE}" != /* ]]; then
  SIMNOW_SESSION_CALENDAR_FILE="${QUANT_ROOT}/${SIMNOW_SESSION_CALENDAR_FILE}"
fi
require_controlled_file "${DEPLOY_CHILD_ENV_FILE}" "delegated empty environment"

case "${CTP_SIM_MARKET_FRONT:-}|${CTP_SIM_TRADER_FRONT:-}" in
  tcp://182.254.243.31:30011\|tcp://182.254.243.31:30001|\
  tcp://182.254.243.31:30012\|tcp://182.254.243.31:30002|\
  tcp://182.254.243.31:30013\|tcp://182.254.243.31:30003)
    ;;
  *)
    die "SimNow fronts must be one matching trading-hours 300xx group"
    ;;
esac

[[ -f "${CONFIG_PATH}" ]] || die "configured-universe config not found: ${CONFIG_PATH}"
[[ -f "${TRADING_SESSIONS_CONFIG}" ]] ||
  die "trading-session config not found: ${TRADING_SESSIONS_CONFIG}"
validate_universe_bindings "${UNIVERSE_FILE}" "${CONFIG_PATH}" "${TRADING_SESSIONS_CONFIG}"
derive_deployment_windows "${UNIVERSE_FILE}" "${TRADING_SESSIONS_CONFIG}"
require_controlled_calendar "${SIMNOW_SESSION_CALENDAR_FILE}" "${UNIVERSE_PRODUCT_SCOPE}" \
  "${UNIVERSE_NIGHT_SCOPE}"
SIMNOW_SESSION_CALENDAR_FILE="$(readlink -f "${SIMNOW_SESSION_CALENDAR_FILE}")"
export SIMNOW_SESSION_CALENDAR_FILE
export SIMNOW_TRADING_WINDOWS="${TENCENT_TRADING_WINDOWS}"
export SIMNOW_PREWARM_WINDOWS="${TENCENT_PREWARM_WINDOWS}"
[[ "$(yaml_scalar settlement_confirm_required "${CONFIG_PATH}")" == "true" ]] ||
  die "configured-universe config must set settlement_confirm_required: true"
[[ "$(yaml_scalar session_gate_enabled "${CONFIG_PATH}")" == "true" ]] ||
  die "configured-universe config must set session_gate_enabled: true for prewarm CloseOnly safety"
[[ "$(yaml_scalar active_contract_mode "${CONFIG_PATH}")" == "static" ]] ||
  die "configured-universe config must set active_contract_mode: static"
[[ "$(yaml_scalar instruments "${CONFIG_PATH}")" == '${CTP_SIM_INSTRUMENTS}' ]] ||
  die "configured-universe config must source instruments exclusively from CTP_SIM_INSTRUMENTS"
[[ -z "$(yaml_scalar product_ids "${CONFIG_PATH}")" ]] ||
  die "configured-universe config must leave product_ids empty in static mode"
[[ "$(yaml_scalar strategy_ids "${CONFIG_PATH}")" == "${UNIVERSE_STRATEGY_IDS}" ]] ||
  die "configured-universe strategy_ids must exactly match the universe file"
[[ "$(yaml_scalar strategy_state_file_dir "${CONFIG_PATH}")" == "runtime/trading/state" ]] ||
  die "strategy_state_file_dir must use the instance-isolated runtime default"
[[ "$(yaml_scalar risk_default_max_order_volume "${CONFIG_PATH}")" == "100" ]] ||
  die "bounded SimNow risk_default_max_order_volume must be 100"
[[ "$(yaml_scalar risk_default_max_active_orders "${CONFIG_PATH}")" == "1" ]] ||
  die "bounded SimNow risk_default_max_active_orders must be 1"
[[ "$(yaml_scalar risk_default_max_order_notional "${CONFIG_PATH}")" == "1000000" ]] ||
  die "bounded SimNow risk_default_max_order_notional must be 1000000"
[[ "$(yaml_scalar risk_default_max_position_notional "${CONFIG_PATH}")" == "0" ]] ||
  die "bounded SimNow fixed position-notional guard must be disabled"
[[ "$(yaml_scalar risk_max_margin_to_equity_ratio "${CONFIG_PATH}")" == "0.30" ]] ||
  die "bounded SimNow risk_max_margin_to_equity_ratio must be 0.30"

CMAKE_CACHE="${BUILD_DIR}/CMakeCache.txt"
[[ -f "${CMAKE_CACHE}" ]] || die "CMake cache not found: ${CMAKE_CACHE}"
grep -Fxq 'QUANT_HFT_ENABLE_CTP_REAL_API:BOOL=ON' "${CMAKE_CACHE}" ||
  die "build-real-server was not configured with QUANT_HFT_ENABLE_CTP_REAL_API=ON"

require_executable "${CORE_ENGINE_BIN}" core_engine
require_executable "${SIMNOW_PROBE_BIN}" simnow_probe
require_executable "${RUNTIME_PATHS_BIN}" runtime_paths_cli
require_executable "${DAILY_SETTLEMENT_BIN}" daily_settlement
require_executable "${POSITION_SNAPSHOT_BIN}" simnow_flatten_positions
require_executable "${DAILY_SETTLEMENT_SCRIPT}" run_daily_settlement.sh
require_executable "${EXPORT_SCRIPT}" export_simnow_trading_day.sh
require_executable "${SUPERVISOR}" supervise_simnow_trading.sh
check_linked_libraries "${CORE_ENGINE_BIN}" core_engine
check_linked_libraries "${SIMNOW_PROBE_BIN}" simnow_probe

runtime_paths_output="$("${RUNTIME_PATHS_BIN}" --config "${CONFIG_PATH}" 2>/dev/null)" ||
  die "runtime_paths_cli rejected the configured-universe runtime configuration"
declare -A resolved_paths=()
while IFS='=' read -r path_key path_value; do
  [[ "${path_key}" =~ ^(recovery_root|wal_file|market_data_dir|readiness_file|run_root|report_root|export_root|reconcile_root)$ &&
     -n "${path_value}" && -z "${resolved_paths[${path_key}]+x}" ]] ||
    die "runtime_paths_cli returned an invalid path protocol"
  resolved_paths["${path_key}"]="${path_value}"
done <<< "${runtime_paths_output}"
[[ ${#resolved_paths[@]} -eq 8 ]] || die "runtime_paths_cli output is incomplete"
runtime_root="$(readlink -m "${QUANT_ROOT}/runtime")"
recovery_prefix="${runtime_root}/simnow/"
[[ "${resolved_paths[recovery_root]}" == "${recovery_prefix}"* ]] ||
  die "resolved recovery root escaped the controlled runtime"
identity_suffix="${resolved_paths[recovery_root]#${recovery_prefix}}"
IFS='/' read -r -a identity_parts <<< "${identity_suffix}"
[[ ${#identity_parts[@]} -eq 3 && "${identity_parts[2]}" == "${QUANT_HFT_INSTANCE}" ]] ||
  die "resolved recovery root does not match the controlled runtime epoch"
[[ "${resolved_paths[wal_file]}" == "${resolved_paths[recovery_root]}/wal/events.wal" &&
   "${resolved_paths[market_data_dir]}" == "${resolved_paths[recovery_root]}/market" &&
   "${resolved_paths[readiness_file]}" == "${resolved_paths[recovery_root]}/monitor/readiness.json" &&
   "${resolved_paths[run_root]}" == "${runtime_root}/runs/simnow/${identity_suffix}" &&
   "${resolved_paths[report_root]}" == "${runtime_root}/reports/simnow/${identity_suffix}" &&
   "${resolved_paths[export_root]}" == "${runtime_root}/exports/simnow/${identity_suffix}" &&
   "${resolved_paths[reconcile_root]}" == "${runtime_root}/reconcile/simnow/${identity_suffix}" ]] ||
  die "resolved runtime artifacts are not isolated under the controlled epoch"

info "Tencent SimNow preflight passed"
info "scope=${UNIVERSE_PRODUCT_SCOPE} instruments=${UNIVERSE_INSTRUMENTS} strategies=${UNIVERSE_STRATEGY_IDS} contract_count=${UNIVERSE_CONTRACT_COUNT}"
info "instance=${QUANT_HFT_INSTANCE} env_file=${ENV_FILE} build_dir=${BUILD_DIR}"
info "windows=${TENCENT_TRADING_WINDOWS}"
info "prewarm_windows=${TENCENT_PREWARM_WINDOWS} timezone=${TZ}"
info "probe_contract_count=${UNIVERSE_CONTRACT_COUNT} probe_timeout_seconds=${SIMNOW_PROBE_TIMEOUT_SECONDS}"
info "eod_time=${SIMNOW_EOD_TIME} eod_execute=${SIMNOW_EOD_EXECUTE}"
info "initial_balance_gate=enabled reset_epoch=${QUANT_HFT_INSTANCE}"

if [[ ${CHECK_ONLY} -eq 1 ]]; then
  info "check-only completed; no SimNow connection was attempted"
  exit 0
fi

supervisor_args=(
  # Credentials were loaded and validated exactly once above.  Passing an empty controlled
  # environment keeps the
  # generic supervisor/start scripts from sourcing .env again and overriding this wrapper's
  # controlled universe, runtime epoch, balance gate, or settlement settings.
  --env-file "${DEPLOY_CHILD_ENV_FILE}"
  --config "${CONFIG_PATH}"
  --build-dir "${BUILD_DIR}"
  --windows "${TENCENT_TRADING_WINDOWS}"
  --prewarm-windows "${TENCENT_PREWARM_WINDOWS}"
  --session-calendar-file "${SIMNOW_SESSION_CALENDAR_FILE}"
  --product-scope "${UNIVERSE_PRODUCT_SCOPE}"
  --eod-time "${SIMNOW_EOD_TIME}"
)

info "starting long-lived supervisor"
exec "${SUPERVISOR}" "${supervisor_args[@]}"
