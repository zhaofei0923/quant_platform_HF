#!/usr/bin/env bash
set -euo pipefail
umask 022

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"
WRAPPER="${REPO_ROOT}/scripts/ops/run_tencent_simnow_schedule.sh"
UNIT_FILE="${REPO_ROOT}/infra/systemd/quant-hft-simnow-trading.service"
TEST_ROOT="$(mktemp -d /tmp/quant_hft_tencent_schedule_test.XXXXXX)"
trap 'rm -rf "${TEST_ROOT}"' EXIT

fail() {
  echo "[fail] $*" >&2
  exit 1
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  grep -Fq -- "${needle}" <<< "${haystack}" ||
    fail "expected output to contain: ${needle}"
}

assert_not_contains() {
  local haystack="$1"
  local needle="$2"
  if grep -Fq -- "${needle}" <<< "${haystack}"; then
    fail "output unexpectedly contained: ${needle}"
  fi
}

make_fixture() {
  local root="$1"
  mkdir -p "${root}/build-real-server" "${root}/configs/sim" \
    "${root}/configs/strategies" "${root}/configs" "${root}/runtime" "${root}/scripts/ops"
  cp /bin/true "${root}/build-real-server/core_engine"
  cp /bin/true "${root}/build-real-server/simnow_probe"
  cat > "${root}/build-real-server/runtime_paths_cli" <<EOF
#!/usr/bin/env bash
set -euo pipefail
runtime_root="${root}/runtime"
identity="simnow/9999/test_investor/\${QUANT_HFT_INSTANCE:?}"
printf 'recovery_root=%s/%s\n' "\${runtime_root}" "\${identity}"
printf 'wal_file=%s/%s/wal/events.wal\n' "\${runtime_root}" "\${identity}"
printf 'market_data_dir=%s/%s/market\n' "\${runtime_root}" "\${identity}"
printf 'readiness_file=%s/%s/monitor/readiness.json\n' "\${runtime_root}" "\${identity}"
printf 'run_root=%s/runs/%s\n' "\${runtime_root}" "\${identity}"
printf 'report_root=%s/reports/%s\n' "\${runtime_root}" "\${identity}"
printf 'export_root=%s/exports/%s\n' "\${runtime_root}" "\${identity}"
printf 'reconcile_root=%s/reconcile/%s\n' "\${runtime_root}" "\${identity}"
EOF
  cat > "${root}/build-real-server/simnow_accounting_policy_check_cli" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ $# -eq 4 && "$1" == "--file" && "$3" == "--required-exchanges" ]]
grep -Fq '"schema_version":1' "$2"
IFS=',' read -r -a required_exchanges <<< "$4"
for exchange_id in "${required_exchanges[@]}"; do
  grep -Fq "\"exchange_id\":\"${exchange_id}\"" "$2"
done
EOF
  cp /bin/true "${root}/build-real-server/daily_settlement"
  cp /bin/true "${root}/build-real-server/simnow_flatten_positions"
  cat > "${root}/scripts/ops/supervise_simnow_trading.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'child_env_file=%s\n' "${2:-}"
printf 'child_instance=%s\n' "${QUANT_HFT_INSTANCE:-}"
printf 'child_instruments=%s\n' "${CTP_SIM_INSTRUMENTS:-}"
printf 'child_contract_count=%s\n' "${SIMNOW_EXPECTED_CONTRACT_COUNT:-}"
printf 'child_probe_seconds=%s\n' "${SIMNOW_PROBE_SECONDS:-}"
printf 'child_instrument_timeout=%s\n' "${SIMNOW_INSTRUMENT_TIMEOUT_SECONDS:-}"
printf 'child_probe_timeout=%s\n' "${SIMNOW_PROBE_TIMEOUT_SECONDS:-}"
printf 'child_singular_instrument=%s\n' "${CTP_SIM_INSTRUMENT-unset}"
printf 'child_balance=%s\n' "${SIMNOW_EXPECTED_INITIAL_BALANCE:-}"
printf 'child_trading_day=%s\n' "${SIMNOW_EXPECTED_INITIAL_TRADING_DAY:-}"
printf 'child_balance_tolerance=%s\n' "${SIMNOW_INITIAL_BALANCE_TOLERANCE:-}"
printf 'child_margin_tolerance=%s\n' "${SIMNOW_INITIAL_MARGIN_TOLERANCE:-}"
printf 'child_epoch_first_day=%s\n' "${SIMNOW_EPOCH_FIRST_TRADING_DAY:-}"
printf 'child_eod_execute=%s\n' "${SIMNOW_EOD_EXECUTE:-}"
printf 'child_strict_reconcile=%s\n' "${SIMNOW_STRICT_RECONCILE:-}"
printf 'child_wal_override=%s\n' "${SIMNOW_WAL_FILE-unset}"
printf 'child_policy_file=%s\n' "${QUANT_HFT_ACCOUNTING_POLICY_FILE-unset}"
printf 'child_generic_close_policy=%s\n' \
  "${QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE-unset}"
EOF
  cp /bin/true "${root}/scripts/ops/run_daily_settlement.sh"
  cp /bin/true "${root}/scripts/ops/export_simnow_trading_day.sh"
  printf '# intentionally empty\n' > "${root}/configs/sim/empty_delegated_runtime.env"
  cat > "${root}/build-real-server/CMakeCache.txt" <<'EOF'
QUANT_HFT_ENABLE_CTP_REAL_API:BOOL=ON
QUANT_HFT_BUILD_TESTS:BOOL=OFF
EOF
  cat > "${root}/configs/sim/ctp_sim_trade_hc.yaml" <<'EOF'
ctp:
  settlement_confirm_required: true
  session_gate_enabled: true
  instruments: "${CTP_SIM_INSTRUMENTS}"
  product_ids: ""
  active_contract_mode: "static"
  strategy_ids: "kama_candidate_hc"
  strategy_composite_config_map:
    kama_candidate_hc: "configs/strategies/main_sim_trade_candidate_hc.yaml"
  strategy_state_file_dir: "runtime/trading/state"
  risk_default_max_order_volume: 100
  risk_default_max_order_notional: 1000000
  risk_default_max_active_orders: 1
  risk_default_max_position_notional: 0
  risk_max_margin_to_equity_ratio: 0.30
EOF
  cat > "${root}/configs/sim/tencent_simnow_universe.csv" <<'EOF'
instrument,product,exchange,strategy_id,strategy_config
hc2701,hc,SHFE,kama_candidate_hc,configs/strategies/main_sim_trade_candidate_hc.yaml
EOF
  cat > "${root}/configs/strategies/main_sim_trade_candidate_hc.yaml" <<'EOF'
run_type: backtest
composite:
  product_id: hc
EOF
  cat > "${root}/configs/trading_sessions.yaml" <<'EOF'
sessions:
  - exchange: SHFE
    instrument_prefix: "hc"
    day: "09:00-10:15,10:30-11:30,13:30-15:00"
    night: "21:00-23:00"
EOF
  cat > "${root}/runtime/hc_session_calendar.csv" <<'EOF'
natural_date,session,trading_day,exchange,product
# product_scope=hc:SHFE
2026-09-07,day_am,2026-09-07,SHFE,hc
EOF
  chmod 0644 "${root}/runtime/hc_session_calendar.csv"
  cat > "${root}/.env" <<'EOF'
CTP_SIM_IS_PRODUCTION_MODE=true
CTP_SIM_ENABLE_REAL_API=true
CTP_SIM_BROKER_ID=9999
CTP_SIM_USER_ID=test_user
CTP_SIM_INVESTOR_ID=test_investor
CTP_SIM_PASSWORD=super-secret-test-password
CTP_SIM_AUTH_CODE=0000000000000000
CTP_SIM_APP_ID=simnow_client_test
CTP_SIM_MARKET_FRONT=tcp://182.254.243.31:30011
CTP_SIM_TRADER_FRONT=tcp://182.254.243.31:30001
SIMNOW_SESSION_CALENDAR_FILE=runtime/hc_session_calendar.csv
QUANT_HFT_SIMNOW_BROKER_OBSERVED_ACCOUNTING=1
QUANT_HFT_SIMNOW_ALLOW_ASSUMED_ZERO_ORDER_FEES=1
BUILD_DIR=/tmp/must-not-win
SIMNOW_TRADING_WINDOWS=unsafe=00:00-23:59
QUANT_HFT_INSTANCE=unsafe-old-instance
CTP_SIM_INSTRUMENT=evil9999
CTP_SIM_INSTRUMENTS=evil9999
SIMNOW_EXPECTED_INITIAL_BALANCE=1
SIMNOW_EXPECTED_INITIAL_TRADING_DAY=19990101
SIMNOW_INITIAL_BALANCE_TOLERANCE=1000000
SIMNOW_INITIAL_MARGIN_TOLERANCE=1000000
SIMNOW_EPOCH_FIRST_TRADING_DAY=19990101
SIMNOW_EOD_EXECUTE=0
SIMNOW_STRICT_RECONCILE=0
SIMNOW_WAL_FILE=/tmp/old-events.wal
QUANT_HFT_ACCOUNTING_POLICY_FILE=/tmp/old-policy.json
UNIVERSE_INSTRUMENTS=evil2701
UNIVERSE_PRODUCT_SCOPE=evil:CZCE
UNIVERSE_STRATEGY_IDS=unsafe_strategy
UNIVERSE_CONTRACT_COUNT=99
UNIVERSE_REQUIRES_GENERIC_CLOSE_POLICY=1
UNIVERSE_GENERIC_CLOSE_EXCHANGES=CZCE
EOF
  chmod 0600 "${root}/.env"
  chmod 0755 "${root}/build-real-server/runtime_paths_cli" \
    "${root}/build-real-server/simnow_accounting_policy_check_cli" \
    "${root}/scripts/ops/supervise_simnow_trading.sh"
}

valid_root="${TEST_ROOT}/valid"
make_fixture "${valid_root}"
valid_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${valid_root}" \
  bash "${WRAPPER}" --check-only 2>&1)" ||
  fail "valid strict preflight failed: ${valid_output}"
assert_contains "${valid_output}" "Tencent SimNow preflight passed"
assert_contains "${valid_output}" "scope=hc:SHFE instruments=hc2701 strategies=kama_candidate_hc"
assert_contains "${valid_output}" "instance=simnow-test-epoch"
assert_contains "${valid_output}" "build_dir=${valid_root}/build-real-server"
assert_contains "${valid_output}" \
  "windows=night=21:00-23:05,day_am=09:00-11:35,day_pm=13:30-15:20"
assert_contains "${valid_output}" \
  "prewarm_windows=night=20:45-21:00,day_am=08:45-09:00,day_pm=13:25-13:30"
assert_contains "${valid_output}" "eod_time=15:25 eod_execute=1"
assert_contains "${valid_output}" "probe_contract_count=1 probe_timeout_seconds=245"
assert_not_contains "${valid_output}" "super-secret-test-password"
assert_not_contains "${valid_output}" "/tmp/must-not-win"
assert_not_contains "${valid_output}" "unsafe=00:00-23:59"

delegated_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${valid_root}" bash "${WRAPPER}" 2>&1)" ||
  fail "controlled delegation failed: ${delegated_output}"
assert_contains "${delegated_output}" \
  "child_env_file=${valid_root}/configs/sim/empty_delegated_runtime.env"
assert_contains "${delegated_output}" "child_instance=simnow-test-epoch"
assert_contains "${delegated_output}" "child_instruments=hc2701"
assert_contains "${delegated_output}" "child_contract_count=1"
assert_contains "${delegated_output}" "child_probe_seconds=5"
assert_contains "${delegated_output}" "child_instrument_timeout=45"
assert_contains "${delegated_output}" "child_probe_timeout=245"
assert_contains "${delegated_output}" "child_singular_instrument=unset"
assert_contains "${delegated_output}" "child_balance=200000"
assert_contains "${delegated_output}" "child_trading_day=20260908"
assert_contains "${delegated_output}" "child_balance_tolerance=0.01"
assert_contains "${delegated_output}" "child_margin_tolerance=0.01"
assert_contains "${delegated_output}" "child_epoch_first_day=20260908"
assert_contains "${delegated_output}" "child_eod_execute=1"
assert_contains "${delegated_output}" "child_strict_reconcile=1"
assert_contains "${delegated_output}" "child_wal_override=unset"
assert_contains "${delegated_output}" "child_policy_file=unset"
assert_contains "${delegated_output}" "child_generic_close_policy=unset"
assert_not_contains "${delegated_output}" "super-secret-test-password"

generic_root="${TEST_ROOT}/generic"
make_fixture "${generic_root}"
cp "${generic_root}/configs/strategies/main_sim_trade_candidate_hc.yaml" \
  "${generic_root}/configs/strategies/main_sim_trade_candidate_c.yaml"
sed -i 's/hc2701,hc,SHFE,kama_candidate_hc,configs\/strategies\/main_sim_trade_candidate_hc.yaml/rb2701,rb,SHFE,kama_candidate_rb,configs\/strategies\/main_sim_trade_candidate_rb.yaml/' \
  "${generic_root}/configs/sim/tencent_simnow_universe.csv"
mv "${generic_root}/configs/strategies/main_sim_trade_candidate_c.yaml" \
  "${generic_root}/configs/strategies/main_sim_trade_candidate_rb.yaml"
sed -i 's/kama_candidate_hc/kama_candidate_rb/g; s/main_sim_trade_candidate_hc/main_sim_trade_candidate_rb/g' \
  "${generic_root}/configs/sim/ctp_sim_trade_hc.yaml"
sed -i 's/product_id: hc/product_id: rb/' \
  "${generic_root}/configs/strategies/main_sim_trade_candidate_rb.yaml"
sed -i 's/# product_scope=hc:SHFE/# product_scope=rb:SHFE/; s/,SHFE,hc$/,SHFE,rb/' \
  "${generic_root}/runtime/hc_session_calendar.csv"
sed -i 's/instrument_prefix: "hc"/instrument_prefix: "rb"/' \
  "${generic_root}/configs/trading_sessions.yaml"
generic_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${generic_root}" \
  bash "${WRAPPER}" --check-only 2>&1)" ||
  fail "non-hc configured universe failed generic preflight: ${generic_output}"
assert_contains "${generic_output}" \
  "scope=rb:SHFE instruments=rb2701 strategies=kama_candidate_rb"

unsupported_root="${TEST_ROOT}/unsupported_exchange"
make_fixture "${unsupported_root}"
sed -i 's/hc2701,hc,SHFE/IF2701,IF,CFFEX/' \
  "${unsupported_root}/configs/sim/tencent_simnow_universe.csv"
if unsupported_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${unsupported_root}" bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "unsupported CFFEX scope unexpectedly passed"
fi
assert_contains "${unsupported_output}" \
  "exchange CFFEX is outside the supported commodity-exchange set"

all_exchanges_root="${TEST_ROOT}/all_supported_exchanges"
make_fixture "${all_exchanges_root}"
cat > "${all_exchanges_root}/configs/sim/tencent_simnow_universe.csv" <<'EOF'
instrument,product,exchange,strategy_id,strategy_config
hc2701,hc,SHFE,kama_candidate_hc,configs/strategies/main_sim_trade_candidate_hc.yaml
sc2701,sc,INE,kama_candidate_sc,configs/strategies/main_sim_trade_candidate_sc.yaml
c2701,c,DCE,kama_candidate_c,configs/strategies/main_sim_trade_candidate_c.yaml
MA701,MA,CZCE,kama_candidate_ma,configs/strategies/main_sim_trade_candidate_ma.yaml
lc2701,lc,GFEX,kama_candidate_lc,configs/strategies/main_sim_trade_candidate_lc.yaml
EOF
cat > "${all_exchanges_root}/configs/sim/ctp_sim_trade_hc.yaml" <<'EOF'
ctp:
  settlement_confirm_required: true
  session_gate_enabled: true
  instruments: "${CTP_SIM_INSTRUMENTS}"
  product_ids: ""
  active_contract_mode: "static"
  strategy_ids: "kama_candidate_hc,kama_candidate_sc,kama_candidate_c,kama_candidate_ma,kama_candidate_lc"
  strategy_composite_config_map:
    kama_candidate_hc: "configs/strategies/main_sim_trade_candidate_hc.yaml"
    kama_candidate_sc: "configs/strategies/main_sim_trade_candidate_sc.yaml"
    kama_candidate_c: "configs/strategies/main_sim_trade_candidate_c.yaml"
    kama_candidate_ma: "configs/strategies/main_sim_trade_candidate_ma.yaml"
    kama_candidate_lc: "configs/strategies/main_sim_trade_candidate_lc.yaml"
  strategy_state_file_dir: "runtime/trading/state"
  risk_default_max_order_volume: 100
  risk_default_max_order_notional: 1000000
  risk_default_max_active_orders: 1
  risk_default_max_position_notional: 0
  risk_max_margin_to_equity_ratio: 0.30
EOF
for strategy_product in sc c ma lc; do
  cat > "${all_exchanges_root}/configs/strategies/main_sim_trade_candidate_${strategy_product}.yaml" <<EOF
run_type: backtest
composite:
  product_id: ${strategy_product}
EOF
done
cat > "${all_exchanges_root}/configs/trading_sessions.yaml" <<'EOF'
sessions:
  - exchange: SHFE
    instrument_prefix: "hc"
    day: "09:00-10:15,10:30-11:30,13:30-15:00"
    night: "21:00-23:00"
  - exchange: INE
    instrument_prefix: "sc"
    day: "09:00-10:15,10:30-11:30,13:30-15:00"
    night: "21:00-02:30"
  - exchange: DCE
    instrument_prefix: "c"
    day: "09:00-10:15,10:30-11:30,13:30-15:00"
    night: "21:00-23:00"
  - exchange: CZCE
    instrument_prefix: "MA"
    day: "09:00-10:15,10:30-11:30,13:30-15:00"
    night: "21:00-23:00"
  - exchange: GFEX
    instrument_prefix: "lc"
    day: "09:00-10:15,10:30-11:30,13:30-15:00"
    night: "21:00-23:00"
EOF
cat > "${all_exchanges_root}/runtime/hc_session_calendar.csv" <<'EOF'
natural_date,session,trading_day,exchange,product
# product_scope=hc:SHFE,sc:INE,c:DCE,MA:CZCE,lc:GFEX
2026-09-07,night,2026-09-08,SHFE,hc
2026-09-07,night,2026-09-08,INE,sc
2026-09-07,night,2026-09-08,DCE,c
2026-09-07,night,2026-09-08,CZCE,MA
2026-09-07,night,2026-09-08,GFEX,lc
EOF
if missing_generic_policy_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${all_exchanges_root}" bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "five-exchange universe without generic-close accounting policy unexpectedly passed"
fi
assert_contains "${missing_generic_policy_output}" \
  "QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE is missing from repo-root .env"

printf '{}\n' > "${all_exchanges_root}/runtime/outside-policy.json"
printf '%s\n' \
  'QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE=runtime/outside-policy.json' >> \
  "${all_exchanges_root}/.env"
if outside_generic_policy_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${all_exchanges_root}" bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "generic-close policy outside controlled accounting directory unexpectedly passed"
fi
assert_contains "${outside_generic_policy_output}" \
  "generic-close accounting policy must be a .json file under configs/sim/accounting"

mkdir -p "${all_exchanges_root}/configs/sim/accounting"
printf '{}\n' > "${all_exchanges_root}/configs/sim/accounting/generic-close.json"
sed -i 's#QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE=runtime/outside-policy.json#QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE=configs/sim/accounting/generic-close.json#' \
  "${all_exchanges_root}/.env"
if malformed_generic_policy_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${all_exchanges_root}" bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "malformed generic-close accounting policy unexpectedly passed"
fi
assert_contains "${malformed_generic_policy_output}" \
  "generic-close accounting policy failed semantic or exchange-coverage validation"
cat > "${all_exchanges_root}/configs/sim/accounting/generic-close.json" <<'EOF'
{"schema_version":1,"environment":"simnow","conventions":[
{"exchange_id":"DCE","generic_close_priority":"yesterday_first","evidence_source":"test","evidence_version":"v1","evidence_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
{"exchange_id":"CZCE","generic_close_priority":"today_first","evidence_source":"test","evidence_version":"v1","evidence_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
{"exchange_id":"GFEX","generic_close_priority":"today_first","evidence_source":"test","evidence_version":"v1","evidence_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
]}
EOF
all_exchanges_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${all_exchanges_root}" bash "${WRAPPER}" --check-only 2>&1)" ||
  fail "five-exchange configured universe failed preflight: ${all_exchanges_output}"
assert_contains "${all_exchanges_output}" \
  "scope=hc:SHFE,sc:INE,c:DCE,MA:CZCE,lc:GFEX"
assert_contains "${all_exchanges_output}" \
  "windows=night=21:00-02:35,day_am=09:00-11:35,day_pm=13:30-15:20"
assert_contains "${all_exchanges_output}" \
  "probe_contract_count=5 probe_timeout_seconds=785"

mixed_night_root="${TEST_ROOT}/mixed_night_scope"
make_fixture "${mixed_night_root}"
cat >> "${mixed_night_root}/configs/sim/tencent_simnow_universe.csv" <<'EOF'
si2701,si,GFEX,kama_candidate_si,configs/strategies/main_sim_trade_candidate_si.yaml
EOF
cat > "${mixed_night_root}/configs/strategies/main_sim_trade_candidate_si.yaml" <<'EOF'
run_type: backtest
composite:
  product_id: si
EOF
sed -i 's/strategy_ids: "kama_candidate_hc"/strategy_ids: "kama_candidate_hc,kama_candidate_si"/' \
  "${mixed_night_root}/configs/sim/ctp_sim_trade_hc.yaml"
sed -i '/kama_candidate_hc: /a\    kama_candidate_si: "configs/strategies/main_sim_trade_candidate_si.yaml"' \
  "${mixed_night_root}/configs/sim/ctp_sim_trade_hc.yaml"
cat >> "${mixed_night_root}/configs/trading_sessions.yaml" <<'EOF'
  - exchange: GFEX
    instrument_prefix: "si"
    day: "09:00-10:15,10:30-11:30,13:30-15:00"
    night: null
EOF
cat > "${mixed_night_root}/runtime/hc_session_calendar.csv" <<'EOF'
natural_date,session,trading_day,exchange,product
# product_scope=hc:SHFE,si:GFEX
# session_scope.night=hc:SHFE
2026-09-07,night,2026-09-08,SHFE,hc
EOF
mkdir -p "${mixed_night_root}/configs/sim/accounting"
cat > "${mixed_night_root}/configs/sim/accounting/generic-close.json" <<'EOF'
{"schema_version":1,"environment":"simnow","conventions":[
{"exchange_id":"GFEX","generic_close_priority":"today_first","evidence_source":"test","evidence_version":"v1","evidence_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
]}
EOF
printf '%s\n' \
  'QUANT_HFT_SIMNOW_GENERIC_CLOSE_POLICY_FILE=configs/sim/accounting/generic-close.json' >> \
  "${mixed_night_root}/.env"
mixed_night_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${mixed_night_root}" bash "${WRAPPER}" --check-only 2>&1)" ||
  fail "mixed night/no-night universe failed preflight: ${mixed_night_output}"
assert_contains "${mixed_night_output}" \
  "scope=hc:SHFE,si:GFEX instruments=hc2701,si2701"
assert_contains "${mixed_night_output}" \
  "windows=night=21:00-23:05,day_am=09:00-11:35,day_pm=13:30-15:20"

sed -i '/^# session_scope\.night=/d' "${mixed_night_root}/runtime/hc_session_calendar.csv"
if missing_night_scope_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${mixed_night_root}" bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "mixed night/no-night calendar without an exact session scope unexpectedly passed"
fi
assert_contains "${missing_night_scope_output}" \
  "session calendar must declare exact night scope for mixed/no-night products: hc:SHFE"

strategy_reuse_root="${TEST_ROOT}/strategy_cross_product_reuse"
make_fixture "${strategy_reuse_root}"
cat >> "${strategy_reuse_root}/configs/sim/tencent_simnow_universe.csv" <<'EOF'
rb2701,rb,SHFE,kama_candidate_hc,configs/strategies/main_sim_trade_candidate_hc.yaml
EOF
if strategy_reuse_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${strategy_reuse_root}" bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "one strategy id reused across products unexpectedly passed"
fi
assert_contains "${strategy_reuse_output}" \
  "strategy id kama_candidate_hc cannot be reused across products"

strategy_mismatch_root="${TEST_ROOT}/strategy_product_mismatch"
make_fixture "${strategy_mismatch_root}"
sed -i 's/product_id: hc/product_id: rb/' \
  "${strategy_mismatch_root}/configs/strategies/main_sim_trade_candidate_hc.yaml"
if strategy_mismatch_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${strategy_mismatch_root}" bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "strategy composite product mismatch unexpectedly passed"
fi
assert_contains "${strategy_mismatch_output}" \
  "composite.product_id does not match universe product hc"

strategy_missing_product_root="${TEST_ROOT}/strategy_missing_product"
make_fixture "${strategy_missing_product_root}"
sed -i '/^[[:space:]]*product_id:/d' \
  "${strategy_missing_product_root}/configs/strategies/main_sim_trade_candidate_hc.yaml"
if strategy_missing_product_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${strategy_missing_product_root}" bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "strategy config without composite.product_id unexpectedly passed"
fi
assert_contains "${strategy_missing_product_output}" \
  "must contain exactly one direct composite.product_id"

strategy_duplicate_product_root="${TEST_ROOT}/strategy_duplicate_product"
make_fixture "${strategy_duplicate_product_root}"
printf '  product_id: hc\n' >> \
  "${strategy_duplicate_product_root}/configs/strategies/main_sim_trade_candidate_hc.yaml"
if strategy_duplicate_product_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${strategy_duplicate_product_root}" bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "strategy config with duplicate composite.product_id unexpectedly passed"
fi
assert_contains "${strategy_duplicate_product_output}" \
  "must contain exactly one direct composite.product_id"

permissions_root="${TEST_ROOT}/permissions"
make_fixture "${permissions_root}"
chmod 0644 "${permissions_root}/.env"
if permissions_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${permissions_root}" \
  bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "world-readable .env unexpectedly passed"
fi
assert_contains "${permissions_output}" "must not grant group/other permissions"

scope_root="${TEST_ROOT}/scope"
make_fixture "${scope_root}"
sed -i 's/hc2701,hc,SHFE/c2701,hc,SHFE/' \
  "${scope_root}/configs/sim/tencent_simnow_universe.csv"
if scope_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${scope_root}" \
  bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "instrument/product mismatch unexpectedly passed"
fi
assert_contains "${scope_output}" "instrument/product mismatch"
assert_not_contains "${scope_output}" "super-secret-test-password"

instance_root="${TEST_ROOT}/instance"
make_fixture "${instance_root}"
if instance_output="$(SIMNOW_EXPECTED_INITIAL_BALANCE=200000 \
  SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 QUANT_ROOT="${instance_root}" \
  bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "missing runtime epoch unexpectedly passed"
fi
assert_contains "${instance_output}" "QUANT_HFT_INSTANCE must identify the new funds/runtime epoch"

grep -Fq 'run_tencent_simnow_schedule.sh' "${UNIT_FILE}" ||
  fail "systemd unit does not invoke the Tencent wrapper"
grep -Fq 'UMask=0077' "${UNIT_FILE}" || fail "systemd unit does not set a private umask"
grep -Fq 'Environment=QUANT_HFT_INSTANCE=simnow-200k-reset-20260907' "${UNIT_FILE}" ||
  fail "systemd unit does not select the post-reset runtime epoch"
grep -Fq 'Environment=SIMNOW_EXPECTED_INITIAL_BALANCE=200000' "${UNIT_FILE}" ||
  fail "systemd unit does not pin the reset balance expectation"
grep -Fq 'Environment=SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908' "${UNIT_FILE}" ||
  fail "systemd unit does not pin the first broker trading day"
grep -Fq 'Environment=SIMNOW_INITIAL_BALANCE_TOLERANCE=0.01' "${UNIT_FILE}" ||
  fail "systemd unit does not pin the initial balance tolerance"
grep -Fq 'Environment=SIMNOW_INITIAL_MARGIN_TOLERANCE=0.01' "${UNIT_FILE}" ||
  fail "systemd unit does not pin the initial margin tolerance"
grep -Fq 'Environment=SIMNOW_EPOCH_FIRST_TRADING_DAY=20260908' "${UNIT_FILE}" ||
  fail "systemd unit does not pin the new epoch first trading day"

balance_root="${TEST_ROOT}/balance"
make_fixture "${balance_root}"
if balance_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=199999 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260908 \
  QUANT_ROOT="${balance_root}" \
  bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "wrong reset balance expectation unexpectedly passed"
fi
assert_contains "${balance_output}" \
  "SIMNOW_EXPECTED_INITIAL_BALANCE must be 200000 for this reset epoch"

trading_day_root="${TEST_ROOT}/trading_day"
make_fixture "${trading_day_root}"
if trading_day_output="$(QUANT_HFT_INSTANCE=simnow-test-epoch \
  SIMNOW_EXPECTED_INITIAL_BALANCE=200000 SIMNOW_EXPECTED_INITIAL_TRADING_DAY=20260909 \
  QUANT_ROOT="${trading_day_root}" bash "${WRAPPER}" --check-only 2>&1)"; then
  fail "wrong first broker trading day unexpectedly passed"
fi
assert_contains "${trading_day_output}" \
  "SIMNOW_EXPECTED_INITIAL_TRADING_DAY must be 20260908 for this reset epoch"
if grep -Fq -- '--no-eod' "${WRAPPER}"; then
  fail "Tencent wrapper still disables end-of-day settlement"
fi

echo "[ok] Tencent SimNow schedule wrapper tests passed"
