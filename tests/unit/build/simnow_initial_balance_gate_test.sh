#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
START_SCRIPT="${REPO_ROOT}/scripts/ops/start_simnow_trading.sh"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "${TEST_ROOT}"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

write_fixture() {
  local fixture_root="$1"
  mkdir -p "${fixture_root}/build"
  cat > "${fixture_root}/simnow.env" <<'EOF'
CTP_SIM_BROKER_ID=fixture-broker
CTP_SIM_USER_ID=fixture-user
CTP_SIM_INVESTOR_ID=fixture-investor
CTP_SIM_PASSWORD=fixture-password
CTP_SIM_AUTH_CODE=fixture-auth-code
CTP_SIM_APP_ID=fixture-app
CTP_SIM_IS_PRODUCTION_MODE=true
CTP_SIM_ENABLE_REAL_API=true
CTP_SIM_MARKET_FRONT=tcp://182.254.243.31:30011
CTP_SIM_TRADER_FRONT=tcp://182.254.243.31:30001
EOF
  cat > "${fixture_root}/config.yaml" <<'EOF'
settlement_confirm_required: true
EOF
cat > "${fixture_root}/build/simnow_probe" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "${SNAPSHOT_PAYLOAD:?}"
if [[ "${PROBE_MARKER_COUNT:-1}" != "omit" ]]; then
  for ((marker_index = 0; marker_index < ${PROBE_MARKER_REPEATS:-1}; ++marker_index)); do
    printf 'ts_ns=2 level=info app=simnow_probe event=configured_universe_probe_complete contract_count="%s" state="ready" trading_day="20260908"\n' \
      "${PROBE_MARKER_COUNT:-1}"
  done
fi
EOF
  cat > "${fixture_root}/build/core_engine" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'started\n' >> "${FAKE_CORE_CALLED_FILE:?}"
EOF
  chmod +x "${fixture_root}/build/simnow_probe" "${fixture_root}/build/core_engine"
}

run_start() {
  local fixture_root="$1"
  local run_root="$2"
  local run_id="$3"
  local snapshot_payload="$4"
  local expected_balance="$5"
  local output_file="$6"
  local core_called_file="$7"
  local expected_contract_count="${8-1}"
  local probe_marker_count="${9-1}"
  local probe_marker_repeats="${10-1}"

  env \
    -u SIMNOW_EXPECTED_INITIAL_BALANCE \
    -u SIMNOW_EXPECTED_INITIAL_TRADING_DAY \
    -u SIMNOW_INITIAL_BALANCE_TOLERANCE \
    -u SIMNOW_INITIAL_MARGIN_TOLERANCE \
    -u SIMNOW_EXPECTED_CONTRACT_COUNT \
    SNAPSHOT_PAYLOAD="${snapshot_payload}" \
    PROBE_MARKER_COUNT="${probe_marker_count}" \
    PROBE_MARKER_REPEATS="${probe_marker_repeats}" \
    FAKE_CORE_CALLED_FILE="${core_called_file}" \
    SIMNOW_EXPECTED_CONTRACT_COUNT="${expected_contract_count}" \
    SIMNOW_EXPECTED_INITIAL_BALANCE="${expected_balance}" \
    SIMNOW_EXPECTED_INITIAL_TRADING_DAY="${expected_balance:+20260908}" \
    SIMNOW_STARTUP_GRACE_SECONDS=0 \
    bash "${START_SCRIPT}" \
      --env-file "${fixture_root}/simnow.env" \
      --config "${fixture_root}/config.yaml" \
      --build-dir "${fixture_root}/build" \
      --run-root "${run_root}" \
      --wal-file "${run_root}/wal/events.wal" \
      --run-id "${run_id}" \
      --probe-seconds 1 \
      --probe-timeout-seconds 5 \
      --instrument-timeout-seconds 1 \
      --min-free-mb 1 \
      --foreground > "${output_file}" 2>&1
}

snapshot() {
  local balance="$1"
  local curr_margin="$2"
  local frozen_margin="$3"
  local trading_day="${4:-20260908}"
  local source="${5:-ctp}"
  printf 'ts_ns=1 level=info app=simnow_probe event=trading_account_snapshot account_id="fixture-investor" investor_id="fixture-investor" balance="%s" available="%s" curr_margin="%s" frozen_margin="%s" close_profit="0.000000" position_profit="0.000000" trading_day="%s" source="%s"' \
    "${balance}" "${balance}" "${curr_margin}" "${frozen_margin}" "${trading_day}" "${source}"
}

success_root="${TEST_ROOT}/success"
write_fixture "${success_root}"
success_run_root="${success_root}/runs/sim/fixture/default"
success_output="${success_root}/first.out"
success_core="${success_root}/core.called"
run_start "${success_root}" "${success_run_root}" first \
  "$(snapshot 200000.000000 0.000000 -0.000000)" 200000 \
  "${success_output}" "${success_core}" || fail "matching reset snapshot was rejected"

marker="${success_run_root}/gates/initial_account_reset_verified.env"
[[ -f "${marker}" ]] || fail "successful first probe did not write the instance marker"
[[ "$(stat -c '%a' "${marker}")" == "600" ]] || fail "marker permissions are not 600"
grep -qx 'status=verified' "${marker}" || fail "marker does not record verification"
grep -qx 'trading_day=20260908' "${marker}" || fail "marker does not record broker trading day"
grep -Eq '^configured_identity_sha256=[0-9a-f]{64}$' "${marker}" ||
  fail "marker does not bind the configured broker identity"
if grep -Eq 'fixture-(account|investor)|200000' "${marker}"; then
  fail "marker contains account identity or raw balance"
fi
[[ "$(wc -l < "${success_core}")" -eq 1 ]] || fail "core was not started after verification"
grep -q 'initial account reset gate verified trading_day=20260908' "${success_output}" ||
  fail "successful gate did not emit its redacted status"

restart_output="${success_root}/restart.out"
run_start "${success_root}" "${success_run_root}" restart \
  "$(snapshot 198765.000000 1234.000000 5.000000)" 200000 \
  "${restart_output}" "${success_core}" || fail "verified instance restart re-applied reset values"
[[ "$(wc -l < "${success_core}")" -eq 2 ]] || fail "core was not started on verified restart"
grep -q 'already verified for this runtime instance' "${restart_output}" ||
  fail "restart did not use the persistent instance marker"

changed_expectation_output="${success_root}/changed_expectation.out"
if run_start "${success_root}" "${success_run_root}" changed-expectation \
  "$(snapshot 300000.000000 0.000000 0.000000)" 300000 \
  "${changed_expectation_output}" "${success_core}"; then
  fail "an existing instance marker was reused for a different reset expectation"
fi
[[ "$(wc -l < "${success_core}")" -eq 2 ]] ||
  fail "core started after the reset expectation changed on the same instance"
grep -q 'belongs to a different expectation' "${changed_expectation_output}" ||
  fail "changed expectation did not require a new runtime instance"

mismatch_root="${TEST_ROOT}/balance_mismatch"
write_fixture "${mismatch_root}"
mismatch_output="${mismatch_root}/start.out"
mismatch_core="${mismatch_root}/core.called"
if run_start "${mismatch_root}" "${mismatch_root}/runs/sim/fixture/default" first \
  "$(snapshot 199998.000000 0.000000 0.000000)" 200000 \
  "${mismatch_output}" "${mismatch_core}"; then
  fail "balance outside tolerance was accepted"
fi
[[ ! -e "${mismatch_core}" ]] || fail "core started after balance rejection"
[[ ! -e "${mismatch_root}/runs/sim/fixture/default/gates/initial_account_reset_verified.env" ]] ||
  fail "rejected balance wrote a verification marker"
grep -q 'rejected the broker balance' "${mismatch_output}" ||
  fail "balance rejection reason was not emitted"
if grep -Eq 'fixture-(account|investor)|199998' "${mismatch_output}"; then
  fail "balance rejection leaked account snapshot data to console output"
fi

margin_root="${TEST_ROOT}/margin_mismatch"
write_fixture "${margin_root}"
if run_start "${margin_root}" "${margin_root}/runs/sim/fixture/default" first \
  "$(snapshot 200000.000000 0.020000 0.000000)" 200000 \
  "${margin_root}/start.out" "${margin_root}/core.called"; then
  fail "non-zero current margin was accepted"
fi
[[ ! -e "${margin_root}/core.called" ]] || fail "core started after margin rejection"
grep -q 'rejected non-zero current margin' "${margin_root}/start.out" ||
  fail "margin rejection reason was not emitted"

frozen_root="${TEST_ROOT}/frozen_margin_mismatch"
write_fixture "${frozen_root}"
if run_start "${frozen_root}" "${frozen_root}/runs/sim/fixture/default" first \
  "$(snapshot 200000.000000 0.000000 0.020000)" 200000 \
  "${frozen_root}/start.out" "${frozen_root}/core.called"; then
  fail "non-zero frozen margin was accepted"
fi
[[ ! -e "${frozen_root}/core.called" ]] || fail "core started after frozen margin rejection"
grep -q 'rejected non-zero frozen margin' "${frozen_root}/start.out" ||
  fail "frozen margin rejection reason was not emitted"

malformed_root="${TEST_ROOT}/malformed"
write_fixture "${malformed_root}"
if run_start "${malformed_root}" "${malformed_root}/runs/sim/fixture/default" first \
  "$(snapshot nan 0.000000 0.000000 not-a-day)" 200000 \
  "${malformed_root}/start.out" "${malformed_root}/core.called"; then
  fail "malformed broker snapshot was accepted"
fi
[[ ! -e "${malformed_root}/core.called" ]] || fail "core started after malformed snapshot"

wrong_day_root="${TEST_ROOT}/wrong_day"
write_fixture "${wrong_day_root}"
if run_start "${wrong_day_root}" "${wrong_day_root}/runs/sim/fixture/default" first \
  "$(snapshot 200000.000000 0.000000 0.000000 20260909)" 200000 \
  "${wrong_day_root}/start.out" "${wrong_day_root}/core.called"; then
  fail "unexpected broker trading day was accepted"
fi
[[ ! -e "${wrong_day_root}/core.called" ]] || fail "core started after trading-day rejection"
grep -q 'rejected the broker trading day' "${wrong_day_root}/start.out" ||
  fail "trading-day rejection reason was not emitted"

wrong_identity_root="${TEST_ROOT}/wrong_identity"
write_fixture "${wrong_identity_root}"
wrong_identity_snapshot="$(snapshot 200000.000000 0.000000 0.000000)"
wrong_identity_snapshot="${wrong_identity_snapshot//fixture-investor/other-investor}"
if run_start "${wrong_identity_root}" \
  "${wrong_identity_root}/runs/sim/fixture/default" first \
  "${wrong_identity_snapshot}" 200000 \
  "${wrong_identity_root}/start.out" "${wrong_identity_root}/core.called"; then
  fail "unexpected broker account identity was accepted"
fi
[[ ! -e "${wrong_identity_root}/core.called" ]] ||
  fail "core started after account-identity rejection"
grep -q 'rejected the broker account identity' "${wrong_identity_root}/start.out" ||
  fail "account-identity rejection reason was not emitted"

optional_root="${TEST_ROOT}/optional"
write_fixture "${optional_root}"
run_start "${optional_root}" "${optional_root}/runs/sim/fixture/default" first \
  "$(snapshot 1.000000 999.000000 999.000000)" "" \
  "${optional_root}/start.out" "${optional_root}/core.called" ||
  fail "unset optional gate changed legacy startup behavior"
[[ -e "${optional_root}/core.called" ]] || fail "core did not start with the optional gate unset"
[[ ! -e "${optional_root}/runs/sim/fixture/default/gates/initial_account_reset_verified.env" ]] ||
  fail "unset optional gate wrote a marker"

probe_count_root="${TEST_ROOT}/probe_count_mismatch"
write_fixture "${probe_count_root}"
if run_start "${probe_count_root}" "${probe_count_root}/runs/sim/fixture/default" first \
  "$(snapshot 1.000000 0.000000 0.000000)" "" \
  "${probe_count_root}/start.out" "${probe_count_root}/core.called" 2 1; then
  fail "mismatched configured-universe probe count was accepted"
fi
[[ ! -e "${probe_count_root}/core.called" ]] ||
  fail "core started after configured-universe probe count mismatch"
grep -q 'probe gate contract count mismatch' "${probe_count_root}/start.out" ||
  fail "configured-universe probe count rejection reason was not emitted"

duplicate_marker_root="${TEST_ROOT}/duplicate_probe_marker"
write_fixture "${duplicate_marker_root}"
if run_start "${duplicate_marker_root}" \
  "${duplicate_marker_root}/runs/sim/fixture/default" first \
  "$(snapshot 1.000000 0.000000 0.000000)" "" \
  "${duplicate_marker_root}/start.out" "${duplicate_marker_root}/core.called" 1 1 2; then
  fail "duplicate configured-universe ready markers were accepted"
fi
[[ ! -e "${duplicate_marker_root}/core.called" ]] ||
  fail "core started after duplicate configured-universe ready markers"
grep -q 'requires exactly one final ready marker' "${duplicate_marker_root}/start.out" ||
  fail "duplicate configured-universe marker rejection reason was not emitted"

legacy_probe_root="${TEST_ROOT}/legacy_probe_gate_unset"
write_fixture "${legacy_probe_root}"
run_start "${legacy_probe_root}" "${legacy_probe_root}/runs/sim/fixture/default" first \
  "$(snapshot 1.000000 0.000000 0.000000)" "" \
  "${legacy_probe_root}/start.out" "${legacy_probe_root}/core.called" "" omit ||
  fail "unset configured-universe count changed legacy startup behavior"
[[ -e "${legacy_probe_root}/core.called" ]] ||
  fail "core did not start when the configured-universe probe gate was unset"

echo "PASS: SimNow initial balance reset gate"
