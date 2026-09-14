#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"
account_unit="${repo_root}/infra/systemd/quant-hft-account@.service"
settlement_unit="${repo_root}/infra/systemd/quant-hft-daily-settlement.service"
settlement_timer="${repo_root}/infra/systemd/quant-hft-daily-settlement.timer"
legacy_unit="${repo_root}/infra/systemd/quant-hft-simnow-trading.service"
legacy_monitor_unit="${repo_root}/infra/systemd/quant-hft-simnow-signal-monitor.service"
legacy_installer="${repo_root}/scripts/ops/install_simnow_systemd_user.sh"
stop_script="${repo_root}/scripts/ops/stop_simnow_trading.sh"
package_script="${repo_root}/scripts/build/package_nonhotpath_release.sh"
operations_doc="${repo_root}/docs/ops/simnow_unattended_trading.md"

grep -Fxq \
  'ExecStart=/bin/bash /opt/quant_platform_HF/current/scripts/ops/run_packaged_account.sh' \
  "${account_unit}"
if grep -Fq 'run_account_deployment.sh' "${account_unit}"; then
  echo "generic account unit bypasses packaged runtime binding" >&2
  exit 1
fi

grep -Fxq 'RefuseManualStart=yes' "${settlement_unit}"
grep -Fxq 'ExecStart=/bin/false' "${settlement_unit}"
if grep -Eq 'workspace/quant_platform_HF|run_daily_settlement|--execute' "${settlement_unit}"; then
  echo "retired daily settlement unit still contains an executable settlement path" >&2
  exit 1
fi
grep -Fxq 'RefuseManualStart=yes' "${settlement_timer}"
if grep -Eq '^\[Install\]|WantedBy=' "${settlement_timer}"; then
  echo "retired daily settlement timer is still installable" >&2
  exit 1
fi
grep -Fxq 'RefuseManualStart=yes' "${legacy_unit}"
grep -Fxq 'ExecStart=/bin/false' "${legacy_unit}"
if grep -Eq 'run_(account_deployment|packaged_account|packaged_supervisor)\.sh' "${legacy_unit}"; then
  echo "retired SimNow user unit still contains a trading launcher" >&2
  exit 1
fi
grep -Fxq 'RefuseManualStart=yes' "${legacy_monitor_unit}"
grep -Fxq 'ExecStart=/bin/false' "${legacy_monitor_unit}"
if grep -Eq 'workspace/quant_platform_HF|quant-hft-simnow-trading\.service|monitor_simnow_signal_execution' \
  "${legacy_monitor_unit}"; then
  echo "retired signal monitor still binds an old checkout or service" >&2
  exit 1
fi

set +e
retired_output="$(bash "${legacy_installer}" --dry-run 2>&1)"
retired_status=$?
set -e
if [[ ${retired_status} -eq 0 ]]; then
  echo "retired SimNow installer unexpectedly accepted an install request" >&2
  exit 1
fi
grep -Fq 'installation is retired' <<< "${retired_output}"
grep -Fq 'no account is inferred' <<< "${retired_output}"
grep -Fq 'stop_simnow_trading.sh' "${package_script}"
grep -Fq '/opt/quant_platform_HF/current/infra/quant-hft-simnow-account@.service' \
  "${operations_doc}"
if grep -Fq 'sudo install -m 0644 infra/systemd/quant-hft-simnow-account@.service' \
  "${operations_doc}"; then
  echo "operations doc installs the formal unit from a mutable checkout" >&2
  exit 1
fi

stop_test_root="$(mktemp -d)"
selected_supervisor_pid=""
decoy_core_pid=""
cleanup() {
  [[ -z "${selected_supervisor_pid}" ]] || kill "${selected_supervisor_pid}" 2>/dev/null || true
  [[ -z "${decoy_core_pid}" ]] || kill "${decoy_core_pid}" 2>/dev/null || true
  [[ -z "${selected_supervisor_pid}" ]] || wait "${selected_supervisor_pid}" 2>/dev/null || true
  [[ -z "${decoy_core_pid}" ]] || wait "${decoy_core_pid}" 2>/dev/null || true
  rm -rf -- "${stop_test_root}"
}
trap cleanup EXIT
unique_config="${stop_test_root}/no-running-core.yaml"

set +e
missing_identity_output="$(SIMNOW_SYSTEMD_UNIT=ignored.service \
  bash "${stop_script}" --all --dry-run --run-root "${stop_test_root}/runs" \
  --config "${unique_config}" 2>&1)"
missing_identity_status=$?
set -e
if [[ ${missing_identity_status} -eq 0 ]]; then
  echo "systemd stop unexpectedly inferred its unit or scope" >&2
  exit 1
fi
grep -Fq -- '--systemd-unit is required' <<< "${missing_identity_output}"

set +e
missing_scope_output="$(bash "${stop_script}" --stop-systemd \
  --systemd-unit reviewed.service --dry-run --run-root "${stop_test_root}/runs" \
  --config "${unique_config}" 2>&1)"
missing_scope_status=$?
set -e
if [[ ${missing_scope_status} -eq 0 ]]; then
  echo "systemd stop unexpectedly accepted a missing scope" >&2
  exit 1
fi
grep -Fq -- '--systemd-scope user|system is required' <<< "${missing_scope_output}"

user_stop_output="$(bash "${stop_script}" --stop-systemd \
  --systemd-unit reviewed-user.service --systemd-scope user --dry-run \
  --run-root "${stop_test_root}/runs" --config "${unique_config}" 2>&1)"
grep -Fq '[dry-run] systemctl --user stop reviewed-user.service' <<< "${user_stop_output}"

system_stop_output="$(bash "${stop_script}" --stop-systemd \
  --systemd-unit reviewed-system.service --systemd-scope system --dry-run \
  --run-root "${stop_test_root}/runs" --config "${unique_config}" 2>&1)"
grep -Fq '[dry-run] systemctl stop reviewed-system.service' <<< "${system_stop_output}"
grep -Fq 'no process scan performed' <<< "${system_stop_output}"

for invalid_unit in 'wild*.service' 'wild?.service' 'wild[1].service'; do
  set +e
  invalid_unit_output="$(bash "${stop_script}" --stop-systemd \
    --systemd-unit "${invalid_unit}" --systemd-scope system --dry-run 2>&1)"
  invalid_unit_status=$?
  set -e
  if [[ ${invalid_unit_status} -eq 0 ]]; then
    echo "systemd stop accepted wildcard unit name: ${invalid_unit}" >&2
    exit 1
  fi
  grep -Fq -- '--systemd-unit must be one explicit .service name' <<< "${invalid_unit_output}"
done

set +e
missing_supervisor_identity="$(bash "${stop_script}" --stop-supervisor --dry-run 2>&1)"
missing_supervisor_status=$?
set -e
if [[ ${missing_supervisor_status} -eq 0 ]]; then
  echo "shell supervisor stop accepted missing PID/run-root identity" >&2
  exit 1
fi
grep -Fq -- '--stop-supervisor requires an explicit --run-root identity' \
  <<< "${missing_supervisor_identity}"

selected_run_root="${stop_test_root}/selected-runs"
mkdir -p "${selected_run_root}/locks"
supervisor_fixture="${stop_test_root}/supervise_simnow_trading.sh"
cat > "${supervisor_fixture}" <<'FIXTURE'
#!/usr/bin/env bash
set -euo pipefail
run_root="$1"
exec 8> "${run_root}/locks/supervisor.lock"
flock -n 8
trap 'exit 0' TERM
while true; do sleep 1; done
FIXTURE
chmod +x "${supervisor_fixture}"
bash "${supervisor_fixture}" "${selected_run_root}" &
selected_supervisor_pid=$!
sleep 0.1

shell_stop_output="$(bash "${stop_script}" --stop-supervisor \
  --supervisor-pid "${selected_supervisor_pid}" --run-root "${selected_run_root}" \
  --dry-run 2>&1)"
grep -Fq "[dry-run] kill -TERM ${selected_supervisor_pid}" <<< "${shell_stop_output}"

set +e
wrong_run_root_output="$(bash "${stop_script}" --stop-supervisor \
  --supervisor-pid "${selected_supervisor_pid}" \
  --run-root "${stop_test_root}/wrong-runs" --dry-run 2>&1)"
wrong_run_root_status=$?
set -e
if [[ ${wrong_run_root_status} -eq 0 ]]; then
  echo "shell supervisor stop accepted a mismatched run-root identity" >&2
  exit 1
fi
grep -Fq 'does not own the reviewed run-root supervisor lock' <<< "${wrong_run_root_output}"

bash -c 'exec -a /tmp/unrelated/core_engine sleep 30' &
decoy_core_pid=$!
shell_stop_actual_output="$(bash "${stop_script}" --stop-supervisor \
  --supervisor-pid "${selected_supervisor_pid}" --run-root "${selected_run_root}" \
  --timeout-seconds 3 2>&1)"
grep -Fq "simnow supervisor pid=${selected_supervisor_pid} stopped" <<< "${shell_stop_actual_output}"
wait "${selected_supervisor_pid}" 2>/dev/null || true
selected_supervisor_pid=""
kill -0 "${decoy_core_pid}"

manual_no_identity_output="$(bash "${stop_script}" --run-root \
  "${stop_test_root}/empty-runs" --dry-run 2>&1)"
grep -Fq 'no current core_engine process found' <<< "${manual_no_identity_output}"
kill -0 "${decoy_core_pid}"

if grep -Eq 'ps -eo|find_(core_engine|supervisor)_pids' "${stop_script}"; then
  echo "stop script still contains host-wide process discovery" >&2
  exit 1
fi

if command -v systemd-analyze >/dev/null 2>&1; then
  SYSTEMD_LOG_LEVEL=err systemd-analyze verify \
    "${account_unit}" "${settlement_unit}" "${settlement_timer}" "${legacy_unit}" \
    "${legacy_monitor_unit}"
fi
