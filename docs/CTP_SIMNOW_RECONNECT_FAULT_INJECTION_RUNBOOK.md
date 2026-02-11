# CTP SimNow Reconnect Fault Injection Runbook

## Goal
- Validate reconnect resilience and recovery SLO for real CTP mode.
- Target SLO:
  - CTP disconnect auto-recovery P99 `< 10s`
  - No process crash
  - Gateway health flips to unhealthy during fault and returns healthy after recovery

## Scope
- Environment: SimNow only.
  - 7x24 profile uses eval key-mode: `is_production_mode=false` (`configs/sim/ctp.yaml`)
  - trading-hours fronts (3000x) use production key-mode: `is_production_mode=true`
    (`configs/sim/ctp_trading_hours*.yaml`) because the official front_se uses monitoring-center
    production secret key (CTP v6.7.11+ merged production/eval and selects key-mode via config).
- Gateway mode: `enable_real_api=true`
- Fault types:
  - hard disconnect (iptables DROP for SimNow IP/ports)
  - latency/jitter (tc netem delay+jitter)
  - packet loss (tc netem loss)

## Prerequisites
1. Build real API binary:
```bash
cmake -S . -B build-real -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_ENABLE_CTP_REAL_API=ON
cmake --build build-real -j
```
2. Set credentials:
```bash
export CTP_SIM_PASSWORD='<your_password>'
```
3. Confirm config profile (example 7x24):
   - `configs/sim/ctp.yaml`
4. Use a test host where network shaping is allowed.
5. Host tooling:
   - `iptables` (for disconnect fault)
     - the runner auto-resolves `/usr/sbin/iptables` even when `sudo` PATH is restricted
     - also supports `iptables-nft` fallback
   - `tc` from `iproute2` (for latency/jitter/loss fault)
   - root privilege (`sudo`) when running with `--execute`

## Tooling
- Preflight checker:
  - `scripts/ops/ctp_preflight_check.py`
- Fault injection CLI:
  - `scripts/ops/ctp_fault_inject.py`
- SLO report generator:
  - `scripts/ops/reconnect_slo_report.py`
- One-shot evidence orchestrator:
  - `scripts/ops/run_reconnect_evidence.py`
- Dry-run command plan example:
```bash
scripts/ops/ctp_fault_inject.py plan --scenario disconnect
```

## Execution Procedure

### Cleanup (recommended)
Before each evidence run, either:
- delete old logs:
  - `rm -f runtime/fault_events.jsonl runtime/reconnect_probe.log`
- or pass build-specific paths via `--event-log/--probe-log/--report-file` to avoid mixing runs.

### 0) Optional one-shot mode
```bash
.venv/bin/python scripts/ops/run_reconnect_evidence.py \
  --config configs/sim/ctp_trading_hours.yaml \
  --execute-faults \
  --use-sudo \
  --build "build-real-$(date +%Y%m%d)"
```
Outputs:
- `runtime/reconnect_probe.log`
- `runtime/fault_events.jsonl`
- `docs/results/reconnect_fault_result.md`

Optional flags:
- `--scenarios disconnect,jitter,loss,combined`
- `--disconnect-mode reset|drop` (default: `reset`)
- `--skip-preflight` (only for temporary bypass/debug)
- `--no-auto-fallback-trading-groups` (disable auto switch to group2/group3)

### 0.5) Recommended preflight before probe
```bash
.venv/bin/python scripts/ops/ctp_preflight_check.py \
  --config configs/sim/ctp_trading_hours.yaml
```
Expected:
- `password_source` is `PASS`
- both `tcp_connect_*` checks are `PASS`

If `tcp_connect_*` shows `connection refused`:
- the selected SimNow front is not accepting connections in current time window
- verify profile (`ctp_trading_hours.yaml` vs `ctp.yaml`)
- switch to `configs/sim/ctp.yaml` (7x24) for connectivity sanity check

If `service_window_hint` shows `reachable groups`:
- rerun with `configs/sim/ctp_trading_hours_group2.yaml` or `configs/sim/ctp_trading_hours_group3.yaml`
- or keep `configs/sim/ctp_trading_hours.yaml`; one-shot runner auto-fallback is enabled by default

### 1) Start probe process (terminal A)
```bash
LD_LIBRARY_PATH=$PWD/ctp_api/v6.7.11_20250617_api_traderapi_se_linux64:$LD_LIBRARY_PATH \
  ./build-real/simnow_probe configs/sim/ctp.yaml --monitor-seconds 900 --health-interval-ms 1000 \
  | tee runtime/reconnect_probe.log
```

### 2) Baseline observation (terminal B)
- Observe for 2-3 minutes:
  - connect success
  - market data prints
  - no crash/exception

### 3) Inject faults

#### A. Hard disconnect (20s)
```bash
sudo scripts/ops/ctp_fault_inject.py run --scenario disconnect --disconnect-mode reset --duration-sec 20 --execute \
  --event-log-file runtime/fault_events.jsonl
```
Notes:
- `--disconnect-mode reset` uses `iptables REJECT --reject-with tcp-reset` so the CTP client observes
  disconnect quickly. Plain `drop` may not flip `OnFrontDisconnected` within 20s due to TCP retries.
Optional overrides:
- force firewall tool:
  - `--firewall-cmd /usr/sbin/iptables-nft`

#### B. Latency + jitter (30s)
```bash
sudo scripts/ops/ctp_fault_inject.py run --scenario jitter --iface eth0 --delay-ms 250 --jitter-ms 30 \
  --duration-sec 30 --execute --event-log-file runtime/fault_events.jsonl
```
Optional overrides:
- force traffic control tool:
  - `--tc-cmd /usr/sbin/tc`

#### C. Packet loss (30s)
```bash
sudo scripts/ops/ctp_fault_inject.py run --scenario loss --iface eth0 --loss-percent 5 \
  --duration-sec 30 --execute --event-log-file runtime/fault_events.jsonl
```

#### D. Combined degradation (30s)
```bash
sudo scripts/ops/ctp_fault_inject.py run --scenario combined --iface eth0 --delay-ms 200 --jitter-ms 40 \
  --loss-percent 3 --duration-sec 30 --execute --event-log-file runtime/fault_events.jsonl
```

### 4) Emergency clear
If any fault injection command exits abnormally, clear immediately:
```bash
sudo scripts/ops/ctp_fault_inject.py clear --scenario disconnect --execute
sudo scripts/ops/ctp_fault_inject.py clear --scenario combined --iface eth0 --execute
```

### 5) Generate evidence report
```bash
.venv/bin/python scripts/ops/reconnect_slo_report.py \
  --fault-events-file runtime/fault_events.jsonl \
  --probe-log-file runtime/reconnect_probe.log \
  --output-file docs/results/reconnect_fault_result.md \
  --operator "$USER" \
  --host "$(hostname)" \
  --config-profile configs/sim/ctp.yaml \
  --interface eth0
```

## Acceptance Checklist
- [ ] Gateway reports unhealthy when fault starts
- [ ] Gateway reconnects automatically after fault clear
- [ ] Recovery duration P99 `< 10s`
- [ ] No process crash in probe/core engine
- [ ] No unrecoverable stuck state after 3 repeated injections

## Evidence Collection
- Fault command timestamps are automatically recorded by `--event-log-file`.
- Probe health timeline is recorded from `[health]` lines in probe log.
- `reconnect_slo_report.py` auto-calculates per-scenario recovery seconds and P99.
- Optional manual narrative can still use:
  - `docs/templates/RECONNECT_FAULT_INJECTION_RESULT.md`

## Notes
- `tc netem` affects interface-level traffic. Run on isolated test host.
- Never run this runbook against production endpoints.

## Troubleshooting

- `iptables: not found`
  - the runner resolves common locations (`/usr/sbin/iptables`, `/usr/sbin/iptables-nft`) automatically
  - if the binary is truly missing, install package:
    - `sudo apt-get update && sudo apt-get install -y iptables`
  - or temporarily skip disconnect scenario:
    - `--scenarios jitter,loss,combined`
- `tcp_connect_*: connection refused`
  - remote front not listening / blocked in current time window
  - retry with 7x24 profile:
    - `.venv/bin/python scripts/ops/ctp_preflight_check.py --config configs/sim/ctp.yaml`
- `tcp_connect_*: timed out`
  - trading-hours front (`3000x`) often follows production session window
  - if preflight shows `service_window_hint` with reachable groups, switch config to that group
  - if outside session, switch to 7x24 profile for connectivity checks:
    - `.venv/bin/python scripts/ops/ctp_preflight_check.py --config configs/sim/ctp.yaml`
- `Decrypt handshake data failed`
  - look-through front handshake/auth flow likely rejected
  - try disable terminal auth for simulation account:
    - set `enable_terminal_auth: false` in selected config and retry
- generic `connect failed`
  - check probe output `Connect diagnostic:` for per-front attempts and CTP `ErrorID/ErrorMsg`
