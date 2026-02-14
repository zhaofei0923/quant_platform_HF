# WAL Recovery Runbook

## Goal
- Rebuild in-memory order state and position state after process restart.
- Verify replay correctness before reconnecting live trading flow.

## Components
- Writer: `LocalWalRegulatorySink`
- Replayer: `WalReplayLoader`
- Validation CLI: `wal_replay_tool`

## Steps

### 1. Build
```bash
cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON
cmake --build build -j
```

### 2. Offline replay verification
```bash
./build/wal_replay_tool runtime_events.wal
```

Expected output pattern:
```text
WAL replay completed path=runtime_events.wal lines=<N> events=<M> ignored=<I> parse_errors=<P> state_rejected=<R> ledger_applied=<L>
```

Acceptance:
- `parse_errors=0`
- `state_rejected=0`
- `events` and `ledger_applied` are non-zero for non-empty order/trade WAL
- `ignored` is allowed for non-order WAL kinds (for example `kind=rollover` audit lines)

### 3. Start core engine
- `core_engine` startup now runs WAL replay first, then starts live path.
- If replay has parse/state errors, keep process in observation mode and inspect WAL anomalies before enabling live order flow.

## Legacy compatibility
- Replay supports historical WAL lines that only include:
  - `seq`, `kind`, `ts_ns`, `account_id`, `client_order_id`, `instrument_id`, `status`, `filled_volume`
- New WAL lines include extended fields:
  - `exchange_order_id`, `total_volume`, `avg_fill_price`, `reason`, `trace_id`

## Operational notes
- Keep WAL on durable storage.
- Archive rotated WAL files before truncation.
- For incident analysis, preserve the original WAL and replay a copy.

## Recovery Evidence Template (RTO/RPO)

Record after each drill:

```text
drill_id=<YYYYMMDD-HHMM>
release_version=<vX.Y.Z or timestamp>
wal_path=<path>
failure_start_utc=<ISO8601>
recovery_complete_utc=<ISO8601>
rto_seconds=<measured>
rpo_events=<lost_or_replayed_gap_count>
operator=<name>
result=<pass|fail>
notes=<short summary>
```

Template file:
- `docs/templates/WAL_RECOVERY_RESULT.md`

Machine verification:

```bash
python3 scripts/ops/verify_wal_recovery_evidence.py \
  --evidence-file docs/results/wal_recovery_result.env \
  --max-rto-seconds 10 \
  --max-rpo-events 0
```

Acceptance target:
- RTO measured and documented for every drill
- RPO explicitly measured (expected 0 for clean WAL replay path)

## Multi-Host Failover Drill Linkage

WAL recovery drills should be paired with multi-host failover evidence in the same operation window:

```bash
bash scripts/infra/bootstrap_prodlike_multi_host.sh \
  --compose-file infra/docker-compose.prodlike.multi-host.yaml \
  --project-name quant-hft-prodlike-multi-host \
  --dry-run \
  --output-file docs/results/prodlike_multi_host_bootstrap_result.env

python3 scripts/ops/failover_orchestrator.py \
  --env-config configs/deploy/environments/prodlike_multi_host.yaml \
  --output-file docs/results/failover_result.env

python3 scripts/ops/verify_failover_evidence.py \
  --evidence-file docs/results/failover_result.env \
  --max-failover-seconds 300 \
  --max-data-lag-events 0
```
