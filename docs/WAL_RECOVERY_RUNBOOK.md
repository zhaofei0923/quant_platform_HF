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
WAL replay completed path=runtime_events.wal lines=<N> events=<M> parse_errors=<P> state_rejected=<R> ledger_applied=<L>
```

Acceptance:
- `parse_errors=0`
- `state_rejected=0`
- `events` and `ledger_applied` are non-zero for non-empty WAL

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
