#!/usr/bin/env bash
set -u
cd /home/kevin/quant_platform_HF
evidence=/tmp/codex-trading-smoke-20260906-verified
engine=/tmp/quant-hft-trading-build/core_engine
run_engine() {
    env -i PATH=/usr/bin:/bin HOME="$evidence/home" TZ=UTC \
        QUANT_HFT_RUNTIME_ROOT="$evidence/runtime" QUANT_HFT_INSTANCE=smoke \
        QUANT_HFT_WAL_FILE="$evidence/wal/events.wal" \
        QUANT_HFT_READINESS_FILE="$evidence/readiness.json" \
        QUANT_HFT_PENDING_EXIT_WAL="$evidence/pending_exit/events.jsonl" \
        QUANT_HFT_MARKET_DATA_DIR="$evidence/market" \
        QUANT_HFT_SIMULATED_TRADING_DAY=20260906 \
        QUANT_HFT_REDIS_MODE=in_memory QUANT_HFT_TIMESCALE_MODE=in_memory \
        QUANT_HFT_CLICKHOUSE_MODE=in_memory QUANT_HFT_MARKET_BUS_MODE=disabled \
        RISK_RULE_FILE_PATH=/home/kevin/quant_platform_HF/configs/risk_rules.yaml \
        timeout --kill-after=5s 45s "$engine" --config "$evidence/offline.yaml" --run-seconds "$1"
}
run_engine 2 > "$evidence/first.log" 2>&1
first=$?
printf 'first_exit=%s\n' "$first" > "$evidence/outcomes.txt"
if [ "$first" -ne 0 ]; then cat "$evidence/outcomes.txt"; exit "$first"; fi
cp "$evidence/readiness.json" "$evidence/first-readiness.json"
cp "$evidence/runtime/sim/TEST_BROKER/TEST_ACCOUNT/smoke/identity.manifest" "$evidence/first-identity.manifest"
run_engine 8 > "$evidence/restart.log" 2>&1 &
running=$!
for attempt in $(seq 1 100); do
    if grep -q 'market_data_recorder_started' "$evidence/restart.log"; then break; fi
    if ! kill -0 "$running" 2>/dev/null; then break; fi
    sleep 0.1
done
run_engine 1 > "$evidence/second-instance.log" 2>&1
second=$?
printf 'second_instance_exit=%s\n' "$second" >> "$evidence/outcomes.txt"
wait "$running"
restart=$?
printf 'restart_exit=%s\n' "$restart" >> "$evidence/outcomes.txt"
if [ -f "$evidence/readiness.json" ]; then cp "$evidence/readiness.json" "$evidence/restart-readiness.json"; fi
cmp -s "$evidence/runtime/sim/TEST_BROKER/TEST_ACCOUNT/smoke/identity.manifest" "$evidence/first-identity.manifest"
identity=$?
printf 'identity_compare_exit=%s\n' "$identity" >> "$evidence/outcomes.txt"
cat "$evidence/outcomes.txt"
test "$restart" -eq 0 && test "$second" -eq 7 && test "$identity" -eq 0
