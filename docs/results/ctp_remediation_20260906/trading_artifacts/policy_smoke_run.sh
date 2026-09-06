#!/usr/bin/env bash
set -eu
cd /home/kevin/quant_platform_HF
evidence=/tmp/codex-policy-smoke-20260906
mkdir -p "$evidence/home"
sed 's@/tmp/codex-trading-smoke-20260906-verified@/tmp/codex-policy-smoke-20260906@g' /tmp/codex-trading-smoke-20260906-verified/offline.yaml > "$evidence/offline.yaml"
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
    timeout --kill-after=5s 30s /tmp/quant-hft-trading-build/core_engine \
      --config "$evidence/offline.yaml" --run-seconds 2 > "$evidence/run.log" 2>&1
printf 'offline_smoke_exit=0\n' > "$evidence/outcome.txt"
grep 'core_engine stopped cleanly' "$evidence/run.log"
cat "$evidence/outcome.txt"
