# Ops Health Report

- Scope: core_engine + strategy_engine + storage
- Generated TS (ns): 1771315781043498716
- Overall healthy: yes

## SLI
| Name | Value | Target | Healthy | Detail |
|---|---:|---:|---|---|
| quant_hft_core_process_alive | 1 | 1 | yes | probe process stayed alive during collection |
| quant_hft_strategy_engine_latency_p99_ms | 0 | 1500 | yes | derived from reconnect recovery samples |
| quant_hft_strategy_engine_chain_integrity | 1 | 1 | yes | input=complete |
| quant_hft_storage_redis_health | 1 | 1 | yes | input=healthy |
| quant_hft_storage_timescale_health | 1 | 1 | yes | input=healthy |
| quant_hft_storage_postgres_health | 1 | 1 | yes | input=healthy |

## Metadata
- environment: unknown
- service: core_engine
- strategy_engine_chain_source: in_process
- strategy_engine_intent_count: 1
- strategy_engine_order_key_count: 1
- strategy_engine_state_key_count: 2
