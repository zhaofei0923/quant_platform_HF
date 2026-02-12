# M2 Single-Host Delivery Notes

## Delivered Components

- `single-host-m2` compose profile with Kafka/Connect/ClickHouse.
- Kafka topic bootstrap and ClickHouse schema bootstrap scripts.
- Debezium connector register/check scripts and default connector config.
- Core engine market bus producer interface and local spool replay path.
- Partitioned parquet export script and object-store lifecycle runner.
- M2 evidence verifier: `scripts/ops/verify_m2_dataflow_evidence.py`.

## Recommended Evidence Bundle

- `docs/results/prodlike_bootstrap_result.env`
- `docs/results/prodlike_health_report.json`
- `docs/results/debezium_connectors_health_report.json`
- `docs/results/parquet_partitions_report.json`
- `docs/results/data_lifecycle_report.json`
- `docs/results/data_reconcile_report.json`
- `docs/results/ctp_readiness.json`
