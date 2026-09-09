# Real CTP build and isolated PostgreSQL validation

Date: 2026-09-09. This is compilation and isolated database evidence, not a SimNow or live-trading acceptance run.

## Build

- Source: `/home/kevin/worktrees/quant_platform_HF-split` effective migration worktree.
- Build: `/home/kevin/quant_split_evidence/20260909/online-real-build`.
- Compiler: GCC 11.4.0; C++17; Ninja; Release.
- Flags: `QUANT_HFT_ENABLE_CTP_REAL_API=ON`, `QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL=ON`, `QUANT_HFT_BUILD_TESTS=ON`.
- Installed strategy package: `/home/kevin/quant_packages/1.0.0`.
- Package manifest SHA256 embedded in this build: `c13b174e0aed50002c85c61026613d1937ac554255d3ae6f203159fd9fef6cd4`.
- External SDK, read only: `/home/kevin/quant_platform_HF/ctp_api/v6.7.11_20250617_api_traderapi_se_linux64`.
- Targets built successfully: `core_engine`, `atomic_trade_postgres_test`.
- `ldd core_engine` resolves both CTP shared libraries from that external SDK directory; no unresolved library was reported.
- No trading executable was launched for this validation. No front connection, account login, or order was attempted.

SHA256 of the tested binaries:

| Artifact | SHA256 |
|---|---|
| `core_engine` | `6068030d0e1cc03bf30be0c066fee7de086c1824c51f8d2bfa8f637ea1664247` |
| `atomic_trade_postgres_test` | `f38b72513db2c138d3def4b8dd0b8feda2e09162eaf269cd5ce3695a695c58b2` |

Build evidence: [configure](online_real_configure.log), [compile and link](online_real_build.log). Existing unused-parameter and initializer warnings remain; this evidence does not claim a warning-free build.

## External SQL

PostgreSQL 14.24 Ubuntu packages were downloaded and extracted under the evidence directory. Only libpq runtime/development dependencies were installed system-wide; no PostgreSQL package post-install service or default system cluster was created.

The disposable server used:

- Data: `/home/kevin/quant_split_evidence/20260909/postgres-ephemeral-data`.
- Socket: `/home/kevin/quant_split_evidence/20260909/postgres-ephemeral-socket/.s.PGSQL.55439`.
- `listen_addresses=''`: Unix socket only, no TCP listener.
- Socket directory permissions: `0700`; local fixture identity `kevin`.
- Fresh database: `quant_split_check`.
- Applied real migrations: `004_trading_core_domain_tables.sql`, `007_atomic_trade_ledger.sql`, `008_verified_trade_fees.sql`, `009_independent_strategy_books.sql`, plus disposable default partitions for orders, trades and position detail.

The actual libpq integration binary passed **8 of 8 tests, with no skips**. It covered transaction rollback on false and exception, partitioned order upserts, fill deduplication and receipt conflicts, concurrent account writes, verified mixed-date fees, day rollover/outbox persistence, separate economic and physical close allocations, and account-wide reservations across independent strategy owners.

Test evidence: [PostgreSQL integration output](p4_postgres_atomic_tests.log). Full setup and server logs remain under `/home/kevin/quant_split_evidence/20260909`; `validate_postgres.sh` records the exact one-time provisioning commands. The server was stopped by the cleanup trap and `pg_ctl status` confirmed no server running. Fixture data remains for audit; no existing database or service was accessed.

This used PostgreSQL core tables through libpq. It does not validate Timescale extension-specific compression, hypertables, external Redis, production schema migration, historical data migration, or the five-day SimNow run. Later binary or package changes require matching release-build evidence; the hashes above identify this precise validation build.
