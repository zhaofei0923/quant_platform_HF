# System Architecture Overview (Pure C++)

## Runtime Topology

- `core_engine` is the primary process for market data, strategy dispatch, risk, and execution.
- `StrategyEngine` runs in-process and drives `ILiveStrategy` instances.
- `ExecutionPlanner + Risk + ExecutionEngine` handle order planning and routing.

## Data and Event Flow

1. Market snapshots enter `core_engine`.
2. State snapshots and bar updates are dispatched to `StrategyEngine`.
3. Strategies emit `SignalIntent`.
4. Intents flow through risk checks and execution planning.
5. Order/trade events update storage and evidence outputs.

## Backtest and Simulation

- `backtest_cli` performs deterministic replay and emits JSON/Markdown reports.
- `simnow_compare_cli` and `simnow_weekly_stress_cli` provide parity and drift checks.

## Ops and Evidence

- `reconnect_evidence_cli` produces reconnect + health + alert artifacts.
- `ops_health_report_cli` and `ops_alert_report_cli` generate SLI/SLO outputs.
- `ctp_cutover_orchestrator_cli` produces cutover/rollback env evidence files.

## Repository Rules

- No Python runtime assets are allowed.
- CI gates include dependency audit and repository purity check.
- Contract and requirement synchronization are verified via C++ CLIs.
