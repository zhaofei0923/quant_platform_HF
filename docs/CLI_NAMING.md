# CLI Naming Convention

## Source File Naming

All application entrypoints under `src/apps/` follow:

- `*_main.cpp` for source filenames
- Example: `src/apps/backtest_cli_main.cpp`

## Executable Naming

CMake target names drop the `_main` suffix:

- `src/apps/backtest_cli_main.cpp` -> `backtest_cli`
- `src/apps/ops_health_report_cli_main.cpp` -> `ops_health_report_cli`

This convention keeps entrypoint files consistent while preserving concise binary names.
