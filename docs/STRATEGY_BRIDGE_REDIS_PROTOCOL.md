# Strategy Engine In-Process Protocol (Pure C++)

## Status

The historical Redis strategy bridge has been removed from runtime.
Strategy execution is now fully in-process.

## Runtime Contract

1. `core_engine` builds `StateSnapshot7D`.
2. `StrategyEngine` dispatches state/order/timer events to `ILiveStrategy` instances.
3. Strategies emit `SignalIntent` in-process.
4. `ExecutionPlanner + Risk + ExecutionEngine` consume intents.

## Key Interfaces

- `include/quant_hft/strategy/live_strategy.h`
- `include/quant_hft/strategy/strategy_engine.h`
- `include/quant_hft/strategy/strategy_registry.h`

## Redis Role (Optional)

Redis may still be used as an external storage backend when external mode is enabled,
but not as a strategy-process bridge.

## Verification

```bash
ctest --test-dir build -R "(StrategyRegistryTest|StrategyEngineTest|DemoLiveStrategyTest|CallbackDispatcherTest)" --output-on-failure
```
