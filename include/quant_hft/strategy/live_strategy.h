#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct StrategyContext {
    std::string strategy_id;
    std::string account_id;
    std::unordered_map<std::string, std::string> metadata;
};

struct StrategyMetric {
    std::string name;
    double value{0.0};
    std::unordered_map<std::string, std::string> labels;
};

struct ContractSwitchContext {
    std::string product_id;
    std::string previous_instrument_id;
    std::string current_instrument_id;
    std::uint64_t generation{0};
};

using StrategyState = std::unordered_map<std::string, std::string>;

class ILiveStrategy {
   public:
    virtual ~ILiveStrategy() = default;

    virtual void Initialize(const StrategyContext& ctx) = 0;
    virtual std::vector<SignalIntent> OnState(const StateSnapshot7D& state) = 0;
    virtual std::vector<SignalIntent> OnMarketTick(const MarketSnapshot& snapshot) {
        (void)snapshot;
        return {};
    }
    virtual void OnOrderEvent(const OrderEvent& event) = 0;
    virtual void OnAccountSnapshot(const TradingAccountSnapshot& snapshot) { (void)snapshot; }
    // Reconcile the strategy's believed net positions against an authoritative
    // (broker-truth) signed net position map keyed by instrument id. Returns the
    // number of instruments adjusted. `authoritative_avg_open`, when populated,
    // provides broker-derived average open prices used to backfill the open price
    // of reconcile-sourced positions so risk logic (e.g. trailing stops) can run.
    // `adjustments`, when non-null, receives a human-readable description of each
    // adjustment for auditing.
    virtual std::size_t ReconcileNetPositions(
        const std::unordered_map<std::string, std::int32_t>& authoritative_net,
        const std::unordered_map<std::string, double>& authoritative_avg_open,
        std::vector<std::string>* adjustments) {
        (void)authoritative_net;
        (void)authoritative_avg_open;
        (void)adjustments;
        return 0;
    }
    virtual std::vector<SignalIntent> OnTimer(EpochNanos now_ns) = 0;
    virtual std::vector<StrategyMetric> CollectMetrics() const { return {}; }
    virtual bool SaveState(StrategyState* out, std::string* error) const {
        (void)out;
        (void)error;
        return true;
    }
    virtual bool LoadState(const StrategyState& state, std::string* error) {
        (void)state;
        (void)error;
        return true;
    }
    // Resets only market-derived state for a flat-only dominant-contract switch.  Account and
    // daily risk state must be preserved.  The default keeps non-composite strategies source
    // compatible and reports no special reset requirement.
    virtual bool ResetForContractSwitch(const ContractSwitchContext& context, std::string* error) {
        (void)context;
        (void)error;
        return true;
    }
    virtual std::int32_t RequiredContractWarmupBars(const ContractSwitchContext& context) const {
        (void)context;
        return 0;
    }
    virtual void Shutdown() = 0;
};

}  // namespace quant_hft
