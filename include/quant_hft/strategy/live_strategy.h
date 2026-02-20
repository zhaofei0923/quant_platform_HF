#pragma once

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

using StrategyState = std::unordered_map<std::string, std::string>;

class ILiveStrategy {
public:
    virtual ~ILiveStrategy() = default;

    virtual void Initialize(const StrategyContext& ctx) = 0;
    virtual std::vector<SignalIntent> OnState(const StateSnapshot7D& state) = 0;
    virtual void OnOrderEvent(const OrderEvent& event) = 0;
    virtual void OnAccountSnapshot(const TradingAccountSnapshot& snapshot) { (void)snapshot; }
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
    virtual void Shutdown() = 0;
};

}  // namespace quant_hft
