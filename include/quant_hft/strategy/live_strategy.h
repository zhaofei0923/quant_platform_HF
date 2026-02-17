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

class ILiveStrategy {
public:
    virtual ~ILiveStrategy() = default;

    virtual void Initialize(const StrategyContext& ctx) = 0;
    virtual std::vector<SignalIntent> OnState(const StateSnapshot7D& state) = 0;
    virtual void OnOrderEvent(const OrderEvent& event) = 0;
    virtual std::vector<SignalIntent> OnTimer(EpochNanos now_ns) = 0;
    virtual void Shutdown() = 0;
};

}  // namespace quant_hft
