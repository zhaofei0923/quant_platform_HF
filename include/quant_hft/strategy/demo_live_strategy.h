#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "quant_hft/strategy/live_strategy.h"

namespace quant_hft {

class DemoLiveStrategy final : public ILiveStrategy {
public:
    void Initialize(const StrategyContext& ctx) override;
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state) override;
    void OnOrderEvent(const OrderEvent& event) override;
    std::vector<SignalIntent> OnTimer(EpochNanos now_ns) override;
    void Shutdown() override;

private:
    std::string strategy_id_;
    std::uint64_t signal_counter_{0};
};

bool RegisterDemoLiveStrategy(std::string* error = nullptr);

}  // namespace quant_hft
