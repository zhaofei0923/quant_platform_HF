#pragma once

#include <string>
#include <vector>

#include "quant_hft/strategy/atomic_strategy.h"

namespace quant_hft {

class MaxPositionRiskControl : public IRiskControlStrategy {
   public:
    MaxPositionRiskControl() = default;

    void Init(const AtomicParams& params) override;
    std::string GetId() const override;
    void Reset() override;
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override;

   private:
    std::string id_{"MaxPositionRiskControl"};
    int max_abs_position_{1};
};

}  // namespace quant_hft
