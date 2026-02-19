#pragma once

#include <memory>
#include <string>
#include <vector>

#include "quant_hft/indicators/atr.h"
#include "quant_hft/strategy/atomic_strategy.h"

namespace quant_hft {

class ATRTakeProfit : public ITakeProfitStrategy, public IAtomicIndicatorTraceProvider {
   public:
    ATRTakeProfit() = default;

    void Init(const AtomicParams& params) override;
    std::string GetId() const override;
    void Reset() override;
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override;
    std::optional<AtomicIndicatorSnapshot> IndicatorSnapshot() const override;

   private:
    std::string id_{"ATRTakeProfit"};
    int atr_period_{14};
    double atr_multiplier_{2.0};
    std::unique_ptr<ATR> atr_;
};

}  // namespace quant_hft
