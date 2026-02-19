#pragma once

#include <memory>
#include <string>
#include <vector>

#include "quant_hft/indicators/kama.h"
#include "quant_hft/strategy/atomic_strategy.h"

namespace quant_hft {

class TrendOpening : public IOpeningStrategy, public IAtomicIndicatorTraceProvider {
   public:
    TrendOpening() = default;

    void Init(const AtomicParams& params) override;
    std::string GetId() const override;
    void Reset() override;
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override;
    std::optional<AtomicIndicatorSnapshot> IndicatorSnapshot() const override;

   private:
    std::string id_{"TrendOpening"};
    std::string instrument_id_;
    int er_period_{10};
    int fast_period_{2};
    int slow_period_{30};
    int volume_{1};
    std::unique_ptr<KAMA> kama_;
};

}  // namespace quant_hft
