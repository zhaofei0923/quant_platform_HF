#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/indicators/atr.h"
#include "quant_hft/strategy/atomic_strategy.h"

namespace quant_hft {

class TrailingStopLoss : public IStopLossStrategy, public IAtomicIndicatorTraceProvider {
   public:
    TrailingStopLoss() = default;

    void Init(const AtomicParams& params) override;
    std::string GetId() const override;
    void Reset() override;
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override;
    std::optional<AtomicIndicatorSnapshot> IndicatorSnapshot() const override;

   private:
    std::string id_{"TrailingStopLoss"};
    std::string instrument_id_;
    int atr_period_{14};
    double stop_loss_multi_{2.0};
    std::unique_ptr<ATR> atr_;
    std::unordered_map<std::string, double> trailing_stop_by_instrument_;
    std::unordered_map<std::string, int> direction_by_instrument_;
    std::optional<double> last_atr_;
};

}  // namespace quant_hft
