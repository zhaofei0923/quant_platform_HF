#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "quant_hft/indicators/atr.h"
#include "quant_hft/indicators/kama.h"
#include "quant_hft/strategy/atomic_strategy.h"

namespace quant_hft {

class KamaTrendOpening : public IOpeningStrategy, public IAtomicIndicatorTraceProvider {
   public:
    KamaTrendOpening() = default;

    void Init(const AtomicParams& params) override;
    std::string GetId() const override;
    void Reset() override;
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override;
    std::optional<AtomicIndicatorSnapshot> IndicatorSnapshot() const override;

   private:
    int ClassifyDiff(double diff, double threshold) const;
    int ComputeOrderVolume(const AtomicStrategyContext& ctx, const std::string& instrument_id,
                           double atr_value) const;
    double ComputeStdKama() const;

    std::string id_{"KamaTrendOpening"};
    int er_period_{10};
    int fast_period_{2};
    int slow_period_{30};
    int std_period_{10};
    double kama_filter_{0.5};
    int atr_period_{14};
    double stop_loss_multi_{2.0};
    int default_volume_{1};

    std::unique_ptr<KAMA> kama_;
    std::unique_ptr<ATR> atr_;
    std::deque<double> kama_recent_;
    std::deque<double> kama_window_;
    double kama_window_sum_{0.0};
    double kama_window_sum_sq_{0.0};
    std::optional<double> last_kama_;
    std::optional<double> last_atr_;
    std::optional<double> last_er_;
};

}  // namespace quant_hft
