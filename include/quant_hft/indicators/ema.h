#pragma once

#include <deque>
#include <optional>

#include "quant_hft/indicators/indicator.h"

namespace quant_hft {

class EMA : public Indicator {
   public:
    explicit EMA(int period);

    void Update(double high, double low, double close, double volume = 0.0) override;
    std::optional<double> Value() const override;
    bool IsReady() const override;
    void Reset() override;

   private:
    int period_;
    double alpha_;
    bool initialized_{false};
    std::deque<double> seed_prices_;
    double ema_{0.0};
};

}  // namespace quant_hft
