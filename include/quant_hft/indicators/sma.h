#pragma once

#include <deque>
#include <optional>

#include "quant_hft/indicators/indicator.h"

namespace quant_hft {

class SMA : public Indicator {
   public:
    explicit SMA(int period);

    void Update(double high, double low, double close, double volume = 0.0) override;
    std::optional<double> Value() const override;
    bool IsReady() const override;
    void Reset() override;

   private:
    int period_;
    std::deque<double> prices_;
    double sum_{0.0};
};

}  // namespace quant_hft
