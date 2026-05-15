#pragma once

#include <deque>
#include <optional>
#include <vector>

#include "quant_hft/indicators/indicator.h"

namespace quant_hft {

class KAMA : public Indicator {
   public:
    struct State {
        bool initialized{false};
        std::vector<double> closes;
        double volatility_sum{0.0};
        bool has_efficiency_ratio{false};
        double efficiency_ratio{0.0};
        double kama{0.0};
    };

    explicit KAMA(int er_period = 10, int fast_period = 2, int slow_period = 30);

    void Update(double high, double low, double close, double volume = 0.0) override;
    std::optional<double> Value() const override;
    std::optional<double> EfficiencyRatio() const;
    bool IsReady() const override;
    void Reset() override;
    State ExportState() const;
    bool ImportState(const State& state);

   private:
    int er_period_;
    int fast_period_;
    int slow_period_;
    double fast_sc_;
    double slow_sc_;
    bool initialized_{false};
    std::deque<double> closes_;
    double volatility_sum_{0.0};
    bool has_efficiency_ratio_{false};
    double efficiency_ratio_{0.0};
    double kama_{0.0};
};

}  // namespace quant_hft
