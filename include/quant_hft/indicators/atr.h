#pragma once

#include <deque>
#include <optional>
#include <vector>

#include "quant_hft/indicators/indicator.h"

namespace quant_hft {

class ATR : public Indicator {
   public:
    struct State {
        bool initialized{false};
        bool has_prev_close{false};
        double prev_close{0.0};
        std::vector<double> tr_seed;
        double tr_seed_sum{0.0};
        double atr{0.0};
    };

    explicit ATR(int period);

    void Update(double high, double low, double close, double volume = 0.0) override;
    std::optional<double> Value() const override;
    bool IsReady() const override;
    void Reset() override;
    State ExportState() const;
    bool ImportState(const State& state);

   private:
    int period_;
    bool initialized_{false};
    bool has_prev_close_{false};
    double prev_close_{0.0};
    std::deque<double> tr_seed_;
    double tr_seed_sum_{0.0};
    double atr_{0.0};
};

}  // namespace quant_hft
