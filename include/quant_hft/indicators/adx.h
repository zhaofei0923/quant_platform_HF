#pragma once

#include <optional>

#include "quant_hft/indicators/indicator.h"

namespace quant_hft {

class ADX : public Indicator {
   public:
    explicit ADX(int period = 14);

    void Update(double high, double low, double close, double volume = 0.0) override;
    std::optional<double> Value() const override;
    bool IsReady() const override;
    void Reset() override;

    std::optional<double> PlusDI() const;
    std::optional<double> MinusDI() const;
    std::optional<double> Dx() const;

   private:
    void UpdateDirectionalValues(double tr, double plus_dm, double minus_dm);
    void UpdateAdxFromDx(double dx);

    int period_;

    bool has_prev_bar_{false};
    double prev_high_{0.0};
    double prev_low_{0.0};
    double prev_close_{0.0};

    int seed_count_{0};
    double tr_seed_sum_{0.0};
    double plus_dm_seed_sum_{0.0};
    double minus_dm_seed_sum_{0.0};

    bool di_ready_{false};
    double tr_smoothed_{0.0};
    double plus_dm_smoothed_{0.0};
    double minus_dm_smoothed_{0.0};
    double plus_di_{0.0};
    double minus_di_{0.0};
    double dx_{0.0};

    int dx_seed_count_{0};
    double dx_seed_sum_{0.0};
    bool adx_ready_{false};
    double adx_{0.0};
};

}  // namespace quant_hft
