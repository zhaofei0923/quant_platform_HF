#pragma once

#include <cstddef>
#include <optional>

#include "quant_hft/contracts/types.h"
#include "quant_hft/indicators/adx.h"
#include "quant_hft/indicators/atr.h"
#include "quant_hft/indicators/kama.h"

namespace quant_hft {

struct MarketStateDetectorConfig {
    int adx_period{14};
    double adx_strong_threshold{40.0};
    double adx_weak_lower{25.0};
    double adx_weak_upper{40.0};

    int kama_er_period{10};
    int kama_fast_period{2};
    int kama_slow_period{30};
    double kama_er_strong{0.6};
    double kama_er_weak_lower{0.3};

    int atr_period{14};
    double atr_flat_ratio{0.001};
    bool require_adx_for_trend{true};
    bool use_kama_er{true};
    int min_bars_for_flat{20};
};

class MarketStateDetector {
   public:
    explicit MarketStateDetector(const MarketStateDetectorConfig& config = {});

    void Update(double high, double low, double close);

    MarketRegime GetRegime() const;
    std::optional<double> GetADX() const;
    std::optional<double> GetKAMAER() const;
    std::optional<double> GetATRRatio() const;

    void Reset();

   private:
    static void ValidateConfig(const MarketStateDetectorConfig& config);
    static bool IsFiniteBar(double high, double low, double close);

    std::optional<double> ComputeAtrRatio() const;
    MarketRegime DetermineRegime() const;

    MarketStateDetectorConfig config_;
    ADX adx_;
    KAMA kama_;
    ATR atr_;
    double last_close_{0.0};
    bool has_last_close_{false};
    std::size_t bars_seen_{0};
    MarketRegime current_regime_{MarketRegime::kUnknown};
};

}  // namespace quant_hft
