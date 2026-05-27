#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

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
    bool require_adx_for_trend{true};
    bool use_kama_er{true};
};

using MarketStateDetectorConfigByProduct =
    std::unordered_map<std::string, MarketStateDetectorConfig>;

std::string NormalizeMarketStateProductId(std::string_view product_id);
std::string MarketStateProductIdFromInstrument(std::string_view instrument_id);
const MarketStateDetectorConfig& ResolveMarketStateDetectorConfig(
    std::string_view instrument_id, const MarketStateDetectorConfig& global_config,
    const MarketStateDetectorConfigByProduct& by_product_config);

class MarketStateDetector {
   public:
    struct State {
        ADX::State adx;
        KAMA::State kama;
        ATR::State atr;
        double last_close{0.0};
        bool has_last_close{false};
        std::size_t bars_seen{0};
        MarketRegime current_regime{MarketRegime::kUnknown};
        std::string decision_reason{"adx_warmup"};
    };

    explicit MarketStateDetector(const MarketStateDetectorConfig& config = {});

    void Update(double high, double low, double close);

    MarketRegime GetRegime() const;
    std::optional<double> GetKAMA() const;
    std::optional<double> GetATR() const;
    std::optional<double> GetADX() const;
    std::optional<double> GetKAMAER() const;
    std::optional<double> GetATRRatio() const;
    std::size_t GetBarsSeen() const noexcept;
    const std::string& GetDecisionReason() const noexcept;

    void Reset();
    State ExportState() const;
    bool ImportState(const State& state);

   private:
    struct DetectionDecision {
        MarketRegime regime{MarketRegime::kUnknown};
        const char* reason{"adx_warmup"};
    };

    static void ValidateConfig(const MarketStateDetectorConfig& config);
    static bool IsFiniteBar(double high, double low, double close);

    std::optional<double> ComputeAtrRatio() const;
    DetectionDecision DetermineRegime() const;

    MarketStateDetectorConfig config_;
    ADX adx_;
    KAMA kama_;
    ATR atr_;
    double last_close_{0.0};
    bool has_last_close_{false};
    std::size_t bars_seen_{0};
    MarketRegime current_regime_{MarketRegime::kUnknown};
    std::string decision_reason_{"adx_warmup"};
};

void PopulateMarketStateDiagnostics(const MarketStateDetector& detector, StateSnapshot7D* state);

}  // namespace quant_hft
