#include "quant_hft/services/market_state_detector.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace quant_hft {

MarketStateDetector::MarketStateDetector(const MarketStateDetectorConfig& config)
    : config_(config),
      adx_(config.adx_period),
      kama_(config.kama_er_period, config.kama_fast_period, config.kama_slow_period),
      atr_(config.atr_period) {
    ValidateConfig(config_);
}

void MarketStateDetector::Update(double high, double low, double close) {
    if (!IsFiniteBar(high, low, close)) {
        return;
    }

    adx_.Update(high, low, close);
    kama_.Update(high, low, close);
    atr_.Update(high, low, close);

    last_close_ = close;
    has_last_close_ = true;
    ++bars_seen_;
    current_regime_ = DetermineRegime();
}

MarketRegime MarketStateDetector::GetRegime() const { return current_regime_; }

std::optional<double> MarketStateDetector::GetKAMA() const { return kama_.Value(); }

std::optional<double> MarketStateDetector::GetATR() const { return atr_.Value(); }

std::optional<double> MarketStateDetector::GetADX() const { return adx_.Value(); }

std::optional<double> MarketStateDetector::GetKAMAER() const { return kama_.EfficiencyRatio(); }

std::optional<double> MarketStateDetector::GetATRRatio() const { return ComputeAtrRatio(); }

void MarketStateDetector::Reset() {
    adx_.Reset();
    kama_.Reset();
    atr_.Reset();
    last_close_ = 0.0;
    has_last_close_ = false;
    bars_seen_ = 0;
    current_regime_ = MarketRegime::kUnknown;
}

void MarketStateDetector::ValidateConfig(const MarketStateDetectorConfig& config) {
    if (config.adx_period <= 0 || config.kama_er_period <= 0 || config.kama_fast_period <= 0 ||
        config.kama_slow_period <= 0 || config.atr_period <= 0 || config.min_bars_for_flat <= 0) {
        throw std::invalid_argument("market state detector periods must be positive");
    }
    if (!std::isfinite(config.adx_strong_threshold) || !std::isfinite(config.adx_weak_lower) ||
        !std::isfinite(config.adx_weak_upper) || !std::isfinite(config.kama_er_strong) ||
        !std::isfinite(config.kama_er_weak_lower) || !std::isfinite(config.atr_flat_ratio)) {
        throw std::invalid_argument("market state detector thresholds must be finite");
    }
    if (config.adx_weak_lower > config.adx_weak_upper ||
        config.adx_weak_upper > config.adx_strong_threshold) {
        throw std::invalid_argument("invalid ADX threshold ordering");
    }
    if (config.kama_er_weak_lower > config.kama_er_strong) {
        throw std::invalid_argument("invalid KAMA ER threshold ordering");
    }
    if (config.kama_er_weak_lower < 0.0 || config.kama_er_strong > 1.0) {
        throw std::invalid_argument("KAMA ER thresholds must be in [0, 1]");
    }
    if (config.atr_flat_ratio < 0.0) {
        throw std::invalid_argument("atr_flat_ratio must be non-negative");
    }
}

bool MarketStateDetector::IsFiniteBar(double high, double low, double close) {
    return std::isfinite(high) && std::isfinite(low) && std::isfinite(close);
}

std::optional<double> MarketStateDetector::ComputeAtrRatio() const {
    if (!atr_.IsReady() || !has_last_close_ || std::fabs(last_close_) <= 1e-12) {
        return std::nullopt;
    }
    const auto atr = atr_.Value();
    if (!atr.has_value()) {
        return std::nullopt;
    }
    return *atr / std::fabs(last_close_);
}

MarketRegime MarketStateDetector::DetermineRegime() const {
    const auto atr_ratio = ComputeAtrRatio();
    if (atr_ratio.has_value() &&
        bars_seen_ >= static_cast<std::size_t>(config_.min_bars_for_flat) &&
        config_.atr_flat_ratio > 0.0 && *atr_ratio < config_.atr_flat_ratio) {
        return MarketRegime::kFlat;
    }

    const auto adx = adx_.Value();
    const auto kama_er = kama_.EfficiencyRatio();

    if (!adx.has_value()) {
        if (config_.require_adx_for_trend || !kama_er.has_value()) {
            return MarketRegime::kUnknown;
        }
        if (*kama_er > config_.kama_er_strong) {
            return MarketRegime::kStrongTrend;
        }
        if (*kama_er >= config_.kama_er_weak_lower) {
            return MarketRegime::kWeakTrend;
        }
        return MarketRegime::kRanging;
    }

    bool is_strong_trend = *adx > config_.adx_strong_threshold;
    bool is_weak_trend = *adx >= config_.adx_weak_lower && *adx <= config_.adx_weak_upper;

    if (config_.use_kama_er && kama_er.has_value()) {
        if (*kama_er > config_.kama_er_strong) {
            is_strong_trend = true;
            is_weak_trend = false;
        } else if (*kama_er < config_.kama_er_weak_lower) {
            is_weak_trend = false;
        }
    }

    if (is_strong_trend) {
        return MarketRegime::kStrongTrend;
    }
    if (is_weak_trend) {
        return MarketRegime::kWeakTrend;
    }
    return MarketRegime::kRanging;
}

}  // namespace quant_hft
