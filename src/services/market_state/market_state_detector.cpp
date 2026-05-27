#include "quant_hft/services/market_state_detector.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <limits>
#include <stdexcept>

#include "quant_hft/contracts/instrument_utils.h"

namespace quant_hft {

std::string NormalizeMarketStateProductId(std::string_view product_id) {
    if (product_id.empty()) {
        return "";
    }
    std::string normalized;
    normalized.reserve(product_id.size());
    for (unsigned char ch : product_id) {
        if (std::isalpha(ch) == 0) {
            return "";
        }
        normalized.push_back(static_cast<char>(std::tolower(ch)));
    }
    return normalized;
}

std::string MarketStateProductIdFromInstrument(std::string_view instrument_id) {
    return NormalizeMarketStateProductId(ExtractProductIdFromInstrumentId(instrument_id));
}

const MarketStateDetectorConfig& ResolveMarketStateDetectorConfig(
    std::string_view instrument_id, const MarketStateDetectorConfig& global_config,
    const MarketStateDetectorConfigByProduct& by_product_config) {
    const std::string product_id = MarketStateProductIdFromInstrument(instrument_id);
    if (!product_id.empty()) {
        const auto it = by_product_config.find(product_id);
        if (it != by_product_config.end()) {
            return it->second;
        }
    }
    return global_config;
}

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
    const DetectionDecision decision = DetermineRegime();
    current_regime_ = decision.regime;
    decision_reason_ = decision.reason;
}

MarketRegime MarketStateDetector::GetRegime() const { return current_regime_; }

std::optional<double> MarketStateDetector::GetKAMA() const { return kama_.Value(); }

std::optional<double> MarketStateDetector::GetATR() const { return atr_.Value(); }

std::optional<double> MarketStateDetector::GetADX() const { return adx_.Value(); }

std::optional<double> MarketStateDetector::GetKAMAER() const { return kama_.EfficiencyRatio(); }

std::optional<double> MarketStateDetector::GetATRRatio() const { return ComputeAtrRatio(); }

std::size_t MarketStateDetector::GetBarsSeen() const noexcept { return bars_seen_; }

const std::string& MarketStateDetector::GetDecisionReason() const noexcept {
    return decision_reason_;
}

void MarketStateDetector::Reset() {
    adx_.Reset();
    kama_.Reset();
    atr_.Reset();
    last_close_ = 0.0;
    has_last_close_ = false;
    bars_seen_ = 0;
    current_regime_ = MarketRegime::kUnknown;
    decision_reason_ = "adx_warmup";
}

MarketStateDetector::State MarketStateDetector::ExportState() const {
    State state;
    state.adx = adx_.ExportState();
    state.kama = kama_.ExportState();
    state.atr = atr_.ExportState();
    state.last_close = last_close_;
    state.has_last_close = has_last_close_;
    state.bars_seen = bars_seen_;
    state.current_regime = current_regime_;
    state.decision_reason = decision_reason_;
    return state;
}

bool MarketStateDetector::ImportState(const State& state) {
    if (!std::isfinite(state.last_close)) {
        return false;
    }
    if (!adx_.ImportState(state.adx) || !kama_.ImportState(state.kama) ||
        !atr_.ImportState(state.atr)) {
        return false;
    }
    last_close_ = state.last_close;
    has_last_close_ = state.has_last_close;
    bars_seen_ = state.bars_seen;
    current_regime_ = state.current_regime;
    decision_reason_ = state.decision_reason.empty() ? "adx_warmup" : state.decision_reason;
    return true;
}

void MarketStateDetector::ValidateConfig(const MarketStateDetectorConfig& config) {
    if (config.adx_period <= 0 || config.kama_er_period <= 0 || config.kama_fast_period <= 0 ||
        config.kama_slow_period <= 0 || config.atr_period <= 0) {
        throw std::invalid_argument("market state detector periods must be positive");
    }
    if (!std::isfinite(config.adx_strong_threshold) || !std::isfinite(config.adx_weak_lower) ||
        !std::isfinite(config.adx_weak_upper) || !std::isfinite(config.kama_er_strong) ||
        !std::isfinite(config.kama_er_weak_lower)) {
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

MarketStateDetector::DetectionDecision MarketStateDetector::DetermineRegime() const {
    const auto adx = adx_.Value();
    const auto kama_er = kama_.EfficiencyRatio();

    if (!adx.has_value()) {
        if (config_.require_adx_for_trend || !kama_er.has_value()) {
            return {MarketRegime::kUnknown, "adx_warmup"};
        }
        if (*kama_er > config_.kama_er_strong) {
            return {MarketRegime::kStrongTrend, "kama_strong"};
        }
        if (*kama_er >= config_.kama_er_weak_lower) {
            return {MarketRegime::kWeakTrend, "kama_weak"};
        }
        return {MarketRegime::kRanging, "ranging"};
    }

    bool is_strong_trend = *adx > config_.adx_strong_threshold;
    bool is_weak_trend = *adx >= config_.adx_weak_lower && *adx <= config_.adx_weak_upper;

    if (config_.use_kama_er && kama_er.has_value()) {
        if (*kama_er > config_.kama_er_strong) {
            return {MarketRegime::kStrongTrend, "kama_strong"};
        } else if (*kama_er < config_.kama_er_weak_lower) {
            is_weak_trend = false;
        }
    }

    if (is_strong_trend) {
        return {MarketRegime::kStrongTrend, "adx_strong"};
    }
    if (is_weak_trend) {
        return {MarketRegime::kWeakTrend, "adx_weak"};
    }
    return {MarketRegime::kRanging, "ranging"};
}

void PopulateMarketStateDiagnostics(const MarketStateDetector& detector, StateSnapshot7D* state) {
    if (state == nullptr) {
        return;
    }
    const auto adx = detector.GetADX();
    const auto kama_er = detector.GetKAMAER();
    const auto atr_ratio = detector.GetATRRatio();
    state->market_regime = detector.GetRegime();
    state->market_state_adx = adx.has_value() ? *adx : std::numeric_limits<double>::quiet_NaN();
    state->market_state_kama_er =
        kama_er.has_value() ? *kama_er : std::numeric_limits<double>::quiet_NaN();
    state->market_state_atr_ratio =
        atr_ratio.has_value() ? *atr_ratio : std::numeric_limits<double>::quiet_NaN();
    state->market_state_bars_seen = static_cast<std::uint64_t>(detector.GetBarsSeen());
    state->market_state_decision_reason = detector.GetDecisionReason();
}

}  // namespace quant_hft
