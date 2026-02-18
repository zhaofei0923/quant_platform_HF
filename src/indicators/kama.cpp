#include "quant_hft/indicators/kama.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace quant_hft {

KAMA::KAMA(int er_period, int fast_period, int slow_period)
    : er_period_(er_period),
      fast_period_(fast_period),
      slow_period_(slow_period),
      fast_sc_(2.0 / static_cast<double>(fast_period_ + 1)),
      slow_sc_(2.0 / static_cast<double>(slow_period_ + 1)) {
    if (er_period_ <= 0 || fast_period_ <= 0 || slow_period_ <= 0) {
        throw std::invalid_argument("KAMA periods must be positive");
    }
}

void KAMA::Update(double high, double low, double close, double volume) {
    (void)high;
    (void)low;
    (void)volume;

    if (!std::isfinite(close)) {
        return;
    }

    if (!closes_.empty()) {
        volatility_sum_ += std::fabs(close - closes_.back());
    }
    closes_.push_back(close);

    const std::size_t window_size = static_cast<std::size_t>(er_period_ + 1);
    if (closes_.size() > window_size) {
        volatility_sum_ -= std::fabs(closes_[1] - closes_[0]);
        closes_.pop_front();
        volatility_sum_ = std::max(0.0, volatility_sum_);
    }

    if (closes_.size() < window_size) {
        return;
    }

    const double change = std::fabs(closes_.back() - closes_.front());
    const double er = volatility_sum_ > 0.0 ? change / volatility_sum_ : 0.0;
    efficiency_ratio_ = er;
    has_efficiency_ratio_ = true;

    if (!initialized_) {
        double seed_sum = 0.0;
        for (double value : closes_) {
            seed_sum += value;
        }
        kama_ = seed_sum / static_cast<double>(window_size);
        initialized_ = true;
        return;
    }

    const double smoothing = std::pow(er * (fast_sc_ - slow_sc_) + slow_sc_, 2.0);
    kama_ = kama_ + smoothing * (close - kama_);
}

std::optional<double> KAMA::Value() const {
    if (!initialized_) {
        return std::nullopt;
    }
    return kama_;
}

std::optional<double> KAMA::EfficiencyRatio() const {
    if (!has_efficiency_ratio_) {
        return std::nullopt;
    }
    return efficiency_ratio_;
}

bool KAMA::IsReady() const { return initialized_; }

void KAMA::Reset() {
    initialized_ = false;
    closes_.clear();
    volatility_sum_ = 0.0;
    has_efficiency_ratio_ = false;
    efficiency_ratio_ = 0.0;
    kama_ = 0.0;
}

}  // namespace quant_hft
