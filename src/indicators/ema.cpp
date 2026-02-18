#include "quant_hft/indicators/ema.h"

#include <cmath>
#include <stdexcept>

namespace quant_hft {

EMA::EMA(int period) : period_(period), alpha_(2.0 / static_cast<double>(period_ + 1)) {
    if (period_ <= 0) {
        throw std::invalid_argument("EMA period must be positive");
    }
}

void EMA::Update(double high, double low, double close, double volume) {
    (void)high;
    (void)low;
    (void)volume;

    if (!std::isfinite(close)) {
        return;
    }

    if (!initialized_) {
        seed_prices_.push_back(close);
        if (seed_prices_.size() < static_cast<std::size_t>(period_)) {
            return;
        }
        if (seed_prices_.size() > static_cast<std::size_t>(period_)) {
            seed_prices_.pop_front();
        }

        double sum = 0.0;
        for (double value : seed_prices_) {
            sum += value;
        }
        ema_ = sum / static_cast<double>(period_);
        initialized_ = true;
        return;
    }

    ema_ = ema_ + alpha_ * (close - ema_);
}

std::optional<double> EMA::Value() const {
    if (!initialized_) {
        return std::nullopt;
    }
    return ema_;
}

bool EMA::IsReady() const { return initialized_; }

void EMA::Reset() {
    initialized_ = false;
    seed_prices_.clear();
    ema_ = 0.0;
}

}  // namespace quant_hft
