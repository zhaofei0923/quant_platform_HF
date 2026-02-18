#include "quant_hft/indicators/sma.h"

#include <cmath>
#include <stdexcept>

namespace quant_hft {

SMA::SMA(int period) : period_(period) {
    if (period_ <= 0) {
        throw std::invalid_argument("SMA period must be positive");
    }
}

void SMA::Update(double high, double low, double close, double volume) {
    (void)high;
    (void)low;
    (void)volume;

    if (!std::isfinite(close)) {
        return;
    }

    if (prices_.size() >= static_cast<std::size_t>(period_)) {
        sum_ -= prices_.front();
        prices_.pop_front();
    }
    prices_.push_back(close);
    sum_ += close;
}

std::optional<double> SMA::Value() const {
    if (!IsReady()) {
        return std::nullopt;
    }
    return sum_ / static_cast<double>(period_);
}

bool SMA::IsReady() const { return prices_.size() >= static_cast<std::size_t>(period_); }

void SMA::Reset() {
    prices_.clear();
    sum_ = 0.0;
}

}  // namespace quant_hft
