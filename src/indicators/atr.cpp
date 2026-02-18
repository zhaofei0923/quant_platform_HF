#include "quant_hft/indicators/atr.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace quant_hft {

ATR::ATR(int period) : period_(period) {
    if (period_ <= 0) {
        throw std::invalid_argument("ATR period must be positive");
    }
}

void ATR::Update(double high, double low, double close, double volume) {
    (void)volume;

    if (!std::isfinite(high) || !std::isfinite(low) || !std::isfinite(close)) {
        return;
    }

    const double range = std::fabs(high - low);
    double tr = range;
    if (has_prev_close_) {
        tr = std::max({range, std::fabs(high - prev_close_), std::fabs(low - prev_close_)});
    }

    if (!initialized_) {
        tr_seed_.push_back(tr);
        tr_seed_sum_ += tr;

        if (tr_seed_.size() >= static_cast<std::size_t>(period_)) {
            atr_ = tr_seed_sum_ / static_cast<double>(period_);
            initialized_ = true;
        }
    } else {
        atr_ = ((atr_ * static_cast<double>(period_ - 1)) + tr) / static_cast<double>(period_);
    }

    prev_close_ = close;
    has_prev_close_ = true;
}

std::optional<double> ATR::Value() const {
    if (!initialized_) {
        return std::nullopt;
    }
    return atr_;
}

bool ATR::IsReady() const { return initialized_; }

void ATR::Reset() {
    initialized_ = false;
    has_prev_close_ = false;
    prev_close_ = 0.0;
    tr_seed_.clear();
    tr_seed_sum_ = 0.0;
    atr_ = 0.0;
}

}  // namespace quant_hft
