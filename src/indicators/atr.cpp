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

ATR::State ATR::ExportState() const {
    State state;
    state.initialized = initialized_;
    state.has_prev_close = has_prev_close_;
    state.prev_close = prev_close_;
    state.tr_seed.assign(tr_seed_.begin(), tr_seed_.end());
    state.tr_seed_sum = tr_seed_sum_;
    state.atr = atr_;
    return state;
}

bool ATR::ImportState(const State& state) {
    if (!std::isfinite(state.prev_close) || !std::isfinite(state.tr_seed_sum) ||
        !std::isfinite(state.atr) || state.tr_seed.size() > static_cast<std::size_t>(period_)) {
        return false;
    }
    for (const double tr : state.tr_seed) {
        if (!std::isfinite(tr) || tr < 0.0) {
            return false;
        }
    }
    initialized_ = state.initialized;
    has_prev_close_ = state.has_prev_close;
    prev_close_ = state.prev_close;
    tr_seed_.assign(state.tr_seed.begin(), state.tr_seed.end());
    tr_seed_sum_ = std::max(0.0, state.tr_seed_sum);
    atr_ = state.atr;
    if (initialized_ && tr_seed_.size() < static_cast<std::size_t>(period_)) {
        return false;
    }
    return true;
}

}  // namespace quant_hft
