#include "quant_hft/indicators/adx.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace quant_hft {

ADX::ADX(int period) : period_(period) {
    if (period_ <= 0) {
        throw std::invalid_argument("ADX period must be positive");
    }
}

void ADX::Update(double high, double low, double close, double volume) {
    (void)volume;

    if (!std::isfinite(high) || !std::isfinite(low) || !std::isfinite(close)) {
        return;
    }

    if (!has_prev_bar_) {
        const double first_tr = std::fabs(high - low);
        UpdateDirectionalValues(first_tr, 0.0, 0.0);
        prev_high_ = high;
        prev_low_ = low;
        prev_close_ = close;
        has_prev_bar_ = true;
        return;
    }

    const double up_move = high - prev_high_;
    const double down_move = prev_low_ - low;

    const double plus_dm = (up_move > down_move && up_move > 0.0) ? up_move : 0.0;
    const double minus_dm = (down_move > up_move && down_move > 0.0) ? down_move : 0.0;

    const double tr_range = std::fabs(high - low);
    const double tr =
        std::max({tr_range, std::fabs(high - prev_close_), std::fabs(low - prev_close_)});

    UpdateDirectionalValues(tr, plus_dm, minus_dm);

    prev_high_ = high;
    prev_low_ = low;
    prev_close_ = close;
}

std::optional<double> ADX::Value() const {
    if (!adx_ready_) {
        return std::nullopt;
    }
    return adx_;
}

bool ADX::IsReady() const { return adx_ready_; }

void ADX::Reset() {
    has_prev_bar_ = false;
    prev_high_ = 0.0;
    prev_low_ = 0.0;
    prev_close_ = 0.0;

    seed_count_ = 0;
    tr_seed_sum_ = 0.0;
    plus_dm_seed_sum_ = 0.0;
    minus_dm_seed_sum_ = 0.0;

    di_ready_ = false;
    tr_smoothed_ = 0.0;
    plus_dm_smoothed_ = 0.0;
    minus_dm_smoothed_ = 0.0;
    plus_di_ = 0.0;
    minus_di_ = 0.0;
    dx_ = 0.0;

    dx_seed_count_ = 0;
    dx_seed_sum_ = 0.0;
    adx_ready_ = false;
    adx_ = 0.0;
}

std::optional<double> ADX::PlusDI() const {
    if (!di_ready_) {
        return std::nullopt;
    }
    return plus_di_;
}

std::optional<double> ADX::MinusDI() const {
    if (!di_ready_) {
        return std::nullopt;
    }
    return minus_di_;
}

std::optional<double> ADX::Dx() const {
    if (!di_ready_) {
        return std::nullopt;
    }
    return dx_;
}

ADX::State ADX::ExportState() const {
    State state;
    state.has_prev_bar = has_prev_bar_;
    state.prev_high = prev_high_;
    state.prev_low = prev_low_;
    state.prev_close = prev_close_;
    state.seed_count = seed_count_;
    state.tr_seed_sum = tr_seed_sum_;
    state.plus_dm_seed_sum = plus_dm_seed_sum_;
    state.minus_dm_seed_sum = minus_dm_seed_sum_;
    state.di_ready = di_ready_;
    state.tr_smoothed = tr_smoothed_;
    state.plus_dm_smoothed = plus_dm_smoothed_;
    state.minus_dm_smoothed = minus_dm_smoothed_;
    state.plus_di = plus_di_;
    state.minus_di = minus_di_;
    state.dx = dx_;
    state.dx_seed_count = dx_seed_count_;
    state.dx_seed_sum = dx_seed_sum_;
    state.adx_ready = adx_ready_;
    state.adx = adx_;
    return state;
}

bool ADX::ImportState(const State& state) {
    if (state.seed_count < 0 || state.seed_count > period_ || state.dx_seed_count < 0 ||
        state.dx_seed_count > period_) {
        return false;
    }
    const double values[] = {state.prev_high,         state.prev_low,
                             state.prev_close,        state.tr_seed_sum,
                             state.plus_dm_seed_sum,  state.minus_dm_seed_sum,
                             state.tr_smoothed,       state.plus_dm_smoothed,
                             state.minus_dm_smoothed, state.plus_di,
                             state.minus_di,          state.dx,
                             state.dx_seed_sum,       state.adx};
    for (const double value : values) {
        if (!std::isfinite(value)) {
            return false;
        }
    }
    has_prev_bar_ = state.has_prev_bar;
    prev_high_ = state.prev_high;
    prev_low_ = state.prev_low;
    prev_close_ = state.prev_close;
    seed_count_ = state.seed_count;
    tr_seed_sum_ = state.tr_seed_sum;
    plus_dm_seed_sum_ = state.plus_dm_seed_sum;
    minus_dm_seed_sum_ = state.minus_dm_seed_sum;
    di_ready_ = state.di_ready;
    tr_smoothed_ = state.tr_smoothed;
    plus_dm_smoothed_ = state.plus_dm_smoothed;
    minus_dm_smoothed_ = state.minus_dm_smoothed;
    plus_di_ = state.plus_di;
    minus_di_ = state.minus_di;
    dx_ = state.dx;
    dx_seed_count_ = state.dx_seed_count;
    dx_seed_sum_ = state.dx_seed_sum;
    adx_ready_ = state.adx_ready;
    adx_ = state.adx;
    return true;
}

void ADX::UpdateDirectionalValues(double tr, double plus_dm, double minus_dm) {
    if (!di_ready_) {
        tr_seed_sum_ += tr;
        plus_dm_seed_sum_ += plus_dm;
        minus_dm_seed_sum_ += minus_dm;
        ++seed_count_;

        if (seed_count_ < period_) {
            return;
        }

        tr_smoothed_ = tr_seed_sum_;
        plus_dm_smoothed_ = plus_dm_seed_sum_;
        minus_dm_smoothed_ = minus_dm_seed_sum_;
        di_ready_ = true;
    } else {
        const double period = static_cast<double>(period_);
        tr_smoothed_ = tr_smoothed_ - (tr_smoothed_ / period) + tr;
        plus_dm_smoothed_ = plus_dm_smoothed_ - (plus_dm_smoothed_ / period) + plus_dm;
        minus_dm_smoothed_ = minus_dm_smoothed_ - (minus_dm_smoothed_ / period) + minus_dm;
    }

    if (tr_smoothed_ <= 0.0) {
        plus_di_ = 0.0;
        minus_di_ = 0.0;
        dx_ = 0.0;
    } else {
        plus_di_ = 100.0 * plus_dm_smoothed_ / tr_smoothed_;
        minus_di_ = 100.0 * minus_dm_smoothed_ / tr_smoothed_;
        const double denominator = plus_di_ + minus_di_;
        dx_ = denominator > 0.0 ? 100.0 * std::fabs(plus_di_ - minus_di_) / denominator : 0.0;
    }

    UpdateAdxFromDx(dx_);
}

void ADX::UpdateAdxFromDx(double dx) {
    if (!adx_ready_) {
        dx_seed_sum_ += dx;
        ++dx_seed_count_;
        if (dx_seed_count_ >= period_) {
            adx_ = dx_seed_sum_ / static_cast<double>(period_);
            adx_ready_ = true;
        }
        return;
    }

    adx_ = ((adx_ * static_cast<double>(period_ - 1)) + dx) / static_cast<double>(period_);
}

}  // namespace quant_hft
