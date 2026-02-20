#include "quant_hft/strategy/atomic/trailing_stop_loss.h"

#include <cmath>
#include <cstdlib>
#include <stdexcept>

#include "atomic_param_parsing.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {

void TrailingStopLoss::Init(const AtomicParams& params) {
    id_ = atomic_internal::GetString(params, "id", "TrailingStopLoss");
    instrument_id_ = atomic_internal::GetString(params, "instrument_id", "");
    atr_period_ = atomic_internal::GetInt(params, "atr_period", 14);
    stop_loss_multi_ = atomic_internal::GetDouble(params, "stop_loss_multi", 2.0);

    if (id_.empty()) {
        throw std::invalid_argument("TrailingStopLoss id must not be empty");
    }
    if (atr_period_ <= 0) {
        throw std::invalid_argument("TrailingStopLoss atr_period must be positive");
    }
    if (!(stop_loss_multi_ > 0.0)) {
        throw std::invalid_argument("TrailingStopLoss stop_loss_multi must be positive");
    }

    atr_ = std::make_unique<ATR>(atr_period_);
    trailing_stop_by_instrument_.clear();
    direction_by_instrument_.clear();
    last_atr_.reset();
}

std::string TrailingStopLoss::GetId() const { return id_; }

void TrailingStopLoss::Reset() {
    if (atr_ != nullptr) {
        atr_->Reset();
    }
    trailing_stop_by_instrument_.clear();
    direction_by_instrument_.clear();
    last_atr_.reset();
}

std::vector<SignalIntent> TrailingStopLoss::OnState(const StateSnapshot7D& state,
                                                    const AtomicStrategyContext& ctx) {
    if (atr_ == nullptr || !state.has_bar || !std::isfinite(state.bar_high) ||
        !std::isfinite(state.bar_low) || !std::isfinite(state.bar_close)) {
        return {};
    }
    if (!instrument_id_.empty() && state.instrument_id != instrument_id_) {
        return {};
    }

    atr_->Update(state.bar_high, state.bar_low, state.bar_close, state.bar_volume);
    if (!atr_->IsReady()) {
        return {};
    }

    const std::optional<double> atr_value = atr_->Value();
    if (!atr_value.has_value() || !std::isfinite(*atr_value) || *atr_value <= 0.0) {
        return {};
    }
    last_atr_ = atr_value;

    const auto pos_it = ctx.net_positions.find(state.instrument_id);
    const int position = pos_it == ctx.net_positions.end() ? 0 : pos_it->second;
    if (position == 0) {
        trailing_stop_by_instrument_.erase(state.instrument_id);
        direction_by_instrument_.erase(state.instrument_id);
        return {};
    }

    const auto avg_price_it = ctx.avg_open_prices.find(state.instrument_id);
    if (avg_price_it == ctx.avg_open_prices.end() || !std::isfinite(avg_price_it->second)) {
        return {};
    }
    const double avg_open_price = avg_price_it->second;
    const int direction = position > 0 ? 1 : -1;
    const double stop_distance = stop_loss_multi_ * (*atr_value);
    const double init_stop =
        direction > 0 ? (avg_open_price - stop_distance) : (avg_open_price + stop_distance);
    double stop_price = init_stop;

    const auto stop_it = trailing_stop_by_instrument_.find(state.instrument_id);
    const auto dir_it = direction_by_instrument_.find(state.instrument_id);
    if (stop_it != trailing_stop_by_instrument_.end() && dir_it != direction_by_instrument_.end() &&
        dir_it->second == direction) {
        stop_price = stop_it->second;
    }

    const double candidate =
        direction > 0 ? (state.bar_close - stop_distance) : (state.bar_close + stop_distance);
    if (direction > 0) {
        stop_price = std::max(stop_price, candidate);
    } else {
        stop_price = std::min(stop_price, candidate);
    }

    trailing_stop_by_instrument_[state.instrument_id] = stop_price;
    direction_by_instrument_[state.instrument_id] = direction;

    const bool triggered =
        direction > 0 ? (state.bar_close <= stop_price) : (state.bar_close >= stop_price);
    if (!triggered) {
        return {};
    }

    SignalIntent signal;
    signal.strategy_id = id_;
    signal.instrument_id = state.instrument_id;
    signal.signal_type = SignalType::kStopLoss;
    signal.side = direction > 0 ? Side::kSell : Side::kBuy;
    signal.offset = OffsetFlag::kClose;
    signal.volume = std::abs(position);
    signal.limit_price = state.bar_close;
    signal.ts_ns = state.ts_ns;
    return {signal};
}

std::optional<AtomicIndicatorSnapshot> TrailingStopLoss::IndicatorSnapshot() const {
    if (!last_atr_.has_value()) {
        return std::nullopt;
    }
    AtomicIndicatorSnapshot snapshot;
    snapshot.atr = last_atr_;
    return snapshot;
}

QUANT_HFT_REGISTER_ATOMIC_STRATEGY("TrailingStopLoss", TrailingStopLoss);

}  // namespace quant_hft
