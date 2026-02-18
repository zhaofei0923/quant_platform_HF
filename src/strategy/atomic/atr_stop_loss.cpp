#include "quant_hft/strategy/atomic/atr_stop_loss.h"

#include <cmath>
#include <cstdlib>
#include <stdexcept>

#include "atomic_param_parsing.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {

void ATRStopLoss::Init(const AtomicParams& params) {
    id_ = atomic_internal::GetString(params, "id", "ATRStopLoss");
    atr_period_ = atomic_internal::GetInt(params, "atr_period", 14);
    atr_multiplier_ = atomic_internal::GetDouble(params, "atr_multiplier", 2.0);

    if (id_.empty()) {
        throw std::invalid_argument("ATRStopLoss id must not be empty");
    }
    if (atr_period_ <= 0) {
        throw std::invalid_argument("ATRStopLoss atr_period must be positive");
    }
    if (!(atr_multiplier_ > 0.0)) {
        throw std::invalid_argument("ATRStopLoss atr_multiplier must be positive");
    }

    atr_ = std::make_unique<ATR>(atr_period_);
}

std::string ATRStopLoss::GetId() const { return id_; }

void ATRStopLoss::Reset() {
    if (atr_ != nullptr) {
        atr_->Reset();
    }
}

std::vector<SignalIntent> ATRStopLoss::OnState(const StateSnapshot7D& state,
                                               const AtomicStrategyContext& ctx) {
    if (atr_ == nullptr || !state.has_bar || !std::isfinite(state.bar_high) ||
        !std::isfinite(state.bar_low) || !std::isfinite(state.bar_close)) {
        return {};
    }

    atr_->Update(state.bar_high, state.bar_low, state.bar_close, state.bar_volume);
    if (!atr_->IsReady()) {
        return {};
    }

    const auto pos_it = ctx.net_positions.find(state.instrument_id);
    if (pos_it == ctx.net_positions.end() || pos_it->second == 0) {
        return {};
    }

    const auto price_it = ctx.avg_open_prices.find(state.instrument_id);
    if (price_it == ctx.avg_open_prices.end()) {
        return {};
    }

    const std::optional<double> atr_value = atr_->Value();
    if (!atr_value.has_value()) {
        return {};
    }

    const int position = pos_it->second;
    const double avg_open_price = price_it->second;
    const double stop_distance = atr_multiplier_ * (*atr_value);
    const double stop_price =
        (position > 0) ? (avg_open_price - stop_distance) : (avg_open_price + stop_distance);
    const bool triggered =
        (position > 0) ? (state.bar_close <= stop_price) : (state.bar_close >= stop_price);
    if (!triggered) {
        return {};
    }

    SignalIntent signal;
    signal.strategy_id = id_;
    signal.instrument_id = state.instrument_id;
    signal.signal_type = SignalType::kStopLoss;
    signal.side = (position > 0) ? Side::kSell : Side::kBuy;
    signal.offset = OffsetFlag::kClose;
    signal.volume = std::abs(position);
    signal.limit_price = state.bar_close;
    signal.ts_ns = state.ts_ns;
    return {signal};
}

QUANT_HFT_REGISTER_ATOMIC_STRATEGY("ATRStopLoss", ATRStopLoss);

}  // namespace quant_hft
