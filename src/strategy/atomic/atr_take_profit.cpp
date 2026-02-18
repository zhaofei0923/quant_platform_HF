#include "quant_hft/strategy/atomic/atr_take_profit.h"

#include <cmath>
#include <cstdlib>
#include <stdexcept>

#include "atomic_param_parsing.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {

void ATRTakeProfit::Init(const AtomicParams& params) {
    id_ = atomic_internal::GetString(params, "id", "ATRTakeProfit");
    atr_period_ = atomic_internal::GetInt(params, "atr_period", 14);
    atr_multiplier_ = atomic_internal::GetDouble(params, "atr_multiplier", 2.0);

    if (id_.empty()) {
        throw std::invalid_argument("ATRTakeProfit id must not be empty");
    }
    if (atr_period_ <= 0) {
        throw std::invalid_argument("ATRTakeProfit atr_period must be positive");
    }
    if (!(atr_multiplier_ > 0.0)) {
        throw std::invalid_argument("ATRTakeProfit atr_multiplier must be positive");
    }

    atr_ = std::make_unique<ATR>(atr_period_);
}

std::string ATRTakeProfit::GetId() const { return id_; }

void ATRTakeProfit::Reset() {
    if (atr_ != nullptr) {
        atr_->Reset();
    }
}

std::vector<SignalIntent> ATRTakeProfit::OnState(const StateSnapshot7D& state,
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
    const double target_distance = atr_multiplier_ * (*atr_value);
    const double target_price =
        (position > 0) ? (avg_open_price + target_distance) : (avg_open_price - target_distance);
    const bool triggered =
        (position > 0) ? (state.bar_close >= target_price) : (state.bar_close <= target_price);
    if (!triggered) {
        return {};
    }

    SignalIntent signal;
    signal.strategy_id = id_;
    signal.instrument_id = state.instrument_id;
    signal.signal_type = SignalType::kTakeProfit;
    signal.side = (position > 0) ? Side::kSell : Side::kBuy;
    signal.offset = OffsetFlag::kClose;
    signal.volume = std::abs(position);
    signal.limit_price = state.bar_close;
    signal.ts_ns = state.ts_ns;
    return {signal};
}

QUANT_HFT_REGISTER_ATOMIC_STRATEGY("ATRTakeProfit", ATRTakeProfit);

}  // namespace quant_hft
