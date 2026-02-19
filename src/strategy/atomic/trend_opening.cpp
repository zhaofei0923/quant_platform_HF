#include "quant_hft/strategy/atomic/trend_opening.h"

#include <cmath>
#include <stdexcept>

#include "atomic_param_parsing.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {

void TrendOpening::Init(const AtomicParams& params) {
    id_ = atomic_internal::GetString(params, "id", "TrendOpening");
    instrument_id_ = atomic_internal::GetString(params, "instrument_id", "");
    er_period_ = atomic_internal::GetInt(params, "er_period", 10);
    fast_period_ = atomic_internal::GetInt(params, "fast_period", 2);
    slow_period_ = atomic_internal::GetInt(params, "slow_period", 30);
    volume_ = atomic_internal::GetInt(params, "volume", 1);

    if (id_.empty()) {
        throw std::invalid_argument("TrendOpening id must not be empty");
    }
    if (er_period_ <= 0 || fast_period_ <= 0 || slow_period_ <= 0) {
        throw std::invalid_argument("TrendOpening periods must be positive");
    }
    if (volume_ <= 0) {
        throw std::invalid_argument("TrendOpening volume must be positive");
    }

    kama_ = std::make_unique<KAMA>(er_period_, fast_period_, slow_period_);
}

std::string TrendOpening::GetId() const { return id_; }

void TrendOpening::Reset() {
    if (kama_ != nullptr) {
        kama_->Reset();
    }
}

std::vector<SignalIntent> TrendOpening::OnState(const StateSnapshot7D& state,
                                                const AtomicStrategyContext& ctx) {
    if (kama_ == nullptr || !state.has_bar || !std::isfinite(state.bar_high) ||
        !std::isfinite(state.bar_low) || !std::isfinite(state.bar_close)) {
        return {};
    }
    if (!instrument_id_.empty() && state.instrument_id != instrument_id_) {
        return {};
    }

    kama_->Update(state.bar_high, state.bar_low, state.bar_close, state.bar_volume);
    if (!kama_->IsReady()) {
        return {};
    }

    const auto pos_it = ctx.net_positions.find(state.instrument_id);
    const int position = (pos_it == ctx.net_positions.end()) ? 0 : pos_it->second;
    if (position != 0) {
        return {};
    }

    const std::optional<double> kama_value = kama_->Value();
    if (!kama_value.has_value()) {
        return {};
    }

    Side side = Side::kBuy;
    if (state.bar_close > *kama_value) {
        side = Side::kBuy;
    } else if (state.bar_close < *kama_value) {
        side = Side::kSell;
    } else {
        return {};
    }

    SignalIntent signal;
    signal.strategy_id = id_;
    signal.instrument_id = state.instrument_id;
    signal.signal_type = SignalType::kOpen;
    signal.side = side;
    signal.offset = OffsetFlag::kOpen;
    signal.volume = volume_;
    signal.limit_price = state.bar_close;
    signal.ts_ns = state.ts_ns;
    return {signal};
}

std::optional<AtomicIndicatorSnapshot> TrendOpening::IndicatorSnapshot() const {
    if (kama_ == nullptr || !kama_->IsReady()) {
        return std::nullopt;
    }
    AtomicIndicatorSnapshot snapshot;
    snapshot.kama = kama_->Value();
    snapshot.er = kama_->EfficiencyRatio();
    return snapshot;
}

QUANT_HFT_REGISTER_ATOMIC_STRATEGY("TrendOpening", TrendOpening);

}  // namespace quant_hft
