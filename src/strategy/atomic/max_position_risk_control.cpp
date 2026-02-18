#include "quant_hft/strategy/atomic/max_position_risk_control.h"

#include <cstdlib>
#include <stdexcept>

#include "atomic_param_parsing.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {

void MaxPositionRiskControl::Init(const AtomicParams& params) {
    id_ = atomic_internal::GetString(params, "id", "MaxPositionRiskControl");
    max_abs_position_ = atomic_internal::GetInt(params, "max_abs_position", 1);

    if (id_.empty()) {
        throw std::invalid_argument("MaxPositionRiskControl id must not be empty");
    }
    if (max_abs_position_ <= 0) {
        throw std::invalid_argument("MaxPositionRiskControl max_abs_position must be positive");
    }
}

std::string MaxPositionRiskControl::GetId() const { return id_; }

void MaxPositionRiskControl::Reset() {}

std::vector<SignalIntent> MaxPositionRiskControl::OnState(const StateSnapshot7D& state,
                                                          const AtomicStrategyContext& ctx) {
    const auto pos_it = ctx.net_positions.find(state.instrument_id);
    if (pos_it == ctx.net_positions.end()) {
        return {};
    }

    const int position = pos_it->second;
    const int abs_position = std::abs(position);
    if (abs_position <= max_abs_position_) {
        return {};
    }

    SignalIntent signal;
    signal.strategy_id = id_;
    signal.instrument_id = state.instrument_id;
    signal.signal_type = SignalType::kForceClose;
    signal.side = (position > 0) ? Side::kSell : Side::kBuy;
    signal.offset = OffsetFlag::kClose;
    signal.volume = abs_position - max_abs_position_;
    signal.limit_price = state.has_bar ? state.bar_close : 0.0;
    signal.ts_ns = state.ts_ns;
    return {signal};
}

QUANT_HFT_REGISTER_ATOMIC_STRATEGY("MaxPositionRiskControl", MaxPositionRiskControl);

}  // namespace quant_hft
