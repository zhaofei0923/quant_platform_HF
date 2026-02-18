#include "quant_hft/strategy/demo_live_strategy.h"

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/strategy/strategy_registry.h"

namespace quant_hft {

void DemoLiveStrategy::Initialize(const StrategyContext& ctx) {
    strategy_id_ = ctx.strategy_id;
    signal_counter_ = 0;
}

std::vector<SignalIntent> DemoLiveStrategy::OnState(const StateSnapshot7D& state) {
    ++signal_counter_;

    SignalIntent intent;
    intent.strategy_id = strategy_id_;
    intent.instrument_id = state.instrument_id;
    intent.signal_type = SignalType::kOpen;
    intent.side = state.trend.score >= 0.0 ? Side::kBuy : Side::kSell;
    intent.offset = OffsetFlag::kOpen;
    intent.volume = 1;
    intent.limit_price = 4500.0;
    intent.ts_ns = state.ts_ns;
    intent.trace_id = strategy_id_ + "-" + state.instrument_id + "-" + std::to_string(state.ts_ns) +
                      "-" + std::to_string(signal_counter_);
    return {intent};
}

void DemoLiveStrategy::OnOrderEvent(const OrderEvent& event) {
    (void)event;
}

std::vector<SignalIntent> DemoLiveStrategy::OnTimer(EpochNanos now_ns) {
    (void)now_ns;
    return {};
}

void DemoLiveStrategy::Shutdown() {}

bool RegisterDemoLiveStrategy(std::string* error) {
    static std::once_flag register_once;
    static bool registered = false;
    static std::string register_error;
    std::call_once(register_once, [&]() {
        registered = StrategyRegistry::Instance().RegisterFactory(
            "demo",
            []() { return std::make_unique<DemoLiveStrategy>(); },
            &register_error);
    });
    if (!registered && error != nullptr) {
        *error = register_error;
    }
    return registered;
}

}  // namespace quant_hft
