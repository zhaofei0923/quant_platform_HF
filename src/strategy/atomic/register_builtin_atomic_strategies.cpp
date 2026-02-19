#include <mutex>
#include <string>

#include "quant_hft/strategy/atomic/atr_stop_loss.h"
#include "quant_hft/strategy/atomic/atr_take_profit.h"
#include "quant_hft/strategy/atomic/max_position_risk_control.h"
#include "quant_hft/strategy/atomic/time_filter.h"
#include "quant_hft/strategy/atomic/trend_opening.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {
namespace {

bool RegisterOne(const std::string& type, AtomicFactory::Creator creator, std::string* error) {
    AtomicFactory& factory = AtomicFactory::Instance();
    if (factory.Has(type)) {
        return true;
    }
    return factory.Register(type, std::move(creator), error);
}

}  // namespace

bool RegisterBuiltinAtomicStrategies(std::string* error) {
    static std::once_flag register_once;
    static bool registered = false;
    static std::string register_error;
    std::call_once(register_once, [&]() {
        if (!RegisterOne(
                "TrendOpening", []() { return std::make_unique<TrendOpening>(); },
                &register_error)) {
            return;
        }
        if (!RegisterOne(
                "ATRStopLoss", []() { return std::make_unique<ATRStopLoss>(); }, &register_error)) {
            return;
        }
        if (!RegisterOne(
                "ATRTakeProfit", []() { return std::make_unique<ATRTakeProfit>(); },
                &register_error)) {
            return;
        }
        if (!RegisterOne(
                "TimeFilter", []() { return std::make_unique<TimeFilter>(); }, &register_error)) {
            return;
        }
        if (!RegisterOne(
                "MaxPositionRiskControl",
                []() { return std::make_unique<MaxPositionRiskControl>(); }, &register_error)) {
            return;
        }
        registered = true;
    });
    if (!registered && error != nullptr) {
        *error = register_error.empty() ? "failed to register builtin atomic strategies"
                                        : register_error;
    }
    return registered;
}

}  // namespace quant_hft
