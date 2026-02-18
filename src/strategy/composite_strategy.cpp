#include "quant_hft/strategy/composite_strategy.h"

#include <algorithm>
#include <cmath>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "quant_hft/strategy/atomic_factory.h"
#include "quant_hft/strategy/composite_config_loader.h"
#include "quant_hft/strategy/strategy_registry.h"

namespace quant_hft {
namespace {

template <typename T>
std::vector<T*> BuildTypedStrategies(const std::vector<AtomicStrategyDefinition>& definitions,
                                     AtomicFactory* factory,
                                     std::vector<std::unique_ptr<IAtomicStrategy>>* owned,
                                     std::vector<IAtomicOrderAware*>* order_aware) {
    std::vector<T*> result;
    result.reserve(definitions.size());
    for (const auto& definition : definitions) {
        std::string error;
        std::unique_ptr<IAtomicStrategy> strategy = factory->Create(definition, &error);
        if (!strategy) {
            throw std::runtime_error(error.empty() ? "failed to create atomic strategy" : error);
        }
        strategy->Init(definition.params);
        T* typed = dynamic_cast<T*>(strategy.get());
        if (typed == nullptr) {
            throw std::runtime_error("atomic strategy id '" + definition.id +
                                     "' does not match required strategy interface");
        }
        if (auto* order_aware_strategy = dynamic_cast<IAtomicOrderAware*>(strategy.get());
            order_aware_strategy != nullptr) {
            order_aware->push_back(order_aware_strategy);
        }
        result.push_back(typed);
        owned->push_back(std::move(strategy));
    }
    return result;
}

}  // namespace

CompositeStrategy::CompositeStrategy() = default;

CompositeStrategy::CompositeStrategy(CompositeStrategyDefinition definition, AtomicFactory* factory)
    : definition_(std::move(definition)), has_embedded_definition_(true), factory_(factory) {}

void CompositeStrategy::Initialize(const StrategyContext& ctx) {
    strategy_context_ = ctx;
    atomic_context_.account_id = ctx.account_id;
    atomic_context_.net_positions.clear();
    atomic_context_.avg_open_prices.clear();
    last_filled_volume_by_order_.clear();

    if (factory_ == nullptr) {
        factory_ = &AtomicFactory::Instance();
    }

    if (!has_embedded_definition_) {
        LoadDefinitionFromContext();
    }

    if (definition_.merge_rule != SignalMergeRule::kPriority) {
        throw std::runtime_error("CompositeStrategy only supports merge_rule = kPriority");
    }

    BuildAtomicStrategies();
}

std::vector<SignalIntent> CompositeStrategy::OnState(const StateSnapshot7D& state) {
    std::vector<SignalIntent> all_signals;

    for (IRiskControlStrategy* strategy : risk_control_strategies_) {
        std::vector<SignalIntent> signals = strategy->OnState(state, atomic_context_);
        all_signals.insert(all_signals.end(), signals.begin(), signals.end());
    }

    for (IStopLossStrategy* strategy : stop_loss_strategies_) {
        std::vector<SignalIntent> signals = strategy->OnState(state, atomic_context_);
        all_signals.insert(all_signals.end(), signals.begin(), signals.end());
    }

    for (ITakeProfitStrategy* strategy : take_profit_strategies_) {
        std::vector<SignalIntent> signals = strategy->OnState(state, atomic_context_);
        all_signals.insert(all_signals.end(), signals.begin(), signals.end());
    }

    bool opening_allowed = true;
    for (ITimeFilterStrategy* strategy : time_filters_) {
        opening_allowed = opening_allowed && strategy->AllowOpening(state.ts_ns);
    }

    if (opening_allowed) {
        for (const OpeningSlot& slot : opening_strategies_) {
            if (!IsOpeningAllowedByRegime(slot, state.market_regime)) {
                continue;
            }
            std::vector<SignalIntent> signals = slot.strategy->OnState(state, atomic_context_);
            all_signals.insert(all_signals.end(), signals.begin(), signals.end());
        }
    }

    return MergeSignals(all_signals);
}

void CompositeStrategy::OnOrderEvent(const OrderEvent& event) {
    const std::string order_id =
        !event.client_order_id.empty() ? event.client_order_id : event.exchange_order_id;
    if (order_id.empty() || event.instrument_id.empty()) {
        return;
    }

    const std::int32_t current_filled = std::max<std::int32_t>(event.filled_volume, 0);
    const std::int32_t previous_filled = last_filled_volume_by_order_.count(order_id) == 0
                                             ? 0
                                             : last_filled_volume_by_order_[order_id];
    const std::int32_t delta_filled = current_filled - previous_filled;
    if (delta_filled <= 0) {
        return;
    }
    if (!std::isfinite(event.avg_fill_price)) {
        return;
    }
    last_filled_volume_by_order_[order_id] = current_filled;

    const std::int32_t signed_delta = (event.side == Side::kBuy) ? delta_filled : -delta_filled;
    const std::string& instrument = event.instrument_id;
    const std::int32_t old_position = atomic_context_.net_positions[instrument];
    const std::int32_t new_position = old_position + signed_delta;
    const double fill_price = event.avg_fill_price;

    const auto avg_it = atomic_context_.avg_open_prices.find(instrument);
    const bool had_avg = avg_it != atomic_context_.avg_open_prices.end();
    const double old_avg = had_avg ? avg_it->second : fill_price;

    if (new_position == 0) {
        atomic_context_.net_positions[instrument] = 0;
        atomic_context_.avg_open_prices.erase(instrument);
    } else if (old_position == 0) {
        atomic_context_.net_positions[instrument] = new_position;
        atomic_context_.avg_open_prices[instrument] = fill_price;
    } else if ((old_position > 0 && new_position > 0) || (old_position < 0 && new_position < 0)) {
        const std::int32_t abs_old = std::abs(old_position);
        const std::int32_t abs_new = std::abs(new_position);
        if (abs_new > abs_old) {
            const double weighted_old = old_avg * static_cast<double>(abs_old);
            const double weighted_new = fill_price * static_cast<double>(std::abs(signed_delta));
            atomic_context_.avg_open_prices[instrument] =
                (weighted_old + weighted_new) / static_cast<double>(abs_new);
        } else {
            atomic_context_.avg_open_prices[instrument] = old_avg;
        }
        atomic_context_.net_positions[instrument] = new_position;
    } else {
        atomic_context_.net_positions[instrument] = new_position;
        atomic_context_.avg_open_prices[instrument] = fill_price;
    }

    for (IAtomicOrderAware* strategy : order_aware_strategies_) {
        strategy->OnOrderEvent(event, atomic_context_);
    }
}

std::vector<SignalIntent> CompositeStrategy::OnTimer(EpochNanos now_ns) {
    (void)now_ns;
    return {};
}

void CompositeStrategy::Shutdown() {
    for (auto& strategy : owned_atomic_strategies_) {
        strategy->Reset();
    }
    opening_strategies_.clear();
    stop_loss_strategies_.clear();
    take_profit_strategies_.clear();
    time_filters_.clear();
    risk_control_strategies_.clear();
    order_aware_strategies_.clear();
    owned_atomic_strategies_.clear();
    atomic_context_.net_positions.clear();
    atomic_context_.avg_open_prices.clear();
    last_filled_volume_by_order_.clear();
}

bool CompositeStrategy::IsOpeningAllowedByRegime(const OpeningSlot& slot,
                                                 MarketRegime regime) const {
    if (slot.market_regimes.empty()) {
        return true;
    }
    return std::find(slot.market_regimes.begin(), slot.market_regimes.end(), regime) !=
           slot.market_regimes.end();
}

std::vector<SignalIntent> CompositeStrategy::MergeSignals(
    const std::vector<SignalIntent>& signals) const {
    if (signals.empty()) {
        return {};
    }

    std::unordered_map<std::string, SignalIntent> best_by_instrument;
    for (const SignalIntent& signal : signals) {
        if (signal.instrument_id.empty()) {
            continue;
        }
        const auto it = best_by_instrument.find(signal.instrument_id);
        if (it == best_by_instrument.end() || IsPreferredSignal(signal, it->second)) {
            best_by_instrument[signal.instrument_id] = signal;
        }
    }

    std::vector<SignalIntent> merged;
    merged.reserve(best_by_instrument.size());
    for (const auto& item : best_by_instrument) {
        merged.push_back(item.second);
    }
    std::sort(merged.begin(), merged.end(), [](const SignalIntent& lhs, const SignalIntent& rhs) {
        return lhs.instrument_id < rhs.instrument_id;
    });
    return merged;
}

int CompositeStrategy::SignalPriority(SignalType signal_type) {
    switch (signal_type) {
        case SignalType::kForceClose:
            return 0;
        case SignalType::kStopLoss:
            return 1;
        case SignalType::kTakeProfit:
            return 2;
        case SignalType::kClose:
            return 3;
        case SignalType::kOpen:
        default:
            return 4;
    }
}

bool CompositeStrategy::IsPreferredSignal(const SignalIntent& lhs, const SignalIntent& rhs) {
    const int lhs_priority = SignalPriority(lhs.signal_type);
    const int rhs_priority = SignalPriority(rhs.signal_type);
    if (lhs_priority != rhs_priority) {
        return lhs_priority < rhs_priority;
    }
    if (lhs.volume != rhs.volume) {
        return lhs.volume > rhs.volume;
    }
    if (lhs.ts_ns != rhs.ts_ns) {
        return lhs.ts_ns > rhs.ts_ns;
    }
    return lhs.trace_id < rhs.trace_id;
}

void CompositeStrategy::LoadDefinitionFromContext() {
    const auto path_it = strategy_context_.metadata.find("composite_config_path");
    if (path_it == strategy_context_.metadata.end() || path_it->second.empty()) {
        throw std::runtime_error("CompositeStrategy requires metadata key `composite_config_path`");
    }
    std::string error;
    if (!LoadCompositeStrategyDefinition(path_it->second, &definition_, &error)) {
        throw std::runtime_error(error.empty() ? "failed to load composite strategy config"
                                               : error);
    }
}

void CompositeStrategy::BuildAtomicStrategies() {
    opening_strategies_.clear();
    stop_loss_strategies_.clear();
    take_profit_strategies_.clear();
    time_filters_.clear();
    risk_control_strategies_.clear();
    order_aware_strategies_.clear();
    owned_atomic_strategies_.clear();

    risk_control_strategies_ = BuildTypedStrategies<IRiskControlStrategy>(
        definition_.risk_control_strategies, factory_, &owned_atomic_strategies_,
        &order_aware_strategies_);
    stop_loss_strategies_ = BuildTypedStrategies<IStopLossStrategy>(
        definition_.stop_loss_strategies, factory_, &owned_atomic_strategies_,
        &order_aware_strategies_);
    take_profit_strategies_ = BuildTypedStrategies<ITakeProfitStrategy>(
        definition_.take_profit_strategies, factory_, &owned_atomic_strategies_,
        &order_aware_strategies_);
    time_filters_ = BuildTypedStrategies<ITimeFilterStrategy>(
        definition_.time_filters, factory_, &owned_atomic_strategies_, &order_aware_strategies_);

    std::vector<IOpeningStrategy*> opening =
        BuildTypedStrategies<IOpeningStrategy>(definition_.opening_strategies, factory_,
                                               &owned_atomic_strategies_, &order_aware_strategies_);
    opening_strategies_.reserve(opening.size());
    for (std::size_t i = 0; i < opening.size(); ++i) {
        OpeningSlot slot;
        slot.strategy = opening[i];
        slot.market_regimes = definition_.opening_strategies[i].market_regimes;
        opening_strategies_.push_back(std::move(slot));
    }
}

bool RegisterCompositeStrategy(std::string* error) {
    static std::once_flag register_once;
    static bool registered = false;
    static std::string register_error;

    std::call_once(register_once, [&]() {
        registered = StrategyRegistry::Instance().RegisterFactory(
            "composite", []() { return std::make_unique<CompositeStrategy>(); }, &register_error);
    });

    if (!registered && error != nullptr) {
        *error = register_error;
    }
    return registered;
}

}  // namespace quant_hft
