#include "quant_hft/strategy/composite_strategy.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "quant_hft/strategy/atomic_factory.h"
#include "quant_hft/strategy/composite_config_loader.h"
#include "quant_hft/strategy/strategy_registry.h"

namespace quant_hft {
namespace {

template <typename T>
std::vector<T*> BuildTypedStrategies(
    const std::vector<SubStrategyDefinition>& definitions, AtomicFactory* factory,
    std::vector<std::unique_ptr<IAtomicStrategy>>* owned,
    std::vector<IAtomicOrderAware*>* order_aware,
    const std::function<void(const SubStrategyDefinition&, IAtomicStrategy*)>& on_strategy) {
    std::vector<T*> result;
    result.reserve(definitions.size());
    for (const auto& definition : definitions) {
        std::string error;
        std::unique_ptr<IAtomicStrategy> strategy =
            factory->Create(definition.type, &error, definition.id);
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
        if (on_strategy) {
            on_strategy(definition, strategy.get());
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
    atomic_context_.contract_multipliers.clear();
    atomic_context_.account_equity = 0.0;
    atomic_context_.total_pnl_after_cost = 0.0;
    last_filled_volume_by_order_.clear();
    position_owner_by_instrument_.clear();
    pending_reverse_open_by_instrument_.clear();

    if (factory_ == nullptr) {
        factory_ = &AtomicFactory::Instance();
        std::string error;
        if (!RegisterBuiltinAtomicStrategies(&error)) {
            throw std::runtime_error(error.empty() ? "failed to register builtin atomic strategies"
                                                   : error);
        }
    }

    if (!has_embedded_definition_) {
        LoadDefinitionFromContext();
    }
    if (const auto run_type_it = strategy_context_.metadata.find("run_type");
        run_type_it != strategy_context_.metadata.end() && !run_type_it->second.empty()) {
        definition_.run_type = run_type_it->second;
    }

    if (!IsValidRunType(definition_.run_type)) {
        throw std::runtime_error("unsupported run_type: " + definition_.run_type);
    }
    if (definition_.run_type != "backtest") {
        throw std::runtime_error("CompositeStrategy V2 only supports run_type=backtest");
    }
    atomic_context_.run_type = definition_.run_type;

    if (definition_.merge_rule != SignalMergeRule::kPriority) {
        throw std::runtime_error("CompositeStrategy only supports merge_rule = kPriority");
    }

    BuildAtomicStrategies();
}

std::vector<SignalIntent> CompositeStrategy::OnState(const StateSnapshot7D& state) {
    std::vector<SignalIntent> non_open_signals;
    std::vector<SignalIntent> opening_signals;

    for (const SubStrategySlot& slot : sub_strategies_) {
        std::vector<SignalIntent> signals = slot.strategy->OnState(state, atomic_context_);
        for (const SignalIntent& signal : signals) {
            if (signal.instrument_id.empty()) {
                continue;
            }
            if (signal.signal_type == SignalType::kOpen) {
                if (definition_.market_state_mode &&
                    !IsOpenSignalAllowedByRegime(slot, state.market_regime)) {
                    continue;
                }
                opening_signals.push_back(signal);
                continue;
            }
            non_open_signals.push_back(signal);
        }
    }

    std::vector<SignalIntent> all_signals = ApplyNonOpenSignalGate(non_open_signals);
    const std::vector<SignalIntent> gated_openings = ApplyOpeningGate(opening_signals, state);
    all_signals.insert(all_signals.end(), gated_openings.begin(), gated_openings.end());

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
        position_owner_by_instrument_.erase(instrument);
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

    if (new_position != 0) {
        if (event.offset == OffsetFlag::kOpen && !event.strategy_id.empty()) {
            position_owner_by_instrument_[instrument] = event.strategy_id;
        } else if (position_owner_by_instrument_.find(instrument) ==
                       position_owner_by_instrument_.end() &&
                   !event.strategy_id.empty()) {
            position_owner_by_instrument_[instrument] = event.strategy_id;
        }
    }

    for (IAtomicOrderAware* strategy : order_aware_strategies_) {
        strategy->OnOrderEvent(event, atomic_context_);
    }
}

std::vector<SignalIntent> CompositeStrategy::OnTimer(EpochNanos now_ns) {
    (void)now_ns;
    return {};
}

std::vector<CompositeAtomicTraceRow> CompositeStrategy::CollectAtomicIndicatorTrace() const {
    std::vector<CompositeAtomicTraceRow> rows;
    rows.reserve(trace_providers_.size());
    for (const auto& slot : trace_providers_) {
        if (slot.provider == nullptr) {
            continue;
        }
        CompositeAtomicTraceRow row;
        row.strategy_id = slot.strategy_id;
        row.strategy_type = slot.strategy_type;
        const std::optional<AtomicIndicatorSnapshot> snapshot = slot.provider->IndicatorSnapshot();
        if (snapshot.has_value()) {
            row.kama = snapshot->kama;
            row.atr = snapshot->atr;
            row.adx = snapshot->adx;
            row.er = snapshot->er;
            row.stop_loss_price = snapshot->stop_loss_price;
            row.take_profit_price = snapshot->take_profit_price;
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

void CompositeStrategy::SetBacktestAccountSnapshot(double equity, double pnl_after_cost) {
    atomic_context_.account_equity = equity;
    atomic_context_.total_pnl_after_cost = pnl_after_cost;
}

void CompositeStrategy::SetBacktestContractMultiplier(const std::string& instrument_id,
                                                      double multiplier) {
    if (instrument_id.empty() || !std::isfinite(multiplier) || multiplier <= 0.0) {
        return;
    }
    atomic_context_.contract_multipliers[instrument_id] = multiplier;
}

void CompositeStrategy::Shutdown() {
    for (auto& strategy : owned_atomic_strategies_) {
        strategy->Reset();
    }
    sub_strategies_.clear();
    order_aware_strategies_.clear();
    trace_providers_.clear();
    owned_atomic_strategies_.clear();
    atomic_context_.net_positions.clear();
    atomic_context_.avg_open_prices.clear();
    atomic_context_.contract_multipliers.clear();
    last_filled_volume_by_order_.clear();
    position_owner_by_instrument_.clear();
    pending_reverse_open_by_instrument_.clear();
}

bool CompositeStrategy::IsOpenSignalAllowedByRegime(const SubStrategySlot& slot,
                                                    MarketRegime regime) const {
    if (slot.entry_market_regimes.empty()) {
        return true;
    }
    return std::find(slot.entry_market_regimes.begin(), slot.entry_market_regimes.end(), regime) !=
           slot.entry_market_regimes.end();
}

std::vector<SignalIntent> CompositeStrategy::ApplyNonOpenSignalGate(
    const std::vector<SignalIntent>& signals) {
    std::vector<SignalIntent> gated;
    gated.reserve(signals.size());
    for (const SignalIntent& signal : signals) {
        if (signal.instrument_id.empty()) {
            continue;
        }
        const auto pos_it = atomic_context_.net_positions.find(signal.instrument_id);
        const std::int32_t position =
            pos_it == atomic_context_.net_positions.end() ? 0 : pos_it->second;
        if (position == 0) {
            continue;
        }

        const auto owner_it = position_owner_by_instrument_.find(signal.instrument_id);
        const bool has_owner =
            owner_it != position_owner_by_instrument_.end() && !owner_it->second.empty();
        if (has_owner && owner_it->second != signal.strategy_id) {
            continue;
        }

        SignalIntent normalized = signal;
        normalized.signal_type =
            signal.signal_type == SignalType::kOpen ? SignalType::kClose : signal.signal_type;
        normalized.offset = OffsetFlag::kClose;
        normalized.side = position > 0 ? Side::kSell : Side::kBuy;
        const std::int32_t abs_pos = std::abs(position);
        if (normalized.volume <= 0 || normalized.volume > abs_pos) {
            normalized.volume = abs_pos;
        }
        gated.push_back(std::move(normalized));
    }
    return gated;
}

std::vector<SignalIntent> CompositeStrategy::ApplyOpeningGate(
    const std::vector<SignalIntent>& opening_signals, const StateSnapshot7D& state) {
    std::vector<SignalIntent> gated;

    for (const SignalIntent& signal : opening_signals) {
        if (signal.instrument_id.empty()) {
            continue;
        }

        const auto pos_it = atomic_context_.net_positions.find(signal.instrument_id);
        const std::int32_t position =
            pos_it == atomic_context_.net_positions.end() ? 0 : pos_it->second;
        const auto owner_it = position_owner_by_instrument_.find(signal.instrument_id);
        const bool has_owner =
            owner_it != position_owner_by_instrument_.end() && !owner_it->second.empty();

        if (position == 0) {
            if (pending_reverse_open_by_instrument_.count(signal.instrument_id) != 0) {
                continue;
            }
            gated.push_back(signal);
            continue;
        }

        if (has_owner && owner_it->second != signal.strategy_id) {
            continue;
        }

        const Side position_side = position > 0 ? Side::kBuy : Side::kSell;
        if (signal.side == position_side) {
            continue;
        }

        SignalIntent close_signal;
        close_signal.strategy_id =
            !signal.strategy_id.empty()
                ? signal.strategy_id
                : (has_owner ? owner_it->second : strategy_context_.strategy_id);
        close_signal.instrument_id = signal.instrument_id;
        close_signal.signal_type = SignalType::kClose;
        close_signal.side = position_side == Side::kBuy ? Side::kSell : Side::kBuy;
        close_signal.offset = OffsetFlag::kClose;
        close_signal.volume = std::abs(position);
        close_signal.limit_price =
            std::isfinite(signal.limit_price) ? signal.limit_price : state.bar_close;
        close_signal.ts_ns = state.ts_ns;
        close_signal.trace_id = signal.trace_id.empty()
                                    ? (close_signal.strategy_id + "-reverse-close")
                                    : (signal.trace_id + "-reverse-close");
        gated.push_back(close_signal);
        pending_reverse_open_by_instrument_[signal.instrument_id] = signal;
    }

    for (auto it = pending_reverse_open_by_instrument_.begin();
         it != pending_reverse_open_by_instrument_.end();) {
        const auto pos_it = atomic_context_.net_positions.find(it->first);
        const std::int32_t position =
            pos_it == atomic_context_.net_positions.end() ? 0 : pos_it->second;
        if (position != 0) {
            ++it;
            continue;
        }
        SignalIntent pending_open = it->second;
        pending_open.ts_ns = state.ts_ns;
        if (std::isfinite(state.bar_close) && state.bar_close > 0.0) {
            pending_open.limit_price = state.bar_close;
        }
        gated.push_back(pending_open);
        it = pending_reverse_open_by_instrument_.erase(it);
    }

    return gated;
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

bool CompositeStrategy::IsValidRunType(const std::string& run_type) {
    return run_type == "live" || run_type == "sim" || run_type == "backtest";
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
    sub_strategies_.clear();
    order_aware_strategies_.clear();
    trace_providers_.clear();
    owned_atomic_strategies_.clear();

    std::vector<SubStrategyDefinition> active_definitions;
    active_definitions.reserve(definition_.sub_strategies.size());
    for (const auto& definition : definition_.sub_strategies) {
        if (definition.enabled) {
            active_definitions.push_back(definition);
        }
    }

    const auto on_atomic_strategy = [&](const SubStrategyDefinition& definition,
                                        IAtomicStrategy* strategy) {
        if (auto* trace_provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(strategy);
            trace_provider != nullptr) {
            AtomicTraceSlot slot;
            slot.strategy_id = definition.id;
            slot.strategy_type = definition.type;
            slot.provider = trace_provider;
            trace_providers_.push_back(std::move(slot));
        }
    };

    std::vector<ISubStrategy*> typed = BuildTypedStrategies<ISubStrategy>(
        active_definitions, factory_, &owned_atomic_strategies_, &order_aware_strategies_,
        on_atomic_strategy);
    sub_strategies_.reserve(typed.size());
    for (std::size_t i = 0; i < typed.size(); ++i) {
        SubStrategySlot slot;
        slot.strategy = typed[i];
        slot.entry_market_regimes = active_definitions[i].entry_market_regimes;
        sub_strategies_.push_back(std::move(slot));
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
