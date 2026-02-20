#include "quant_hft/strategy/composite_strategy.h"

#include <algorithm>
#include <cmath>
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

constexpr const char* kStateRunType = "run_type";
constexpr const char* kStateRunMode = "run_mode";
constexpr const char* kStateAccountEquity = "account_equity";
constexpr const char* kStateTotalPnlAfterCost = "total_pnl_after_cost";
constexpr const char* kStateMarginUsed = "margin_used";
constexpr const char* kStateAvailable = "available";
constexpr const char* kStateNetPosPrefix = "net_pos.";
constexpr const char* kStateAvgOpenPrefix = "avg_open.";
constexpr const char* kStateMultiplierPrefix = "multiplier.";
constexpr const char* kStateOwnerPrefix = "owner.";
constexpr const char* kStateAtomicPrefix = "atomic.";

bool ParseInt32(const std::string& text, std::int32_t* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const long value = std::stol(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = static_cast<std::int32_t>(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseDouble(const std::string& text, double* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const double value = std::stod(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = value;
        return true;
    } catch (...) {
        return false;
    }
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
    if (definition_.run_type != "backtest" && !definition_.enable_non_backtest) {
        throw std::runtime_error(
            "run_mode is not backtest but enable_non_backtest is false; set "
            "enable_non_backtest=true to allow sim/live");
    }
    atomic_context_.run_type = definition_.run_type;
    atomic_context_.run_mode = RunModeFromString(definition_.run_type);
    std::string merger_error;
    signal_merger_ = CreateSignalMerger(definition_.merge_rule, &merger_error);
    if (signal_merger_ == nullptr) {
        throw std::runtime_error(merger_error.empty() ? "failed to create signal merger"
                                                      : merger_error);
    }

    BuildAtomicStrategies();
}

std::vector<SignalIntent> CompositeStrategy::OnState(const StateSnapshot7D& state) {
    atomic_context_.market_regime = state.market_regime;
    std::vector<SignalIntent> non_open_signals;
    std::vector<SignalIntent> opening_signals;
    const bool allow_opening_by_time_filter = AllowOpeningByTimeFilters(state.ts_ns);

    for (IRiskControlStrategy* risk_strategy : risk_control_strategies_) {
        if (risk_strategy == nullptr) {
            continue;
        }
        std::vector<SignalIntent> signals = risk_strategy->OnState(state, atomic_context_);
        non_open_signals.insert(non_open_signals.end(), signals.begin(), signals.end());
    }

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
                if (!allow_opening_by_time_filter) {
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

void CompositeStrategy::OnAccountSnapshot(const TradingAccountSnapshot& snapshot) {
    atomic_context_.account_equity = snapshot.balance;
    atomic_context_.available = snapshot.available;
    atomic_context_.margin_used = snapshot.curr_margin;
}

std::vector<SignalIntent> CompositeStrategy::OnTimer(EpochNanos now_ns) {
    (void)now_ns;
    return {};
}

std::vector<StrategyMetric> CompositeStrategy::CollectMetrics() const {
    return {
        StrategyMetric{"strategy.composite.sub_strategy_count",
                       static_cast<double>(sub_strategies_.size()),
                       {{"strategy_id", strategy_context_.strategy_id}}},
        StrategyMetric{"strategy.composite.time_filter_count",
                       static_cast<double>(time_filters_.size()),
                       {{"strategy_id", strategy_context_.strategy_id}}},
        StrategyMetric{"strategy.composite.risk_control_count",
                       static_cast<double>(risk_control_strategies_.size()),
                       {{"strategy_id", strategy_context_.strategy_id}}},
        StrategyMetric{"strategy.composite.net_position_instruments",
                       static_cast<double>(atomic_context_.net_positions.size()),
                       {{"strategy_id", strategy_context_.strategy_id}}},
        StrategyMetric{"strategy.composite.account_equity", atomic_context_.account_equity,
                       {{"strategy_id", strategy_context_.strategy_id}}},
        StrategyMetric{"strategy.composite.margin_used", atomic_context_.margin_used,
                       {{"strategy_id", strategy_context_.strategy_id}}},
        StrategyMetric{"strategy.composite.available", atomic_context_.available,
                       {{"strategy_id", strategy_context_.strategy_id}}},
    };
}

bool CompositeStrategy::SaveState(StrategyState* out, std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "state output is null";
        }
        return false;
    }
    out->clear();
    (*out)[kStateRunType] = atomic_context_.run_type;
    (*out)[kStateRunMode] = RunModeToString(atomic_context_.run_mode);
    (*out)[kStateAccountEquity] = std::to_string(atomic_context_.account_equity);
    (*out)[kStateTotalPnlAfterCost] = std::to_string(atomic_context_.total_pnl_after_cost);
    (*out)[kStateMarginUsed] = std::to_string(atomic_context_.margin_used);
    (*out)[kStateAvailable] = std::to_string(atomic_context_.available);

    for (const auto& [instrument_id, net_position] : atomic_context_.net_positions) {
        (*out)[std::string(kStateNetPosPrefix) + instrument_id] = std::to_string(net_position);
    }
    for (const auto& [instrument_id, avg_open] : atomic_context_.avg_open_prices) {
        (*out)[std::string(kStateAvgOpenPrefix) + instrument_id] = std::to_string(avg_open);
    }
    for (const auto& [instrument_id, multiplier] : atomic_context_.contract_multipliers) {
        (*out)[std::string(kStateMultiplierPrefix) + instrument_id] = std::to_string(multiplier);
    }
    for (const auto& [instrument_id, owner] : position_owner_by_instrument_) {
        (*out)[std::string(kStateOwnerPrefix) + instrument_id] = owner;
    }

    for (const auto& strategy : owned_atomic_strategies_) {
        auto* serializable = dynamic_cast<IAtomicStateSerializable*>(strategy.get());
        if (serializable == nullptr) {
            continue;
        }
        AtomicState atomic_state;
        std::string atomic_error;
        if (!serializable->SaveState(&atomic_state, &atomic_error)) {
            if (error != nullptr) {
                *error = "save atomic state failed for strategy `" + strategy->GetId() + "`: " +
                         atomic_error;
            }
            return false;
        }
        const std::string prefix = std::string(kStateAtomicPrefix) + strategy->GetId() + ".";
        for (const auto& [key, value] : atomic_state) {
            (*out)[prefix + key] = value;
        }
    }
    return true;
}

bool CompositeStrategy::LoadState(const StrategyState& state, std::string* error) {
    atomic_context_.net_positions.clear();
    atomic_context_.avg_open_prices.clear();
    atomic_context_.contract_multipliers.clear();
    position_owner_by_instrument_.clear();

    std::unordered_map<std::string, AtomicState> atomic_state_by_strategy;
    for (const auto& [key, value] : state) {
        if (key == kStateRunType) {
            atomic_context_.run_type = value;
            continue;
        }
        if (key == kStateRunMode) {
            atomic_context_.run_mode = RunModeFromString(value);
            continue;
        }
        if (key == kStateAccountEquity) {
            if (!ParseDouble(value, &atomic_context_.account_equity)) {
                if (error != nullptr) {
                    *error = "invalid account_equity";
                }
                return false;
            }
            continue;
        }
        if (key == kStateTotalPnlAfterCost) {
            if (!ParseDouble(value, &atomic_context_.total_pnl_after_cost)) {
                if (error != nullptr) {
                    *error = "invalid total_pnl_after_cost";
                }
                return false;
            }
            continue;
        }
        if (key == kStateMarginUsed) {
            if (!ParseDouble(value, &atomic_context_.margin_used)) {
                if (error != nullptr) {
                    *error = "invalid margin_used";
                }
                return false;
            }
            continue;
        }
        if (key == kStateAvailable) {
            if (!ParseDouble(value, &atomic_context_.available)) {
                if (error != nullptr) {
                    *error = "invalid available";
                }
                return false;
            }
            continue;
        }

        if (key.rfind(kStateNetPosPrefix, 0) == 0) {
            std::int32_t parsed = 0;
            if (!ParseInt32(value, &parsed)) {
                if (error != nullptr) {
                    *error = "invalid net position for key `" + key + "`";
                }
                return false;
            }
            atomic_context_.net_positions[key.substr(std::string(kStateNetPosPrefix).size())] =
                parsed;
            continue;
        }
        if (key.rfind(kStateAvgOpenPrefix, 0) == 0) {
            double parsed = 0.0;
            if (!ParseDouble(value, &parsed)) {
                if (error != nullptr) {
                    *error = "invalid avg open price for key `" + key + "`";
                }
                return false;
            }
            atomic_context_.avg_open_prices[key.substr(std::string(kStateAvgOpenPrefix).size())] =
                parsed;
            continue;
        }
        if (key.rfind(kStateMultiplierPrefix, 0) == 0) {
            double parsed = 0.0;
            if (!ParseDouble(value, &parsed)) {
                if (error != nullptr) {
                    *error = "invalid multiplier for key `" + key + "`";
                }
                return false;
            }
            atomic_context_.contract_multipliers
                [key.substr(std::string(kStateMultiplierPrefix).size())] = parsed;
            continue;
        }
        if (key.rfind(kStateOwnerPrefix, 0) == 0) {
            position_owner_by_instrument_[key.substr(std::string(kStateOwnerPrefix).size())] =
                value;
            continue;
        }
        if (key.rfind(kStateAtomicPrefix, 0) == 0) {
            const std::string rest = key.substr(std::string(kStateAtomicPrefix).size());
            const std::size_t split = rest.find('.');
            if (split == std::string::npos || split == 0 || split + 1 >= rest.size()) {
                if (error != nullptr) {
                    *error = "invalid atomic state key `" + key + "`";
                }
                return false;
            }
            const std::string strategy_id = rest.substr(0, split);
            const std::string atomic_key = rest.substr(split + 1);
            atomic_state_by_strategy[strategy_id][atomic_key] = value;
            continue;
        }
    }

    for (const auto& strategy : owned_atomic_strategies_) {
        auto* serializable = dynamic_cast<IAtomicStateSerializable*>(strategy.get());
        if (serializable == nullptr) {
            continue;
        }
        const auto it = atomic_state_by_strategy.find(strategy->GetId());
        if (it == atomic_state_by_strategy.end()) {
            continue;
        }
        std::string atomic_error;
        if (!serializable->LoadState(it->second, &atomic_error)) {
            if (error != nullptr) {
                *error = "load atomic state failed for strategy `" + strategy->GetId() + "`: " +
                         atomic_error;
            }
            return false;
        }
    }
    return true;
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
    time_filters_.clear();
    risk_control_strategies_.clear();
    order_aware_strategies_.clear();
    trace_providers_.clear();
    owned_atomic_strategies_.clear();
    signal_merger_.reset();
    atomic_context_.net_positions.clear();
    atomic_context_.avg_open_prices.clear();
    atomic_context_.contract_multipliers.clear();
    atomic_context_.risk_limits.clear();
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

bool CompositeStrategy::AllowOpeningByTimeFilters(EpochNanos now_ns) {
    for (ITimeFilterStrategy* time_filter : time_filters_) {
        if (time_filter == nullptr) {
            continue;
        }
        if (!time_filter->AllowOpening(now_ns)) {
            return false;
        }
    }
    return true;
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
    if (signal_merger_ == nullptr) {
        return signals;
    }
    return signal_merger_->Merge(signals);
}

AtomicParams CompositeStrategy::MergeParamsForRunMode(const SubStrategyDefinition& definition,
                                                      RunMode run_mode) {
    AtomicParams merged = definition.params;
    const AtomicParams* override_params = nullptr;
    switch (run_mode) {
        case RunMode::kBacktest:
            override_params = &definition.overrides.backtest_params;
            break;
        case RunMode::kSim:
            override_params = &definition.overrides.sim_params;
            break;
        case RunMode::kLive:
        default:
            override_params = &definition.overrides.live_params;
            break;
    }
    if (override_params != nullptr) {
        for (const auto& [key, value] : *override_params) {
            merged[key] = value;
        }
    }
    return merged;
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
    time_filters_.clear();
    risk_control_strategies_.clear();
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

    for (const auto& definition : active_definitions) {
        std::string error;
        std::unique_ptr<IAtomicStrategy> strategy =
            factory_->Create(definition.type, &error, definition.id);
        if (!strategy) {
            throw std::runtime_error(error.empty() ? "failed to create atomic strategy" : error);
        }
        strategy->Init(MergeParamsForRunMode(definition, atomic_context_.run_mode));

        bool bound_to_supported_interface = false;
        if (auto* sub_strategy = dynamic_cast<ISubStrategy*>(strategy.get());
            sub_strategy != nullptr) {
            SubStrategySlot slot;
            slot.strategy = sub_strategy;
            slot.entry_market_regimes = definition.entry_market_regimes;
            sub_strategies_.push_back(std::move(slot));
            bound_to_supported_interface = true;
        }
        if (auto* time_filter = dynamic_cast<ITimeFilterStrategy*>(strategy.get());
            time_filter != nullptr) {
            time_filters_.push_back(time_filter);
            bound_to_supported_interface = true;
        }
        if (auto* risk_control = dynamic_cast<IRiskControlStrategy*>(strategy.get());
            risk_control != nullptr) {
            risk_control_strategies_.push_back(risk_control);
            bound_to_supported_interface = true;
        }
        if (!bound_to_supported_interface) {
            throw std::runtime_error("atomic strategy id '" + definition.id +
                                     "' does not implement supported interface");
        }
        if (auto* order_aware_strategy = dynamic_cast<IAtomicOrderAware*>(strategy.get());
            order_aware_strategy != nullptr) {
            order_aware_strategies_.push_back(order_aware_strategy);
        }
        if (auto* trace_provider = dynamic_cast<IAtomicIndicatorTraceProvider*>(strategy.get());
            trace_provider != nullptr) {
            AtomicTraceSlot slot;
            slot.strategy_id = definition.id;
            slot.strategy_type = definition.type;
            slot.provider = trace_provider;
            trace_providers_.push_back(std::move(slot));
        }
        owned_atomic_strategies_.push_back(std::move(strategy));
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
