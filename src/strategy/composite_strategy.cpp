#include "quant_hft/strategy/composite_strategy.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <initializer_list>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "quant_hft/core/structured_log.h"
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
constexpr std::int64_t kNanosPerSecond = 1'000'000'000LL;
constexpr std::int64_t kSecondsPerMinute = 60LL;
constexpr std::int64_t kSecondsPerHour = 60LL * kSecondsPerMinute;
constexpr std::int64_t kMinutesPerDay = 24LL * 60LL;
constexpr std::int64_t kSecondsPerDay = kMinutesPerDay * kSecondsPerMinute;
constexpr std::int32_t kAsiaShanghaiUtcOffsetHours = 8;

std::string FormatDouble(double value) {
    if (!std::isfinite(value)) {
        return "nan";
    }
    std::ostringstream out;
    out << value;
    return out.str();
}

CtpRuntimeConfig BuildLogRuntime(const AtomicStrategyContext& ctx) {
    CtpRuntimeConfig runtime;
    runtime.log_level = ctx.log_level;
    runtime.log_sink = ctx.log_sink;
    return runtime;
}

void EmitCompositeLog(const AtomicStrategyContext& ctx,
                      const std::string& level,
                      const std::string& event,
                      const LogFields& fields) {
    const CtpRuntimeConfig runtime = BuildLogRuntime(ctx);
    EmitStructuredLog(&runtime, "composite_strategy", level, event, fields);
}

std::string BuildBacktestRolloverOrderId(const std::string& action,
                                         const std::string& previous_instrument_id,
                                         const std::string& current_instrument_id,
                                         EpochNanos ts_ns) {
    return "backtest-rollover-" + action + "-" + previous_instrument_id + "-" +
           current_instrument_id + "-" + std::to_string(ts_ns);
}

std::string Trim(std::string text) {
    const auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    while (!text.empty() && is_space(static_cast<unsigned char>(text.front()))) {
        text.erase(text.begin());
    }
    while (!text.empty() && is_space(static_cast<unsigned char>(text.back()))) {
        text.pop_back();
    }
    return text;
}

std::string ToLower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return text;
}

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

bool ParseBoolText(const std::string& text, bool* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string normalized = ToLower(Trim(text));
    if (normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on") {
        *out = true;
        return true;
    }
    if (normalized == "0" || normalized == "false" || normalized == "no" || normalized == "off") {
        *out = false;
        return true;
    }
    return false;
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

std::optional<std::string> FindParamValue(const AtomicParams& params,
                                          std::initializer_list<const char*> keys) {
    for (const char* key : keys) {
        const auto it = params.find(key);
        if (it != params.end() && !Trim(it->second).empty()) {
            return it->second;
        }
    }
    return std::nullopt;
}

std::optional<double> ParseOptionalDoubleParam(const AtomicParams& params,
                                               std::initializer_list<const char*> keys,
                                               const std::string& field_name) {
    const std::optional<std::string> value = FindParamValue(params, keys);
    if (!value.has_value()) {
        return std::nullopt;
    }
    double parsed = 0.0;
    if (!ParseDouble(*value, &parsed)) {
        throw std::runtime_error("sub-strategy risk param `" + field_name + "` must be numeric");
    }
    return parsed;
}

std::optional<std::int32_t> ParseOptionalIntParam(const AtomicParams& params,
                                                  std::initializer_list<const char*> keys,
                                                  const std::string& field_name) {
    const std::optional<std::string> value = FindParamValue(params, keys);
    if (!value.has_value()) {
        return std::nullopt;
    }
    std::int32_t parsed = 0;
    if (!ParseInt32(*value, &parsed)) {
        throw std::runtime_error("sub-strategy risk param `" + field_name + "` must be an integer");
    }
    return parsed;
}

bool ParseHourMinute(const std::string& text, std::int32_t* out_minutes) {
    if (out_minutes == nullptr) {
        return false;
    }
    const std::string normalized = Trim(text);
    if (normalized.size() != 5 || normalized[2] != ':' ||
        !std::isdigit(static_cast<unsigned char>(normalized[0])) ||
        !std::isdigit(static_cast<unsigned char>(normalized[1])) ||
        !std::isdigit(static_cast<unsigned char>(normalized[3])) ||
        !std::isdigit(static_cast<unsigned char>(normalized[4]))) {
        return false;
    }
    const std::int32_t hour =
        static_cast<std::int32_t>((normalized[0] - '0') * 10 + (normalized[1] - '0'));
    const std::int32_t minute =
        static_cast<std::int32_t>((normalized[3] - '0') * 10 + (normalized[4] - '0'));
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        return false;
    }
    *out_minutes = hour * 60 + minute;
    return true;
}

bool ParseTimeWindowList(const std::string& text,
                         std::vector<std::pair<std::int32_t, std::int32_t>>* out,
                         std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "time window output is null";
        }
        return false;
    }
    out->clear();
    const std::string normalized = Trim(text);
    if (normalized.empty()) {
        return true;
    }

    std::size_t start = 0;
    while (start <= normalized.size()) {
        const std::size_t comma = normalized.find(',', start);
        const std::string token =
            Trim(comma == std::string::npos ? normalized.substr(start)
                                            : normalized.substr(start, comma - start));
        if (token.empty()) {
            if (error != nullptr) {
                *error = "time window list contains an empty item";
            }
            return false;
        }
        const std::size_t dash = token.find('-');
        if (dash == std::string::npos || dash == 0 || dash + 1 >= token.size()) {
            if (error != nullptr) {
                *error = "invalid time window `" + token + "`";
            }
            return false;
        }
        std::int32_t start_minute = 0;
        std::int32_t end_minute = 0;
        if (!ParseHourMinute(token.substr(0, dash), &start_minute) ||
            !ParseHourMinute(token.substr(dash + 1), &end_minute)) {
            if (error != nullptr) {
                *error = "invalid time window `" + token + "`";
            }
            return false;
        }
        if (start_minute == end_minute) {
            if (error != nullptr) {
                *error = "time window `" + token + "` must not be zero length";
            }
            return false;
        }
        out->emplace_back(start_minute, end_minute);
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return true;
}

bool ResolveTimezoneOffsetHours(const std::string& timezone, std::int32_t* out,
                                std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "timezone output is null";
        }
        return false;
    }
    const std::string normalized = ToLower(Trim(timezone));
    if (normalized.empty() || normalized == "asia/shanghai") {
        *out = 8;
        return true;
    }
    if (normalized == "utc") {
        *out = 0;
        return true;
    }
    if (error != nullptr) {
        *error = "window_timezone only supports UTC and Asia/Shanghai";
    }
    return false;
}

std::int32_t MinuteOfDayFromEpochNs(EpochNanos now_ns, std::int32_t timezone_offset_hours,
                                    RunMode run_mode) {
    const std::int64_t utc_seconds = now_ns / kNanosPerSecond;
    std::int32_t effective_timezone_offset_hours = timezone_offset_hours;
    if (run_mode == RunMode::kBacktest) {
        // Backtest replay encodes exchange-local (Asia/Shanghai) wall clock time directly into
        // EpochNanos. Normalize requested window timezone against that baseline instead of adding
        // another +8 hour shift on top of an already-local timestamp.
        effective_timezone_offset_hours -= kAsiaShanghaiUtcOffsetHours;
    }
    const std::int64_t local_seconds =
        utc_seconds + static_cast<std::int64_t>(effective_timezone_offset_hours) * kSecondsPerHour;
    const std::int64_t seconds_into_day =
        (local_seconds % kSecondsPerDay + kSecondsPerDay) % kSecondsPerDay;
    return static_cast<std::int32_t>(seconds_into_day / kSecondsPerMinute);
}

std::int64_t DayIndexFromEpochNs(EpochNanos now_ns, std::int32_t timezone_offset_hours,
                                 RunMode run_mode) {
    const std::int64_t utc_seconds = now_ns / kNanosPerSecond;
    std::int32_t effective_timezone_offset_hours = timezone_offset_hours;
    if (run_mode == RunMode::kBacktest) {
        effective_timezone_offset_hours -= kAsiaShanghaiUtcOffsetHours;
    }
    const std::int64_t local_seconds =
        utc_seconds + static_cast<std::int64_t>(effective_timezone_offset_hours) * kSecondsPerHour;
    return local_seconds >= 0 ? local_seconds / kSecondsPerDay
                              : ((local_seconds - kSecondsPerDay + 1) / kSecondsPerDay);
}

EpochNanos ResolveOrderEventTs(const OrderEvent& event) {
    if (event.ts_ns != 0) {
        return event.ts_ns;
    }
    if (event.recv_ts_ns != 0) {
        return event.recv_ts_ns;
    }
    return event.exchange_ts_ns;
}

std::string ExtractSymbolPrefixLower(const std::string& instrument_id) {
    std::string prefix;
    for (unsigned char ch : instrument_id) {
        if (std::isalpha(ch) == 0) {
            break;
        }
        prefix.push_back(static_cast<char>(std::tolower(ch)));
    }
    return prefix;
}

std::string ToUpper(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    return text;
}

double ResolveContractMultiplier(const AtomicStrategyContext& ctx,
                                 const std::string& instrument_id) {
    auto find_multiplier = [&](const std::string& key) -> std::optional<double> {
        if (key.empty()) {
            return std::nullopt;
        }
        const auto it = ctx.contract_multipliers.find(key);
        if (it != ctx.contract_multipliers.end() && std::isfinite(it->second) && it->second > 0.0) {
            return it->second;
        }
        return std::nullopt;
    };

    if (const auto exact = find_multiplier(instrument_id); exact.has_value()) {
        return *exact;
    }
    const std::string prefix = ExtractSymbolPrefixLower(instrument_id);
    if (const auto lower = find_multiplier(prefix); lower.has_value()) {
        return *lower;
    }
    if (const auto upper = find_multiplier(ToUpper(prefix)); upper.has_value()) {
        return *upper;
    }
    return 1.0;
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
    atomic_context_.log_level = "info";
    atomic_context_.log_sink = "stderr";
    last_filled_volume_by_order_.clear();
    position_owner_by_instrument_.clear();
    active_force_close_window_by_instrument_.clear();
    risk_guard_state_by_strategy_.clear();

    if (const auto log_level_it = strategy_context_.metadata.find("log_level");
        log_level_it != strategy_context_.metadata.end() && !log_level_it->second.empty()) {
        atomic_context_.log_level = log_level_it->second;
    }
    if (const auto log_sink_it = strategy_context_.metadata.find("log_sink");
        log_sink_it != strategy_context_.metadata.end() && !log_sink_it->second.empty()) {
        atomic_context_.log_sink = log_sink_it->second;
    }

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
    const std::int32_t state_timeframe_minutes =
        state.timeframe_minutes > 0 ? state.timeframe_minutes : 1;
    const bool allow_opening_by_time_filter =
        AllowOpeningByTimeFilters(state.ts_ns, state_timeframe_minutes);

    for (const RiskControlSlot& slot : risk_control_strategies_) {
        if (slot.strategy == nullptr || slot.timeframe_minutes != state_timeframe_minutes) {
            continue;
        }
        std::vector<SignalIntent> signals = slot.strategy->OnState(state, atomic_context_);
        non_open_signals.insert(non_open_signals.end(), signals.begin(), signals.end());
    }

    for (const SubStrategySlot& slot : sub_strategies_) {
        if (slot.strategy == nullptr || slot.timeframe_minutes != state_timeframe_minutes) {
            continue;
        }
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
                if (IsOpenSignalBlockedByStrategyWindows(slot, state.ts_ns)) {
                    continue;
                }
                if (IsOpenSignalBlockedByRiskGuards(slot, state.ts_ns)) {
                    continue;
                }
                opening_signals.push_back(signal);
                continue;
            }
            non_open_signals.push_back(signal);
        }
    }

    OpeningGateResult opening_gate = ApplyOpeningGate(opening_signals, state);
    std::vector<SignalIntent> all_signals = ApplyNonOpenSignalGate(non_open_signals);
    all_signals.insert(all_signals.end(), opening_gate.immediate_open_signals.begin(),
                       opening_gate.immediate_open_signals.end());
    all_signals.insert(all_signals.end(), opening_gate.reverse_close_signals.begin(),
                       opening_gate.reverse_close_signals.end());

    std::vector<SignalIntent> merged_signals = MergeSignals(all_signals);
    if (opening_gate.reverse_open_by_close_trace.empty()) {
        return merged_signals;
    }

    std::vector<SignalIntent> final_signals;
    final_signals.reserve(merged_signals.size() + opening_gate.reverse_open_by_close_trace.size());
    for (const SignalIntent& signal : merged_signals) {
        final_signals.push_back(signal);
        const auto reverse_it = opening_gate.reverse_open_by_close_trace.find(signal.trace_id);
        if (reverse_it == opening_gate.reverse_open_by_close_trace.end()) {
            continue;
        }
        final_signals.push_back(reverse_it->second);
    }
    return final_signals;
}

void CompositeStrategy::OnOrderEvent(const OrderEvent& event) {
    const std::string order_id =
        !event.exchange_order_id.empty() ? event.exchange_order_id : event.client_order_id;
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

    double realized_pnl_delta = 0.0;
    bool has_close_fill = false;
    if (old_position != 0 && signed_delta != 0 &&
        ((old_position > 0 && signed_delta < 0) || (old_position < 0 && signed_delta > 0))) {
        const std::int32_t close_volume = std::min(std::abs(old_position), std::abs(signed_delta));
        if (close_volume > 0) {
            has_close_fill = true;
            const double multiplier = ResolveContractMultiplier(atomic_context_, instrument);
            realized_pnl_delta =
                old_position > 0
                    ? (fill_price - old_avg) * static_cast<double>(close_volume) * multiplier
                    : (old_avg - fill_price) * static_cast<double>(close_volume) * multiplier;
        }
    }
    if (has_close_fill) {
        ApplyRiskGuardRealizedPnl(event.strategy_id, ResolveOrderEventTs(event),
                                  realized_pnl_delta);
    }

    if (new_position == 0) {
        atomic_context_.net_positions[instrument] = 0;
        atomic_context_.avg_open_prices.erase(instrument);
        position_owner_by_instrument_.erase(instrument);
        active_force_close_window_by_instrument_.erase(instrument);
    } else if (old_position == 0) {
        atomic_context_.net_positions[instrument] = new_position;
        atomic_context_.avg_open_prices[instrument] = fill_price;
        active_force_close_window_by_instrument_.erase(instrument);
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

std::vector<SignalIntent> CompositeStrategy::OnBacktestTick(const std::string& instrument_id,
                                                            EpochNanos now_ns, double last_price) {
    if (instrument_id.empty()) {
        return {};
    }

    const auto pos_it = atomic_context_.net_positions.find(instrument_id);
    const std::int32_t position =
        pos_it == atomic_context_.net_positions.end() ? 0 : pos_it->second;
    if (position == 0) {
        active_force_close_window_by_instrument_.erase(instrument_id);
        return {};
    }

    const auto owner_it = position_owner_by_instrument_.find(instrument_id);
    if (owner_it == position_owner_by_instrument_.end() || owner_it->second.empty()) {
        active_force_close_window_by_instrument_.erase(instrument_id);
        return {};
    }

    const SubStrategySlot* slot = FindSubStrategySlot(owner_it->second);
    if (slot == nullptr) {
        active_force_close_window_by_instrument_.erase(instrument_id);
        return {};
    }

    std::vector<SignalIntent> tick_signals;
    if (slot->backtest_tick_aware != nullptr && std::isfinite(last_price)) {
        AtomicTickSnapshot tick;
        tick.instrument_id = instrument_id;
        tick.ts_ns = now_ns;
        tick.last_price = last_price;
        tick_signals = slot->backtest_tick_aware->OnBacktestTick(tick, atomic_context_);
    }

    std::string window_key;
    if (!FindMatchingWindow(slot->force_close_windows, now_ns, slot->timezone_offset_hours,
                            &window_key)) {
        active_force_close_window_by_instrument_.erase(instrument_id);
        return MergeSignals(ApplyNonOpenSignalGate(tick_signals));
    }

    const auto active_it = active_force_close_window_by_instrument_.find(instrument_id);
    if (active_it != active_force_close_window_by_instrument_.end() &&
        active_it->second == window_key) {
        return MergeSignals(ApplyNonOpenSignalGate(tick_signals));
    }

    active_force_close_window_by_instrument_[instrument_id] = window_key;
    SignalIntent signal;
    signal.strategy_id = owner_it->second;
    signal.instrument_id = instrument_id;
    signal.signal_type = SignalType::kForceClose;
    signal.side = position > 0 ? Side::kSell : Side::kBuy;
    signal.offset = OffsetFlag::kClose;
    signal.volume = std::abs(position);
    signal.limit_price = std::isfinite(last_price) ? last_price : 0.0;
    signal.ts_ns = now_ns;
    signal.trace_id = owner_it->second + "-window-force-close-" + instrument_id + "-" + window_key;
    tick_signals.push_back(std::move(signal));
    return MergeSignals(ApplyNonOpenSignalGate(tick_signals));
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
        StrategyMetric{"strategy.composite.account_equity",
                       atomic_context_.account_equity,
                       {{"strategy_id", strategy_context_.strategy_id}}},
        StrategyMetric{"strategy.composite.margin_used",
                       atomic_context_.margin_used,
                       {{"strategy_id", strategy_context_.strategy_id}}},
        StrategyMetric{"strategy.composite.available",
                       atomic_context_.available,
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
                *error = "save atomic state failed for strategy `" + strategy->GetId() +
                         "`: " + atomic_error;
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
    active_force_close_window_by_instrument_.clear();
    risk_guard_state_by_strategy_.clear();

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
            atomic_context_
                .contract_multipliers[key.substr(std::string(kStateMultiplierPrefix).size())] =
                parsed;
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
                *error = "load atomic state failed for strategy `" + strategy->GetId() +
                         "`: " + atomic_error;
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

std::string CompositeStrategy::GetBacktestPositionOwner(const std::string& instrument_id) const {
    const auto it = position_owner_by_instrument_.find(instrument_id);
    return it == position_owner_by_instrument_.end() ? "" : it->second;
}

void CompositeStrategy::ApplyBacktestRollover(const std::string& previous_instrument_id,
                                              const std::string& current_instrument_id,
                                              double close_price, double open_price,
                                              EpochNanos ts_ns) {
    if (previous_instrument_id.empty() || current_instrument_id.empty() ||
        previous_instrument_id == current_instrument_id) {
        return;
    }

    const auto pos_it = atomic_context_.net_positions.find(previous_instrument_id);
    if (pos_it == atomic_context_.net_positions.end() || pos_it->second == 0) {
        return;
    }

    const auto owner_it = position_owner_by_instrument_.find(previous_instrument_id);
    if (owner_it == position_owner_by_instrument_.end() || owner_it->second.empty()) {
        return;
    }

    const std::string owner = owner_it->second;
    const std::int32_t rollover_volume = std::abs(pos_it->second);
    const Side close_side = pos_it->second > 0 ? Side::kSell : Side::kBuy;
    const Side open_side = pos_it->second > 0 ? Side::kBuy : Side::kSell;

    OrderEvent close_event;
    close_event.strategy_id = owner;
    close_event.client_order_id =
        BuildBacktestRolloverOrderId("close", previous_instrument_id, current_instrument_id, ts_ns);
    close_event.exchange_order_id = close_event.client_order_id;
    close_event.instrument_id = previous_instrument_id;
    close_event.side = close_side;
    close_event.offset = OffsetFlag::kClose;
    close_event.status = OrderStatus::kFilled;
    close_event.total_volume = rollover_volume;
    close_event.filled_volume = rollover_volume;
    close_event.avg_fill_price = close_price;
    close_event.trade_id = close_event.client_order_id;
    close_event.event_source = "backtest_rollover";
    close_event.exchange_ts_ns = ts_ns;
    close_event.recv_ts_ns = ts_ns;
    close_event.ts_ns = ts_ns;
    OnOrderEvent(close_event);

    OrderEvent open_event;
    open_event.strategy_id = owner;
    open_event.client_order_id =
        BuildBacktestRolloverOrderId("open", previous_instrument_id, current_instrument_id, ts_ns);
    open_event.exchange_order_id = open_event.client_order_id;
    open_event.instrument_id = current_instrument_id;
    open_event.side = open_side;
    open_event.offset = OffsetFlag::kOpen;
    open_event.status = OrderStatus::kFilled;
    open_event.total_volume = rollover_volume;
    open_event.filled_volume = rollover_volume;
    open_event.avg_fill_price = open_price;
    open_event.trade_id = open_event.client_order_id;
    open_event.event_source = "backtest_rollover";
    open_event.exchange_ts_ns = ts_ns;
    open_event.recv_ts_ns = ts_ns;
    open_event.ts_ns = ts_ns;
    OnOrderEvent(open_event);
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
    sub_strategy_slot_index_by_id_.clear();
    owned_atomic_strategies_.clear();
    signal_merger_.reset();
    atomic_context_.net_positions.clear();
    atomic_context_.avg_open_prices.clear();
    atomic_context_.contract_multipliers.clear();
    atomic_context_.risk_limits.clear();
    last_filled_volume_by_order_.clear();
    position_owner_by_instrument_.clear();
    active_force_close_window_by_instrument_.clear();
    risk_guard_state_by_strategy_.clear();
}

bool CompositeStrategy::IsOpenSignalAllowedByRegime(const SubStrategySlot& slot,
                                                    MarketRegime regime) const {
    if (slot.entry_market_regimes.empty()) {
        return true;
    }
    return std::find(slot.entry_market_regimes.begin(), slot.entry_market_regimes.end(), regime) !=
           slot.entry_market_regimes.end();
}

bool CompositeStrategy::AllowOpeningByTimeFilters(EpochNanos now_ns,
                                                  std::int32_t timeframe_minutes) {
    for (const TimeFilterSlot& slot : time_filters_) {
        if (slot.strategy == nullptr || slot.timeframe_minutes != timeframe_minutes) {
            continue;
        }
        if (!slot.strategy->AllowOpening(now_ns)) {
            return false;
        }
    }
    return true;
}

bool CompositeStrategy::IsOpenSignalBlockedByStrategyWindows(const SubStrategySlot& slot,
                                                             EpochNanos now_ns) const {
    if (FindMatchingWindow(slot.forbid_open_windows, now_ns, slot.timezone_offset_hours)) {
        return true;
    }
    if (FindMatchingWindow(slot.session_start_no_open_windows, now_ns,
                           slot.timezone_offset_hours)) {
        return true;
    }
    return FindMatchingWindow(slot.force_close_windows, now_ns, slot.timezone_offset_hours);
}

bool CompositeStrategy::IsOpenSignalBlockedByRiskGuards(const SubStrategySlot& slot,
                                                        EpochNanos now_ns) {
    const bool has_daily_loss_guard = slot.daily_max_loss_r > 0.0 && slot.fixed_r > 0.0;
    const bool has_consecutive_loss_guard = slot.max_consecutive_losses > 0;
    if (!has_daily_loss_guard && !has_consecutive_loss_guard) {
        return false;
    }

    const std::int64_t day_index =
        DayIndexFromEpochNs(now_ns, slot.timezone_offset_hours, atomic_context_.run_mode);
    StrategyRiskGuardState& state = risk_guard_state_by_strategy_[slot.strategy_id];
    if (!state.has_daily_day || state.daily_day_index != day_index) {
        state.has_daily_day = true;
        state.daily_day_index = day_index;
        state.daily_realized_pnl = 0.0;
    }
    if (state.has_loss_pause_day && state.loss_pause_day_index != day_index) {
        state.has_loss_pause_day = false;
        state.loss_pause_day_index = 0;
        state.consecutive_losses = 0;
    }

    if (has_daily_loss_guard &&
        state.daily_realized_pnl <= -(slot.daily_max_loss_r * slot.fixed_r)) {
        EmitCompositeLog(
            atomic_context_, "warn", "risk_guard_blocked",
            {{"strategy_id", slot.strategy_id},
             {"event_type", "risk_guard_blocked"},
             {"reason", "daily_max_loss_R"},
             {"event_ts_ns", std::to_string(now_ns)},
             {"current_value", FormatDouble(-state.daily_realized_pnl)},
             {"threshold_value", FormatDouble(slot.daily_max_loss_r * slot.fixed_r)}});
        return true;
    }
    if (has_consecutive_loss_guard &&
        (state.has_loss_pause_day || state.consecutive_losses >= slot.max_consecutive_losses)) {
        EmitCompositeLog(
            atomic_context_, "warn", "risk_guard_blocked",
            {{"strategy_id", slot.strategy_id},
             {"event_type", "risk_guard_blocked"},
             {"reason", "max_consecutive_losses"},
             {"event_ts_ns", std::to_string(now_ns)},
             {"current_value", std::to_string(state.consecutive_losses)},
             {"threshold_value", std::to_string(slot.max_consecutive_losses)}});
        return true;
    }
    return false;
}

void CompositeStrategy::ApplyRiskGuardRealizedPnl(const std::string& strategy_id, EpochNanos ts_ns,
                                                  double realized_pnl) {
    const SubStrategySlot* slot = FindSubStrategySlot(strategy_id);
    if (slot == nullptr) {
        return;
    }
    const bool has_daily_loss_guard = slot->daily_max_loss_r > 0.0 && slot->fixed_r > 0.0;
    const bool has_consecutive_loss_guard = slot->max_consecutive_losses > 0;
    if (!has_daily_loss_guard && !has_consecutive_loss_guard) {
        return;
    }

    const std::int64_t day_index =
        DayIndexFromEpochNs(ts_ns, slot->timezone_offset_hours, atomic_context_.run_mode);
    StrategyRiskGuardState& state = risk_guard_state_by_strategy_[strategy_id];
    if (!state.has_daily_day || state.daily_day_index != day_index) {
        state.has_daily_day = true;
        state.daily_day_index = day_index;
        state.daily_realized_pnl = 0.0;
    }

    state.daily_realized_pnl += realized_pnl;
    if (realized_pnl < 0.0) {
        ++state.consecutive_losses;
        if (has_consecutive_loss_guard &&
            state.consecutive_losses >= slot->max_consecutive_losses) {
            state.has_loss_pause_day = true;
            state.loss_pause_day_index = day_index;
        }
    } else {
        state.consecutive_losses = 0;
    }
}

bool CompositeStrategy::FindMatchingWindow(const std::vector<TimeWindow>& windows,
                                           EpochNanos now_ns, std::int32_t timezone_offset_hours,
                                           std::string* window_key) const {
    const std::int32_t minute_of_day =
        MinuteOfDayFromEpochNs(now_ns, timezone_offset_hours, atomic_context_.run_mode);
    for (const TimeWindow& window : windows) {
        bool contains = false;
        if (window.start_minute < window.end_minute) {
            contains = minute_of_day >= window.start_minute && minute_of_day < window.end_minute;
        } else {
            contains = minute_of_day >= window.start_minute || minute_of_day < window.end_minute;
        }
        if (!contains) {
            continue;
        }
        if (window_key != nullptr) {
            *window_key =
                std::to_string(window.start_minute) + "-" + std::to_string(window.end_minute);
        }
        return true;
    }
    return false;
}

const CompositeStrategy::SubStrategySlot* CompositeStrategy::FindSubStrategySlot(
    const std::string& strategy_id) const {
    const auto index_it = sub_strategy_slot_index_by_id_.find(strategy_id);
    if (index_it == sub_strategy_slot_index_by_id_.end()) {
        return nullptr;
    }
    if (index_it->second >= sub_strategies_.size()) {
        return nullptr;
    }
    return &sub_strategies_[index_it->second];
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

CompositeStrategy::OpeningGateResult CompositeStrategy::ApplyOpeningGate(
    const std::vector<SignalIntent>& opening_signals, const StateSnapshot7D& state) {
    OpeningGateResult gated;
    gated.immediate_open_signals.reserve(opening_signals.size());
    gated.reverse_close_signals.reserve(opening_signals.size());

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
            gated.immediate_open_signals.push_back(signal);
            continue;
        }

        if (has_owner && owner_it->second != signal.strategy_id) {
            continue;
        }

        const Side position_side = position > 0 ? Side::kBuy : Side::kSell;
        if (signal.side == position_side) {
            continue;
        }
        const SubStrategySlot* slot = FindSubStrategySlot(signal.strategy_id);
        const bool allow_reverse_open = slot == nullptr ? true : slot->allow_reverse_open;
        if (!allow_reverse_open) {
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
        gated.reverse_open_by_close_trace.emplace(close_signal.trace_id, signal);
        gated.reverse_close_signals.push_back(std::move(close_signal));
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
    sub_strategy_slot_index_by_id_.clear();
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
        AtomicParams merged_params = MergeParamsForRunMode(definition, atomic_context_.run_mode);

        bool allow_reverse_open = true;
        if (const auto allow_it = merged_params.find("allow_reverse_open");
            allow_it != merged_params.end() && !Trim(allow_it->second).empty()) {
            if (!ParseBoolText(allow_it->second, &allow_reverse_open)) {
                throw std::runtime_error("sub-strategy `" + definition.id +
                                         "` has invalid allow_reverse_open: " + allow_it->second);
            }
        }

        std::int32_t timezone_offset_hours = 8;
        const std::string window_timezone = merged_params.count("window_timezone") == 0
                                                ? "Asia/Shanghai"
                                                : merged_params["window_timezone"];
        if (!ResolveTimezoneOffsetHours(window_timezone, &timezone_offset_hours, &error)) {
            throw std::runtime_error("sub-strategy `" + definition.id + "`: " + error);
        }

        std::vector<std::pair<std::int32_t, std::int32_t>> parsed_forbid_windows;
        if (!ParseTimeWindowList(merged_params.count("forbid_open_windows") == 0
                                     ? ""
                                     : merged_params["forbid_open_windows"],
                                 &parsed_forbid_windows, &error)) {
            throw std::runtime_error("sub-strategy `" + definition.id +
                                     "` has invalid forbid_open_windows: " + error);
        }

        std::vector<std::pair<std::int32_t, std::int32_t>> parsed_force_close_windows;
        if (!ParseTimeWindowList(merged_params.count("force_close_windows") == 0
                                     ? ""
                                     : merged_params["force_close_windows"],
                                 &parsed_force_close_windows, &error)) {
            throw std::runtime_error("sub-strategy `" + definition.id +
                                     "` has invalid force_close_windows: " + error);
        }

        const double daily_max_loss_r =
            ParseOptionalDoubleParam(merged_params, {"daily_max_loss_R", "daily_max_loss_r"},
                                     "daily_max_loss_R")
                .value_or(0.0);
        const double fixed_r =
            ParseOptionalDoubleParam(merged_params, {"R_fixed", "r_fixed", "fixed_R", "fixed_r"},
                                     "R_fixed")
                .value_or(1000.0);
        const std::int32_t max_consecutive_losses =
            ParseOptionalIntParam(merged_params, {"max_consecutive_losses"},
                                  "max_consecutive_losses")
                .value_or(0);
        const std::int32_t no_open_minutes_after_session_start =
            ParseOptionalIntParam(merged_params, {"no_open_minutes_after_session_start"},
                                  "no_open_minutes_after_session_start")
                .value_or(0);
        if (!std::isfinite(daily_max_loss_r) || daily_max_loss_r < 0.0) {
            throw std::runtime_error("sub-strategy `" + definition.id +
                                     "` has invalid daily_max_loss_R");
        }
        if (!std::isfinite(fixed_r) || fixed_r <= 0.0) {
            throw std::runtime_error("sub-strategy `" + definition.id + "` has invalid R_fixed");
        }
        if (max_consecutive_losses < 0) {
            throw std::runtime_error("sub-strategy `" + definition.id +
                                     "` has invalid max_consecutive_losses");
        }
        if (no_open_minutes_after_session_start < 0 ||
            no_open_minutes_after_session_start >= static_cast<std::int32_t>(kMinutesPerDay)) {
            throw std::runtime_error("sub-strategy `" + definition.id +
                                     "` has invalid no_open_minutes_after_session_start");
        }

        strategy->Init(merged_params);

        bool bound_to_supported_interface = false;
        if (auto* sub_strategy = dynamic_cast<ISubStrategy*>(strategy.get());
            sub_strategy != nullptr) {
            SubStrategySlot slot;
            slot.strategy_id = definition.id;
            slot.strategy = sub_strategy;
            slot.backtest_tick_aware = dynamic_cast<IAtomicBacktestTickAware*>(strategy.get());
            slot.timeframe_minutes =
                definition.timeframe_minutes > 0 ? definition.timeframe_minutes : 1;
            slot.entry_market_regimes = definition.entry_market_regimes;
            slot.allow_reverse_open = allow_reverse_open;
            slot.timezone_offset_hours = timezone_offset_hours;
            for (const auto& window : parsed_forbid_windows) {
                slot.forbid_open_windows.push_back(TimeWindow{window.first, window.second});
            }
            for (const auto& window : parsed_force_close_windows) {
                slot.force_close_windows.push_back(TimeWindow{window.first, window.second});
            }
            if (no_open_minutes_after_session_start > 0) {
                const std::int32_t day_start = 9 * 60;
                const std::int32_t night_start = 21 * 60;
                slot.session_start_no_open_windows.push_back(
                    TimeWindow{day_start, (day_start + no_open_minutes_after_session_start) %
                                              static_cast<std::int32_t>(kMinutesPerDay)});
                slot.session_start_no_open_windows.push_back(
                    TimeWindow{night_start, (night_start + no_open_minutes_after_session_start) %
                                                static_cast<std::int32_t>(kMinutesPerDay)});
            }
            slot.daily_max_loss_r = daily_max_loss_r;
            slot.fixed_r = fixed_r;
            slot.max_consecutive_losses = max_consecutive_losses;
            sub_strategy_slot_index_by_id_[slot.strategy_id] = sub_strategies_.size();
            sub_strategies_.push_back(std::move(slot));
            bound_to_supported_interface = true;
        }
        if (auto* time_filter = dynamic_cast<ITimeFilterStrategy*>(strategy.get());
            time_filter != nullptr) {
            TimeFilterSlot slot;
            slot.strategy = time_filter;
            slot.timeframe_minutes =
                definition.timeframe_minutes > 0 ? definition.timeframe_minutes : 1;
            time_filters_.push_back(std::move(slot));
            bound_to_supported_interface = true;
        }
        if (auto* risk_control = dynamic_cast<IRiskControlStrategy*>(strategy.get());
            risk_control != nullptr) {
            RiskControlSlot slot;
            slot.strategy = risk_control;
            slot.timeframe_minutes =
                definition.timeframe_minutes > 0 ? definition.timeframe_minutes : 1;
            risk_control_strategies_.push_back(std::move(slot));
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
