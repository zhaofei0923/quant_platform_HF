#include "quant_hft/strategy/atomic/kama_trend_strategy.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "atomic_param_parsing.h"
#include "quant_hft/core/structured_log.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {
namespace {

std::string NormalizeMode(std::string mode) {
    std::transform(mode.begin(), mode.end(), mode.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return mode;
}

std::int32_t ResolvePosition(const AtomicStrategyContext& ctx, const std::string& instrument_id) {
    const auto position_it = ctx.net_positions.find(instrument_id);
    return position_it == ctx.net_positions.end() ? 0 : position_it->second;
}

std::string FormatDouble(double value) {
    if (!std::isfinite(value)) {
        return "nan";
    }
    std::ostringstream out;
    out << value;
    return out.str();
}

std::string FormatStateDouble(double value) {
    std::ostringstream out;
    out << std::setprecision(17) << value;
    return out.str();
}

std::string FormatStateBool(bool value) { return value ? "true" : "false"; }

void SetError(std::string* error, const std::string& message) {
    if (error != nullptr) {
        *error = message;
    }
}

const std::string* FindStateValue(const AtomicState& state, const std::string& key,
                                  std::string* error) {
    const auto it = state.find(key);
    if (it == state.end()) {
        SetError(error, "missing state key: " + key);
        return nullptr;
    }
    return &it->second;
}

bool ParseStateBool(const std::string& text, bool* out) {
    if (text == "true" || text == "1") {
        *out = true;
        return true;
    }
    if (text == "false" || text == "0") {
        *out = false;
        return true;
    }
    return false;
}

bool ParseStateDoubleValue(const std::string& text, double* out) {
    try {
        std::size_t consumed = 0;
        const double value = std::stod(text, &consumed);
        if (consumed != text.size() || !std::isfinite(value)) {
            return false;
        }
        *out = value;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParseStateIntValue(const std::string& text, int* out) {
    try {
        std::size_t consumed = 0;
        const int value = std::stoi(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = value;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParseStateSizeValue(const std::string& text, std::size_t* out) {
    try {
        std::size_t consumed = 0;
        const unsigned long long value = std::stoull(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = static_cast<std::size_t>(value);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ReadStateBool(const AtomicState& state, const std::string& key, bool* out,
                   std::string* error) {
    const std::string* value = FindStateValue(state, key, error);
    if (value == nullptr) {
        return false;
    }
    if (!ParseStateBool(*value, out)) {
        SetError(error, "invalid bool state key: " + key);
        return false;
    }
    return true;
}

bool ReadStateDouble(const AtomicState& state, const std::string& key, double* out,
                     std::string* error) {
    const std::string* value = FindStateValue(state, key, error);
    if (value == nullptr) {
        return false;
    }
    if (!ParseStateDoubleValue(*value, out)) {
        SetError(error, "invalid double state key: " + key);
        return false;
    }
    return true;
}

bool ReadStateInt(const AtomicState& state, const std::string& key, int* out, std::string* error) {
    const std::string* value = FindStateValue(state, key, error);
    if (value == nullptr) {
        return false;
    }
    if (!ParseStateIntValue(*value, out)) {
        SetError(error, "invalid int state key: " + key);
        return false;
    }
    return true;
}

bool ReadStateSize(const AtomicState& state, const std::string& key, std::size_t* out,
                   std::string* error) {
    const std::string* value = FindStateValue(state, key, error);
    if (value == nullptr) {
        return false;
    }
    if (!ParseStateSizeValue(*value, out)) {
        SetError(error, "invalid size state key: " + key);
        return false;
    }
    return true;
}

void WriteOptionalDouble(AtomicState* out, const std::string& key,
                         const std::optional<double>& value) {
    (*out)[key + ".has"] = FormatStateBool(value.has_value());
    if (value.has_value()) {
        (*out)[key + ".value"] = FormatStateDouble(*value);
    }
}

void WriteOptionalInt(AtomicState* out, const std::string& key, const std::optional<int>& value) {
    (*out)[key + ".has"] = FormatStateBool(value.has_value());
    if (value.has_value()) {
        (*out)[key + ".value"] = std::to_string(*value);
    }
}

bool ReadOptionalDouble(const AtomicState& state, const std::string& key,
                        std::optional<double>* out, std::string* error) {
    bool has_value = false;
    if (!ReadStateBool(state, key + ".has", &has_value, error)) {
        return false;
    }
    if (!has_value) {
        out->reset();
        return true;
    }
    double value = 0.0;
    if (!ReadStateDouble(state, key + ".value", &value, error)) {
        return false;
    }
    *out = value;
    return true;
}

bool ReadOptionalInt(const AtomicState& state, const std::string& key, std::optional<int>* out,
                     std::string* error) {
    bool has_value = false;
    if (!ReadStateBool(state, key + ".has", &has_value, error)) {
        return false;
    }
    if (!has_value) {
        out->reset();
        return true;
    }
    int value = 0;
    if (!ReadStateInt(state, key + ".value", &value, error)) {
        return false;
    }
    *out = value;
    return true;
}

void WriteDoubleVector(AtomicState* out, const std::string& prefix,
                       const std::vector<double>& values) {
    (*out)[prefix + ".count"] = std::to_string(values.size());
    for (std::size_t i = 0; i < values.size(); ++i) {
        (*out)[prefix + "." + std::to_string(i)] = FormatStateDouble(values[i]);
    }
}

bool ReadDoubleVector(const AtomicState& state, const std::string& prefix,
                      std::vector<double>* values, std::string* error) {
    std::size_t count = 0;
    if (!ReadStateSize(state, prefix + ".count", &count, error)) {
        return false;
    }
    values->clear();
    values->reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        double value = 0.0;
        if (!ReadStateDouble(state, prefix + "." + std::to_string(i), &value, error)) {
            return false;
        }
        values->push_back(value);
    }
    return true;
}

void WriteKamaState(AtomicState* out, const std::string& prefix, const KAMA::State& state) {
    (*out)[prefix + ".initialized"] = FormatStateBool(state.initialized);
    WriteDoubleVector(out, prefix + ".closes", state.closes);
    (*out)[prefix + ".volatility_sum"] = FormatStateDouble(state.volatility_sum);
    (*out)[prefix + ".has_efficiency_ratio"] = FormatStateBool(state.has_efficiency_ratio);
    (*out)[prefix + ".efficiency_ratio"] = FormatStateDouble(state.efficiency_ratio);
    (*out)[prefix + ".kama"] = FormatStateDouble(state.kama);
}

bool ReadKamaState(const AtomicState& state, const std::string& prefix, KAMA::State* out,
                   std::string* error) {
    return ReadStateBool(state, prefix + ".initialized", &out->initialized, error) &&
           ReadDoubleVector(state, prefix + ".closes", &out->closes, error) &&
           ReadStateDouble(state, prefix + ".volatility_sum", &out->volatility_sum, error) &&
           ReadStateBool(state, prefix + ".has_efficiency_ratio", &out->has_efficiency_ratio,
                         error) &&
           ReadStateDouble(state, prefix + ".efficiency_ratio", &out->efficiency_ratio, error) &&
           ReadStateDouble(state, prefix + ".kama", &out->kama, error);
}

void WriteAtrState(AtomicState* out, const std::string& prefix, const ATR::State& state) {
    (*out)[prefix + ".initialized"] = FormatStateBool(state.initialized);
    (*out)[prefix + ".has_prev_close"] = FormatStateBool(state.has_prev_close);
    (*out)[prefix + ".prev_close"] = FormatStateDouble(state.prev_close);
    WriteDoubleVector(out, prefix + ".tr_seed", state.tr_seed);
    (*out)[prefix + ".tr_seed_sum"] = FormatStateDouble(state.tr_seed_sum);
    (*out)[prefix + ".atr"] = FormatStateDouble(state.atr);
}

bool ReadAtrState(const AtomicState& state, const std::string& prefix, ATR::State* out,
                  std::string* error) {
    return ReadStateBool(state, prefix + ".initialized", &out->initialized, error) &&
           ReadStateBool(state, prefix + ".has_prev_close", &out->has_prev_close, error) &&
           ReadStateDouble(state, prefix + ".prev_close", &out->prev_close, error) &&
           ReadDoubleVector(state, prefix + ".tr_seed", &out->tr_seed, error) &&
           ReadStateDouble(state, prefix + ".tr_seed_sum", &out->tr_seed_sum, error) &&
           ReadStateDouble(state, prefix + ".atr", &out->atr, error);
}

void WriteAdxState(AtomicState* out, const std::string& prefix, const ADX::State& state) {
    (*out)[prefix + ".has_prev_bar"] = FormatStateBool(state.has_prev_bar);
    (*out)[prefix + ".prev_high"] = FormatStateDouble(state.prev_high);
    (*out)[prefix + ".prev_low"] = FormatStateDouble(state.prev_low);
    (*out)[prefix + ".prev_close"] = FormatStateDouble(state.prev_close);
    (*out)[prefix + ".seed_count"] = std::to_string(state.seed_count);
    (*out)[prefix + ".tr_seed_sum"] = FormatStateDouble(state.tr_seed_sum);
    (*out)[prefix + ".plus_dm_seed_sum"] = FormatStateDouble(state.plus_dm_seed_sum);
    (*out)[prefix + ".minus_dm_seed_sum"] = FormatStateDouble(state.minus_dm_seed_sum);
    (*out)[prefix + ".di_ready"] = FormatStateBool(state.di_ready);
    (*out)[prefix + ".tr_smoothed"] = FormatStateDouble(state.tr_smoothed);
    (*out)[prefix + ".plus_dm_smoothed"] = FormatStateDouble(state.plus_dm_smoothed);
    (*out)[prefix + ".minus_dm_smoothed"] = FormatStateDouble(state.minus_dm_smoothed);
    (*out)[prefix + ".plus_di"] = FormatStateDouble(state.plus_di);
    (*out)[prefix + ".minus_di"] = FormatStateDouble(state.minus_di);
    (*out)[prefix + ".dx"] = FormatStateDouble(state.dx);
    (*out)[prefix + ".dx_seed_count"] = std::to_string(state.dx_seed_count);
    (*out)[prefix + ".dx_seed_sum"] = FormatStateDouble(state.dx_seed_sum);
    (*out)[prefix + ".adx_ready"] = FormatStateBool(state.adx_ready);
    (*out)[prefix + ".adx"] = FormatStateDouble(state.adx);
}

bool ReadAdxState(const AtomicState& state, const std::string& prefix, ADX::State* out,
                  std::string* error) {
    return ReadStateBool(state, prefix + ".has_prev_bar", &out->has_prev_bar, error) &&
           ReadStateDouble(state, prefix + ".prev_high", &out->prev_high, error) &&
           ReadStateDouble(state, prefix + ".prev_low", &out->prev_low, error) &&
           ReadStateDouble(state, prefix + ".prev_close", &out->prev_close, error) &&
           ReadStateInt(state, prefix + ".seed_count", &out->seed_count, error) &&
           ReadStateDouble(state, prefix + ".tr_seed_sum", &out->tr_seed_sum, error) &&
           ReadStateDouble(state, prefix + ".plus_dm_seed_sum", &out->plus_dm_seed_sum, error) &&
           ReadStateDouble(state, prefix + ".minus_dm_seed_sum", &out->minus_dm_seed_sum, error) &&
           ReadStateBool(state, prefix + ".di_ready", &out->di_ready, error) &&
           ReadStateDouble(state, prefix + ".tr_smoothed", &out->tr_smoothed, error) &&
           ReadStateDouble(state, prefix + ".plus_dm_smoothed", &out->plus_dm_smoothed, error) &&
           ReadStateDouble(state, prefix + ".minus_dm_smoothed", &out->minus_dm_smoothed, error) &&
           ReadStateDouble(state, prefix + ".plus_di", &out->plus_di, error) &&
           ReadStateDouble(state, prefix + ".minus_di", &out->minus_di, error) &&
           ReadStateDouble(state, prefix + ".dx", &out->dx, error) &&
           ReadStateInt(state, prefix + ".dx_seed_count", &out->dx_seed_count, error) &&
           ReadStateDouble(state, prefix + ".dx_seed_sum", &out->dx_seed_sum, error) &&
           ReadStateBool(state, prefix + ".adx_ready", &out->adx_ready, error) &&
           ReadStateDouble(state, prefix + ".adx", &out->adx, error);
}

void WriteDeque(AtomicState* out, const std::string& prefix, const std::deque<double>& values) {
    (*out)[prefix + ".count"] = std::to_string(values.size());
    for (std::size_t i = 0; i < values.size(); ++i) {
        (*out)[prefix + "." + std::to_string(i)] = FormatStateDouble(values[i]);
    }
}

bool ReadDeque(const AtomicState& state, const std::string& prefix, std::deque<double>* out,
               std::string* error) {
    std::size_t count = 0;
    if (!ReadStateSize(state, prefix + ".count", &count, error)) {
        return false;
    }
    out->clear();
    for (std::size_t i = 0; i < count; ++i) {
        double value = 0.0;
        if (!ReadStateDouble(state, prefix + "." + std::to_string(i), &value, error)) {
            return false;
        }
        out->push_back(value);
    }
    return true;
}

std::string SideToString(Side side) { return side == Side::kBuy ? "buy" : "sell"; }

std::string SignalTypeToTraceToken(SignalType signal_type) {
    switch (signal_type) {
        case SignalType::kOpen:
            return "open";
        case SignalType::kClose:
            return "close";
        case SignalType::kStopLoss:
            return "stop_loss";
        case SignalType::kTakeProfit:
            return "take_profit";
        case SignalType::kForceClose:
            return "force_close";
    }
    return "signal";
}

std::string BuildSignalTraceId(const std::string& strategy_id, SignalType signal_type,
                               const std::string& instrument_id, EpochNanos ts_ns) {
    return strategy_id + "-" + SignalTypeToTraceToken(signal_type) + "-" + instrument_id + "-" +
           std::to_string(ts_ns);
}

std::string ExtractSymbolPrefixLowerLocal(const std::string& instrument_id) {
    std::string prefix;
    for (unsigned char ch : instrument_id) {
        if (std::isalpha(ch) == 0) {
            break;
        }
        prefix.push_back(static_cast<char>(std::tolower(ch)));
    }
    return prefix;
}

std::string ToUpperLocal(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    return text;
}

std::optional<double> ResolveContractMultiplier(const AtomicStrategyContext& ctx,
                                                const std::string& instrument_id) {
    std::optional<double> contract_multiplier;
    auto maybe_set_multiplier = [&](const std::string& key) {
        if (key.empty() || contract_multiplier.has_value()) {
            return;
        }
        const auto multiplier_it = ctx.contract_multipliers.find(key);
        if (multiplier_it != ctx.contract_multipliers.end() &&
            std::isfinite(multiplier_it->second) && multiplier_it->second > 0.0) {
            contract_multiplier = multiplier_it->second;
        }
    };

    maybe_set_multiplier(instrument_id);
    const std::string symbol_prefix = ExtractSymbolPrefixLowerLocal(instrument_id);
    maybe_set_multiplier(symbol_prefix);
    maybe_set_multiplier(ToUpperLocal(symbol_prefix));
    return contract_multiplier;
}

CtpRuntimeConfig BuildLogRuntime(const AtomicStrategyContext& ctx) {
    CtpRuntimeConfig runtime;
    runtime.log_level = ctx.log_level;
    runtime.log_sink = ctx.log_sink;
    return runtime;
}

void EmitKamaStrategyLog(const AtomicStrategyContext& ctx, const std::string& level,
                         const std::string& event, const LogFields& fields) {
    const CtpRuntimeConfig runtime = BuildLogRuntime(ctx);
    EmitStructuredLog(&runtime, "kama_trend_strategy", level, event, fields);
}

double ComputePositionPnl(const AtomicStrategyContext& ctx, const std::string& instrument_id,
                          std::int32_t position, double price) {
    const auto avg_price_it = ctx.avg_open_prices.find(instrument_id);
    if (avg_price_it == ctx.avg_open_prices.end() || !std::isfinite(avg_price_it->second) ||
        !std::isfinite(price)) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    const double multiplier = ResolveContractMultiplier(ctx, instrument_id).value_or(1.0);
    const double signed_points = (price - avg_price_it->second) * (position > 0 ? 1.0 : -1.0);
    return signed_points * multiplier * static_cast<double>(std::abs(position));
}

double ComputeRiskBudgetR(const AtomicStrategyContext& ctx, double risk_per_trade_pct) {
    const double equity =
        std::isfinite(ctx.account_equity) ? std::max(0.0, ctx.account_equity) : 0.0;
    const double risk_budget = equity * risk_per_trade_pct;
    return std::isfinite(risk_budget) ? risk_budget : 0.0;
}

}  // namespace

void KamaTrendStrategy::Init(const AtomicParams& params) {
    id_ = atomic_internal::GetString(params, "id", "KamaTrendStrategy");
    er_period_ = atomic_internal::GetInt(params, "er_period", 10);
    fast_period_ = atomic_internal::GetInt(params, "fast_period", 2);
    slow_period_ = atomic_internal::GetInt(params, "slow_period", 30);
    std_period_ = atomic_internal::GetInt(params, "std_period", 20);
    kama_filter_ = atomic_internal::GetDouble(params, "kama_filter", 0.5);
    risk_per_trade_pct_ = atomic_internal::GetDouble(params, "risk_per_trade_pct", 0.01);
    default_volume_ = atomic_internal::GetInt(params, "default_volume", 1);
    stop_loss_mode_ =
        NormalizeMode(atomic_internal::GetString(params, "stop_loss_mode", "trailing_atr"));
    stop_loss_atr_period_ = atomic_internal::GetInt(params, "stop_loss_atr_period", 14);
    stop_loss_atr_multiplier_ = atomic_internal::GetDouble(params, "stop_loss_atr_multiplier", 2.0);
    take_profit_mode_ =
        NormalizeMode(atomic_internal::GetString(params, "take_profit_mode", "atr_target"));
    take_profit_atr_period_ = atomic_internal::GetInt(params, "take_profit_atr_period", 14);
    take_profit_atr_multiplier_ =
        atomic_internal::GetDouble(params, "take_profit_atr_multiplier", 3.0);
    adx_period_ = atomic_internal::GetInt(params, "adx_period", 14);

    if (id_.empty()) {
        throw std::invalid_argument("KamaTrendStrategy id must not be empty");
    }
    if (er_period_ <= 0 || fast_period_ <= 0 || slow_period_ <= 0 || std_period_ <= 0) {
        throw std::invalid_argument("KamaTrendStrategy periods must be positive");
    }
    if (!std::isfinite(kama_filter_) || kama_filter_ < 0.0) {
        throw std::invalid_argument("KamaTrendStrategy kama_filter must be non-negative");
    }
    if (!std::isfinite(risk_per_trade_pct_) || risk_per_trade_pct_ <= 0.0 ||
        risk_per_trade_pct_ > 1.0) {
        throw std::invalid_argument("KamaTrendStrategy risk_per_trade_pct must be in (0, 1]");
    }
    if (default_volume_ <= 0) {
        throw std::invalid_argument("KamaTrendStrategy default_volume must be positive");
    }
    if (adx_period_ <= 0) {
        throw std::invalid_argument("KamaTrendStrategy adx_period must be positive");
    }
    if (stop_loss_mode_ != "none" && stop_loss_mode_ != "trailing_atr") {
        throw std::invalid_argument(
            "KamaTrendStrategy stop_loss_mode must be one of: trailing_atr, none");
    }
    if (take_profit_mode_ != "none" && take_profit_mode_ != "atr_target") {
        throw std::invalid_argument(
            "KamaTrendStrategy take_profit_mode must be one of: atr_target, none");
    }
    if (stop_loss_mode_ == "trailing_atr") {
        if (stop_loss_atr_period_ <= 0) {
            throw std::invalid_argument("KamaTrendStrategy stop_loss_atr_period must be positive");
        }
        if (!std::isfinite(stop_loss_atr_multiplier_) || stop_loss_atr_multiplier_ <= 0.0) {
            throw std::invalid_argument(
                "KamaTrendStrategy stop_loss_atr_multiplier must be positive");
        }
    }
    if (take_profit_mode_ == "atr_target") {
        if (take_profit_atr_period_ <= 0) {
            throw std::invalid_argument(
                "KamaTrendStrategy take_profit_atr_period must be positive");
        }
        if (!std::isfinite(take_profit_atr_multiplier_) || take_profit_atr_multiplier_ <= 0.0) {
            throw std::invalid_argument(
                "KamaTrendStrategy take_profit_atr_multiplier must be positive");
        }
    }

    kama_ = std::make_unique<KAMA>(er_period_, fast_period_, slow_period_);
    adx_ = std::make_unique<ADX>(adx_period_);
    stop_loss_atr_.reset();
    take_profit_atr_.reset();
    if (stop_loss_mode_ == "trailing_atr") {
        stop_loss_atr_ = std::make_unique<ATR>(stop_loss_atr_period_);
    }
    if (take_profit_mode_ == "atr_target") {
        take_profit_atr_ = std::make_unique<ATR>(take_profit_atr_period_);
    }
    kama_recent_.clear();
    kama_window_.clear();
    kama_window_sum_ = 0.0;
    kama_window_sum_sq_ = 0.0;
    trailing_stop_by_instrument_.clear();
    trailing_direction_by_instrument_.clear();
    initial_stop_by_instrument_.clear();
    take_profit_by_instrument_.clear();
    last_kama_.reset();
    last_er_.reset();
    last_adx_.reset();
    last_stop_atr_.reset();
    last_take_atr_.reset();
    last_threshold_.reset();
    last_diff_1_.reset();
    last_diff_2_.reset();
    last_diff_3_.reset();
    last_diff_class_1_.reset();
    last_diff_class_2_.reset();
    last_diff_class_3_.reset();
    last_trend_sum_.reset();
    last_stop_loss_price_.reset();
    last_take_profit_price_.reset();
    last_raw_signal_.clear();
}

std::string KamaTrendStrategy::GetId() const { return id_; }

void KamaTrendStrategy::Reset() {
    if (kama_ != nullptr) {
        kama_->Reset();
    }
    if (adx_ != nullptr) {
        adx_->Reset();
    }
    if (stop_loss_atr_ != nullptr) {
        stop_loss_atr_->Reset();
    }
    if (take_profit_atr_ != nullptr) {
        take_profit_atr_->Reset();
    }
    kama_recent_.clear();
    kama_window_.clear();
    kama_window_sum_ = 0.0;
    kama_window_sum_sq_ = 0.0;
    trailing_stop_by_instrument_.clear();
    trailing_direction_by_instrument_.clear();
    initial_stop_by_instrument_.clear();
    take_profit_by_instrument_.clear();
    last_kama_.reset();
    last_er_.reset();
    last_adx_.reset();
    last_stop_atr_.reset();
    last_take_atr_.reset();
    last_threshold_.reset();
    last_diff_1_.reset();
    last_diff_2_.reset();
    last_diff_3_.reset();
    last_diff_class_1_.reset();
    last_diff_class_2_.reset();
    last_diff_class_3_.reset();
    last_trend_sum_.reset();
    last_stop_loss_price_.reset();
    last_take_profit_price_.reset();
    last_raw_signal_.clear();
}

std::vector<SignalIntent> KamaTrendStrategy::OnState(const StateSnapshot7D& state,
                                                     const AtomicStrategyContext& ctx) {
    last_kama_.reset();
    last_er_.reset();
    last_adx_.reset();
    last_stop_atr_.reset();
    last_take_atr_.reset();
    last_threshold_.reset();
    last_diff_1_.reset();
    last_diff_2_.reset();
    last_diff_3_.reset();
    last_diff_class_1_.reset();
    last_diff_class_2_.reset();
    last_diff_class_3_.reset();
    last_trend_sum_.reset();
    last_stop_loss_price_.reset();
    last_take_profit_price_.reset();
    last_raw_signal_.clear();

    const double analysis_high = state.effective_bar_high();
    const double analysis_low = state.effective_bar_low();
    const double analysis_close = state.effective_bar_close();
    if (kama_ == nullptr || !state.has_bar || !std::isfinite(analysis_high) ||
        !std::isfinite(analysis_low) || !std::isfinite(analysis_close)) {
        return {};
    }

    kama_->Update(analysis_high, analysis_low, analysis_close, state.bar_volume);
    if (adx_ != nullptr) {
        adx_->Update(analysis_high, analysis_low, analysis_close, state.bar_volume);
    }
    if (stop_loss_atr_ != nullptr) {
        stop_loss_atr_->Update(analysis_high, analysis_low, analysis_close, state.bar_volume);
    }
    if (take_profit_atr_ != nullptr) {
        take_profit_atr_->Update(analysis_high, analysis_low, analysis_close, state.bar_volume);
    }

    if (kama_->IsReady()) {
        last_kama_ = kama_->Value();
        last_er_ = kama_->EfficiencyRatio();
    }
    if (adx_ != nullptr && adx_->IsReady()) {
        last_adx_ = adx_->Value();
    }
    if (stop_loss_atr_ != nullptr && stop_loss_atr_->IsReady()) {
        last_stop_atr_ = stop_loss_atr_->Value();
    }
    if (take_profit_atr_ != nullptr && take_profit_atr_->IsReady()) {
        last_take_atr_ = take_profit_atr_->Value();
    }

    if (last_kama_.has_value() && std::isfinite(*last_kama_)) {
        kama_recent_.push_back(*last_kama_);
        if (kama_recent_.size() > 4) {
            kama_recent_.pop_front();
        }
        kama_window_.push_back(*last_kama_);
        kama_window_sum_ += *last_kama_;
        kama_window_sum_sq_ += (*last_kama_) * (*last_kama_);
        if (kama_window_.size() > static_cast<std::size_t>(std_period_)) {
            const double removed = kama_window_.front();
            kama_window_.pop_front();
            kama_window_sum_ -= removed;
            kama_window_sum_sq_ -= removed * removed;
            kama_window_sum_sq_ = std::max(0.0, kama_window_sum_sq_);
        }
    }

    bool has_entry_signal = false;
    SignalIntent entry_signal;
    if (kama_recent_.size() >= 4 && kama_window_.size() >= static_cast<std::size_t>(std_period_)) {
        const double threshold = kama_filter_ * ComputeStdKama();
        const double kama_t = kama_recent_[3];
        const double diff_1 = kama_t - kama_recent_[2];
        const double diff_2 = kama_t - kama_recent_[1];
        const double diff_3 = kama_t - kama_recent_[0];
        const int diff_1st = ClassifyDiff(diff_1, threshold);
        const int diff_2nd = ClassifyDiff(diff_2, threshold);
        const int diff_3rd = ClassifyDiff(diff_3, threshold);
        const int trend_sum = diff_1st + diff_2nd + diff_3rd;
        last_threshold_ = threshold;
        last_diff_1_ = diff_1;
        last_diff_2_ = diff_2;
        last_diff_3_ = diff_3;
        last_diff_class_1_ = diff_1st;
        last_diff_class_2_ = diff_2nd;
        last_diff_class_3_ = diff_3rd;
        last_trend_sum_ = trend_sum;
        const bool price_on_kama_side = (trend_sum == 3 && analysis_close > kama_t) ||
                                        (trend_sum == -3 && analysis_close < kama_t);
        if (price_on_kama_side) {
            const int volume =
                ComputeOrderVolume(ctx, state.instrument_id, last_stop_atr_.value_or(std::nan("")));
            if (volume > 0) {
                entry_signal.strategy_id = id_;
                entry_signal.instrument_id = state.instrument_id;
                entry_signal.signal_type = SignalType::kOpen;
                entry_signal.side = trend_sum > 0 ? Side::kBuy : Side::kSell;
                last_raw_signal_ = SideToString(entry_signal.side);
                entry_signal.offset = OffsetFlag::kOpen;
                entry_signal.volume = volume;
                entry_signal.limit_price = state.bar_close;
                entry_signal.ts_ns = state.ts_ns;
                entry_signal.trace_id = BuildSignalTraceId(id_, entry_signal.signal_type,
                                                           state.instrument_id, state.ts_ns);
                EmitKamaStrategyLog(
                    ctx, "info", "open_signal_emitted",
                    {{"strategy_id", id_},
                     {"event_type", "open_signal"},
                     {"event_ts_ns", std::to_string(state.ts_ns)},
                     {"trace_id", entry_signal.trace_id},
                     {"instrument_id", state.instrument_id},
                     {"side", SideToString(entry_signal.side)},
                     {"volume", std::to_string(volume)},
                     {"entry_price", FormatDouble(entry_signal.limit_price)},
                     {"kama_value", FormatDouble(kama_t)},
                     {"r_value", FormatDouble(ComputeRiskBudgetR(ctx, risk_per_trade_pct_))}});
                has_entry_signal = true;
            }
        }
    }

    std::vector<SignalIntent> signals;
    const std::int32_t position = ResolvePosition(ctx, state.instrument_id);
    if (position != 0) {
        const auto avg_price_it = ctx.avg_open_prices.find(state.instrument_id);
        if (avg_price_it == ctx.avg_open_prices.end() || !std::isfinite(avg_price_it->second)) {
            return signals;
        }
        const double avg_open_price = avg_price_it->second;
        const int direction = position > 0 ? 1 : -1;

        if (stop_loss_mode_ == "trailing_atr" && last_stop_atr_.has_value() &&
            std::isfinite(*last_stop_atr_) && *last_stop_atr_ > 0.0) {
            const double stop_distance = stop_loss_atr_multiplier_ * (*last_stop_atr_);
            const double base_stop =
                direction > 0 ? (avg_open_price - stop_distance) : (avg_open_price + stop_distance);
            double stop_price = base_stop;
            const auto stop_it = trailing_stop_by_instrument_.find(state.instrument_id);
            const auto direction_it = trailing_direction_by_instrument_.find(state.instrument_id);
            const bool continuing = stop_it != trailing_stop_by_instrument_.end() &&
                                    direction_it != trailing_direction_by_instrument_.end() &&
                                    direction_it->second == direction;
            if (continuing) {
                stop_price = stop_it->second;
            } else {
                initial_stop_by_instrument_[state.instrument_id] = base_stop;
            }
            trailing_stop_by_instrument_[state.instrument_id] = stop_price;
            trailing_direction_by_instrument_[state.instrument_id] = direction;
            last_stop_loss_price_ = stop_price;
        } else {
            trailing_stop_by_instrument_.erase(state.instrument_id);
            trailing_direction_by_instrument_.erase(state.instrument_id);
            initial_stop_by_instrument_.erase(state.instrument_id);
        }

        if (take_profit_mode_ == "atr_target" && last_take_atr_.has_value() &&
            std::isfinite(*last_take_atr_) && *last_take_atr_ > 0.0) {
            const double take_distance = take_profit_atr_multiplier_ * (*last_take_atr_);
            const double take_price =
                direction > 0 ? (avg_open_price + take_distance) : (avg_open_price - take_distance);
            last_take_profit_price_ = take_price;
            take_profit_by_instrument_[state.instrument_id] = take_price;
        } else {
            take_profit_by_instrument_.erase(state.instrument_id);
        }

        if (!has_entry_signal) {
            return {};
        }
        const Side position_side = position > 0 ? Side::kBuy : Side::kSell;
        if (entry_signal.side == position_side) {
            return {};
        }
        return {entry_signal};
    }

    trailing_stop_by_instrument_.erase(state.instrument_id);
    trailing_direction_by_instrument_.erase(state.instrument_id);
    initial_stop_by_instrument_.erase(state.instrument_id);
    take_profit_by_instrument_.erase(state.instrument_id);
    if (!has_entry_signal) {
        return {};
    }
    return {entry_signal};
}

std::vector<SignalIntent> KamaTrendStrategy::OnBacktestTick(const AtomicTickSnapshot& tick,
                                                            const AtomicStrategyContext& ctx) {
    if (tick.instrument_id.empty() || !std::isfinite(tick.last_price) || tick.last_price <= 0.0) {
        return {};
    }
    const std::int32_t position = ResolvePosition(ctx, tick.instrument_id);
    if (position == 0) {
        trailing_stop_by_instrument_.erase(tick.instrument_id);
        trailing_direction_by_instrument_.erase(tick.instrument_id);
        initial_stop_by_instrument_.erase(tick.instrument_id);
        take_profit_by_instrument_.erase(tick.instrument_id);
        return {};
    }

    const auto avg_price_it = ctx.avg_open_prices.find(tick.instrument_id);
    if (avg_price_it == ctx.avg_open_prices.end() || !std::isfinite(avg_price_it->second)) {
        return {};
    }

    const double avg_open_price = avg_price_it->second;
    const int direction = position > 0 ? 1 : -1;
    if (stop_loss_mode_ == "trailing_atr") {
        if (last_stop_atr_.has_value() && std::isfinite(*last_stop_atr_) &&
            *last_stop_atr_ > 0.0) {
            const double stop_distance = stop_loss_atr_multiplier_ * (*last_stop_atr_);
            const double base_stop =
                direction > 0 ? (avg_open_price - stop_distance) : (avg_open_price + stop_distance);
            double stop_price = base_stop;
            const auto stop_it = trailing_stop_by_instrument_.find(tick.instrument_id);
            const auto direction_it = trailing_direction_by_instrument_.find(tick.instrument_id);
            const bool continuing = stop_it != trailing_stop_by_instrument_.end() &&
                                    direction_it != trailing_direction_by_instrument_.end() &&
                                    direction_it->second == direction;
            if (continuing) {
                stop_price = stop_it->second;
            } else {
                initial_stop_by_instrument_[tick.instrument_id] = base_stop;
            }
            const double candidate = direction > 0 ? (tick.last_price - stop_distance)
                                                   : (tick.last_price + stop_distance);
            stop_price =
                direction > 0 ? std::max(stop_price, candidate) : std::min(stop_price, candidate);
            trailing_stop_by_instrument_[tick.instrument_id] = stop_price;
            trailing_direction_by_instrument_[tick.instrument_id] = direction;
            last_stop_loss_price_ = stop_price;
        }
    } else {
        trailing_stop_by_instrument_.erase(tick.instrument_id);
        trailing_direction_by_instrument_.erase(tick.instrument_id);
        initial_stop_by_instrument_.erase(tick.instrument_id);
        last_stop_loss_price_.reset();
    }

    if (take_profit_mode_ == "atr_target" && last_take_atr_.has_value() &&
        std::isfinite(*last_take_atr_) && *last_take_atr_ > 0.0) {
        const double take_distance = take_profit_atr_multiplier_ * (*last_take_atr_);
        const double take_price =
            direction > 0 ? (avg_open_price + take_distance) : (avg_open_price - take_distance);
        last_take_profit_price_ = take_price;
        take_profit_by_instrument_[tick.instrument_id] = take_price;
    } else if (take_profit_mode_ != "atr_target") {
        last_take_profit_price_.reset();
        take_profit_by_instrument_.erase(tick.instrument_id);
    }

    return EvaluateRiskSignals(ctx, tick.instrument_id, tick.last_price, tick.ts_ns);
}

std::optional<AtomicIndicatorSnapshot> KamaTrendStrategy::IndicatorSnapshot() const {
    if (!last_kama_.has_value() && !last_er_.has_value() && !last_adx_.has_value() &&
        !last_stop_atr_.has_value() && !last_take_atr_.has_value() &&
        !last_threshold_.has_value() && !last_stop_loss_price_.has_value() &&
        !last_take_profit_price_.has_value() && last_raw_signal_.empty()) {
        return std::nullopt;
    }
    AtomicIndicatorSnapshot snapshot;
    snapshot.kama = last_kama_;
    snapshot.er = last_er_;
    snapshot.adx = last_adx_;
    snapshot.atr = last_stop_atr_.has_value() ? last_stop_atr_ : last_take_atr_;
    snapshot.threshold = last_threshold_;
    snapshot.diff_1 = last_diff_1_;
    snapshot.diff_2 = last_diff_2_;
    snapshot.diff_3 = last_diff_3_;
    snapshot.diff_class_1 = last_diff_class_1_;
    snapshot.diff_class_2 = last_diff_class_2_;
    snapshot.diff_class_3 = last_diff_class_3_;
    snapshot.trend_sum = last_trend_sum_;
    snapshot.stop_loss_price = last_stop_loss_price_;
    snapshot.take_profit_price = last_take_profit_price_;
    snapshot.raw_signal = last_raw_signal_;
    return snapshot;
}

std::unordered_map<std::string, AtomicRiskPrices> KamaTrendStrategy::RiskPricesByInstrument()
    const {
    std::unordered_map<std::string, AtomicRiskPrices> prices;
    for (const auto& [instrument_id, stop_price] : trailing_stop_by_instrument_) {
        prices[instrument_id].trailing_stop = stop_price;
    }
    for (const auto& [instrument_id, stop_price] : initial_stop_by_instrument_) {
        prices[instrument_id].initial_stop = stop_price;
    }
    for (const auto& [instrument_id, take_price] : take_profit_by_instrument_) {
        prices[instrument_id].take_profit = take_price;
    }
    return prices;
}

bool KamaTrendStrategy::SaveState(AtomicState* out, std::string* error) const {
    if (out == nullptr) {
        SetError(error, "KamaTrendStrategy SaveState output must not be null");
        return false;
    }
    if (kama_ == nullptr || adx_ == nullptr) {
        SetError(error, "KamaTrendStrategy is not initialized");
        return false;
    }

    out->clear();
    (*out)["version"] = "1";
    (*out)["id"] = id_;
    WriteKamaState(out, "kama", kama_->ExportState());
    WriteAdxState(out, "adx", adx_->ExportState());
    (*out)["stop_atr.enabled"] = FormatStateBool(stop_loss_atr_ != nullptr);
    if (stop_loss_atr_ != nullptr) {
        WriteAtrState(out, "stop_atr", stop_loss_atr_->ExportState());
    }
    (*out)["take_atr.enabled"] = FormatStateBool(take_profit_atr_ != nullptr);
    if (take_profit_atr_ != nullptr) {
        WriteAtrState(out, "take_atr", take_profit_atr_->ExportState());
    }

    WriteDeque(out, "kama_recent", kama_recent_);
    WriteDeque(out, "kama_window", kama_window_);
    (*out)["kama_window_sum"] = FormatStateDouble(kama_window_sum_);
    (*out)["kama_window_sum_sq"] = FormatStateDouble(kama_window_sum_sq_);

    (*out)["trailing_stop.count"] = std::to_string(trailing_stop_by_instrument_.size());
    std::size_t stop_index = 0;
    for (const auto& [instrument_id, stop_price] : trailing_stop_by_instrument_) {
        const std::string prefix = "trailing_stop." + std::to_string(stop_index);
        (*out)[prefix + ".instrument"] = instrument_id;
        (*out)[prefix + ".price"] = FormatStateDouble(stop_price);
        ++stop_index;
    }
    (*out)["trailing_direction.count"] = std::to_string(trailing_direction_by_instrument_.size());
    std::size_t direction_index = 0;
    for (const auto& [instrument_id, direction] : trailing_direction_by_instrument_) {
        const std::string prefix = "trailing_direction." + std::to_string(direction_index);
        (*out)[prefix + ".instrument"] = instrument_id;
        (*out)[prefix + ".direction"] = std::to_string(direction);
        ++direction_index;
    }

    (*out)["initial_stop.count"] = std::to_string(initial_stop_by_instrument_.size());
    std::size_t initial_stop_index = 0;
    for (const auto& [instrument_id, stop_price] : initial_stop_by_instrument_) {
        const std::string prefix = "initial_stop." + std::to_string(initial_stop_index);
        (*out)[prefix + ".instrument"] = instrument_id;
        (*out)[prefix + ".price"] = FormatStateDouble(stop_price);
        ++initial_stop_index;
    }
    (*out)["take_profit.count"] = std::to_string(take_profit_by_instrument_.size());
    std::size_t take_profit_index = 0;
    for (const auto& [instrument_id, take_price] : take_profit_by_instrument_) {
        const std::string prefix = "take_profit." + std::to_string(take_profit_index);
        (*out)[prefix + ".instrument"] = instrument_id;
        (*out)[prefix + ".price"] = FormatStateDouble(take_price);
        ++take_profit_index;
    }

    WriteOptionalDouble(out, "last_kama", last_kama_);
    WriteOptionalDouble(out, "last_er", last_er_);
    WriteOptionalDouble(out, "last_adx", last_adx_);
    WriteOptionalDouble(out, "last_stop_atr", last_stop_atr_);
    WriteOptionalDouble(out, "last_take_atr", last_take_atr_);
    WriteOptionalDouble(out, "last_threshold", last_threshold_);
    WriteOptionalDouble(out, "last_diff_1", last_diff_1_);
    WriteOptionalDouble(out, "last_diff_2", last_diff_2_);
    WriteOptionalDouble(out, "last_diff_3", last_diff_3_);
    WriteOptionalInt(out, "last_diff_class_1", last_diff_class_1_);
    WriteOptionalInt(out, "last_diff_class_2", last_diff_class_2_);
    WriteOptionalInt(out, "last_diff_class_3", last_diff_class_3_);
    WriteOptionalInt(out, "last_trend_sum", last_trend_sum_);
    WriteOptionalDouble(out, "last_stop_loss_price", last_stop_loss_price_);
    WriteOptionalDouble(out, "last_take_profit_price", last_take_profit_price_);
    (*out)["last_raw_signal"] = last_raw_signal_;
    return true;
}

bool KamaTrendStrategy::LoadState(const AtomicState& state, std::string* error) {
    if (kama_ == nullptr || adx_ == nullptr) {
        SetError(error, "KamaTrendStrategy is not initialized");
        return false;
    }

    KAMA::State kama_state;
    ADX::State adx_state;
    if (!ReadKamaState(state, "kama", &kama_state, error) ||
        !ReadAdxState(state, "adx", &adx_state, error)) {
        return false;
    }

    bool stop_atr_enabled = false;
    if (!ReadStateBool(state, "stop_atr.enabled", &stop_atr_enabled, error)) {
        return false;
    }
    ATR::State stop_atr_state;
    if (stop_atr_enabled && !ReadAtrState(state, "stop_atr", &stop_atr_state, error)) {
        return false;
    }

    bool take_atr_enabled = false;
    if (!ReadStateBool(state, "take_atr.enabled", &take_atr_enabled, error)) {
        return false;
    }
    ATR::State take_atr_state;
    if (take_atr_enabled && !ReadAtrState(state, "take_atr", &take_atr_state, error)) {
        return false;
    }

    std::deque<double> kama_recent;
    std::deque<double> kama_window;
    if (!ReadDeque(state, "kama_recent", &kama_recent, error) ||
        !ReadDeque(state, "kama_window", &kama_window, error)) {
        return false;
    }
    if (kama_recent.size() > 4 || kama_window.size() > static_cast<std::size_t>(std_period_)) {
        SetError(error, "invalid KamaTrendStrategy KAMA window sizes");
        return false;
    }

    double loaded_window_sum = 0.0;
    double loaded_window_sum_sq = 0.0;
    for (const double value : kama_window) {
        if (!std::isfinite(value)) {
            SetError(error, "invalid KamaTrendStrategy KAMA window value");
            return false;
        }
        loaded_window_sum += value;
        loaded_window_sum_sq += value * value;
    }
    for (const double value : kama_recent) {
        if (!std::isfinite(value)) {
            SetError(error, "invalid KamaTrendStrategy recent KAMA value");
            return false;
        }
    }

    std::unordered_map<std::string, double> trailing_stop_by_instrument;
    std::size_t trailing_stop_count = 0;
    if (!ReadStateSize(state, "trailing_stop.count", &trailing_stop_count, error)) {
        return false;
    }
    for (std::size_t i = 0; i < trailing_stop_count; ++i) {
        const std::string prefix = "trailing_stop." + std::to_string(i);
        const std::string* instrument = FindStateValue(state, prefix + ".instrument", error);
        if (instrument == nullptr || instrument->empty()) {
            return false;
        }
        double price = 0.0;
        if (!ReadStateDouble(state, prefix + ".price", &price, error)) {
            return false;
        }
        trailing_stop_by_instrument[*instrument] = price;
    }

    std::unordered_map<std::string, int> trailing_direction_by_instrument;
    std::size_t trailing_direction_count = 0;
    if (!ReadStateSize(state, "trailing_direction.count", &trailing_direction_count, error)) {
        return false;
    }
    for (std::size_t i = 0; i < trailing_direction_count; ++i) {
        const std::string prefix = "trailing_direction." + std::to_string(i);
        const std::string* instrument = FindStateValue(state, prefix + ".instrument", error);
        if (instrument == nullptr || instrument->empty()) {
            return false;
        }
        int direction = 0;
        if (!ReadStateInt(state, prefix + ".direction", &direction, error)) {
            return false;
        }
        if (direction != 1 && direction != -1) {
            SetError(error, "invalid trailing direction for instrument: " + *instrument);
            return false;
        }
        trailing_direction_by_instrument[*instrument] = direction;
    }

    // initial_stop / take_profit maps are display-only and were added later; treat
    // a missing count as an empty map so older state files load without error.
    std::unordered_map<std::string, double> initial_stop_by_instrument;
    if (state.find("initial_stop.count") != state.end()) {
        std::size_t initial_stop_count = 0;
        if (!ReadStateSize(state, "initial_stop.count", &initial_stop_count, error)) {
            return false;
        }
        for (std::size_t i = 0; i < initial_stop_count; ++i) {
            const std::string prefix = "initial_stop." + std::to_string(i);
            const std::string* instrument = FindStateValue(state, prefix + ".instrument", error);
            if (instrument == nullptr || instrument->empty()) {
                return false;
            }
            double price = 0.0;
            if (!ReadStateDouble(state, prefix + ".price", &price, error)) {
                return false;
            }
            initial_stop_by_instrument[*instrument] = price;
        }
    }

    std::unordered_map<std::string, double> take_profit_by_instrument;
    if (state.find("take_profit.count") != state.end()) {
        std::size_t take_profit_count = 0;
        if (!ReadStateSize(state, "take_profit.count", &take_profit_count, error)) {
            return false;
        }
        for (std::size_t i = 0; i < take_profit_count; ++i) {
            const std::string prefix = "take_profit." + std::to_string(i);
            const std::string* instrument = FindStateValue(state, prefix + ".instrument", error);
            if (instrument == nullptr || instrument->empty()) {
                return false;
            }
            double price = 0.0;
            if (!ReadStateDouble(state, prefix + ".price", &price, error)) {
                return false;
            }
            take_profit_by_instrument[*instrument] = price;
        }
    }

    std::optional<double> last_kama;
    std::optional<double> last_er;
    std::optional<double> last_adx;
    std::optional<double> last_stop_atr;
    std::optional<double> last_take_atr;
    std::optional<double> last_threshold;
    std::optional<double> last_diff_1;
    std::optional<double> last_diff_2;
    std::optional<double> last_diff_3;
    std::optional<int> last_diff_class_1;
    std::optional<int> last_diff_class_2;
    std::optional<int> last_diff_class_3;
    std::optional<int> last_trend_sum;
    std::optional<double> last_stop_loss_price;
    std::optional<double> last_take_profit_price;
    if (!ReadOptionalDouble(state, "last_kama", &last_kama, error) ||
        !ReadOptionalDouble(state, "last_er", &last_er, error) ||
        !ReadOptionalDouble(state, "last_adx", &last_adx, error) ||
        !ReadOptionalDouble(state, "last_stop_atr", &last_stop_atr, error) ||
        !ReadOptionalDouble(state, "last_take_atr", &last_take_atr, error) ||
        !ReadOptionalDouble(state, "last_threshold", &last_threshold, error) ||
        !ReadOptionalDouble(state, "last_diff_1", &last_diff_1, error) ||
        !ReadOptionalDouble(state, "last_diff_2", &last_diff_2, error) ||
        !ReadOptionalDouble(state, "last_diff_3", &last_diff_3, error) ||
        !ReadOptionalInt(state, "last_diff_class_1", &last_diff_class_1, error) ||
        !ReadOptionalInt(state, "last_diff_class_2", &last_diff_class_2, error) ||
        !ReadOptionalInt(state, "last_diff_class_3", &last_diff_class_3, error) ||
        !ReadOptionalInt(state, "last_trend_sum", &last_trend_sum, error) ||
        !ReadOptionalDouble(state, "last_stop_loss_price", &last_stop_loss_price, error) ||
        !ReadOptionalDouble(state, "last_take_profit_price", &last_take_profit_price, error)) {
        return false;
    }

    const std::string* raw_signal = FindStateValue(state, "last_raw_signal", error);
    if (raw_signal == nullptr) {
        return false;
    }

    if (!kama_->ImportState(kama_state) || !adx_->ImportState(adx_state)) {
        SetError(error, "failed to import KAMA or ADX state");
        return false;
    }
    if (stop_loss_atr_ != nullptr) {
        if (!stop_atr_enabled || !stop_loss_atr_->ImportState(stop_atr_state)) {
            SetError(error, "failed to import stop ATR state");
            return false;
        }
    }
    if (take_profit_atr_ != nullptr) {
        if (!take_atr_enabled || !take_profit_atr_->ImportState(take_atr_state)) {
            SetError(error, "failed to import take ATR state");
            return false;
        }
    }

    kama_recent_ = std::move(kama_recent);
    kama_window_ = std::move(kama_window);
    kama_window_sum_ = loaded_window_sum;
    kama_window_sum_sq_ = loaded_window_sum_sq;
    trailing_stop_by_instrument_ = std::move(trailing_stop_by_instrument);
    trailing_direction_by_instrument_ = std::move(trailing_direction_by_instrument);
    initial_stop_by_instrument_ = std::move(initial_stop_by_instrument);
    take_profit_by_instrument_ = std::move(take_profit_by_instrument);
    last_kama_ = last_kama;
    last_er_ = last_er;
    last_adx_ = last_adx;
    last_stop_atr_ = last_stop_atr;
    last_take_atr_ = last_take_atr;
    last_threshold_ = last_threshold;
    last_diff_1_ = last_diff_1;
    last_diff_2_ = last_diff_2;
    last_diff_3_ = last_diff_3;
    last_diff_class_1_ = last_diff_class_1;
    last_diff_class_2_ = last_diff_class_2;
    last_diff_class_3_ = last_diff_class_3;
    last_trend_sum_ = last_trend_sum;
    last_stop_loss_price_ = last_stop_loss_price;
    last_take_profit_price_ = last_take_profit_price;
    last_raw_signal_ = *raw_signal;
    return true;
}

int KamaTrendStrategy::ClassifyDiff(double diff, double threshold) const {
    if (diff > threshold) {
        return 1;
    }
    if (diff < -threshold) {
        return -1;
    }
    return 0;
}

int KamaTrendStrategy::ComputeOrderVolume(const AtomicStrategyContext& ctx,
                                          const std::string& instrument_id,
                                          double atr_value) const {
    if (!std::isfinite(atr_value) || atr_value <= 0.0) {
        return default_volume_;
    }

    const double risk_budget_r = ComputeRiskBudgetR(ctx, risk_per_trade_pct_);
    if (risk_budget_r <= 0.0) {
        return default_volume_;
    }

    const std::optional<double> contract_multiplier = ResolveContractMultiplier(ctx, instrument_id);
    if (!contract_multiplier.has_value()) {
        return default_volume_;
    }

    const double loss_per_hand =
        std::fabs(stop_loss_atr_multiplier_ * atr_value) * contract_multiplier.value();
    if (!std::isfinite(loss_per_hand) || loss_per_hand <= 0.0) {
        return default_volume_;
    }
    const double raw_volume = std::floor(risk_budget_r / loss_per_hand);
    if (!std::isfinite(raw_volume)) {
        return default_volume_;
    }
    if (raw_volume > static_cast<double>(std::numeric_limits<int>::max())) {
        return std::numeric_limits<int>::max();
    }
    int volume = static_cast<int>(raw_volume);
    if (volume <= 0) {
        volume = 1;
    }
    return volume;
}

double KamaTrendStrategy::ComputeStdKama() const {
    if (kama_window_.empty()) {
        return 0.0;
    }
    const double count = static_cast<double>(kama_window_.size());
    const double mean = kama_window_sum_ / count;
    const double mean_sq = kama_window_sum_sq_ / count;
    return std::sqrt(std::max(0.0, mean_sq - mean * mean));
}

std::vector<SignalIntent> KamaTrendStrategy::EvaluateRiskSignals(const AtomicStrategyContext& ctx,
                                                                 const std::string& instrument_id,
                                                                 double price,
                                                                 EpochNanos ts_ns) const {
    if (instrument_id.empty() || !std::isfinite(price)) {
        return {};
    }

    const std::int32_t position = ResolvePosition(ctx, instrument_id);
    if (position == 0) {
        return {};
    }

    const int direction = position > 0 ? 1 : -1;
    std::vector<SignalIntent> signals;

    if (last_stop_loss_price_.has_value() && std::isfinite(*last_stop_loss_price_)) {
        const bool stop_triggered =
            direction > 0 ? (price <= *last_stop_loss_price_) : (price >= *last_stop_loss_price_);
        if (stop_triggered) {
            const double pnl_amount = ComputePositionPnl(ctx, instrument_id, position, price);
            const Side close_side = position > 0 ? Side::kSell : Side::kBuy;
            SignalIntent close_signal =
                BuildCloseSignal(id_, instrument_id, SignalType::kStopLoss, position, price, ts_ns);
            EmitKamaStrategyLog(ctx, "warn", "stop_loss_triggered",
                                {{"strategy_id", id_},
                                 {"event_type", "stop_loss_triggered"},
                                 {"event_ts_ns", std::to_string(ts_ns)},
                                 {"trace_id", close_signal.trace_id},
                                 {"instrument_id", instrument_id},
                                 {"side", SideToString(close_side)},
                                 {"stop_loss_price", FormatDouble(*last_stop_loss_price_)},
                                 {"actual_fill_price", FormatDouble(price)},
                                 {"loss_amount", FormatDouble(pnl_amount)},
                                 {"stop_loss_type", stop_loss_mode_}});
            EmitKamaStrategyLog(ctx, "info", "close_signal_emitted",
                                {{"strategy_id", id_},
                                 {"event_type", "close_signal"},
                                 {"event_ts_ns", std::to_string(ts_ns)},
                                 {"trace_id", close_signal.trace_id},
                                 {"instrument_id", instrument_id},
                                 {"side", SideToString(close_side)},
                                 {"close_price", FormatDouble(price)},
                                 {"pnl_amount", FormatDouble(pnl_amount)},
                                 {"close_reason", "stop_loss"}});
            signals.push_back(std::move(close_signal));
        }
    }

    if (last_take_profit_price_.has_value() && std::isfinite(*last_take_profit_price_)) {
        const bool take_triggered = direction > 0 ? (price >= *last_take_profit_price_)
                                                  : (price <= *last_take_profit_price_);
        if (take_triggered) {
            const double pnl_amount = ComputePositionPnl(ctx, instrument_id, position, price);
            const Side close_side = position > 0 ? Side::kSell : Side::kBuy;
            SignalIntent close_signal = BuildCloseSignal(
                id_, instrument_id, SignalType::kTakeProfit, position, price, ts_ns);
            EmitKamaStrategyLog(ctx, "info", "close_signal_emitted",
                                {{"strategy_id", id_},
                                 {"event_type", "close_signal"},
                                 {"event_ts_ns", std::to_string(ts_ns)},
                                 {"trace_id", close_signal.trace_id},
                                 {"instrument_id", instrument_id},
                                 {"side", SideToString(close_side)},
                                 {"close_price", FormatDouble(price)},
                                 {"pnl_amount", FormatDouble(pnl_amount)},
                                 {"close_reason", "take_profit"}});
            signals.push_back(std::move(close_signal));
        }
    }

    return signals;
}

std::string KamaTrendStrategy::ExtractSymbolPrefixLower(const std::string& instrument_id) {
    std::string prefix;
    for (unsigned char ch : instrument_id) {
        if (std::isalpha(ch) == 0) {
            break;
        }
        prefix.push_back(static_cast<char>(std::tolower(ch)));
    }
    return prefix;
}

std::string KamaTrendStrategy::ToUpper(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    return text;
}

SignalIntent KamaTrendStrategy::BuildCloseSignal(const std::string& strategy_id,
                                                 const std::string& instrument_id,
                                                 SignalType signal_type, std::int32_t position,
                                                 double limit_price, EpochNanos ts_ns) {
    SignalIntent signal;
    signal.strategy_id = strategy_id;
    signal.instrument_id = instrument_id;
    signal.signal_type = signal_type;
    signal.side = position > 0 ? Side::kSell : Side::kBuy;
    signal.offset = OffsetFlag::kClose;
    signal.volume = std::abs(position);
    signal.limit_price = limit_price;
    signal.ts_ns = ts_ns;
    signal.trace_id = BuildSignalTraceId(strategy_id, signal_type, instrument_id, ts_ns);
    return signal;
}

QUANT_HFT_REGISTER_ATOMIC_STRATEGY("KamaTrendStrategy", KamaTrendStrategy);

}  // namespace quant_hft
