#include "quant_hft/strategy/atomic/trend_strategy.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <utility>

#include "atomic_param_parsing.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {
namespace {

std::string NormalizeMode(std::string mode) {
    std::transform(mode.begin(), mode.end(), mode.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return mode;
}

std::int32_t ResolvePosition(const AtomicStrategyContext& ctx, const std::string& instrument_id) {
    const auto position_it = ctx.net_positions.find(instrument_id);
    return position_it == ctx.net_positions.end() ? 0 : position_it->second;
}

}  // namespace

void TrendStrategy::Init(const AtomicParams& params) {
    id_ = atomic_internal::GetString(params, "id", "TrendStrategy");
    er_period_ = atomic_internal::GetInt(params, "er_period", 10);
    fast_period_ = atomic_internal::GetInt(params, "fast_period", 2);
    slow_period_ = atomic_internal::GetInt(params, "slow_period", 30);
    kama_filter_ = atomic_internal::GetDouble(params, "kama_filter", 0.0);
    risk_per_trade_pct_ = atomic_internal::GetDouble(params, "risk_per_trade_pct", 0.01);
    default_volume_ = atomic_internal::GetInt(params, "default_volume", 1);
    stop_loss_mode_ =
        NormalizeMode(atomic_internal::GetString(params, "stop_loss_mode", "trailing_atr"));
    stop_loss_atr_period_ = atomic_internal::GetInt(params, "stop_loss_atr_period", 14);
    stop_loss_atr_multiplier_ =
        atomic_internal::GetDouble(params, "stop_loss_atr_multiplier", 2.0);
    take_profit_mode_ =
        NormalizeMode(atomic_internal::GetString(params, "take_profit_mode", "atr_target"));
    take_profit_atr_period_ = atomic_internal::GetInt(params, "take_profit_atr_period", 14);
    take_profit_atr_multiplier_ =
        atomic_internal::GetDouble(params, "take_profit_atr_multiplier", 3.0);

    if (id_.empty()) {
        throw std::invalid_argument("TrendStrategy id must not be empty");
    }
    if (er_period_ <= 0 || fast_period_ <= 0 || slow_period_ <= 0) {
        throw std::invalid_argument("TrendStrategy periods must be positive");
    }
    if (!std::isfinite(kama_filter_) || kama_filter_ < 0.0) {
        throw std::invalid_argument("TrendStrategy kama_filter must be non-negative");
    }
    if (!std::isfinite(risk_per_trade_pct_) || risk_per_trade_pct_ <= 0.0 ||
        risk_per_trade_pct_ > 1.0) {
        throw std::invalid_argument("TrendStrategy risk_per_trade_pct must be in (0, 1]");
    }
    if (default_volume_ <= 0) {
        throw std::invalid_argument("TrendStrategy default_volume must be positive");
    }
    if (stop_loss_mode_ != "none" && stop_loss_mode_ != "trailing_atr") {
        throw std::invalid_argument(
            "TrendStrategy stop_loss_mode must be one of: trailing_atr, none");
    }
    if (take_profit_mode_ != "none" && take_profit_mode_ != "atr_target") {
        throw std::invalid_argument(
            "TrendStrategy take_profit_mode must be one of: atr_target, none");
    }
    if (stop_loss_mode_ == "trailing_atr") {
        if (stop_loss_atr_period_ <= 0) {
            throw std::invalid_argument("TrendStrategy stop_loss_atr_period must be positive");
        }
        if (!std::isfinite(stop_loss_atr_multiplier_) || stop_loss_atr_multiplier_ <= 0.0) {
            throw std::invalid_argument("TrendStrategy stop_loss_atr_multiplier must be positive");
        }
    }
    if (take_profit_mode_ == "atr_target") {
        if (take_profit_atr_period_ <= 0) {
            throw std::invalid_argument("TrendStrategy take_profit_atr_period must be positive");
        }
        if (!std::isfinite(take_profit_atr_multiplier_) || take_profit_atr_multiplier_ <= 0.0) {
            throw std::invalid_argument(
                "TrendStrategy take_profit_atr_multiplier must be positive");
        }
    }

    kama_ = std::make_unique<KAMA>(er_period_, fast_period_, slow_period_);
    stop_loss_atr_.reset();
    take_profit_atr_.reset();
    if (stop_loss_mode_ == "trailing_atr") {
        stop_loss_atr_ = std::make_unique<ATR>(stop_loss_atr_period_);
    }
    if (take_profit_mode_ == "atr_target") {
        take_profit_atr_ = std::make_unique<ATR>(take_profit_atr_period_);
    }
    trailing_stop_by_instrument_.clear();
    trailing_direction_by_instrument_.clear();
    last_kama_.reset();
    last_er_.reset();
    last_stop_atr_.reset();
    last_take_atr_.reset();
    last_stop_loss_price_.reset();
    last_take_profit_price_.reset();
}

std::string TrendStrategy::GetId() const { return id_; }

void TrendStrategy::Reset() {
    if (kama_ != nullptr) {
        kama_->Reset();
    }
    if (stop_loss_atr_ != nullptr) {
        stop_loss_atr_->Reset();
    }
    if (take_profit_atr_ != nullptr) {
        take_profit_atr_->Reset();
    }
    trailing_stop_by_instrument_.clear();
    trailing_direction_by_instrument_.clear();
    last_kama_.reset();
    last_er_.reset();
    last_stop_atr_.reset();
    last_take_atr_.reset();
    last_stop_loss_price_.reset();
    last_take_profit_price_.reset();
}

std::vector<SignalIntent> TrendStrategy::OnState(const StateSnapshot7D& state,
                                                 const AtomicStrategyContext& ctx) {
    last_kama_.reset();
    last_er_.reset();
    last_stop_atr_.reset();
    last_take_atr_.reset();
    last_stop_loss_price_.reset();
    last_take_profit_price_.reset();

    if (kama_ == nullptr || !state.has_bar || !std::isfinite(state.bar_high) ||
        !std::isfinite(state.bar_low) || !std::isfinite(state.bar_close)) {
        return {};
    }

    kama_->Update(state.bar_high, state.bar_low, state.bar_close, state.bar_volume);
    if (stop_loss_atr_ != nullptr) {
        stop_loss_atr_->Update(state.bar_high, state.bar_low, state.bar_close, state.bar_volume);
    }
    if (take_profit_atr_ != nullptr) {
        take_profit_atr_->Update(state.bar_high, state.bar_low, state.bar_close, state.bar_volume);
    }

    if (kama_->IsReady()) {
        last_kama_ = kama_->Value();
        last_er_ = kama_->EfficiencyRatio();
    }
    if (stop_loss_atr_ != nullptr && stop_loss_atr_->IsReady()) {
        last_stop_atr_ = stop_loss_atr_->Value();
    }
    if (take_profit_atr_ != nullptr && take_profit_atr_->IsReady()) {
        last_take_atr_ = take_profit_atr_->Value();
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
            double stop_price = direction > 0 ? (avg_open_price - stop_distance)
                                              : (avg_open_price + stop_distance);
            const auto stop_it = trailing_stop_by_instrument_.find(state.instrument_id);
            const auto direction_it = trailing_direction_by_instrument_.find(state.instrument_id);
            if (stop_it != trailing_stop_by_instrument_.end() &&
                direction_it != trailing_direction_by_instrument_.end() &&
                direction_it->second == direction) {
                stop_price = stop_it->second;
            }
            const double candidate = direction > 0 ? (state.bar_close - stop_distance)
                                                   : (state.bar_close + stop_distance);
            stop_price = direction > 0 ? std::max(stop_price, candidate)
                                       : std::min(stop_price, candidate);
            trailing_stop_by_instrument_[state.instrument_id] = stop_price;
            trailing_direction_by_instrument_[state.instrument_id] = direction;
            last_stop_loss_price_ = stop_price;
            const bool stop_triggered =
                direction > 0 ? (state.bar_close <= stop_price) : (state.bar_close >= stop_price);
            if (stop_triggered) {
                signals.push_back(BuildCloseSignal(id_, state.instrument_id, SignalType::kStopLoss,
                                                   position, state.bar_close, state.ts_ns));
            }
        } else {
            trailing_stop_by_instrument_.erase(state.instrument_id);
            trailing_direction_by_instrument_.erase(state.instrument_id);
        }

        if (take_profit_mode_ == "atr_target" && last_take_atr_.has_value() &&
            std::isfinite(*last_take_atr_) && *last_take_atr_ > 0.0) {
            const double take_distance = take_profit_atr_multiplier_ * (*last_take_atr_);
            const double take_price =
                direction > 0 ? (avg_open_price + take_distance) : (avg_open_price - take_distance);
            last_take_profit_price_ = take_price;
            const bool take_triggered =
                direction > 0 ? (state.bar_close >= take_price) : (state.bar_close <= take_price);
            if (take_triggered) {
                signals.push_back(BuildCloseSignal(id_, state.instrument_id,
                                                   SignalType::kTakeProfit, position,
                                                   state.bar_close, state.ts_ns));
            }
        }

        return signals;
    }

    trailing_stop_by_instrument_.erase(state.instrument_id);
    trailing_direction_by_instrument_.erase(state.instrument_id);
    if (!last_kama_.has_value() || !std::isfinite(*last_kama_)) {
        return {};
    }

    const double threshold = kama_filter_ * last_stop_atr_.value_or(0.0);
    const double diff = state.bar_close - (*last_kama_);
    Side open_side = Side::kBuy;
    if (diff > threshold) {
        open_side = Side::kBuy;
    } else if (diff < -threshold) {
        open_side = Side::kSell;
    } else {
        return {};
    }

    const int volume =
        ComputeOrderVolume(ctx, state.instrument_id, last_stop_atr_.value_or(std::nan("")));
    if (volume <= 0) {
        return {};
    }

    SignalIntent open_signal;
    open_signal.strategy_id = id_;
    open_signal.instrument_id = state.instrument_id;
    open_signal.signal_type = SignalType::kOpen;
    open_signal.side = open_side;
    open_signal.offset = OffsetFlag::kOpen;
    open_signal.volume = volume;
    open_signal.limit_price = state.bar_close;
    open_signal.ts_ns = state.ts_ns;
    return {open_signal};
}

std::optional<AtomicIndicatorSnapshot> TrendStrategy::IndicatorSnapshot() const {
    if (!last_kama_.has_value() && !last_er_.has_value() && !last_stop_atr_.has_value() &&
        !last_take_atr_.has_value() && !last_stop_loss_price_.has_value() &&
        !last_take_profit_price_.has_value()) {
        return std::nullopt;
    }
    AtomicIndicatorSnapshot snapshot;
    snapshot.kama = last_kama_;
    snapshot.er = last_er_;
    snapshot.atr = last_stop_atr_.has_value() ? last_stop_atr_ : last_take_atr_;
    snapshot.stop_loss_price = last_stop_loss_price_;
    snapshot.take_profit_price = last_take_profit_price_;
    return snapshot;
}

int TrendStrategy::ComputeOrderVolume(const AtomicStrategyContext& ctx,
                                      const std::string& instrument_id, double atr_value) const {
    if (!std::isfinite(atr_value) || atr_value <= 0.0) {
        return default_volume_;
    }

    const double equity = std::isfinite(ctx.account_equity) ? std::max(0.0, ctx.account_equity)
                                                             : 0.0;
    const double usable_equity = equity * risk_per_trade_pct_;
    if (usable_equity <= 0.0) {
        return default_volume_;
    }

    std::optional<double> contract_multiplier;
    auto maybe_set_multiplier = [&](const std::string& key) {
        if (key.empty() || contract_multiplier.has_value()) {
            return;
        }
        const auto multiplier_it = ctx.contract_multipliers.find(key);
        if (multiplier_it != ctx.contract_multipliers.end() && std::isfinite(multiplier_it->second) &&
            multiplier_it->second > 0.0) {
            contract_multiplier = multiplier_it->second;
        }
    };
    maybe_set_multiplier(instrument_id);
    const std::string symbol_prefix = ExtractSymbolPrefixLower(instrument_id);
    maybe_set_multiplier(symbol_prefix);
    maybe_set_multiplier(ToUpper(symbol_prefix));

    if (!contract_multiplier.has_value()) {
        return default_volume_;
    }

    const double loss_per_hand =
        std::fabs(stop_loss_atr_multiplier_ * atr_value) * contract_multiplier.value();
    if (!std::isfinite(loss_per_hand) || loss_per_hand <= 0.0) {
        return default_volume_;
    }
    const double raw_volume = std::floor(usable_equity / loss_per_hand);
    if (!std::isfinite(raw_volume) || raw_volume < 1.0) {
        return 0;
    }
    if (raw_volume > static_cast<double>(std::numeric_limits<int>::max())) {
        return std::numeric_limits<int>::max();
    }
    return static_cast<int>(raw_volume);
}

std::string TrendStrategy::ExtractSymbolPrefixLower(const std::string& instrument_id) {
    std::string prefix;
    for (unsigned char ch : instrument_id) {
        if (std::isalpha(ch) == 0) {
            break;
        }
        prefix.push_back(static_cast<char>(std::tolower(ch)));
    }
    return prefix;
}

std::string TrendStrategy::ToUpper(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return text;
}

SignalIntent TrendStrategy::BuildCloseSignal(const std::string& strategy_id,
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
    return signal;
}

QUANT_HFT_REGISTER_ATOMIC_STRATEGY("TrendStrategy", TrendStrategy);

}  // namespace quant_hft
