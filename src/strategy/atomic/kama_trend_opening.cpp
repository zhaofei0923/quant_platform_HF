#include "quant_hft/strategy/atomic/kama_trend_opening.h"

#include <cctype>
#include <cmath>
#include <limits>
#include <stdexcept>

#include "atomic_param_parsing.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {
namespace {

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
    for (char& ch : text) {
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    return text;
}

}  // namespace

void KamaTrendOpening::Init(const AtomicParams& params) {
    id_ = atomic_internal::GetString(params, "id", "KamaTrendOpening");
    er_period_ = atomic_internal::GetInt(params, "er_period", 10);
    fast_period_ = atomic_internal::GetInt(params, "fast_period", 2);
    slow_period_ = atomic_internal::GetInt(params, "slow_period", 30);
    std_period_ = atomic_internal::GetInt(params, "std_period", 10);
    kama_filter_ = atomic_internal::GetDouble(params, "kama_filter", 0.5);
    atr_period_ = atomic_internal::GetInt(params, "atr_period", 14);
    stop_loss_multi_ = atomic_internal::GetDouble(params, "stop_loss_multi", 2.0);
    default_volume_ = atomic_internal::GetInt(params, "default_volume", 1);

    if (id_.empty()) {
        throw std::invalid_argument("KamaTrendOpening id must not be empty");
    }
    if (er_period_ <= 0 || fast_period_ <= 0 || slow_period_ <= 0 || std_period_ <= 0 ||
        atr_period_ <= 0) {
        throw std::invalid_argument("KamaTrendOpening periods must be positive");
    }
    if (!std::isfinite(kama_filter_) || kama_filter_ < 0.0) {
        throw std::invalid_argument("KamaTrendOpening kama_filter must be non-negative");
    }
    if (!(stop_loss_multi_ > 0.0)) {
        throw std::invalid_argument("KamaTrendOpening stop_loss_multi must be positive");
    }
    if (default_volume_ <= 0) {
        throw std::invalid_argument("KamaTrendOpening default_volume must be positive");
    }

    kama_ = std::make_unique<KAMA>(er_period_, fast_period_, slow_period_);
    atr_ = std::make_unique<ATR>(atr_period_);
    kama_recent_.clear();
    kama_window_.clear();
    kama_window_sum_ = 0.0;
    kama_window_sum_sq_ = 0.0;
    last_kama_.reset();
    last_atr_.reset();
    last_er_.reset();
}

std::string KamaTrendOpening::GetId() const { return id_; }

void KamaTrendOpening::Reset() {
    if (kama_ != nullptr) {
        kama_->Reset();
    }
    if (atr_ != nullptr) {
        atr_->Reset();
    }
    kama_recent_.clear();
    kama_window_.clear();
    kama_window_sum_ = 0.0;
    kama_window_sum_sq_ = 0.0;
    last_kama_.reset();
    last_atr_.reset();
    last_er_.reset();
}

std::vector<SignalIntent> KamaTrendOpening::OnState(const StateSnapshot7D& state,
                                                    const AtomicStrategyContext& ctx) {
    if (kama_ == nullptr || atr_ == nullptr || !state.has_bar || !std::isfinite(state.bar_high) ||
        !std::isfinite(state.bar_low) || !std::isfinite(state.bar_close)) {
        return {};
    }

    kama_->Update(state.bar_high, state.bar_low, state.bar_close, state.bar_volume);
    atr_->Update(state.bar_high, state.bar_low, state.bar_close, state.bar_volume);

    if (!kama_->IsReady() || !atr_->IsReady()) {
        return {};
    }

    const std::optional<double> kama_value = kama_->Value();
    const std::optional<double> atr_value = atr_->Value();
    const std::optional<double> er_value = kama_->EfficiencyRatio();
    if (!kama_value.has_value() || !atr_value.has_value()) {
        return {};
    }

    last_kama_ = kama_value;
    last_atr_ = atr_value;
    last_er_ = er_value;

    kama_recent_.push_back(*kama_value);
    if (kama_recent_.size() > 4) {
        kama_recent_.pop_front();
    }

    kama_window_.push_back(*kama_value);
    kama_window_sum_ += *kama_value;
    kama_window_sum_sq_ += (*kama_value) * (*kama_value);
    if (kama_window_.size() > static_cast<std::size_t>(std_period_)) {
        const double removed = kama_window_.front();
        kama_window_.pop_front();
        kama_window_sum_ -= removed;
        kama_window_sum_sq_ -= removed * removed;
        kama_window_sum_sq_ = std::max(0.0, kama_window_sum_sq_);
    }

    if (kama_recent_.size() < 4 || kama_window_.size() < static_cast<std::size_t>(std_period_)) {
        return {};
    }

    const auto pos_it = ctx.net_positions.find(state.instrument_id);
    if (pos_it != ctx.net_positions.end() && pos_it->second != 0) {
        return {};
    }

    const double std_kama = ComputeStdKama();
    const double threshold = kama_filter_ * std_kama;
    const double kama_t = kama_recent_[3];
    const int diff_1st = ClassifyDiff(kama_t - kama_recent_[2], threshold);
    const int diff_2nd = ClassifyDiff(kama_t - kama_recent_[1], threshold);
    const int diff_3rd = ClassifyDiff(kama_t - kama_recent_[0], threshold);
    const int sum = diff_1st + diff_2nd + diff_3rd;
    if (sum != 3 && sum != -3) {
        return {};
    }

    const int volume = ComputeOrderVolume(ctx, state.instrument_id, *atr_value);
    if (volume <= 0) {
        return {};
    }

    SignalIntent signal;
    signal.strategy_id = id_;
    signal.instrument_id = state.instrument_id;
    signal.signal_type = SignalType::kOpen;
    signal.side = sum > 0 ? Side::kBuy : Side::kSell;
    signal.offset = OffsetFlag::kOpen;
    signal.volume = volume;
    signal.limit_price = state.bar_close;
    signal.ts_ns = state.ts_ns;
    return {signal};
}

std::optional<AtomicIndicatorSnapshot> KamaTrendOpening::IndicatorSnapshot() const {
    if (!last_kama_.has_value() && !last_atr_.has_value() && !last_er_.has_value()) {
        return std::nullopt;
    }
    AtomicIndicatorSnapshot snapshot;
    snapshot.kama = last_kama_;
    snapshot.atr = last_atr_;
    snapshot.er = last_er_;
    return snapshot;
}

int KamaTrendOpening::ClassifyDiff(double diff, double threshold) const {
    if (diff > threshold) {
        return 1;
    }
    if (diff < -threshold) {
        return -1;
    }
    return 0;
}

int KamaTrendOpening::ComputeOrderVolume(const AtomicStrategyContext& ctx,
                                         const std::string& instrument_id, double atr_value) const {
    if (!std::isfinite(atr_value) || atr_value <= 0.0) {
        return default_volume_;
    }

    const double equity = std::isfinite(ctx.account_equity) ? ctx.account_equity : 0.0;
    const double max_loss_percent =
        (std::isfinite(ctx.max_loss_percent) && ctx.max_loss_percent > 0.0) ? ctx.max_loss_percent
                                                                            : 0.0;
    const double usable_equity = std::max(0.0, equity) * max_loss_percent;
    if (usable_equity <= 0.0) {
        return default_volume_;
    }

    std::optional<double> contract_multiplier;
    auto maybe_set_multiplier = [&](const std::string& key) {
        if (key.empty() || contract_multiplier.has_value()) {
            return;
        }
        const auto it = ctx.contract_multipliers.find(key);
        if (it != ctx.contract_multipliers.end() && std::isfinite(it->second) && it->second > 0.0) {
            contract_multiplier = it->second;
        }
    };
    maybe_set_multiplier(instrument_id);
    const std::string symbol_prefix_lower = ExtractSymbolPrefixLower(instrument_id);
    maybe_set_multiplier(symbol_prefix_lower);
    maybe_set_multiplier(ToUpper(symbol_prefix_lower));

    if (!contract_multiplier.has_value()) {
        return default_volume_;
    }

    const double loss_per_hand =
        std::fabs(stop_loss_multi_ * atr_value) * contract_multiplier.value();
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

double KamaTrendOpening::ComputeStdKama() const {
    if (kama_window_.empty()) {
        return 0.0;
    }
    const double count = static_cast<double>(kama_window_.size());
    const double mean = kama_window_sum_ / count;
    const double mean_sq = kama_window_sum_sq_ / count;
    return std::sqrt(std::max(0.0, mean_sq - mean * mean));
}

QUANT_HFT_REGISTER_ATOMIC_STRATEGY("KamaTrendOpening", KamaTrendOpening);

}  // namespace quant_hft
