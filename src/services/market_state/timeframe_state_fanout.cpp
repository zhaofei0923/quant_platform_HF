#include "quant_hft/services/timeframe_state_fanout.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <ctime>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace quant_hft {
namespace {

constexpr EpochNanos kNanosPerSecond = 1'000'000'000;
constexpr EpochNanos kNanosPerMinute = 60 * kNanosPerSecond;
constexpr std::int64_t kShanghaiUtcOffsetSeconds = 8 * 60 * 60;

void SetError(std::string* error, const std::string& message) {
    if (error != nullptr) {
        *error = message;
    }
}

std::string FormatDouble(double value) {
    std::ostringstream out;
    out.precision(17);
    out << value;
    return out.str();
}

std::string FormatBool(bool value) { return value ? "true" : "false"; }

bool ParseBool(const std::string& text, bool* out) {
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

bool ParseDouble(const std::string& text, double* out) {
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

bool ParseInt(const std::string& text, int* out) {
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

bool ParseInt64(const std::string& text, std::int64_t* out) {
    try {
        std::size_t consumed = 0;
        const long long value = std::stoll(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = static_cast<std::int64_t>(value);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParseSize(const std::string& text, std::size_t* out) {
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

const std::string* RequireValue(const TimeframeStateFanout::PersistenceState& state,
                                const std::string& key, std::string* error) {
    const auto it = state.find(key);
    if (it == state.end()) {
        SetError(error, "missing timeframe fanout state key: " + key);
        return nullptr;
    }
    return &it->second;
}

bool ReadBool(const TimeframeStateFanout::PersistenceState& state, const std::string& key,
              bool* out, std::string* error) {
    const std::string* value = RequireValue(state, key, error);
    if (value == nullptr) {
        return false;
    }
    if (!ParseBool(*value, out)) {
        SetError(error, "invalid bool fanout state key: " + key);
        return false;
    }
    return true;
}

bool ReadDouble(const TimeframeStateFanout::PersistenceState& state, const std::string& key,
                double* out, std::string* error) {
    const std::string* value = RequireValue(state, key, error);
    if (value == nullptr) {
        return false;
    }
    if (!ParseDouble(*value, out)) {
        SetError(error, "invalid double fanout state key: " + key);
        return false;
    }
    return true;
}

bool ReadInt(const TimeframeStateFanout::PersistenceState& state, const std::string& key, int* out,
             std::string* error) {
    const std::string* value = RequireValue(state, key, error);
    if (value == nullptr) {
        return false;
    }
    if (!ParseInt(*value, out)) {
        SetError(error, "invalid int fanout state key: " + key);
        return false;
    }
    return true;
}

bool ReadInt64(const TimeframeStateFanout::PersistenceState& state, const std::string& key,
               std::int64_t* out, std::string* error) {
    const std::string* value = RequireValue(state, key, error);
    if (value == nullptr) {
        return false;
    }
    if (!ParseInt64(*value, out)) {
        SetError(error, "invalid int64 fanout state key: " + key);
        return false;
    }
    return true;
}

bool ReadSize(const TimeframeStateFanout::PersistenceState& state, const std::string& key,
              std::size_t* out, std::string* error) {
    const std::string* value = RequireValue(state, key, error);
    if (value == nullptr) {
        return false;
    }
    if (!ParseSize(*value, out)) {
        SetError(error, "invalid size fanout state key: " + key);
        return false;
    }
    return true;
}

void WriteDoubleVector(TimeframeStateFanout::PersistenceState* out, const std::string& prefix,
                       const std::vector<double>& values) {
    (*out)[prefix + ".count"] = std::to_string(values.size());
    for (std::size_t i = 0; i < values.size(); ++i) {
        (*out)[prefix + "." + std::to_string(i)] = FormatDouble(values[i]);
    }
}

bool ReadDoubleVector(const TimeframeStateFanout::PersistenceState& state,
                      const std::string& prefix, std::vector<double>* values, std::string* error) {
    std::size_t count = 0;
    if (!ReadSize(state, prefix + ".count", &count, error)) {
        return false;
    }
    values->clear();
    values->reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        double value = 0.0;
        if (!ReadDouble(state, prefix + "." + std::to_string(i), &value, error)) {
            return false;
        }
        values->push_back(value);
    }
    return true;
}

void WriteKamaState(TimeframeStateFanout::PersistenceState* out, const std::string& prefix,
                    const KAMA::State& state) {
    (*out)[prefix + ".initialized"] = FormatBool(state.initialized);
    WriteDoubleVector(out, prefix + ".closes", state.closes);
    (*out)[prefix + ".volatility_sum"] = FormatDouble(state.volatility_sum);
    (*out)[prefix + ".has_efficiency_ratio"] = FormatBool(state.has_efficiency_ratio);
    (*out)[prefix + ".efficiency_ratio"] = FormatDouble(state.efficiency_ratio);
    (*out)[prefix + ".kama"] = FormatDouble(state.kama);
}

bool ReadKamaState(const TimeframeStateFanout::PersistenceState& state, const std::string& prefix,
                   KAMA::State* out, std::string* error) {
    return ReadBool(state, prefix + ".initialized", &out->initialized, error) &&
           ReadDoubleVector(state, prefix + ".closes", &out->closes, error) &&
           ReadDouble(state, prefix + ".volatility_sum", &out->volatility_sum, error) &&
           ReadBool(state, prefix + ".has_efficiency_ratio", &out->has_efficiency_ratio, error) &&
           ReadDouble(state, prefix + ".efficiency_ratio", &out->efficiency_ratio, error) &&
           ReadDouble(state, prefix + ".kama", &out->kama, error);
}

void WriteAtrState(TimeframeStateFanout::PersistenceState* out, const std::string& prefix,
                   const ATR::State& state) {
    (*out)[prefix + ".initialized"] = FormatBool(state.initialized);
    (*out)[prefix + ".has_prev_close"] = FormatBool(state.has_prev_close);
    (*out)[prefix + ".prev_close"] = FormatDouble(state.prev_close);
    WriteDoubleVector(out, prefix + ".tr_seed", state.tr_seed);
    (*out)[prefix + ".tr_seed_sum"] = FormatDouble(state.tr_seed_sum);
    (*out)[prefix + ".atr"] = FormatDouble(state.atr);
}

bool ReadAtrState(const TimeframeStateFanout::PersistenceState& state, const std::string& prefix,
                  ATR::State* out, std::string* error) {
    return ReadBool(state, prefix + ".initialized", &out->initialized, error) &&
           ReadBool(state, prefix + ".has_prev_close", &out->has_prev_close, error) &&
           ReadDouble(state, prefix + ".prev_close", &out->prev_close, error) &&
           ReadDoubleVector(state, prefix + ".tr_seed", &out->tr_seed, error) &&
           ReadDouble(state, prefix + ".tr_seed_sum", &out->tr_seed_sum, error) &&
           ReadDouble(state, prefix + ".atr", &out->atr, error);
}

void WriteAdxState(TimeframeStateFanout::PersistenceState* out, const std::string& prefix,
                   const ADX::State& state) {
    (*out)[prefix + ".has_prev_bar"] = FormatBool(state.has_prev_bar);
    (*out)[prefix + ".prev_high"] = FormatDouble(state.prev_high);
    (*out)[prefix + ".prev_low"] = FormatDouble(state.prev_low);
    (*out)[prefix + ".prev_close"] = FormatDouble(state.prev_close);
    (*out)[prefix + ".seed_count"] = std::to_string(state.seed_count);
    (*out)[prefix + ".tr_seed_sum"] = FormatDouble(state.tr_seed_sum);
    (*out)[prefix + ".plus_dm_seed_sum"] = FormatDouble(state.plus_dm_seed_sum);
    (*out)[prefix + ".minus_dm_seed_sum"] = FormatDouble(state.minus_dm_seed_sum);
    (*out)[prefix + ".di_ready"] = FormatBool(state.di_ready);
    (*out)[prefix + ".tr_smoothed"] = FormatDouble(state.tr_smoothed);
    (*out)[prefix + ".plus_dm_smoothed"] = FormatDouble(state.plus_dm_smoothed);
    (*out)[prefix + ".minus_dm_smoothed"] = FormatDouble(state.minus_dm_smoothed);
    (*out)[prefix + ".plus_di"] = FormatDouble(state.plus_di);
    (*out)[prefix + ".minus_di"] = FormatDouble(state.minus_di);
    (*out)[prefix + ".dx"] = FormatDouble(state.dx);
    (*out)[prefix + ".dx_seed_count"] = std::to_string(state.dx_seed_count);
    (*out)[prefix + ".dx_seed_sum"] = FormatDouble(state.dx_seed_sum);
    (*out)[prefix + ".adx_ready"] = FormatBool(state.adx_ready);
    (*out)[prefix + ".adx"] = FormatDouble(state.adx);
}

bool ReadAdxState(const TimeframeStateFanout::PersistenceState& state, const std::string& prefix,
                  ADX::State* out, std::string* error) {
    return ReadBool(state, prefix + ".has_prev_bar", &out->has_prev_bar, error) &&
           ReadDouble(state, prefix + ".prev_high", &out->prev_high, error) &&
           ReadDouble(state, prefix + ".prev_low", &out->prev_low, error) &&
           ReadDouble(state, prefix + ".prev_close", &out->prev_close, error) &&
           ReadInt(state, prefix + ".seed_count", &out->seed_count, error) &&
           ReadDouble(state, prefix + ".tr_seed_sum", &out->tr_seed_sum, error) &&
           ReadDouble(state, prefix + ".plus_dm_seed_sum", &out->plus_dm_seed_sum, error) &&
           ReadDouble(state, prefix + ".minus_dm_seed_sum", &out->minus_dm_seed_sum, error) &&
           ReadBool(state, prefix + ".di_ready", &out->di_ready, error) &&
           ReadDouble(state, prefix + ".tr_smoothed", &out->tr_smoothed, error) &&
           ReadDouble(state, prefix + ".plus_dm_smoothed", &out->plus_dm_smoothed, error) &&
           ReadDouble(state, prefix + ".minus_dm_smoothed", &out->minus_dm_smoothed, error) &&
           ReadDouble(state, prefix + ".plus_di", &out->plus_di, error) &&
           ReadDouble(state, prefix + ".minus_di", &out->minus_di, error) &&
           ReadDouble(state, prefix + ".dx", &out->dx, error) &&
           ReadInt(state, prefix + ".dx_seed_count", &out->dx_seed_count, error) &&
           ReadDouble(state, prefix + ".dx_seed_sum", &out->dx_seed_sum, error) &&
           ReadBool(state, prefix + ".adx_ready", &out->adx_ready, error) &&
           ReadDouble(state, prefix + ".adx", &out->adx, error);
}

void WriteDetectorState(TimeframeStateFanout::PersistenceState* out, const std::string& prefix,
                        const MarketStateDetector::State& state) {
    WriteAdxState(out, prefix + ".adx", state.adx);
    WriteKamaState(out, prefix + ".kama", state.kama);
    WriteAtrState(out, prefix + ".atr", state.atr);
    (*out)[prefix + ".last_close"] = FormatDouble(state.last_close);
    (*out)[prefix + ".has_last_close"] = FormatBool(state.has_last_close);
    (*out)[prefix + ".bars_seen"] = std::to_string(state.bars_seen);
    (*out)[prefix + ".current_regime"] = std::to_string(static_cast<int>(state.current_regime));
    (*out)[prefix + ".decision_reason"] = state.decision_reason;
}

bool ReadDetectorState(const TimeframeStateFanout::PersistenceState& state,
                       const std::string& prefix, MarketStateDetector::State* out,
                       std::string* error) {
    int regime_value = 0;
    std::size_t bars_seen = 0;
    if (!ReadAdxState(state, prefix + ".adx", &out->adx, error) ||
        !ReadKamaState(state, prefix + ".kama", &out->kama, error) ||
        !ReadAtrState(state, prefix + ".atr", &out->atr, error) ||
        !ReadDouble(state, prefix + ".last_close", &out->last_close, error) ||
        !ReadBool(state, prefix + ".has_last_close", &out->has_last_close, error) ||
        !ReadSize(state, prefix + ".bars_seen", &bars_seen, error) ||
        !ReadInt(state, prefix + ".current_regime", &regime_value, error)) {
        return false;
    }
    if (regime_value < static_cast<int>(MarketRegime::kUnknown) ||
        regime_value > static_cast<int>(MarketRegime::kFlat)) {
        SetError(error, "invalid market regime in fanout state");
        return false;
    }
    out->bars_seen = bars_seen;
    out->current_regime = static_cast<MarketRegime>(regime_value);
    const auto decision_reason = state.find(prefix + ".decision_reason");
    out->decision_reason = decision_reason == state.end() ? "adx_warmup" : decision_reason->second;
    return true;
}

void WriteBar(TimeframeStateFanout::PersistenceState* out, const std::string& prefix,
              const BarSnapshot& bar) {
    (*out)[prefix + ".instrument_id"] = bar.instrument_id;
    (*out)[prefix + ".exchange_id"] = bar.exchange_id;
    (*out)[prefix + ".trading_day"] = bar.trading_day;
    (*out)[prefix + ".action_day"] = bar.action_day;
    (*out)[prefix + ".minute"] = bar.minute;
    (*out)[prefix + ".open"] = FormatDouble(bar.open);
    (*out)[prefix + ".high"] = FormatDouble(bar.high);
    (*out)[prefix + ".low"] = FormatDouble(bar.low);
    (*out)[prefix + ".close"] = FormatDouble(bar.close);
    (*out)[prefix + ".analysis_open"] = FormatDouble(bar.analysis_open);
    (*out)[prefix + ".analysis_high"] = FormatDouble(bar.analysis_high);
    (*out)[prefix + ".analysis_low"] = FormatDouble(bar.analysis_low);
    (*out)[prefix + ".analysis_close"] = FormatDouble(bar.analysis_close);
    (*out)[prefix + ".analysis_price_offset"] = FormatDouble(bar.analysis_price_offset);
    (*out)[prefix + ".volume"] = std::to_string(bar.volume);
    (*out)[prefix + ".ts_ns"] = std::to_string(bar.ts_ns);
    (*out)[prefix + ".period_end_ts_ns"] = std::to_string(bar.period_end_ts_ns);
    (*out)[prefix + ".finalized_ts_ns"] = std::to_string(bar.finalized_ts_ns);
    (*out)[prefix + ".expected_source_bars"] = std::to_string(bar.expected_source_bars);
    (*out)[prefix + ".observed_source_bars"] = std::to_string(bar.observed_source_bars);
    (*out)[prefix + ".is_complete"] = FormatBool(bar.is_complete);
    (*out)[prefix + ".is_session_endpoint"] = FormatBool(bar.is_session_endpoint);
    (*out)[prefix + ".strategy_eligible"] = FormatBool(bar.strategy_eligible);
    (*out)[prefix + ".volume_complete"] = FormatBool(bar.volume_complete);
    (*out)[prefix + ".has_conflict"] = FormatBool(bar.has_conflict);
    (*out)[prefix + ".is_recovery_replay"] = FormatBool(bar.is_recovery_replay);
}

bool ReadBar(const TimeframeStateFanout::PersistenceState& state, const std::string& prefix,
             BarSnapshot* bar, std::string* error) {
    const std::string* instrument_id = RequireValue(state, prefix + ".instrument_id", error);
    const std::string* exchange_id = RequireValue(state, prefix + ".exchange_id", error);
    const std::string* trading_day = RequireValue(state, prefix + ".trading_day", error);
    const std::string* action_day = RequireValue(state, prefix + ".action_day", error);
    const std::string* minute = RequireValue(state, prefix + ".minute", error);
    if (instrument_id == nullptr || exchange_id == nullptr || trading_day == nullptr ||
        action_day == nullptr || minute == nullptr) {
        return false;
    }
    bar->instrument_id = *instrument_id;
    bar->exchange_id = *exchange_id;
    bar->trading_day = *trading_day;
    bar->action_day = *action_day;
    bar->minute = *minute;
    const bool base_ok =
        ReadDouble(state, prefix + ".open", &bar->open, error) &&
        ReadDouble(state, prefix + ".high", &bar->high, error) &&
        ReadDouble(state, prefix + ".low", &bar->low, error) &&
        ReadDouble(state, prefix + ".close", &bar->close, error) &&
        ReadDouble(state, prefix + ".analysis_open", &bar->analysis_open, error) &&
        ReadDouble(state, prefix + ".analysis_high", &bar->analysis_high, error) &&
        ReadDouble(state, prefix + ".analysis_low", &bar->analysis_low, error) &&
        ReadDouble(state, prefix + ".analysis_close", &bar->analysis_close, error) &&
        ReadDouble(state, prefix + ".analysis_price_offset", &bar->analysis_price_offset, error) &&
        ReadInt64(state, prefix + ".volume", &bar->volume, error) &&
        ReadInt64(state, prefix + ".ts_ns", &bar->ts_ns, error);
    if (!base_ok) {
        return false;
    }
    if (state.find(prefix + ".period_end_ts_ns") == state.end()) {
        return true;
    }
    return ReadInt64(state, prefix + ".period_end_ts_ns", &bar->period_end_ts_ns, error) &&
           ReadInt64(state, prefix + ".finalized_ts_ns", &bar->finalized_ts_ns, error) &&
           ReadInt(state, prefix + ".expected_source_bars", &bar->expected_source_bars, error) &&
           ReadInt(state, prefix + ".observed_source_bars", &bar->observed_source_bars, error) &&
           ReadBool(state, prefix + ".is_complete", &bar->is_complete, error) &&
           ReadBool(state, prefix + ".is_session_endpoint", &bar->is_session_endpoint, error) &&
           ReadBool(state, prefix + ".strategy_eligible", &bar->strategy_eligible, error) &&
           ReadBool(state, prefix + ".volume_complete", &bar->volume_complete, error) &&
           ReadBool(state, prefix + ".has_conflict", &bar->has_conflict, error) &&
           ReadBool(state, prefix + ".is_recovery_replay", &bar->is_recovery_replay, error);
}

}  // namespace

TimeframeStateFanout::TimeframeStateFanout(
    std::vector<std::int32_t> timeframes, MarketStateDetectorConfig detector_config,
    MarketStateDetectorConfigByProduct detector_config_by_product)
    : detector_config_(detector_config),
      detector_config_by_product_(std::move(detector_config_by_product)) {
    std::sort(timeframes.begin(), timeframes.end());
    timeframes.erase(std::remove_if(timeframes.begin(), timeframes.end(),
                                    [](std::int32_t tf) { return tf <= 1; }),
                     timeframes.end());
    timeframes.erase(std::unique(timeframes.begin(), timeframes.end()), timeframes.end());
    timeframes_ = std::move(timeframes);
}

std::vector<TimeframeStateEmission> TimeframeStateFanout::OnOneMinuteBar(const BarSnapshot& bar) {
    std::vector<TimeframeStateEmission> emissions;
    if (bar.instrument_id.empty() || bar.minute.empty()) {
        return emissions;
    }

    for (const std::int32_t timeframe : timeframes_) {
        const std::string bucket_minute = BuildBucketMinute(bar.minute, timeframe);
        if (bucket_minute.empty()) {
            continue;
        }
        const std::string key = BuildKey(bar.instrument_id, timeframe);
        const std::string finalized_key = key + "|" + bucket_minute;
        if (finalized_bucket_keys_.find(finalized_key) != finalized_bucket_keys_.end()) {
            continue;
        }

        if (bar.is_session_endpoint) {
            BarSnapshot endpoint_bar = bar;
            endpoint_bar.minute = bucket_minute;
            endpoint_bar.expected_source_bars = 1;
            endpoint_bar.observed_source_bars = 1;
            endpoint_bar.is_complete = true;
            endpoint_bar.strategy_eligible = false;
            finalized_bucket_keys_.insert(finalized_key);
            emissions.push_back(BuildEmission(endpoint_bar, timeframe, false));
            continue;
        }

        auto& bucket = buckets_[key];
        if (!bucket.initialized) {
            bucket.initialized = true;
            bucket.timeframe_minutes = timeframe;
            bucket.bucket_minute = bucket_minute;
            bucket.period_end_ts_ns = ResolveBucketPeriodEnd(bar, timeframe);
            bucket.conflicting_duplicate = false;
            bucket.has_incomplete_source = !bar.is_complete || bar.has_conflict;
            bucket.has_non_tradable_source =
                !bar.strategy_eligible || bar.is_recovery_replay || !bar.volume_complete;
            bucket.source_fingerprint_by_minute.clear();
            bucket.source_fingerprint_by_minute.emplace(bar.minute, BarFingerprint(bar));
            bucket.bar = bar;
            bucket.bar.minute = bucket_minute;
        } else if (bucket.bucket_minute == bucket_minute) {
            const std::string fingerprint = BarFingerprint(bar);
            const auto existing = bucket.source_fingerprint_by_minute.find(bar.minute);
            if (existing != bucket.source_fingerprint_by_minute.end()) {
                if (existing->second != fingerprint) {
                    bucket.conflicting_duplicate = true;
                    bucket.bar.has_conflict = true;
                }
                continue;
            }
            bucket.source_fingerprint_by_minute.emplace(bar.minute, fingerprint);
            bucket.has_incomplete_source =
                bucket.has_incomplete_source || !bar.is_complete || bar.has_conflict;
            bucket.has_non_tradable_source = bucket.has_non_tradable_source ||
                                             !bar.strategy_eligible || bar.is_recovery_replay ||
                                             !bar.volume_complete;
            MergeBarIntoBucket(bar, &bucket);
        } else {
            if (bucket_minute < bucket.bucket_minute) {
                continue;
            }
            emissions.push_back(FinalizeBucket(&bucket));
            finalized_bucket_keys_.insert(key + "|" + bucket.bucket_minute);
            bucket = Bucket{};
            bucket.initialized = true;
            bucket.timeframe_minutes = timeframe;
            bucket.bucket_minute = bucket_minute;
            bucket.period_end_ts_ns = ResolveBucketPeriodEnd(bar, timeframe);
            bucket.has_incomplete_source = !bar.is_complete || bar.has_conflict;
            bucket.has_non_tradable_source =
                !bar.strategy_eligible || bar.is_recovery_replay || !bar.volume_complete;
            bucket.source_fingerprint_by_minute.emplace(bar.minute, BarFingerprint(bar));
            bucket.bar = bar;
            bucket.bar.minute = bucket_minute;
        }

        if (IsTerminalSlot(bar.minute, timeframe)) {
            emissions.push_back(FinalizeBucket(&bucket));
            finalized_bucket_keys_.insert(finalized_key);
            buckets_.erase(key);
        }
    }
    return emissions;
}

std::vector<TimeframeStateEmission> TimeframeStateFanout::AdvanceWatermark(
    EpochNanos watermark_ts_ns) {
    std::vector<TimeframeStateEmission> emissions;
    for (auto it = buckets_.begin(); it != buckets_.end();) {
        if (!it->second.initialized || it->second.period_end_ts_ns <= 0 ||
            it->second.period_end_ts_ns > watermark_ts_ns) {
            ++it;
            continue;
        }
        const std::string finalized_key = it->first + "|" + it->second.bucket_minute;
        it->second.bar.finalized_ts_ns = std::max(it->second.bar.finalized_ts_ns, watermark_ts_ns);
        emissions.push_back(FinalizeBucket(&it->second));
        finalized_bucket_keys_.insert(finalized_key);
        it = buckets_.erase(it);
    }
    std::sort(emissions.begin(), emissions.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.bar.period_end_ts_ns != rhs.bar.period_end_ts_ns) {
            return lhs.bar.period_end_ts_ns < rhs.bar.period_end_ts_ns;
        }
        if (lhs.bar.instrument_id != rhs.bar.instrument_id) {
            return lhs.bar.instrument_id < rhs.bar.instrument_id;
        }
        return lhs.timeframe_minutes < rhs.timeframe_minutes;
    });
    return emissions;
}

std::vector<TimeframeStateEmission> TimeframeStateFanout::Flush() {
    std::vector<TimeframeStateEmission> emissions;
    emissions.reserve(buckets_.size());
    for (const auto& [key, bucket] : buckets_) {
        (void)key;
        if (bucket.initialized) {
            Bucket pending = bucket;
            emissions.push_back(FinalizeBucket(&pending));
        }
    }
    buckets_.clear();
    std::sort(emissions.begin(), emissions.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.bar.instrument_id != rhs.bar.instrument_id) {
            return lhs.bar.instrument_id < rhs.bar.instrument_id;
        }
        if (lhs.timeframe_minutes != rhs.timeframe_minutes) {
            return lhs.timeframe_minutes < rhs.timeframe_minutes;
        }
        return lhs.bar.minute < rhs.bar.minute;
    });
    return emissions;
}

void TimeframeStateFanout::DiscardPending() { buckets_.clear(); }

std::vector<TimeframeStateEmission> TimeframeStateFanout::FlushInstrument(
    const std::string& instrument_id) {
    if (instrument_id.empty()) {
        return {};
    }
    std::vector<TimeframeStateEmission> emissions;
    const std::string prefix = instrument_id + "|";
    for (auto it = buckets_.begin(); it != buckets_.end();) {
        if (it->first.rfind(prefix, 0) == 0 && it->second.initialized) {
            const std::string finalized_key = it->first + "|" + it->second.bucket_minute;
            emissions.push_back(FinalizeBucket(&it->second));
            finalized_bucket_keys_.insert(finalized_key);
            it = buckets_.erase(it);
        } else {
            ++it;
        }
    }
    std::sort(emissions.begin(), emissions.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.timeframe_minutes != rhs.timeframe_minutes) {
            return lhs.timeframe_minutes < rhs.timeframe_minutes;
        }
        return lhs.bar.minute < rhs.bar.minute;
    });
    return emissions;
}

void TimeframeStateFanout::ResetInstrument(const std::string& instrument_id) {
    if (instrument_id.empty()) {
        return;
    }
    const std::string prefix = instrument_id + "|";
    for (auto it = buckets_.begin(); it != buckets_.end();) {
        if (it->first.rfind(prefix, 0) == 0) {
            it = buckets_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = detectors_.begin(); it != detectors_.end();) {
        if (it->first.rfind(prefix, 0) == 0) {
            it = detectors_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = finalized_bucket_keys_.begin(); it != finalized_bucket_keys_.end();) {
        if (it->rfind(prefix, 0) == 0) {
            it = finalized_bucket_keys_.erase(it);
        } else {
            ++it;
        }
    }
}

void TimeframeStateFanout::ResetInstrumentBuckets(const std::string& instrument_id) {
    if (instrument_id.empty()) {
        return;
    }
    const std::string prefix = instrument_id + "|";
    for (auto it = buckets_.begin(); it != buckets_.end();) {
        if (it->first.rfind(prefix, 0) == 0) {
            it = buckets_.erase(it);
        } else {
            ++it;
        }
    }
}

bool TimeframeStateFanout::SaveState(PersistenceState* out, std::string* error) const {
    if (out == nullptr) {
        SetError(error, "timeframe fanout state output is null");
        return false;
    }
    out->clear();
    (*out)["version"] = "3";
    (*out)["timeframes.count"] = std::to_string(timeframes_.size());
    for (std::size_t i = 0; i < timeframes_.size(); ++i) {
        (*out)["timeframes." + std::to_string(i)] = std::to_string(timeframes_[i]);
    }

    (*out)["buckets.count"] = std::to_string(buckets_.size());
    std::size_t bucket_index = 0;
    for (const auto& [key, bucket] : buckets_) {
        const std::string prefix = "buckets." + std::to_string(bucket_index);
        (*out)[prefix + ".key"] = key;
        (*out)[prefix + ".initialized"] = FormatBool(bucket.initialized);
        (*out)[prefix + ".timeframe_minutes"] = std::to_string(bucket.timeframe_minutes);
        (*out)[prefix + ".bucket_minute"] = bucket.bucket_minute;
        (*out)[prefix + ".period_end_ts_ns"] = std::to_string(bucket.period_end_ts_ns);
        (*out)[prefix + ".conflicting_duplicate"] = FormatBool(bucket.conflicting_duplicate);
        (*out)[prefix + ".has_incomplete_source"] = FormatBool(bucket.has_incomplete_source);
        (*out)[prefix + ".has_non_tradable_source"] = FormatBool(bucket.has_non_tradable_source);
        (*out)[prefix + ".sources.count"] =
            std::to_string(bucket.source_fingerprint_by_minute.size());
        std::size_t source_index = 0;
        for (const auto& [minute, fingerprint] : bucket.source_fingerprint_by_minute) {
            const std::string source_prefix = prefix + ".sources." + std::to_string(source_index++);
            (*out)[source_prefix + ".minute"] = minute;
            (*out)[source_prefix + ".fingerprint"] = fingerprint;
        }
        WriteBar(out, prefix + ".bar", bucket.bar);
        ++bucket_index;
    }

    (*out)["detectors.count"] = std::to_string(detectors_.size());
    std::size_t detector_index = 0;
    for (const auto& [key, detector] : detectors_) {
        const std::string prefix = "detectors." + std::to_string(detector_index);
        (*out)[prefix + ".key"] = key;
        WriteDetectorState(out, prefix + ".state", detector.ExportState());
        ++detector_index;
    }
    (*out)["finalized.count"] = std::to_string(finalized_bucket_keys_.size());
    std::size_t finalized_index = 0;
    for (const auto& key : finalized_bucket_keys_) {
        (*out)["finalized." + std::to_string(finalized_index++)] = key;
    }
    return true;
}

bool TimeframeStateFanout::LoadState(const PersistenceState& state, std::string* error) {
    int state_version = 1;
    if (const auto version_it = state.find("version");
        version_it != state.end() && !ParseInt(version_it->second, &state_version)) {
        SetError(error, "invalid timeframe fanout state version");
        return false;
    }
    const std::vector<std::int32_t> configured_timeframes = timeframes_;
    std::size_t timeframe_count = 0;
    if (!ReadSize(state, "timeframes.count", &timeframe_count, error)) {
        return false;
    }
    std::vector<std::int32_t> loaded_timeframes;
    loaded_timeframes.reserve(timeframe_count);
    for (std::size_t i = 0; i < timeframe_count; ++i) {
        int timeframe = 0;
        if (!ReadInt(state, "timeframes." + std::to_string(i), &timeframe, error)) {
            return false;
        }
        if (timeframe > 1) {
            loaded_timeframes.push_back(timeframe);
        }
    }
    std::sort(loaded_timeframes.begin(), loaded_timeframes.end());
    loaded_timeframes.erase(std::unique(loaded_timeframes.begin(), loaded_timeframes.end()),
                            loaded_timeframes.end());
    auto is_allowed_timeframe = [&](std::int32_t timeframe_minutes) {
        return configured_timeframes.empty() ||
               std::find(configured_timeframes.begin(), configured_timeframes.end(),
                         timeframe_minutes) != configured_timeframes.end();
    };

    std::unordered_map<std::string, Bucket> loaded_buckets;
    std::size_t bucket_count = 0;
    if (!ReadSize(state, "buckets.count", &bucket_count, error)) {
        return false;
    }
    for (std::size_t i = 0; i < bucket_count; ++i) {
        const std::string prefix = "buckets." + std::to_string(i);
        const std::string* key = RequireValue(state, prefix + ".key", error);
        if (key == nullptr || key->empty()) {
            return false;
        }
        Bucket bucket;
        if (!ReadBool(state, prefix + ".initialized", &bucket.initialized, error) ||
            !ReadInt(state, prefix + ".timeframe_minutes", &bucket.timeframe_minutes, error)) {
            return false;
        }
        const std::string* bucket_minute = RequireValue(state, prefix + ".bucket_minute", error);
        if (bucket_minute == nullptr) {
            return false;
        }
        bucket.bucket_minute = *bucket_minute;
        if (!ReadBar(state, prefix + ".bar", &bucket.bar, error)) {
            return false;
        }
        if (state_version >= 2) {
            if (!ReadInt64(state, prefix + ".period_end_ts_ns", &bucket.period_end_ts_ns, error) ||
                !ReadBool(state, prefix + ".conflicting_duplicate", &bucket.conflicting_duplicate,
                          error)) {
                return false;
            }
            if (state_version >= 3) {
                if (!ReadBool(state, prefix + ".has_incomplete_source",
                              &bucket.has_incomplete_source, error) ||
                    !ReadBool(state, prefix + ".has_non_tradable_source",
                              &bucket.has_non_tradable_source, error)) {
                    return false;
                }
            } else {
                if (!ReadBool(state, prefix + ".has_non_tradable_source",
                              &bucket.has_non_tradable_source, error)) {
                    return false;
                }
                bucket.has_incomplete_source = bucket.has_non_tradable_source;
            }
            std::size_t source_count = 0;
            if (!ReadSize(state, prefix + ".sources.count", &source_count, error)) {
                return false;
            }
            for (std::size_t source_index = 0; source_index < source_count; ++source_index) {
                const std::string source_prefix =
                    prefix + ".sources." + std::to_string(source_index);
                const std::string* minute = RequireValue(state, source_prefix + ".minute", error);
                const std::string* fingerprint =
                    RequireValue(state, source_prefix + ".fingerprint", error);
                if (minute == nullptr || fingerprint == nullptr) {
                    return false;
                }
                bucket.source_fingerprint_by_minute[*minute] = *fingerprint;
            }
        } else {
            bucket.period_end_ts_ns = ResolveBucketPeriodEnd(bucket.bar, bucket.timeframe_minutes);
            bucket.source_fingerprint_by_minute.emplace(bucket.bar.minute,
                                                        BarFingerprint(bucket.bar));
            bucket.has_non_tradable_source =
                !bucket.bar.is_complete || !bucket.bar.strategy_eligible;
            bucket.has_incomplete_source = !bucket.bar.is_complete;
        }
        if (!is_allowed_timeframe(bucket.timeframe_minutes)) {
            continue;
        }
        loaded_buckets[*key] = std::move(bucket);
    }

    std::unordered_map<std::string, MarketStateDetector> loaded_detectors;
    std::size_t detector_count = 0;
    if (!ReadSize(state, "detectors.count", &detector_count, error)) {
        return false;
    }
    for (std::size_t i = 0; i < detector_count; ++i) {
        const std::string prefix = "detectors." + std::to_string(i);
        const std::string* key = RequireValue(state, prefix + ".key", error);
        if (key == nullptr || key->empty()) {
            return false;
        }
        const std::size_t split = key->rfind('|');
        int detector_timeframe = 0;
        if (split == std::string::npos || !ParseInt(key->substr(split + 1), &detector_timeframe) ||
            !is_allowed_timeframe(detector_timeframe)) {
            continue;
        }
        MarketStateDetector::State detector_state;
        if (!ReadDetectorState(state, prefix + ".state", &detector_state, error)) {
            return false;
        }
        const std::string instrument_id = key->substr(0, split);
        const MarketStateDetectorConfig& detector_config = ResolveMarketStateDetectorConfig(
            instrument_id, detector_config_, detector_config_by_product_);
        MarketStateDetector detector(detector_config);
        if (!detector.ImportState(detector_state)) {
            SetError(error, "failed to import detector state for key: " + *key);
            return false;
        }
        loaded_detectors.emplace(*key, std::move(detector));
    }

    timeframes_ =
        configured_timeframes.empty() ? std::move(loaded_timeframes) : configured_timeframes;
    buckets_ = std::move(loaded_buckets);
    detectors_ = std::move(loaded_detectors);
    finalized_bucket_keys_.clear();
    if (state_version >= 2) {
        std::size_t finalized_count = 0;
        if (!ReadSize(state, "finalized.count", &finalized_count, error)) {
            return false;
        }
        for (std::size_t i = 0; i < finalized_count; ++i) {
            const std::string* key = RequireValue(state, "finalized." + std::to_string(i), error);
            if (key == nullptr) {
                return false;
            }
            finalized_bucket_keys_.insert(*key);
        }
    }
    return true;
}

bool TimeframeStateFanout::ParseMinuteValue(const std::string& minute_key, std::string* trading_day,
                                            int* minute_of_day) {
    if (trading_day == nullptr || minute_of_day == nullptr || minute_key.size() < 14 ||
        minute_key[8] != ' ' || minute_key[11] != ':') {
        return false;
    }
    for (int i = 0; i < 8; ++i) {
        if (minute_key[i] < '0' || minute_key[i] > '9') {
            return false;
        }
    }
    if (minute_key[9] < '0' || minute_key[9] > '9' || minute_key[10] < '0' ||
        minute_key[10] > '9' || minute_key[12] < '0' || minute_key[12] > '9' ||
        minute_key[13] < '0' || minute_key[13] > '9') {
        return false;
    }
    const int hour = (minute_key[9] - '0') * 10 + (minute_key[10] - '0');
    const int minute = (minute_key[12] - '0') * 10 + (minute_key[13] - '0');
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        return false;
    }
    *trading_day = minute_key.substr(0, 8);
    *minute_of_day = hour * 60 + minute;
    return true;
}

std::string TimeframeStateFanout::FormatMinuteValue(const std::string& trading_day,
                                                    int minute_of_day) {
    if (trading_day.size() != 8 || minute_of_day < 0 || minute_of_day >= 24 * 60) {
        return "";
    }
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%s %02d:%02d", trading_day.c_str(), minute_of_day / 60,
                  minute_of_day % 60);
    return std::string(buffer);
}

std::string TimeframeStateFanout::BuildKey(const std::string& instrument_id,
                                           std::int32_t timeframe_minutes) {
    return instrument_id + "|" + std::to_string(timeframe_minutes);
}

std::string TimeframeStateFanout::BuildBucketMinute(const std::string& minute_key,
                                                    std::int32_t timeframe_minutes) {
    std::string trading_day;
    int minute_of_day = 0;
    if (timeframe_minutes <= 1 || !ParseMinuteValue(minute_key, &trading_day, &minute_of_day)) {
        return "";
    }
    const int bucket_start = (minute_of_day / timeframe_minutes) * timeframe_minutes;
    return FormatMinuteValue(trading_day, bucket_start);
}

EpochNanos TimeframeStateFanout::ResolveBucketPeriodEnd(const BarSnapshot& bar,
                                                        std::int32_t timeframe_minutes) {
    std::string trading_day;
    int minute_of_day = 0;
    if (timeframe_minutes <= 1 || !ParseMinuteValue(bar.minute, &trading_day, &minute_of_day)) {
        return 0;
    }
    const int slot = minute_of_day % timeframe_minutes;
    if (bar.period_end_ts_ns > 0) {
        return bar.period_end_ts_ns +
               static_cast<EpochNanos>(timeframe_minutes - slot - 1) * kNanosPerMinute;
    }

    const std::string physical_day = bar.action_day.empty() ? trading_day : bar.action_day;
    if (physical_day.size() != 8) {
        return 0;
    }
    std::tm local_tm{};
    try {
        local_tm.tm_year = std::stoi(physical_day.substr(0, 4)) - 1900;
        local_tm.tm_mon = std::stoi(physical_day.substr(4, 2)) - 1;
        local_tm.tm_mday = std::stoi(physical_day.substr(6, 2));
    } catch (const std::exception&) {
        return 0;
    }
    const int bucket_start = (minute_of_day / timeframe_minutes) * timeframe_minutes;
    local_tm.tm_hour = bucket_start / 60;
    local_tm.tm_min = bucket_start % 60;
    const std::time_t local_as_utc = timegm(&local_tm);
    if (local_as_utc <= 0) {
        return 0;
    }
    return (static_cast<EpochNanos>(local_as_utc) - kShanghaiUtcOffsetSeconds) * kNanosPerSecond +
           static_cast<EpochNanos>(timeframe_minutes) * kNanosPerMinute;
}

std::string TimeframeStateFanout::BarFingerprint(const BarSnapshot& bar) {
    std::ostringstream out;
    out.precision(17);
    out << bar.instrument_id << '|' << bar.minute << '|' << bar.open << '|' << bar.high << '|'
        << bar.low << '|' << bar.close << '|' << bar.analysis_open << '|' << bar.analysis_high
        << '|' << bar.analysis_low << '|' << bar.analysis_close << '|' << bar.analysis_price_offset
        << '|' << bar.volume << '|' << bar.ts_ns << '|' << bar.is_complete << '|'
        << bar.is_session_endpoint << '|' << bar.strategy_eligible << '|' << bar.volume_complete
        << '|' << bar.has_conflict << '|' << bar.is_recovery_replay;
    return out.str();
}

bool TimeframeStateFanout::IsTerminalSlot(const std::string& minute_key,
                                          std::int32_t timeframe_minutes) {
    std::string trading_day;
    int minute_of_day = 0;
    return timeframe_minutes > 1 && ParseMinuteValue(minute_key, &trading_day, &minute_of_day) &&
           minute_of_day % timeframe_minutes == timeframe_minutes - 1;
}

void TimeframeStateFanout::MergeBarIntoBucket(const BarSnapshot& bar, Bucket* bucket) {
    if (bucket == nullptr || !bucket->initialized) {
        return;
    }
    bucket->bar.high = std::max(bucket->bar.high, bar.high);
    bucket->bar.low = std::min(bucket->bar.low, bar.low);
    bucket->bar.close = bar.close;
    bucket->bar.analysis_high = std::max(bucket->bar.analysis_high, bar.analysis_high);
    bucket->bar.analysis_low = std::min(bucket->bar.analysis_low, bar.analysis_low);
    bucket->bar.analysis_close = bar.analysis_close;
    bucket->bar.analysis_price_offset = bar.analysis_price_offset;
    bucket->bar.volume += bar.volume;
    bucket->bar.ts_ns = std::max(bucket->bar.ts_ns, bar.ts_ns);
    bucket->bar.finalized_ts_ns = std::max(bucket->bar.finalized_ts_ns, bar.finalized_ts_ns);
    bucket->bar.volume_complete = bucket->bar.volume_complete && bar.volume_complete;
    bucket->bar.has_conflict = bucket->bar.has_conflict || bar.has_conflict;
    bucket->bar.is_recovery_replay = bucket->bar.is_recovery_replay || bar.is_recovery_replay;
    if (!bar.action_day.empty()) {
        bucket->bar.action_day = bar.action_day;
    }
}

TimeframeStateEmission TimeframeStateFanout::BuildEmission(const BarSnapshot& bar,
                                                           std::int32_t timeframe_minutes,
                                                           bool strategy_eligible) {
    MarketStateDetector& detector = DetectorFor(bar.instrument_id, timeframe_minutes);
    if (strategy_eligible) {
        detector.Update(bar.analysis_high, bar.analysis_low, bar.analysis_close);
    }

    StateSnapshot7D state;
    state.instrument_id = bar.instrument_id;
    state.timeframe_minutes = timeframe_minutes;
    state.bar_open = bar.open;
    state.bar_high = bar.high;
    state.bar_low = bar.low;
    state.bar_close = bar.close;
    state.analysis_bar_open = bar.analysis_open;
    state.analysis_bar_high = bar.analysis_high;
    state.analysis_bar_low = bar.analysis_low;
    state.analysis_bar_close = bar.analysis_close;
    state.analysis_price_offset = bar.analysis_price_offset;
    state.bar_volume = static_cast<double>(bar.volume);
    state.has_bar = strategy_eligible;
    PopulateMarketStateDiagnostics(detector, &state);
    state.ts_ns = bar.ts_ns;

    TimeframeStateEmission emission;
    emission.timeframe_minutes = timeframe_minutes;
    emission.bar = bar;
    emission.state = state;
    emission.strategy_eligible = strategy_eligible;
    return emission;
}

TimeframeStateEmission TimeframeStateFanout::FinalizeBucket(Bucket* bucket) {
    if (bucket == nullptr || !bucket->initialized) {
        return {};
    }
    BarSnapshot bar = bucket->bar;
    bar.period_end_ts_ns = bucket->period_end_ts_ns;
    bar.expected_source_bars = bucket->timeframe_minutes;
    bar.observed_source_bars =
        static_cast<std::int32_t>(bucket->source_fingerprint_by_minute.size());
    bar.has_conflict = bar.has_conflict || bucket->conflicting_duplicate;
    bar.is_complete = !bar.has_conflict && !bucket->has_incomplete_source &&
                      bar.observed_source_bars == bar.expected_source_bars;
    bar.strategy_eligible = bar.is_complete && !bucket->has_non_tradable_source &&
                            !bar.is_session_endpoint && !bar.is_recovery_replay &&
                            bar.volume_complete;
    return BuildEmission(bar, bucket->timeframe_minutes, bar.strategy_eligible);
}

MarketStateDetector& TimeframeStateFanout::DetectorFor(const std::string& instrument_id,
                                                       std::int32_t timeframe_minutes) {
    const std::string key = BuildKey(instrument_id, timeframe_minutes);
    auto it = detectors_.find(key);
    if (it != detectors_.end()) {
        return it->second;
    }
    const MarketStateDetectorConfig& detector_config = ResolveMarketStateDetectorConfig(
        instrument_id, detector_config_, detector_config_by_product_);
    auto [inserted, _] = detectors_.emplace(key, MarketStateDetector(detector_config));
    return inserted->second;
}

}  // namespace quant_hft
