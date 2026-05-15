#include "quant_hft/services/timeframe_state_fanout.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace quant_hft {
namespace {

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
    return ReadDouble(state, prefix + ".open", &bar->open, error) &&
           ReadDouble(state, prefix + ".high", &bar->high, error) &&
           ReadDouble(state, prefix + ".low", &bar->low, error) &&
           ReadDouble(state, prefix + ".close", &bar->close, error) &&
           ReadDouble(state, prefix + ".analysis_open", &bar->analysis_open, error) &&
           ReadDouble(state, prefix + ".analysis_high", &bar->analysis_high, error) &&
           ReadDouble(state, prefix + ".analysis_low", &bar->analysis_low, error) &&
           ReadDouble(state, prefix + ".analysis_close", &bar->analysis_close, error) &&
           ReadDouble(state, prefix + ".analysis_price_offset", &bar->analysis_price_offset,
                      error) &&
           ReadInt64(state, prefix + ".volume", &bar->volume, error) &&
           ReadInt64(state, prefix + ".ts_ns", &bar->ts_ns, error);
}

}  // namespace

TimeframeStateFanout::TimeframeStateFanout(std::vector<std::int32_t> timeframes,
                                           MarketStateDetectorConfig detector_config)
    : detector_config_(detector_config) {
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
        auto& bucket = buckets_[key];
        if (!bucket.initialized) {
            bucket.initialized = true;
            bucket.timeframe_minutes = timeframe;
            bucket.bucket_minute = bucket_minute;
            bucket.bar = bar;
            bucket.bar.minute = bucket_minute;
            continue;
        }
        if (bucket.bucket_minute == bucket_minute) {
            MergeBarIntoBucket(bar, &bucket);
            continue;
        }

        emissions.push_back(BuildEmission(bucket.bar, timeframe));
        bucket.initialized = true;
        bucket.timeframe_minutes = timeframe;
        bucket.bucket_minute = bucket_minute;
        bucket.bar = bar;
        bucket.bar.minute = bucket_minute;
    }
    return emissions;
}

std::vector<TimeframeStateEmission> TimeframeStateFanout::Flush() {
    std::vector<TimeframeStateEmission> emissions;
    emissions.reserve(buckets_.size());
    for (const auto& [key, bucket] : buckets_) {
        (void)key;
        if (bucket.initialized) {
            emissions.push_back(BuildEmission(bucket.bar, bucket.timeframe_minutes));
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
}

bool TimeframeStateFanout::SaveState(PersistenceState* out, std::string* error) const {
    if (out == nullptr) {
        SetError(error, "timeframe fanout state output is null");
        return false;
    }
    out->clear();
    (*out)["version"] = "1";
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
    return true;
}

bool TimeframeStateFanout::LoadState(const PersistenceState& state, std::string* error) {
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
        MarketStateDetector detector(detector_config_);
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
    if (!bar.action_day.empty()) {
        bucket->bar.action_day = bar.action_day;
    }
}

TimeframeStateEmission TimeframeStateFanout::BuildEmission(const BarSnapshot& bar,
                                                           std::int32_t timeframe_minutes) {
    const std::string key = BuildKey(bar.instrument_id, timeframe_minutes);
    MarketStateDetector& detector = DetectorFor(key);
    detector.Update(bar.analysis_high, bar.analysis_low, bar.analysis_close);

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
    state.has_bar = true;
    state.market_regime = detector.GetRegime();
    state.ts_ns = bar.ts_ns;

    TimeframeStateEmission emission;
    emission.timeframe_minutes = timeframe_minutes;
    emission.bar = bar;
    emission.state = state;
    return emission;
}

MarketStateDetector& TimeframeStateFanout::DetectorFor(const std::string& key) {
    auto it = detectors_.find(key);
    if (it != detectors_.end()) {
        return it->second;
    }
    auto [inserted, _] = detectors_.emplace(key, MarketStateDetector(detector_config_));
    return inserted->second;
}

}  // namespace quant_hft
