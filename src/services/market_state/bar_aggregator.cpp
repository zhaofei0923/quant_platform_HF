#include "quant_hft/services/bar_aggregator.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>

namespace quant_hft {
namespace {

bool IsFinitePositive(double value) {
    return std::isfinite(value) && value > 0.0;
}

std::string InferExchangeId(const std::string& instrument_id) {
    const auto dot_pos = instrument_id.find('.');
    if (dot_pos == std::string::npos || dot_pos == 0) {
        return "";
    }
    return instrument_id.substr(0, dot_pos);
}

bool ParseMinuteValue(const std::string& minute_key,
                      std::string* trading_day,
                      int* minute_of_day) {
    if (trading_day == nullptr || minute_of_day == nullptr || minute_key.size() < 14) {
        return false;
    }
    if (minute_key[8] != ' ' || minute_key[11] != ':') {
        return false;
    }
    for (int i = 0; i < 8; ++i) {
        if (!std::isdigit(static_cast<unsigned char>(minute_key[i]))) {
            return false;
        }
    }
    if (!std::isdigit(static_cast<unsigned char>(minute_key[9])) ||
        !std::isdigit(static_cast<unsigned char>(minute_key[10])) ||
        !std::isdigit(static_cast<unsigned char>(minute_key[12])) ||
        !std::isdigit(static_cast<unsigned char>(minute_key[13]))) {
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

std::string FormatMinuteValue(const std::string& trading_day, int minute_of_day) {
    if (trading_day.size() != 8 || minute_of_day < 0 || minute_of_day >= 24 * 60) {
        return "";
    }
    const int hour = minute_of_day / 60;
    const int minute = minute_of_day % 60;
    char buffer[32];
    std::snprintf(buffer,
                  sizeof(buffer),
                  "%s %02d:%02d",
                  trading_day.c_str(),
                  hour,
                  minute);
    return std::string(buffer);
}

}  // namespace

BarAggregator::BarAggregator(BarAggregatorConfig config)
    : config_(config) {}

bool BarAggregator::ShouldProcessSnapshot(const MarketSnapshot& snapshot) const {
    if (snapshot.instrument_id.empty() || snapshot.update_time.size() < 5 ||
        !IsFinitePositive(snapshot.last_price)) {
        return false;
    }

    const auto trading_day = ResolveTradingDay(snapshot);
    if (trading_day.empty()) {
        return false;
    }

    if (config_.filter_non_trading_ticks && !IsTradingSessionTime(snapshot.update_time)) {
        return false;
    }

    return true;
}

std::vector<BarSnapshot> BarAggregator::OnMarketSnapshot(const MarketSnapshot& snapshot) {
    std::vector<BarSnapshot> emitted;
    if (!ShouldProcessSnapshot(snapshot)) {
        return emitted;
    }

    const auto exchange_id = ResolveExchangeId(snapshot);
    const auto trading_day = ResolveTradingDay(snapshot);
    const auto action_day = ResolveActionDay(snapshot);
    const auto minute_key = BuildMinuteKey(trading_day, snapshot.update_time);
    if (minute_key.empty()) {
        return emitted;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto& bucket = buckets_[snapshot.instrument_id];
    if (!bucket.initialized) {
        ResetBucketLocked(
            &bucket, snapshot, exchange_id, trading_day, action_day, minute_key);
        return emitted;
    }

    const auto normalized_volume = std::max<std::int64_t>(0, snapshot.volume);
    const auto volume_delta =
        normalized_volume >= bucket.last_cumulative_volume
            ? normalized_volume - bucket.last_cumulative_volume
            : 0;
    bucket.last_cumulative_volume = normalized_volume;

    if (bucket.minute_key != minute_key) {
        emitted.push_back(bucket.bar);
        ResetBucketLocked(
            &bucket, snapshot, exchange_id, trading_day, action_day, minute_key);
        return emitted;
    }

    bucket.bar.exchange_id = exchange_id.empty() ? bucket.bar.exchange_id : exchange_id;
    bucket.bar.trading_day = trading_day;
    bucket.bar.action_day = action_day;
    bucket.bar.high = std::max(bucket.bar.high, snapshot.last_price);
    bucket.bar.low = std::min(bucket.bar.low, snapshot.last_price);
    bucket.bar.close = snapshot.last_price;
    bucket.bar.volume += volume_delta;
    bucket.bar.ts_ns = ResolveTimestamp(snapshot);
    return emitted;
}

std::vector<BarSnapshot> BarAggregator::Flush() {
    std::vector<BarSnapshot> bars;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        bars.reserve(buckets_.size());
        for (const auto& entry : buckets_) {
            if (entry.second.initialized) {
                bars.push_back(entry.second.bar);
            }
        }
        buckets_.clear();
    }
    std::sort(
        bars.begin(),
        bars.end(),
        [](const BarSnapshot& lhs, const BarSnapshot& rhs) {
            if (lhs.instrument_id != rhs.instrument_id) {
                return lhs.instrument_id < rhs.instrument_id;
            }
            return lhs.minute < rhs.minute;
        });
    return bars;
}

std::vector<BarSnapshot> BarAggregator::AggregateFromOneMinute(
    const std::vector<BarSnapshot>& one_minute_bars,
    std::int32_t timeframe_minutes) {
    if (timeframe_minutes <= 0 || one_minute_bars.empty()) {
        return {};
    }

    std::vector<BarSnapshot> sorted = one_minute_bars;
    std::sort(
        sorted.begin(),
        sorted.end(),
        [](const BarSnapshot& lhs, const BarSnapshot& rhs) {
            if (lhs.instrument_id != rhs.instrument_id) {
                return lhs.instrument_id < rhs.instrument_id;
            }
            return lhs.minute < rhs.minute;
        });

    if (timeframe_minutes == 1) {
        return sorted;
    }

    std::vector<BarSnapshot> aggregated;
    std::string active_key;
    BarSnapshot active_bar;
    bool has_active = false;

    for (const auto& bar : sorted) {
        std::string trading_day;
        int minute_of_day = 0;
        if (!ParseMinuteValue(bar.minute, &trading_day, &minute_of_day)) {
            continue;
        }

        const int bucket_start = (minute_of_day / timeframe_minutes) * timeframe_minutes;
        const std::string bucket_minute = FormatMinuteValue(trading_day, bucket_start);
        if (bucket_minute.empty()) {
            continue;
        }
        const std::string key = bar.instrument_id + "|" + bucket_minute;

        if (!has_active || key != active_key) {
            if (has_active) {
                aggregated.push_back(active_bar);
            }
            active_bar = bar;
            active_bar.minute = bucket_minute;
            active_key = key;
            has_active = true;
            continue;
        }

        active_bar.high = std::max(active_bar.high, bar.high);
        active_bar.low = std::min(active_bar.low, bar.low);
        active_bar.close = bar.close;
        active_bar.volume += bar.volume;
        active_bar.ts_ns = std::max(active_bar.ts_ns, bar.ts_ns);
        if (!bar.action_day.empty()) {
            active_bar.action_day = bar.action_day;
        }
    }

    if (has_active) {
        aggregated.push_back(active_bar);
    }
    return aggregated;
}

bool BarAggregator::IsTradingSessionTime(const std::string& update_time) {
    int hhmm = 0;
    if (!ParseHhmm(update_time, &hhmm)) {
        return false;
    }
    const bool day_session = hhmm >= 900 && hhmm <= 1515;
    const bool night_session = hhmm >= 2100 || hhmm <= 230;
    return day_session || night_session;
}

bool BarAggregator::ParseHhmm(const std::string& update_time, int* hhmm) {
    if (hhmm == nullptr || update_time.size() < 5) {
        return false;
    }
    if (!std::isdigit(static_cast<unsigned char>(update_time[0])) ||
        !std::isdigit(static_cast<unsigned char>(update_time[1])) || update_time[2] != ':' ||
        !std::isdigit(static_cast<unsigned char>(update_time[3])) ||
        !std::isdigit(static_cast<unsigned char>(update_time[4]))) {
        return false;
    }
    const int hour = (update_time[0] - '0') * 10 + (update_time[1] - '0');
    const int minute = (update_time[3] - '0') * 10 + (update_time[4] - '0');
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        return false;
    }
    *hhmm = hour * 100 + minute;
    return true;
}

std::string BarAggregator::ResolveExchangeId(const MarketSnapshot& snapshot) {
    if (!snapshot.exchange_id.empty()) {
        return snapshot.exchange_id;
    }
    return InferExchangeId(snapshot.instrument_id);
}

std::string BarAggregator::ResolveTradingDay(const MarketSnapshot& snapshot) {
    if (!snapshot.trading_day.empty()) {
        return snapshot.trading_day;
    }
    return snapshot.action_day;
}

std::string BarAggregator::ResolveActionDay(const MarketSnapshot& snapshot) {
    if (!snapshot.action_day.empty()) {
        return snapshot.action_day;
    }
    return snapshot.trading_day;
}

std::string BarAggregator::BuildMinuteKey(const std::string& trading_day,
                                          const std::string& update_time) {
    if (trading_day.empty() || update_time.size() < 5) {
        return "";
    }
    return trading_day + " " + update_time.substr(0, 5);
}

EpochNanos BarAggregator::ResolveTimestamp(const MarketSnapshot& snapshot) {
    if (snapshot.recv_ts_ns > 0) {
        return snapshot.recv_ts_ns;
    }
    if (snapshot.exchange_ts_ns > 0) {
        return snapshot.exchange_ts_ns;
    }
    return NowEpochNanos();
}

void BarAggregator::ResetBucketLocked(MinuteBucket* bucket,
                                      const MarketSnapshot& snapshot,
                                      const std::string& exchange_id,
                                      const std::string& trading_day,
                                      const std::string& action_day,
                                      const std::string& minute_key) {
    bucket->initialized = true;
    bucket->minute_key = minute_key;
    bucket->last_cumulative_volume = std::max<std::int64_t>(0, snapshot.volume);
    bucket->bar.instrument_id = snapshot.instrument_id;
    bucket->bar.exchange_id = exchange_id;
    bucket->bar.trading_day = trading_day;
    bucket->bar.action_day = action_day;
    bucket->bar.minute = minute_key;
    bucket->bar.open = snapshot.last_price;
    bucket->bar.high = snapshot.last_price;
    bucket->bar.low = snapshot.last_price;
    bucket->bar.close = snapshot.last_price;
    bucket->bar.volume = 0;
    bucket->bar.ts_ns = ResolveTimestamp(snapshot);
}

}  // namespace quant_hft
