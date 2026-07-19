#include "quant_hft/services/bar_aggregator.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <limits>
#include <sstream>
#include <unordered_map>

namespace quant_hft {
namespace {

constexpr EpochNanos kNanosPerMillisecond = 1'000'000;
constexpr EpochNanos kNanosPerSecond = 1'000'000'000;
constexpr EpochNanos kNanosPerMinute = 60 * kNanosPerSecond;
constexpr std::int64_t kShanghaiUtcOffsetSeconds = 8 * 60 * 60;

void SetPersistenceError(std::string* error, const std::string& value) {
    if (error != nullptr) {
        *error = value;
    }
}

std::string FormatPersistenceDouble(double value) {
    std::ostringstream out;
    out.precision(17);
    out << value;
    return out.str();
}

const std::string* RequirePersistenceValue(const BarAggregator::PersistenceState& state,
                                           const std::string& key, std::string* error) {
    const auto it = state.find(key);
    if (it == state.end()) {
        SetPersistenceError(error, "missing bar aggregator state key: " + key);
        return nullptr;
    }
    return &it->second;
}

template <typename Integer>
bool ParsePersistenceInteger(const BarAggregator::PersistenceState& state, const std::string& key,
                             Integer* out, std::string* error) {
    const std::string* value = RequirePersistenceValue(state, key, error);
    if (value == nullptr || out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const long long parsed = std::stoll(*value, &consumed);
        if (consumed != value->size()) {
            throw std::invalid_argument("trailing characters");
        }
        *out = static_cast<Integer>(parsed);
        return true;
    } catch (...) {
        SetPersistenceError(error, "invalid integer bar aggregator state key: " + key);
        return false;
    }
}

bool ParsePersistenceUnsigned(const BarAggregator::PersistenceState& state, const std::string& key,
                              std::uint64_t* out, std::string* error) {
    const std::string* value = RequirePersistenceValue(state, key, error);
    if (value == nullptr || out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const unsigned long long parsed = std::stoull(*value, &consumed);
        if (consumed != value->size()) {
            throw std::invalid_argument("trailing characters");
        }
        *out = static_cast<std::uint64_t>(parsed);
        return true;
    } catch (...) {
        SetPersistenceError(error, "invalid unsigned bar aggregator state key: " + key);
        return false;
    }
}

bool ParsePersistenceDouble(const BarAggregator::PersistenceState& state, const std::string& key,
                            double* out, std::string* error) {
    const std::string* value = RequirePersistenceValue(state, key, error);
    if (value == nullptr || out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const double parsed = std::stod(*value, &consumed);
        if (consumed != value->size()) {
            throw std::invalid_argument("trailing characters");
        }
        *out = parsed;
        return true;
    } catch (...) {
        SetPersistenceError(error, "invalid double bar aggregator state key: " + key);
        return false;
    }
}

bool ParsePersistenceBool(const BarAggregator::PersistenceState& state, const std::string& key,
                          bool* out, std::string* error) {
    const std::string* value = RequirePersistenceValue(state, key, error);
    if (value == nullptr || out == nullptr || (*value != "0" && *value != "1")) {
        SetPersistenceError(error, "invalid bool bar aggregator state key: " + key);
        return false;
    }
    *out = *value == "1";
    return true;
}

bool IsFinitePositive(double value) { return std::isfinite(value) && value > 0.0; }

std::string InferExchangeIdFromDottedInstrument(const std::string& instrument_id) {
    const auto dot_pos = instrument_id.find('.');
    if (dot_pos == std::string::npos || dot_pos == 0) {
        return "";
    }
    return instrument_id.substr(0, dot_pos);
}

std::string ExtractInstrumentSymbol(const std::string& instrument_id) {
    const auto dot_pos = instrument_id.find('.');
    if (dot_pos == std::string::npos || dot_pos + 1 >= instrument_id.size()) {
        return instrument_id;
    }
    return instrument_id.substr(dot_pos + 1);
}

std::string ToUpperAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    return value;
}

std::string Trim(const std::string& value) {
    std::size_t start = 0;
    while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start])) != 0) {
        ++start;
    }
    std::size_t end = value.size();
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
        --end;
    }
    return value.substr(start, end - start);
}

std::string RemoveInlineComment(const std::string& value) {
    bool in_quotes = false;
    for (std::size_t i = 0; i < value.size(); ++i) {
        const char ch = value[i];
        if (ch == '"') {
            in_quotes = !in_quotes;
            continue;
        }
        if (ch == '#' && !in_quotes) {
            return Trim(value.substr(0, i));
        }
    }
    return Trim(value);
}

std::string NormalizeYamlValue(std::string value) {
    value = Trim(value);
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        value = value.substr(1, value.size() - 2);
    }
    if (value == "null" || value == "NULL" || value == "~") {
        return "";
    }
    return value;
}

bool StartsWith(const std::string& value, const std::string& prefix) {
    if (prefix.size() > value.size()) {
        return false;
    }
    return std::equal(prefix.begin(), prefix.end(), value.begin());
}

bool ParseYamlKeyValue(const std::string& line, std::string* key, std::string* value) {
    if (key == nullptr || value == nullptr) {
        return false;
    }
    const auto colon_pos = line.find(':');
    if (colon_pos == std::string::npos) {
        return false;
    }
    *key = Trim(line.substr(0, colon_pos));
    *value = NormalizeYamlValue(RemoveInlineComment(line.substr(colon_pos + 1)));
    return !key->empty();
}

bool ParseSessionTimeToMinute(const std::string& raw, int* minute_of_day) {
    if (minute_of_day == nullptr || (raw.size() != 5 && raw.size() != 8) || raw[2] != ':') {
        return false;
    }
    if (!std::isdigit(static_cast<unsigned char>(raw[0])) ||
        !std::isdigit(static_cast<unsigned char>(raw[1])) ||
        !std::isdigit(static_cast<unsigned char>(raw[3])) ||
        !std::isdigit(static_cast<unsigned char>(raw[4]))) {
        return false;
    }
    if (raw.size() == 8 && (raw[5] != ':' || !std::isdigit(static_cast<unsigned char>(raw[6])) ||
                            !std::isdigit(static_cast<unsigned char>(raw[7])))) {
        return false;
    }
    const int hour = (raw[0] - '0') * 10 + (raw[1] - '0');
    const int minute = (raw[3] - '0') * 10 + (raw[4] - '0');
    const int second = raw.size() == 8 ? (raw[6] - '0') * 10 + (raw[7] - '0') : 0;
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
        return false;
    }
    *minute_of_day = hour * 60 + minute;
    return true;
}

bool ParseSessionRange(const std::string& raw, BarAggregator::SessionInterval* interval) {
    if (interval == nullptr || raw.empty()) {
        return false;
    }
    const auto dash_pos = raw.find('-');
    if (dash_pos == std::string::npos) {
        return false;
    }
    const auto left = Trim(raw.substr(0, dash_pos));
    const auto right = Trim(raw.substr(dash_pos + 1));
    int start = 0;
    int end = 0;
    if (!ParseSessionTimeToMinute(left, &start) || !ParseSessionTimeToMinute(right, &end)) {
        return false;
    }
    interval->start_minute = start;
    interval->end_minute = end;
    return true;
}

bool ParseSessionRanges(const std::string& raw,
                        std::vector<BarAggregator::SessionInterval>* intervals) {
    if (intervals == nullptr || raw.empty()) {
        return false;
    }
    std::vector<BarAggregator::SessionInterval> parsed;
    std::size_t start = 0;
    while (start <= raw.size()) {
        const auto comma_pos = raw.find(',', start);
        const auto token = Trim(raw.substr(
            start, comma_pos == std::string::npos ? std::string::npos : comma_pos - start));
        if (!token.empty()) {
            BarAggregator::SessionInterval interval;
            if (!ParseSessionRange(token, &interval)) {
                return false;
            }
            parsed.push_back(interval);
        }
        if (comma_pos == std::string::npos) {
            break;
        }
        start = comma_pos + 1;
    }
    if (parsed.empty()) {
        return false;
    }
    intervals->insert(intervals->end(), parsed.begin(), parsed.end());
    return true;
}

bool IsMinuteInInterval(const BarAggregator::SessionInterval& interval, int minute_of_day) {
    if (interval.start_minute < interval.end_minute) {
        return minute_of_day >= interval.start_minute && minute_of_day < interval.end_minute;
    }
    if (interval.start_minute > interval.end_minute) {
        return minute_of_day >= interval.start_minute || minute_of_day < interval.end_minute;
    }
    return false;
}

int PreviousMinuteOfDay(int minute_of_day) {
    constexpr int kMinutesPerDay = 24 * 60;
    return (minute_of_day + kMinutesPerDay - 1) % kMinutesPerDay;
}

bool IsLastMinuteInInterval(const BarAggregator::SessionInterval& interval, int minute_of_day) {
    // The last in-session minute (e.g. 14:59 for a PM session ending at 15:00)
    if (IsMinuteInInterval(interval, minute_of_day) &&
        minute_of_day == PreviousMinuteOfDay(interval.end_minute)) {
        return true;
    }
    // The end_minute itself (e.g. 15:00): the exchange sends a closing tick at
    // exactly the end time. Although this minute is outside IsMinuteInInterval
    // (exclusive upper bound), it forms a real bar with closing-auction data.
    return minute_of_day == interval.end_minute;
}

bool ParseSecondOfMinute(const std::string& update_time, int* second) {
    if (second == nullptr) {
        return false;
    }
    if (update_time.size() < 8) {
        *second = 0;
        return true;
    }
    if (update_time[5] != ':' || !std::isdigit(static_cast<unsigned char>(update_time[6])) ||
        !std::isdigit(static_cast<unsigned char>(update_time[7]))) {
        return false;
    }
    *second = (update_time[6] - '0') * 10 + (update_time[7] - '0');
    return *second >= 0 && *second <= 59;
}

bool ParsePhysicalTimestamp(const std::string& action_day, const std::string& update_time,
                            std::int32_t update_millisec, EpochNanos* out) {
    if (out == nullptr || action_day.size() != 8 || update_time.size() < 5) {
        return false;
    }
    for (char ch : action_day) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) {
            return false;
        }
    }
    int hour = 0;
    int minute = 0;
    if (!std::isdigit(static_cast<unsigned char>(update_time[0])) ||
        !std::isdigit(static_cast<unsigned char>(update_time[1])) || update_time[2] != ':' ||
        !std::isdigit(static_cast<unsigned char>(update_time[3])) ||
        !std::isdigit(static_cast<unsigned char>(update_time[4]))) {
        return false;
    }
    hour = (update_time[0] - '0') * 10 + (update_time[1] - '0');
    minute = (update_time[3] - '0') * 10 + (update_time[4] - '0');
    int second = 0;
    if (!ParseSecondOfMinute(update_time, &second) || hour > 23 || minute > 59) {
        return false;
    }

    std::tm local_tm{};
    local_tm.tm_year = std::stoi(action_day.substr(0, 4)) - 1900;
    local_tm.tm_mon = std::stoi(action_day.substr(4, 2)) - 1;
    local_tm.tm_mday = std::stoi(action_day.substr(6, 2));
    local_tm.tm_hour = hour;
    local_tm.tm_min = minute;
    local_tm.tm_sec = second;
    const std::time_t local_as_utc = timegm(&local_tm);
    if (local_as_utc <= 0) {
        return false;
    }
    const auto millis = std::clamp<std::int32_t>(update_millisec, 0, 999);
    *out = (static_cast<EpochNanos>(local_as_utc) - kShanghaiUtcOffsetSeconds) * kNanosPerSecond +
           static_cast<EpochNanos>(millis) * kNanosPerMillisecond;
    return true;
}

int SessionOrderKey(const BarAggregator::SessionInterval& interval) {
    if (interval.start_minute > interval.end_minute || interval.start_minute >= 18 * 60) {
        return interval.start_minute - 24 * 60;
    }
    return interval.start_minute;
}

std::string FormatSessionKey(const BarAggregator::SessionInterval& interval) {
    return std::to_string(interval.start_minute) + "-" + std::to_string(interval.end_minute);
}

std::unordered_map<std::string, std::vector<BarAggregator::SessionRule>> BuildDefaultRules() {
    using SessionInterval = BarAggregator::SessionInterval;
    using SessionRule = BarAggregator::SessionRule;

    auto with_intervals = [](std::initializer_list<SessionInterval> ranges,
                             const std::string& instrument_prefix = "",
                             const std::string& product = "") {
        SessionRule rule;
        rule.instrument_prefix = instrument_prefix;
        rule.product = product;
        rule.intervals = ranges;
        return rule;
    };

    const SessionInterval commodity_morning_1{9 * 60, 10 * 60 + 15};
    const SessionInterval commodity_morning_2{10 * 60 + 30, 11 * 60 + 30};
    const SessionInterval commodity_afternoon{13 * 60 + 30, 15 * 60};
    const SessionInterval cffex_morning{9 * 60 + 30, 11 * 60 + 30};
    const SessionInterval cffex_afternoon{13 * 60, 15 * 60};
    const SessionInterval cffex_treasury_afternoon{13 * 60, 15 * 60 + 15};

    std::unordered_map<std::string, std::vector<SessionRule>> rules;
    rules["SHFE"].push_back(
        with_intervals({commodity_morning_1, commodity_morning_2, commodity_afternoon,
                        SessionInterval{21 * 60, 1 * 60}}));
    rules["DCE"].push_back(
        with_intervals({commodity_morning_1, commodity_morning_2, commodity_afternoon,
                        SessionInterval{21 * 60, 23 * 60}}));
    rules["CFFEX"].push_back(with_intervals({cffex_morning, cffex_afternoon}));
    rules["CFFEX"].push_back(with_intervals({cffex_morning, cffex_treasury_afternoon}, "T"));
    rules["CFFEX"].push_back(with_intervals({cffex_morning, cffex_treasury_afternoon}, "TF"));
    rules["CFFEX"].push_back(with_intervals({cffex_morning, cffex_treasury_afternoon}, "TS"));
    rules["CFFEX"].push_back(with_intervals({cffex_morning, cffex_treasury_afternoon}, "TL"));
    rules["*"].push_back(with_intervals({commodity_morning_1, commodity_morning_2,
                                         SessionInterval{13 * 60 + 30, 15 * 60 + 15},
                                         SessionInterval{21 * 60, 2 * 60 + 30}}));
    return rules;
}

bool ParseMinuteValue(const std::string& minute_key, std::string* trading_day, int* minute_of_day) {
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
    std::snprintf(buffer, sizeof(buffer), "%s %02d:%02d", trading_day.c_str(), hour, minute);
    return std::string(buffer);
}

std::string FormatTimeOfDay(int minute_of_day) {
    if (minute_of_day < 0 || minute_of_day >= 24 * 60) {
        return "";
    }
    const int hour = minute_of_day / 60;
    const int minute = minute_of_day % 60;
    char buffer[8];
    std::snprintf(buffer, sizeof(buffer), "%02d:%02d", hour, minute);
    return std::string(buffer);
}

std::string BuildClosedBoundaryMinuteKey(const std::string& instrument_id,
                                         const std::string& minute_key) {
    if (instrument_id.empty() || minute_key.empty()) {
        return "";
    }
    return instrument_id + "|" + minute_key;
}

}  // namespace

BarAggregator::BarAggregator(BarAggregatorConfig config)
    : config_(std::move(config)),
      session_rules_by_exchange_(
          config_.use_default_session_fallback
              ? BuildDefaultRules()
              : std::unordered_map<std::string, std::vector<SessionRule>>{}) {
    LoadTradingSessions();
}

bool BarAggregator::ShouldProcessSnapshot(const MarketSnapshot& snapshot) const {
    if (snapshot.instrument_id.empty() || snapshot.update_time.size() < 5 ||
        !IsFinitePositive(snapshot.last_price)) {
        return false;
    }

    const auto trading_day = ResolveTradingDay(snapshot);
    if (trading_day.empty()) {
        return false;
    }

    if (config_.filter_non_trading_ticks) {
        const std::string exchange_id = ResolveExchangeId(snapshot);
        const std::string product = ResolveProductCode(snapshot);
        if (!IsInTradingSession(exchange_id, snapshot.instrument_id, product,
                                snapshot.update_time) &&
            !IsExactSessionEndTime(exchange_id, snapshot.instrument_id, product,
                                   snapshot.update_time)) {
            return false;
        }
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
    const bool is_exact_session_end =
        !snapshot.instrument_id.empty() && !minute_key.empty() &&
        IsExactSessionEndTime(exchange_id, snapshot.instrument_id, ResolveProductCode(snapshot),
                              snapshot.update_time);

    if (minute_key.empty()) {
        return emitted;
    }

    const EpochNanos event_ts_ns = ResolveEventTimestamp(snapshot);
    const EpochNanos period_start_ts_ns = ResolvePhysicalMinuteStart(snapshot);
    if (event_ts_ns <= 0 || period_start_ts_ns <= 0) {
        return emitted;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    PruneClosedBoundaryMinutesLocked(snapshot.instrument_id, trading_day);

    const std::string finalized_key =
        BuildClosedBoundaryMinuteKey(snapshot.instrument_id, minute_key);
    if (finalized_minute_keys_.find(finalized_key) != finalized_minute_keys_.end()) {
        return emitted;
    }

    auto& instrument_buckets = buckets_[snapshot.instrument_id];
    auto& bucket = instrument_buckets[period_start_ts_ns];
    const std::uint64_t arrival_seq = next_arrival_seq_++;
    if (!bucket.initialized) {
        ResetBucketLocked(&bucket, snapshot, exchange_id, trading_day, action_day, minute_key,
                          event_ts_ns, period_start_ts_ns, is_exact_session_end, arrival_seq);
    } else {
        const auto normalized_volume = std::max<std::int64_t>(0, snapshot.volume);
        bucket.max_cumulative_volume = std::max(bucket.max_cumulative_volume, normalized_volume);
        bucket.bar.exchange_id = exchange_id.empty() ? bucket.bar.exchange_id : exchange_id;
        bucket.bar.trading_day = trading_day;
        bucket.bar.action_day = action_day;
        bucket.bar.high = std::max(bucket.bar.high, snapshot.last_price);
        bucket.bar.low = std::min(bucket.bar.low, snapshot.last_price);
        if (event_ts_ns < bucket.first_event_ts_ns ||
            (event_ts_ns == bucket.first_event_ts_ns && arrival_seq < bucket.first_arrival_seq)) {
            bucket.first_event_ts_ns = event_ts_ns;
            bucket.first_arrival_seq = arrival_seq;
            bucket.first_cumulative_volume = normalized_volume;
            bucket.bar.open = snapshot.last_price;
            bucket.bar.analysis_open = snapshot.last_price;
        }
        if (event_ts_ns > bucket.last_event_ts_ns ||
            (event_ts_ns == bucket.last_event_ts_ns && arrival_seq > bucket.last_arrival_seq)) {
            bucket.last_event_ts_ns = event_ts_ns;
            bucket.last_arrival_seq = arrival_seq;
            bucket.bar.close = snapshot.last_price;
            bucket.bar.analysis_close = snapshot.last_price;
            bucket.bar.ts_ns = event_ts_ns;
        }
        bucket.bar.analysis_high = bucket.bar.high;
        bucket.bar.analysis_low = bucket.bar.low;
    }

    auto& max_event_ts = max_event_ts_by_instrument_[snapshot.instrument_id];
    max_event_ts = std::max(max_event_ts, event_ts_ns);
    const EpochNanos lateness_ns =
        static_cast<EpochNanos>(std::max(0, config_.allowed_lateness_ms)) * kNanosPerMillisecond;
    EpochNanos watermark_ts_ns = 0;
    if (config_.is_backtest_mode) {
        watermark_ts_ns = max_event_ts - lateness_ns;
    } else if (snapshot.recv_ts_ns > 0) {
        // Live finality follows trusted local receipt time, never a possibly corrupt/future
        // exchange event timestamp. A timer-driven AdvanceWatermark remains the primary path.
        watermark_ts_ns = snapshot.recv_ts_ns - lateness_ns;
    } else {
        return emitted;
    }
    emitted = FinalizeReadyLocked(watermark_ts_ns,
                                  snapshot.recv_ts_ns > 0 ? snapshot.recv_ts_ns : NowEpochNanos(),
                                  &snapshot.instrument_id);
    return emitted;
}

std::vector<BarSnapshot> BarAggregator::AdvanceWatermark(EpochNanos now_ts_ns) {
    if (now_ts_ns <= 0) {
        return {};
    }
    const EpochNanos lateness_ns =
        static_cast<EpochNanos>(std::max(0, config_.allowed_lateness_ms)) * kNanosPerMillisecond;
    std::lock_guard<std::mutex> lock(mutex_);
    return FinalizeReadyLocked(now_ts_ns - lateness_ns, now_ts_ns);
}

std::vector<BarSnapshot> BarAggregator::Flush() {
    std::vector<BarSnapshot> bars;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [instrument_id, instrument_buckets] : buckets_) {
            for (auto& [period_start, bucket] : instrument_buckets) {
                (void)period_start;
                if (bucket.initialized) {
                    BarSnapshot bar = FinalizeBucketLocked(instrument_id, &bucket, NowEpochNanos());
                    bar.is_complete = false;
                    bar.strategy_eligible = false;
                    bars.push_back(std::move(bar));
                }
            }
        }
        buckets_.clear();
        closed_session_boundary_minutes_.clear();
    }
    std::sort(bars.begin(), bars.end(), [](const BarSnapshot& lhs, const BarSnapshot& rhs) {
        if (lhs.instrument_id != rhs.instrument_id) {
            return lhs.instrument_id < rhs.instrument_id;
        }
        return lhs.minute < rhs.minute;
    });
    return bars;
}

void BarAggregator::DiscardPending() {
    std::lock_guard<std::mutex> lock(mutex_);
    buckets_.clear();
    max_event_ts_by_instrument_.clear();
    closed_session_boundary_minutes_.clear();
}

bool BarAggregator::SaveState(PersistenceState* out, std::string* error) const {
    if (out == nullptr) {
        SetPersistenceError(error, "bar aggregator state output is null");
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    out->clear();
    (*out)["version"] = "2";
    (*out)["next_arrival_seq"] = std::to_string(next_arrival_seq_);

    std::size_t bucket_count = 0;
    for (const auto& [instrument_id, instrument_buckets] : buckets_) {
        (void)instrument_id;
        bucket_count += instrument_buckets.size();
    }
    (*out)["buckets.count"] = std::to_string(bucket_count);
    std::size_t bucket_index = 0;
    for (const auto& [instrument_id, instrument_buckets] : buckets_) {
        for (const auto& [period_start, bucket] : instrument_buckets) {
            const std::string prefix = "buckets." + std::to_string(bucket_index++);
            (*out)[prefix + ".instrument_id"] = instrument_id;
            (*out)[prefix + ".period_start_key"] = std::to_string(period_start);
            (*out)[prefix + ".initialized"] = bucket.initialized ? "1" : "0";
            (*out)[prefix + ".minute_key"] = bucket.minute_key;
            (*out)[prefix + ".first_cumulative_volume"] =
                std::to_string(bucket.first_cumulative_volume);
            (*out)[prefix + ".max_cumulative_volume"] =
                std::to_string(bucket.max_cumulative_volume);
            (*out)[prefix + ".first_event_ts_ns"] = std::to_string(bucket.first_event_ts_ns);
            (*out)[prefix + ".last_event_ts_ns"] = std::to_string(bucket.last_event_ts_ns);
            (*out)[prefix + ".first_arrival_seq"] = std::to_string(bucket.first_arrival_seq);
            (*out)[prefix + ".last_arrival_seq"] = std::to_string(bucket.last_arrival_seq);
            (*out)[prefix + ".period_start_ts_ns"] = std::to_string(bucket.period_start_ts_ns);
            (*out)[prefix + ".period_end_ts_ns"] = std::to_string(bucket.period_end_ts_ns);

            const std::string bar_prefix = prefix + ".bar";
            const BarSnapshot& bar = bucket.bar;
            (*out)[bar_prefix + ".instrument_id"] = bar.instrument_id;
            (*out)[bar_prefix + ".exchange_id"] = bar.exchange_id;
            (*out)[bar_prefix + ".trading_day"] = bar.trading_day;
            (*out)[bar_prefix + ".action_day"] = bar.action_day;
            (*out)[bar_prefix + ".minute"] = bar.minute;
            (*out)[bar_prefix + ".open"] = FormatPersistenceDouble(bar.open);
            (*out)[bar_prefix + ".high"] = FormatPersistenceDouble(bar.high);
            (*out)[bar_prefix + ".low"] = FormatPersistenceDouble(bar.low);
            (*out)[bar_prefix + ".close"] = FormatPersistenceDouble(bar.close);
            (*out)[bar_prefix + ".analysis_open"] = FormatPersistenceDouble(bar.analysis_open);
            (*out)[bar_prefix + ".analysis_high"] = FormatPersistenceDouble(bar.analysis_high);
            (*out)[bar_prefix + ".analysis_low"] = FormatPersistenceDouble(bar.analysis_low);
            (*out)[bar_prefix + ".analysis_close"] = FormatPersistenceDouble(bar.analysis_close);
            (*out)[bar_prefix + ".analysis_price_offset"] =
                FormatPersistenceDouble(bar.analysis_price_offset);
            (*out)[bar_prefix + ".volume"] = std::to_string(bar.volume);
            (*out)[bar_prefix + ".ts_ns"] = std::to_string(bar.ts_ns);
            (*out)[bar_prefix + ".period_end_ts_ns"] = std::to_string(bar.period_end_ts_ns);
            (*out)[bar_prefix + ".finalized_ts_ns"] = std::to_string(bar.finalized_ts_ns);
            (*out)[bar_prefix + ".expected_source_bars"] = std::to_string(bar.expected_source_bars);
            (*out)[bar_prefix + ".observed_source_bars"] = std::to_string(bar.observed_source_bars);
            (*out)[bar_prefix + ".is_complete"] = bar.is_complete ? "1" : "0";
            (*out)[bar_prefix + ".is_session_endpoint"] = bar.is_session_endpoint ? "1" : "0";
            (*out)[bar_prefix + ".strategy_eligible"] = bar.strategy_eligible ? "1" : "0";
            (*out)[bar_prefix + ".volume_complete"] = bar.volume_complete ? "1" : "0";
            (*out)[bar_prefix + ".has_conflict"] = bar.has_conflict ? "1" : "0";
            (*out)[bar_prefix + ".is_recovery_replay"] = bar.is_recovery_replay ? "1" : "0";
        }
    }

    (*out)["volume_states.count"] = std::to_string(volume_states_.size());
    std::size_t volume_index = 0;
    for (const auto& [instrument_id, volume] : volume_states_) {
        const std::string prefix = "volume_states." + std::to_string(volume_index++);
        (*out)[prefix + ".instrument_id"] = instrument_id;
        (*out)[prefix + ".trading_day"] = volume.trading_day;
        (*out)[prefix + ".initialized"] = volume.initialized ? "1" : "0";
        (*out)[prefix + ".baseline_complete"] = volume.baseline_complete ? "1" : "0";
        (*out)[prefix + ".cumulative_volume"] = std::to_string(volume.cumulative_volume);
    }

    (*out)["max_events.count"] = std::to_string(max_event_ts_by_instrument_.size());
    std::size_t max_event_index = 0;
    for (const auto& [instrument_id, ts_ns] : max_event_ts_by_instrument_) {
        const std::string prefix = "max_events." + std::to_string(max_event_index++);
        (*out)[prefix + ".instrument_id"] = instrument_id;
        (*out)[prefix + ".ts_ns"] = std::to_string(ts_ns);
    }

    (*out)["finalized.count"] = std::to_string(finalized_minute_keys_.size());
    std::size_t finalized_index = 0;
    for (const auto& key : finalized_minute_keys_) {
        (*out)["finalized." + std::to_string(finalized_index++)] = key;
    }
    (*out)["closed_boundaries.count"] = std::to_string(closed_session_boundary_minutes_.size());
    std::size_t boundary_index = 0;
    for (const auto& key : closed_session_boundary_minutes_) {
        (*out)["closed_boundaries." + std::to_string(boundary_index++)] = key;
    }
    return true;
}

bool BarAggregator::LoadState(const PersistenceState& state, std::string* error) {
    const std::string* version = RequirePersistenceValue(state, "version", error);
    if (version == nullptr || *version != "2") {
        SetPersistenceError(error, "unsupported bar aggregator state version");
        return false;
    }

    std::uint64_t loaded_next_arrival_seq = 1;
    if (!ParsePersistenceUnsigned(state, "next_arrival_seq", &loaded_next_arrival_seq, error)) {
        return false;
    }
    std::unordered_map<std::string, std::map<EpochNanos, MinuteBucket>> loaded_buckets;
    std::int64_t bucket_count = 0;
    if (!ParsePersistenceInteger(state, "buckets.count", &bucket_count, error) ||
        bucket_count < 0) {
        return false;
    }
    for (std::int64_t index = 0; index < bucket_count; ++index) {
        const std::string prefix = "buckets." + std::to_string(index);
        const std::string* instrument_id =
            RequirePersistenceValue(state, prefix + ".instrument_id", error);
        const std::string* minute_key =
            RequirePersistenceValue(state, prefix + ".minute_key", error);
        if (instrument_id == nullptr || instrument_id->empty() || minute_key == nullptr) {
            return false;
        }
        EpochNanos map_period_start = 0;
        MinuteBucket bucket;
        if (!ParsePersistenceInteger(state, prefix + ".period_start_key", &map_period_start,
                                     error) ||
            !ParsePersistenceBool(state, prefix + ".initialized", &bucket.initialized, error) ||
            !ParsePersistenceInteger(state, prefix + ".first_cumulative_volume",
                                     &bucket.first_cumulative_volume, error) ||
            !ParsePersistenceInteger(state, prefix + ".max_cumulative_volume",
                                     &bucket.max_cumulative_volume, error) ||
            !ParsePersistenceInteger(state, prefix + ".first_event_ts_ns",
                                     &bucket.first_event_ts_ns, error) ||
            !ParsePersistenceInteger(state, prefix + ".last_event_ts_ns", &bucket.last_event_ts_ns,
                                     error) ||
            !ParsePersistenceUnsigned(state, prefix + ".first_arrival_seq",
                                      &bucket.first_arrival_seq, error) ||
            !ParsePersistenceUnsigned(state, prefix + ".last_arrival_seq", &bucket.last_arrival_seq,
                                      error) ||
            !ParsePersistenceInteger(state, prefix + ".period_start_ts_ns",
                                     &bucket.period_start_ts_ns, error) ||
            !ParsePersistenceInteger(state, prefix + ".period_end_ts_ns", &bucket.period_end_ts_ns,
                                     error)) {
            return false;
        }
        bucket.minute_key = *minute_key;

        const std::string bar_prefix = prefix + ".bar";
        const std::string* bar_instrument =
            RequirePersistenceValue(state, bar_prefix + ".instrument_id", error);
        const std::string* exchange_id =
            RequirePersistenceValue(state, bar_prefix + ".exchange_id", error);
        const std::string* trading_day =
            RequirePersistenceValue(state, bar_prefix + ".trading_day", error);
        const std::string* action_day =
            RequirePersistenceValue(state, bar_prefix + ".action_day", error);
        const std::string* bar_minute =
            RequirePersistenceValue(state, bar_prefix + ".minute", error);
        if (bar_instrument == nullptr || exchange_id == nullptr || trading_day == nullptr ||
            action_day == nullptr || bar_minute == nullptr) {
            return false;
        }
        BarSnapshot& bar = bucket.bar;
        bar.instrument_id = *bar_instrument;
        bar.exchange_id = *exchange_id;
        bar.trading_day = *trading_day;
        bar.action_day = *action_day;
        bar.minute = *bar_minute;
        if (!ParsePersistenceDouble(state, bar_prefix + ".open", &bar.open, error) ||
            !ParsePersistenceDouble(state, bar_prefix + ".high", &bar.high, error) ||
            !ParsePersistenceDouble(state, bar_prefix + ".low", &bar.low, error) ||
            !ParsePersistenceDouble(state, bar_prefix + ".close", &bar.close, error) ||
            !ParsePersistenceDouble(state, bar_prefix + ".analysis_open", &bar.analysis_open,
                                    error) ||
            !ParsePersistenceDouble(state, bar_prefix + ".analysis_high", &bar.analysis_high,
                                    error) ||
            !ParsePersistenceDouble(state, bar_prefix + ".analysis_low", &bar.analysis_low,
                                    error) ||
            !ParsePersistenceDouble(state, bar_prefix + ".analysis_close", &bar.analysis_close,
                                    error) ||
            !ParsePersistenceDouble(state, bar_prefix + ".analysis_price_offset",
                                    &bar.analysis_price_offset, error) ||
            !ParsePersistenceInteger(state, bar_prefix + ".volume", &bar.volume, error) ||
            !ParsePersistenceInteger(state, bar_prefix + ".ts_ns", &bar.ts_ns, error) ||
            !ParsePersistenceInteger(state, bar_prefix + ".period_end_ts_ns", &bar.period_end_ts_ns,
                                     error) ||
            !ParsePersistenceInteger(state, bar_prefix + ".finalized_ts_ns", &bar.finalized_ts_ns,
                                     error) ||
            !ParsePersistenceInteger(state, bar_prefix + ".expected_source_bars",
                                     &bar.expected_source_bars, error) ||
            !ParsePersistenceInteger(state, bar_prefix + ".observed_source_bars",
                                     &bar.observed_source_bars, error) ||
            !ParsePersistenceBool(state, bar_prefix + ".is_complete", &bar.is_complete, error) ||
            !ParsePersistenceBool(state, bar_prefix + ".is_session_endpoint",
                                  &bar.is_session_endpoint, error) ||
            !ParsePersistenceBool(state, bar_prefix + ".strategy_eligible", &bar.strategy_eligible,
                                  error) ||
            !ParsePersistenceBool(state, bar_prefix + ".volume_complete", &bar.volume_complete,
                                  error) ||
            !ParsePersistenceBool(state, bar_prefix + ".has_conflict", &bar.has_conflict, error) ||
            !ParsePersistenceBool(state, bar_prefix + ".is_recovery_replay",
                                  &bar.is_recovery_replay, error)) {
            return false;
        }
        loaded_buckets[*instrument_id][map_period_start] = std::move(bucket);
    }

    std::unordered_map<std::string, InstrumentVolumeState> loaded_volume_states;
    std::int64_t volume_count = 0;
    if (!ParsePersistenceInteger(state, "volume_states.count", &volume_count, error) ||
        volume_count < 0) {
        return false;
    }
    for (std::int64_t index = 0; index < volume_count; ++index) {
        const std::string prefix = "volume_states." + std::to_string(index);
        const std::string* instrument_id =
            RequirePersistenceValue(state, prefix + ".instrument_id", error);
        const std::string* trading_day =
            RequirePersistenceValue(state, prefix + ".trading_day", error);
        if (instrument_id == nullptr || instrument_id->empty() || trading_day == nullptr) {
            return false;
        }
        InstrumentVolumeState volume;
        volume.trading_day = *trading_day;
        if (!ParsePersistenceBool(state, prefix + ".initialized", &volume.initialized, error) ||
            !ParsePersistenceBool(state, prefix + ".baseline_complete", &volume.baseline_complete,
                                  error) ||
            !ParsePersistenceInteger(state, prefix + ".cumulative_volume",
                                     &volume.cumulative_volume, error)) {
            return false;
        }
        loaded_volume_states[*instrument_id] = std::move(volume);
    }

    std::unordered_map<std::string, EpochNanos> loaded_max_events;
    std::int64_t max_event_count = 0;
    if (!ParsePersistenceInteger(state, "max_events.count", &max_event_count, error) ||
        max_event_count < 0) {
        return false;
    }
    for (std::int64_t index = 0; index < max_event_count; ++index) {
        const std::string prefix = "max_events." + std::to_string(index);
        const std::string* instrument_id =
            RequirePersistenceValue(state, prefix + ".instrument_id", error);
        EpochNanos ts_ns = 0;
        if (instrument_id == nullptr || instrument_id->empty() ||
            !ParsePersistenceInteger(state, prefix + ".ts_ns", &ts_ns, error)) {
            return false;
        }
        loaded_max_events[*instrument_id] = ts_ns;
    }

    auto load_string_set = [&](const std::string& prefix, std::unordered_set<std::string>* output) {
        std::int64_t count = 0;
        if (!ParsePersistenceInteger(state, prefix + ".count", &count, error) || count < 0) {
            return false;
        }
        for (std::int64_t index = 0; index < count; ++index) {
            const std::string* value =
                RequirePersistenceValue(state, prefix + "." + std::to_string(index), error);
            if (value == nullptr) {
                return false;
            }
            output->insert(*value);
        }
        return true;
    };
    std::unordered_set<std::string> loaded_finalized;
    std::unordered_set<std::string> loaded_closed_boundaries;
    if (!load_string_set("finalized", &loaded_finalized) ||
        !load_string_set("closed_boundaries", &loaded_closed_boundaries)) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    buckets_ = std::move(loaded_buckets);
    volume_states_ = std::move(loaded_volume_states);
    max_event_ts_by_instrument_ = std::move(loaded_max_events);
    finalized_minute_keys_ = std::move(loaded_finalized);
    closed_session_boundary_minutes_ = std::move(loaded_closed_boundaries);
    next_arrival_seq_ = std::max<std::uint64_t>(1, loaded_next_arrival_seq);
    return true;
}

bool BarAggregator::IsFinalizedSnapshot(const MarketSnapshot& snapshot) const {
    const std::string trading_day = ResolveTradingDay(snapshot);
    const std::string minute_key = BuildMinuteKey(trading_day, snapshot.update_time);
    const std::string finalized_key =
        BuildClosedBoundaryMinuteKey(snapshot.instrument_id, minute_key);
    if (finalized_key.empty()) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    return finalized_minute_keys_.find(finalized_key) != finalized_minute_keys_.end();
}

std::vector<BarSnapshot> BarAggregator::FlushFinished(
    const std::unordered_map<std::string, EpochNanos>& instrument_last_tick_ts_ns,
    EpochNanos cutoff_ts_ns) {
    (void)instrument_last_tick_ts_ns;
    return AdvanceWatermark(cutoff_ts_ns +
                            static_cast<EpochNanos>(std::max(0, config_.allowed_lateness_ms)) *
                                kNanosPerMillisecond);
}

std::vector<BarSnapshot> BarAggregator::FlushSessionEndBars(
    const std::unordered_map<std::string, EpochNanos>& instrument_last_tick_ts_ns,
    EpochNanos cutoff_ts_ns) {
    std::vector<BarSnapshot> bars;
    std::lock_guard<std::mutex> lock(mutex_);
    const EpochNanos lateness_ns =
        static_cast<EpochNanos>(std::max(0, config_.allowed_lateness_ms)) * kNanosPerMillisecond;
    const EpochNanos watermark_ts_ns = cutoff_ts_ns - lateness_ns;
    for (const auto& [instrument_id, last_tick_ts_ns] : instrument_last_tick_ts_ns) {
        if (last_tick_ts_ns > cutoff_ts_ns) {
            continue;
        }
        auto instrument_it = buckets_.find(instrument_id);
        if (instrument_it == buckets_.end()) {
            continue;
        }
        auto& instrument_buckets = instrument_it->second;
        for (auto bucket_it = instrument_buckets.begin(); bucket_it != instrument_buckets.end();) {
            if (!bucket_it->second.initialized ||
                bucket_it->second.period_end_ts_ns > watermark_ts_ns ||
                !IsSessionEndMinuteKey(bucket_it->second.bar.exchange_id,
                                       bucket_it->second.bar.instrument_id,
                                       bucket_it->second.bar.minute)) {
                ++bucket_it;
                continue;
            }
            bars.push_back(
                FinalizeBucketLocked(instrument_id, &bucket_it->second, NowEpochNanos()));
            bucket_it = instrument_buckets.erase(bucket_it);
        }
        if (instrument_buckets.empty()) {
            buckets_.erase(instrument_it);
        }
    }
    std::sort(bars.begin(), bars.end(), [](const BarSnapshot& lhs, const BarSnapshot& rhs) {
        if (lhs.instrument_id != rhs.instrument_id) {
            return lhs.instrument_id < rhs.instrument_id;
        }
        return lhs.minute < rhs.minute;
    });
    return bars;
}

void BarAggregator::ResetInstrument(const std::string& instrument_id) {
    if (instrument_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    buckets_.erase(instrument_id);
    volume_states_.erase(instrument_id);
    max_event_ts_by_instrument_.erase(instrument_id);
    EraseClosedBoundaryMinutesLocked(instrument_id);
}

std::vector<BarSnapshot> BarAggregator::AggregateFromOneMinute(
    const std::vector<BarSnapshot>& one_minute_bars, std::int32_t timeframe_minutes) {
    if (timeframe_minutes <= 0 || one_minute_bars.empty()) {
        return {};
    }

    std::vector<BarSnapshot> sorted = one_minute_bars;
    std::sort(sorted.begin(), sorted.end(), [](const BarSnapshot& lhs, const BarSnapshot& rhs) {
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
        active_bar.analysis_high = std::max(active_bar.analysis_high, bar.analysis_high);
        active_bar.analysis_low = std::min(active_bar.analysis_low, bar.analysis_low);
        active_bar.analysis_close = bar.analysis_close;
        active_bar.analysis_price_offset = bar.analysis_price_offset;
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

bool BarAggregator::IsInTradingSession(const std::string& exchange_id,
                                       const std::string& update_time) const {
    return IsInTradingSession(exchange_id, "", "", update_time);
}

bool BarAggregator::IsSessionEndMinute(const std::string& exchange_id,
                                       const std::string& instrument_id,
                                       const std::string& update_time) const {
    int minute_of_day = 0;
    if (!ParseMinuteOfDay(update_time, &minute_of_day)) {
        return false;
    }

    MarketSnapshot snapshot;
    snapshot.instrument_id = instrument_id;

    SessionInterval interval;
    const std::string resolved_exchange =
        exchange_id.empty() ? InferExchangeId(instrument_id) : exchange_id;
    if (ResolveSessionInterval(resolved_exchange, instrument_id, ResolveProductCode(snapshot),
                               update_time, &interval)) {
        return IsLastMinuteInInterval(interval, minute_of_day);
    }
    // ResolveSessionInterval uses an exclusive upper bound, so ticks at exactly
    // end_minute (e.g. 15:00, 23:00, 11:30, 10:15) are not matched by it. Check
    // directly whether the minute equals any session's end_minute.
    return IsExactSessionEndTime(resolved_exchange, instrument_id, ResolveProductCode(snapshot),
                                 update_time);
}

std::string BarAggregator::ResolveSessionKey(const std::string& exchange_id,
                                             const std::string& instrument_id,
                                             const std::string& update_time) const {
    MarketSnapshot snapshot;
    snapshot.instrument_id = instrument_id;

    SessionInterval interval;
    const std::string resolved_exchange =
        exchange_id.empty() ? InferExchangeId(instrument_id) : exchange_id;
    if (!ResolveSessionInterval(resolved_exchange, instrument_id, ResolveProductCode(snapshot),
                                update_time, &interval)) {
        return "";
    }
    return FormatSessionKey(interval);
}

int BarAggregator::ResolveSessionOrder(const std::string& exchange_id,
                                       const std::string& instrument_id,
                                       const std::string& update_time) const {
    MarketSnapshot snapshot;
    snapshot.instrument_id = instrument_id;

    SessionInterval interval;
    const std::string resolved_exchange =
        exchange_id.empty() ? InferExchangeId(instrument_id) : exchange_id;
    if (!ResolveSessionInterval(resolved_exchange, instrument_id, ResolveProductCode(snapshot),
                                update_time, &interval)) {
        int minute_of_day = 24 * 60;
        if (ParseMinuteOfDay(update_time, &minute_of_day)) {
            return minute_of_day;
        }
        return std::numeric_limits<int>::max();
    }
    return SessionOrderKey(interval);
}

bool BarAggregator::ParseMinuteOfDay(const std::string& update_time, int* minute_of_day) {
    if (minute_of_day == nullptr || update_time.size() < 5) {
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
    *minute_of_day = hour * 60 + minute;
    return true;
}

std::string BarAggregator::ResolveExchangeId(const MarketSnapshot& snapshot) const {
    if (!snapshot.exchange_id.empty()) {
        return snapshot.exchange_id;
    }
    return InferExchangeId(snapshot.instrument_id);
}

std::string BarAggregator::InferExchangeId(const std::string& instrument_id) const {
    const auto dotted_exchange = InferExchangeIdFromDottedInstrument(instrument_id);
    if (!dotted_exchange.empty()) {
        return ToUpperAscii(dotted_exchange);
    }

    const auto symbol_upper = ToUpperAscii(ExtractInstrumentSymbol(instrument_id));
    if (symbol_upper.empty()) {
        return "";
    }

    std::size_t best_prefix_len = 0;
    std::string best_exchange;
    for (const auto& [exchange_id, rules] : session_rules_by_exchange_) {
        const auto exchange_upper = ToUpperAscii(exchange_id);
        if (exchange_upper.empty() || exchange_upper == "*") {
            continue;
        }
        for (const auto& rule : rules) {
            const auto prefix_upper = ToUpperAscii(rule.instrument_prefix);
            if (prefix_upper.empty() || !StartsWith(symbol_upper, prefix_upper)) {
                continue;
            }
            if (prefix_upper.size() > best_prefix_len ||
                (prefix_upper.size() == best_prefix_len &&
                 (best_exchange.empty() || exchange_upper < best_exchange))) {
                best_prefix_len = prefix_upper.size();
                best_exchange = exchange_upper;
            }
        }
    }
    return best_exchange;
}

std::string BarAggregator::ResolveProductCode(const MarketSnapshot& snapshot) {
    const auto dot_pos = snapshot.instrument_id.find('.');
    const auto symbol = dot_pos == std::string::npos ? snapshot.instrument_id
                                                     : snapshot.instrument_id.substr(dot_pos + 1);
    std::string product;
    for (char ch : symbol) {
        if (!std::isalpha(static_cast<unsigned char>(ch))) {
            break;
        }
        product.push_back(ch);
    }
    return ToUpperAscii(product);
}

std::string BarAggregator::ResolveTradingDay(const MarketSnapshot& snapshot) {
    if (!snapshot.trading_day.empty()) {
        return snapshot.trading_day;
    }
    return snapshot.action_day;
}

std::string BarAggregator::ResolveActionDay(const MarketSnapshot& snapshot) {
    return snapshot.action_day;
}

std::string BarAggregator::BuildMinuteKey(const std::string& trading_day,
                                          const std::string& update_time) {
    if (trading_day.empty() || update_time.size() < 5) {
        return "";
    }
    return trading_day + " " + update_time.substr(0, 5);
}

EpochNanos BarAggregator::ResolveEventTimestamp(const MarketSnapshot& snapshot) {
    if (snapshot.exchange_ts_ns > 0) {
        return snapshot.exchange_ts_ns;
    }
    EpochNanos parsed = 0;
    if (ParsePhysicalTimestamp(ResolveActionDay(snapshot), snapshot.update_time,
                               snapshot.update_millisec, &parsed)) {
        return parsed;
    }
    return snapshot.recv_ts_ns;
}

EpochNanos BarAggregator::ResolvePhysicalMinuteStart(const MarketSnapshot& snapshot) {
    if (snapshot.exchange_ts_ns > 0) {
        return (snapshot.exchange_ts_ns / kNanosPerMinute) * kNanosPerMinute;
    }
    EpochNanos parsed = 0;
    if (!ParsePhysicalTimestamp(ResolveActionDay(snapshot), snapshot.update_time, 0, &parsed)) {
        return 0;
    }
    return (parsed / kNanosPerMinute) * kNanosPerMinute;
}

EpochNanos BarAggregator::ResolveTimestamp(const MarketSnapshot& snapshot) const {
    if (config_.is_backtest_mode) {
        if (snapshot.exchange_ts_ns > 0) {
            return snapshot.exchange_ts_ns;
        }
        if (snapshot.recv_ts_ns > 0) {
            return snapshot.recv_ts_ns;
        }
        return NowEpochNanos();
    }
    if (snapshot.recv_ts_ns > 0) {
        return snapshot.recv_ts_ns;
    }
    if (snapshot.exchange_ts_ns > 0) {
        return snapshot.exchange_ts_ns;
    }
    return NowEpochNanos();
}

bool BarAggregator::IsInTradingSession(const std::string& exchange_id,
                                       const std::string& instrument_id, const std::string& product,
                                       const std::string& update_time) const {
    SessionInterval interval;
    return ResolveSessionInterval(exchange_id, instrument_id, product, update_time, &interval);
}

bool BarAggregator::ResolveSessionInterval(const std::string& exchange_id,
                                           const std::string& instrument_id,
                                           const std::string& product,
                                           const std::string& update_time,
                                           SessionInterval* interval) const {
    int minute_of_day = 0;
    if (!ParseMinuteOfDay(update_time, &minute_of_day)) {
        return false;
    }

    auto evaluate_rules = [&](const std::vector<SessionRule>& rules) {
        if (instrument_id.empty() && product.empty()) {
            for (const auto& rule : rules) {
                for (const auto& rule_interval : rule.intervals) {
                    if (IsMinuteInInterval(rule_interval, minute_of_day)) {
                        if (interval != nullptr) {
                            *interval = rule_interval;
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        const auto symbol_upper = ToUpperAscii(ExtractInstrumentSymbol(instrument_id));
        const auto current_product = ToUpperAscii(product);
        bool has_specific_match = false;
        for (const auto& rule : rules) {
            const bool has_selector = !rule.instrument_prefix.empty() || !rule.product.empty();
            if (!has_selector) {
                continue;
            }

            const auto prefix_upper = ToUpperAscii(rule.instrument_prefix);
            const auto product_upper = ToUpperAscii(rule.product);
            const bool prefix_match =
                prefix_upper.empty() || StartsWith(symbol_upper, prefix_upper);
            const bool product_match = product_upper.empty() || product_upper == current_product;
            if (!prefix_match || !product_match) {
                continue;
            }
            has_specific_match = true;
            for (const auto& rule_interval : rule.intervals) {
                if (IsMinuteInInterval(rule_interval, minute_of_day)) {
                    if (interval != nullptr) {
                        *interval = rule_interval;
                    }
                    return true;
                }
            }
        }
        if (has_specific_match) {
            return false;
        }

        for (const auto& rule : rules) {
            if (!rule.instrument_prefix.empty() || !rule.product.empty()) {
                continue;
            }
            for (const auto& rule_interval : rule.intervals) {
                if (IsMinuteInInterval(rule_interval, minute_of_day)) {
                    if (interval != nullptr) {
                        *interval = rule_interval;
                    }
                    return true;
                }
            }
        }
        return false;
    };

    const auto exchange = ToUpperAscii(exchange_id);
    const auto it = session_rules_by_exchange_.find(exchange);
    if (it != session_rules_by_exchange_.end()) {
        return evaluate_rules(it->second);
    }
    const auto fallback = session_rules_by_exchange_.find("*");
    if (fallback != session_rules_by_exchange_.end()) {
        return evaluate_rules(fallback->second);
    }
    return false;
}

bool BarAggregator::IsExactSessionEndTime(const std::string& exchange_id,
                                          const std::string& instrument_id,
                                          const std::string& product,
                                          const std::string& update_time) const {
    int minute_of_day = 0;
    if (!ParseMinuteOfDay(update_time, &minute_of_day)) {
        return false;
    }
    int second = 0;
    if (!ParseSecondOfMinute(update_time, &second) || second != 0) {
        return false;
    }

    auto evaluate_rules = [&](const std::vector<SessionRule>& rules) {
        if (instrument_id.empty() && product.empty()) {
            for (const auto& rule : rules) {
                for (const auto& rule_interval : rule.intervals) {
                    if (minute_of_day == rule_interval.end_minute) {
                        return true;
                    }
                }
            }
            return false;
        }

        const auto symbol_upper = ToUpperAscii(ExtractInstrumentSymbol(instrument_id));
        const auto current_product = ToUpperAscii(product);
        bool has_specific_match = false;
        for (const auto& rule : rules) {
            const bool has_selector = !rule.instrument_prefix.empty() || !rule.product.empty();
            if (!has_selector) {
                continue;
            }

            const auto prefix_upper = ToUpperAscii(rule.instrument_prefix);
            const auto product_upper = ToUpperAscii(rule.product);
            const bool prefix_match =
                prefix_upper.empty() || StartsWith(symbol_upper, prefix_upper);
            const bool product_match = product_upper.empty() || product_upper == current_product;
            if (!prefix_match || !product_match) {
                continue;
            }
            has_specific_match = true;
            for (const auto& rule_interval : rule.intervals) {
                if (minute_of_day == rule_interval.end_minute) {
                    return true;
                }
            }
        }
        if (has_specific_match) {
            return false;
        }

        for (const auto& rule : rules) {
            if (!rule.instrument_prefix.empty() || !rule.product.empty()) {
                continue;
            }
            for (const auto& rule_interval : rule.intervals) {
                if (minute_of_day == rule_interval.end_minute) {
                    return true;
                }
            }
        }
        return false;
    };

    const auto exchange = ToUpperAscii(exchange_id);
    const auto it = session_rules_by_exchange_.find(exchange);
    if (it != session_rules_by_exchange_.end()) {
        return evaluate_rules(it->second);
    }
    const auto fallback = session_rules_by_exchange_.find("*");
    if (fallback != session_rules_by_exchange_.end()) {
        return evaluate_rules(fallback->second);
    }
    return false;
}

bool BarAggregator::IsSessionEndMinuteKey(const std::string& exchange_id,
                                          const std::string& instrument_id,
                                          const std::string& minute_key) const {
    std::string trading_day;
    int minute_of_day = 0;
    if (!ParseMinuteValue(minute_key, &trading_day, &minute_of_day)) {
        return false;
    }
    return IsSessionEndMinute(exchange_id, instrument_id, FormatTimeOfDay(minute_of_day));
}

void BarAggregator::LoadTradingSessions() {
    const char* env_path = std::getenv("TRADING_SESSIONS_CONFIG_PATH");
    const std::string config_path = env_path != nullptr && std::string(env_path).size() > 0
                                        ? std::string(env_path)
                                        : config_.trading_sessions_config_path;
    if (config_path.empty()) {
        return;
    }

    std::ifstream file(config_path);
    if (!file.is_open()) {
        return;
    }

    struct PendingSession {
        std::string exchange;
        std::string instrument_prefix;
        std::string product;
        std::string day;
        std::string night;
    };

    auto to_rule = [](const PendingSession& pending, std::string* exchange,
                      SessionRule* rule) -> bool {
        if (exchange == nullptr || rule == nullptr || pending.exchange.empty()) {
            return false;
        }
        rule->instrument_prefix = ToUpperAscii(pending.instrument_prefix);
        rule->product = pending.product;
        rule->intervals.clear();

        (void)ParseSessionRanges(pending.day, &rule->intervals);
        (void)ParseSessionRanges(pending.night, &rule->intervals);
        if (rule->intervals.empty()) {
            return false;
        }
        *exchange = ToUpperAscii(pending.exchange);
        return true;
    };

    std::unordered_map<std::string, std::vector<SessionRule>> loaded_rules;
    PendingSession pending;
    bool has_pending = false;
    std::string line;

    auto flush_pending = [&]() {
        if (!has_pending) {
            return;
        }
        SessionRule rule;
        std::string exchange;
        if (to_rule(pending, &exchange, &rule)) {
            loaded_rules[exchange].push_back(std::move(rule));
        }
        pending = PendingSession{};
        has_pending = false;
    };

    while (std::getline(file, line)) {
        std::string trimmed = RemoveInlineComment(Trim(line));
        if (trimmed.empty() || trimmed == "sessions:") {
            continue;
        }

        if (trimmed.rfind("- ", 0) == 0) {
            flush_pending();
            has_pending = true;
            trimmed = Trim(trimmed.substr(2));
            if (trimmed.empty()) {
                continue;
            }
        }

        if (!has_pending) {
            continue;
        }

        std::string key;
        std::string value;
        if (!ParseYamlKeyValue(trimmed, &key, &value)) {
            continue;
        }
        if (key == "exchange") {
            pending.exchange = value;
        } else if (key == "instrument_prefix") {
            pending.instrument_prefix = value;
        } else if (key == "product") {
            pending.product = value;
        } else if (key == "day") {
            pending.day = value;
        } else if (key == "night") {
            pending.night = value;
        }
    }
    flush_pending();

    for (const auto& entry : loaded_rules) {
        session_rules_by_exchange_[entry.first] = entry.second;
    }
}

void BarAggregator::PruneClosedBoundaryMinutesLocked(const std::string& instrument_id,
                                                     const std::string& trading_day) {
    if (instrument_id.empty() || trading_day.empty() || closed_session_boundary_minutes_.empty()) {
        return;
    }
    const std::string instrument_prefix = instrument_id + "|";
    const std::string current_day_prefix = instrument_prefix + trading_day + " ";
    for (auto it = closed_session_boundary_minutes_.begin();
         it != closed_session_boundary_minutes_.end();) {
        if (it->rfind(instrument_prefix, 0) == 0 && it->rfind(current_day_prefix, 0) != 0) {
            it = closed_session_boundary_minutes_.erase(it);
        } else {
            ++it;
        }
    }
}

void BarAggregator::EraseClosedBoundaryMinutesLocked(const std::string& instrument_id) {
    if (instrument_id.empty() || closed_session_boundary_minutes_.empty()) {
        return;
    }
    const std::string instrument_prefix = instrument_id + "|";
    for (auto it = closed_session_boundary_minutes_.begin();
         it != closed_session_boundary_minutes_.end();) {
        if (it->rfind(instrument_prefix, 0) == 0) {
            it = closed_session_boundary_minutes_.erase(it);
        } else {
            ++it;
        }
    }
}

void BarAggregator::ResetBucketLocked(MinuteBucket* bucket, const MarketSnapshot& snapshot,
                                      const std::string& exchange_id,
                                      const std::string& trading_day, const std::string& action_day,
                                      const std::string& minute_key, EpochNanos event_ts_ns,
                                      EpochNanos period_start_ts_ns, bool is_session_endpoint,
                                      std::uint64_t arrival_seq) {
    bucket->initialized = true;
    bucket->minute_key = minute_key;
    bucket->first_cumulative_volume = std::max<std::int64_t>(0, snapshot.volume);
    bucket->max_cumulative_volume = bucket->first_cumulative_volume;
    bucket->first_event_ts_ns = event_ts_ns;
    bucket->last_event_ts_ns = event_ts_ns;
    bucket->first_arrival_seq = arrival_seq;
    bucket->last_arrival_seq = arrival_seq;
    bucket->period_start_ts_ns = period_start_ts_ns;
    bucket->period_end_ts_ns =
        is_session_endpoint ? period_start_ts_ns : period_start_ts_ns + kNanosPerMinute;
    bucket->bar.instrument_id = snapshot.instrument_id;
    bucket->bar.exchange_id = exchange_id;
    bucket->bar.trading_day = trading_day;
    bucket->bar.action_day = action_day;
    bucket->bar.minute = minute_key;
    bucket->bar.open = snapshot.last_price;
    bucket->bar.high = snapshot.last_price;
    bucket->bar.low = snapshot.last_price;
    bucket->bar.close = snapshot.last_price;
    bucket->bar.analysis_open = snapshot.last_price;
    bucket->bar.analysis_high = snapshot.last_price;
    bucket->bar.analysis_low = snapshot.last_price;
    bucket->bar.analysis_close = snapshot.last_price;
    bucket->bar.analysis_price_offset = 0.0;
    bucket->bar.volume = 0;
    bucket->bar.ts_ns = event_ts_ns;
    bucket->bar.period_end_ts_ns = bucket->period_end_ts_ns;
    bucket->bar.finalized_ts_ns = 0;
    bucket->bar.expected_source_bars = 1;
    bucket->bar.observed_source_bars = 1;
    bucket->bar.is_complete = true;
    bucket->bar.is_session_endpoint = is_session_endpoint;
    bucket->bar.strategy_eligible = !is_session_endpoint;
    bucket->bar.volume_complete = false;
    bucket->bar.has_conflict = false;
    bucket->bar.is_recovery_replay = false;
}

BarSnapshot BarAggregator::FinalizeBucketLocked(const std::string& instrument_id,
                                                MinuteBucket* bucket, EpochNanos finalized_ts_ns) {
    BarSnapshot bar = bucket->bar;
    auto& volume_state = volume_states_[instrument_id];
    if (!volume_state.initialized || volume_state.trading_day != bar.trading_day ||
        bucket->max_cumulative_volume < volume_state.cumulative_volume) {
        volume_state.trading_day = bar.trading_day;
        volume_state.initialized = true;
        volume_state.baseline_complete = false;
        volume_state.cumulative_volume = bucket->first_cumulative_volume;
    }
    bar.volume_complete = volume_state.baseline_complete;
    bar.volume =
        std::max<std::int64_t>(0, bucket->max_cumulative_volume - volume_state.cumulative_volume);
    volume_state.cumulative_volume =
        std::max(volume_state.cumulative_volume, bucket->max_cumulative_volume);
    volume_state.baseline_complete = true;
    bar.period_end_ts_ns = bucket->period_end_ts_ns;
    bar.finalized_ts_ns = finalized_ts_ns;
    bar.expected_source_bars = 1;
    bar.observed_source_bars = 1;
    bar.is_complete = true;
    bar.has_conflict = false;
    bar.strategy_eligible = !bar.is_session_endpoint;
    const std::string finalized_key = BuildClosedBoundaryMinuteKey(instrument_id, bar.minute);
    if (!finalized_key.empty()) {
        finalized_minute_keys_.insert(finalized_key);
    }
    return bar;
}

std::vector<BarSnapshot> BarAggregator::FinalizeReadyLocked(EpochNanos watermark_ts_ns,
                                                            EpochNanos finalized_ts_ns,
                                                            const std::string* instrument_filter) {
    std::vector<BarSnapshot> bars;
    for (auto instrument_it = buckets_.begin(); instrument_it != buckets_.end();) {
        if (instrument_filter != nullptr && instrument_it->first != *instrument_filter) {
            ++instrument_it;
            continue;
        }
        auto& instrument_buckets = instrument_it->second;
        for (auto bucket_it = instrument_buckets.begin(); bucket_it != instrument_buckets.end();) {
            if (!bucket_it->second.initialized ||
                bucket_it->second.period_end_ts_ns > watermark_ts_ns) {
                ++bucket_it;
                continue;
            }
            bars.push_back(
                FinalizeBucketLocked(instrument_it->first, &bucket_it->second, finalized_ts_ns));
            bucket_it = instrument_buckets.erase(bucket_it);
        }
        if (instrument_buckets.empty()) {
            instrument_it = buckets_.erase(instrument_it);
        } else {
            ++instrument_it;
        }
    }
    std::sort(bars.begin(), bars.end(), [](const BarSnapshot& lhs, const BarSnapshot& rhs) {
        if (lhs.period_end_ts_ns != rhs.period_end_ts_ns) {
            return lhs.period_end_ts_ns < rhs.period_end_ts_ns;
        }
        if (lhs.instrument_id != rhs.instrument_id) {
            return lhs.instrument_id < rhs.instrument_id;
        }
        return lhs.minute < rhs.minute;
    });
    return bars;
}

}  // namespace quant_hft
