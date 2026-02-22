#include "quant_hft/services/bar_aggregator.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstdio>
#include <fstream>
#include <unordered_map>

namespace quant_hft {
namespace {

bool IsFinitePositive(double value) {
    return std::isfinite(value) && value > 0.0;
}

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
    std::transform(value.begin(),
                   value.end(),
                   value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    return value;
}

std::string Trim(const std::string& value) {
    std::size_t start = 0;
    while (start < value.size() &&
           std::isspace(static_cast<unsigned char>(value[start])) != 0) {
        ++start;
    }
    std::size_t end = value.size();
    while (end > start &&
           std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
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
    if (minute_of_day == nullptr || raw.size() != 5 || raw[2] != ':') {
        return false;
    }
    if (!std::isdigit(static_cast<unsigned char>(raw[0])) ||
        !std::isdigit(static_cast<unsigned char>(raw[1])) ||
        !std::isdigit(static_cast<unsigned char>(raw[3])) ||
        !std::isdigit(static_cast<unsigned char>(raw[4]))) {
        return false;
    }
    const int hour = (raw[0] - '0') * 10 + (raw[1] - '0');
    const int minute = (raw[3] - '0') * 10 + (raw[4] - '0');
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        return false;
    }
    *minute_of_day = hour * 60 + minute;
    return true;
}

bool ParseSessionRange(const std::string& raw,
                       BarAggregator::SessionInterval* interval) {
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

bool IsMinuteInInterval(const BarAggregator::SessionInterval& interval, int minute_of_day) {
    if (interval.start_minute <= interval.end_minute) {
        return minute_of_day >= interval.start_minute && minute_of_day <= interval.end_minute;
    }
    return minute_of_day >= interval.start_minute || minute_of_day <= interval.end_minute;
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

    std::unordered_map<std::string, std::vector<SessionRule>> rules;
    rules["SHFE"].push_back(
        with_intervals({SessionInterval{9 * 60, 15 * 60}, SessionInterval{21 * 60, 1 * 60}}));
    rules["DCE"].push_back(with_intervals(
        {SessionInterval{9 * 60, 15 * 60}, SessionInterval{21 * 60, 23 * 60}}));
    rules["CFFEX"].push_back(with_intervals({SessionInterval{9 * 60 + 30, 15 * 60}}));
    rules["CFFEX"].push_back(
        with_intervals({SessionInterval{9 * 60 + 30, 15 * 60 + 15}}, "T"));
    rules["CFFEX"].push_back(
        with_intervals({SessionInterval{9 * 60 + 30, 15 * 60 + 15}}, "TF"));
    rules["CFFEX"].push_back(
        with_intervals({SessionInterval{9 * 60 + 30, 15 * 60 + 15}}, "TS"));
    rules["CFFEX"].push_back(
        with_intervals({SessionInterval{9 * 60 + 30, 15 * 60 + 15}}, "TL"));
    rules["*"].push_back(with_intervals(
        {SessionInterval{9 * 60, 15 * 60 + 15}, SessionInterval{21 * 60, 2 * 60 + 30}}));
    return rules;
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
    : config_(std::move(config)),
      session_rules_by_exchange_(config_.use_default_session_fallback
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

    if (config_.filter_non_trading_ticks &&
        !IsInTradingSession(ResolveExchangeId(snapshot),
                            snapshot.instrument_id,
                            ResolveProductCode(snapshot),
                            snapshot.update_time)) {
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

void BarAggregator::ResetInstrument(const std::string& instrument_id) {
    if (instrument_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    buckets_.erase(instrument_id);
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

bool BarAggregator::IsInTradingSession(const std::string& exchange_id,
                                       const std::string& update_time) const {
    return IsInTradingSession(exchange_id, "", "", update_time);
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

std::string BarAggregator::ResolveExchangeId(const MarketSnapshot& snapshot) {
    if (!snapshot.exchange_id.empty()) {
        return snapshot.exchange_id;
    }
    return InferExchangeIdFromDottedInstrument(snapshot.instrument_id);
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
                                       const std::string& instrument_id,
                                       const std::string& product,
                                       const std::string& update_time) const {
    int minute_of_day = 0;
    if (!ParseMinuteOfDay(update_time, &minute_of_day)) {
        return false;
    }

    auto evaluate_rules = [&](const std::vector<SessionRule>& rules) {
        if (instrument_id.empty() && product.empty()) {
            for (const auto& rule : rules) {
                for (const auto& interval : rule.intervals) {
                    if (IsMinuteInInterval(interval, minute_of_day)) {
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
            const bool prefix_match = prefix_upper.empty() || StartsWith(symbol_upper, prefix_upper);
            const bool product_match = product_upper.empty() || product_upper == current_product;
            if (!prefix_match || !product_match) {
                continue;
            }
            has_specific_match = true;
            for (const auto& interval : rule.intervals) {
                if (IsMinuteInInterval(interval, minute_of_day)) {
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
            for (const auto& interval : rule.intervals) {
                if (IsMinuteInInterval(interval, minute_of_day)) {
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

    auto to_rule = [](const PendingSession& pending,
                      std::string* exchange,
                      SessionRule* rule) -> bool {
        if (exchange == nullptr || rule == nullptr || pending.exchange.empty()) {
            return false;
        }
        rule->instrument_prefix = ToUpperAscii(pending.instrument_prefix);
        rule->product = pending.product;
        rule->intervals.clear();

        SessionInterval day_interval;
        if (ParseSessionRange(pending.day, &day_interval)) {
            rule->intervals.push_back(day_interval);
        }
        SessionInterval night_interval;
        if (ParseSessionRange(pending.night, &night_interval)) {
            rule->intervals.push_back(night_interval);
        }
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
