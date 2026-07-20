#include "quant_hft/services/canonical_warmup_history.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace quant_hft {
namespace {

namespace fs = std::filesystem;

void SetError(std::string* error, const std::string& message) {
    if (error != nullptr) {
        *error = message;
    }
}

std::vector<std::string> SplitCsvLine(const std::string& line) {
    std::vector<std::string> fields;
    std::size_t start = 0;
    while (start <= line.size()) {
        const std::size_t comma = line.find(',', start);
        if (comma == std::string::npos) {
            fields.push_back(line.substr(start));
            break;
        }
        fields.push_back(line.substr(start, comma - start));
        start = comma + 1;
    }
    return fields;
}

bool ParseInt64(std::string_view text, std::int64_t* value) {
    if (value == nullptr || text.empty()) {
        return false;
    }
    std::int64_t parsed = 0;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
        return false;
    }
    *value = parsed;
    return true;
}

bool ParseInt32(std::string_view text, std::int32_t* value) {
    std::int64_t parsed = 0;
    if (!ParseInt64(text, &parsed) || parsed < std::numeric_limits<std::int32_t>::min() ||
        parsed > std::numeric_limits<std::int32_t>::max()) {
        return false;
    }
    *value = static_cast<std::int32_t>(parsed);
    return true;
}

bool ParseDouble(std::string_view text, double* value) {
    if (value == nullptr || text.empty()) {
        return false;
    }
    std::string copy(text);
    char* end = nullptr;
    const double parsed = std::strtod(copy.c_str(), &end);
    if (end != copy.c_str() + copy.size() || !std::isfinite(parsed)) {
        return false;
    }
    *value = parsed;
    return true;
}

bool ParseFlag(std::string_view text, bool* value) {
    std::int32_t parsed = 0;
    if (value == nullptr || !ParseInt32(text, &parsed) || (parsed != 0 && parsed != 1)) {
        return false;
    }
    *value = parsed == 1;
    return true;
}

std::string StateFingerprint(const StateSnapshot7D& state) {
    std::ostringstream out;
    out.precision(17);
    out << state.instrument_id << '|' << state.timeframe_minutes << '|' << state.ts_ns << '|'
        << state.bar_open << '|' << state.bar_high << '|' << state.bar_low << '|' << state.bar_close
        << '|' << state.analysis_bar_open << '|' << state.analysis_bar_high << '|'
        << state.analysis_bar_low << '|' << state.analysis_bar_close << '|'
        << state.analysis_price_offset << '|' << state.bar_volume;
    return out.str();
}

template <std::size_t N>
bool HasColumns(const std::unordered_map<std::string, std::size_t>& columns,
                const char* const (&required)[N]) {
    for (const char* name : required) {
        if (columns.find(name) == columns.end()) {
            return false;
        }
    }
    return true;
}

bool HasCanonicalColumns(const std::unordered_map<std::string, std::size_t>& columns) {
    static constexpr const char* kRequired[] = {
        "instrument_id",
        "open",
        "high",
        "low",
        "close",
        "analysis_open",
        "analysis_high",
        "analysis_low",
        "analysis_close",
        "analysis_price_offset",
        "volume",
        "ts_ns",
        "period_end_ts_ns",
        "finalized_ts_ns",
        "expected_source_bars",
        "observed_source_bars",
        "is_complete",
        "is_session_endpoint",
        "strategy_eligible",
        "volume_complete",
        "has_conflict",
        "is_recovery_replay",
    };
    return HasColumns(columns, kRequired);
}

bool HasLegacyColumns(const std::unordered_map<std::string, std::size_t>& columns) {
    static constexpr const char* kRequired[] = {
        "instrument_id",
        "minute",
        "open",
        "high",
        "low",
        "close",
        "analysis_open",
        "analysis_high",
        "analysis_low",
        "analysis_close",
        "analysis_price_offset",
        "volume",
        "ts_ns",
    };
    return HasColumns(columns, kRequired);
}

std::string_view Field(const std::vector<std::string>& fields,
                       const std::unordered_map<std::string, std::size_t>& columns,
                       const char* name) {
    const auto it = columns.find(name);
    if (it == columns.end() || it->second >= fields.size()) {
        return {};
    }
    return fields[it->second];
}

bool ParseBaseState(const std::vector<std::string>& fields,
                    const std::unordered_map<std::string, std::size_t>& columns,
                    const CanonicalWarmupHistoryOptions& options, std::int32_t timeframe_minutes,
                    StateSnapshot7D* state) {
    if (state == nullptr || Field(fields, columns, "instrument_id") != options.instrument_id) {
        return false;
    }
    std::int64_t volume = 0;
    EpochNanos ts_ns = 0;
    if (!ParseInt64(Field(fields, columns, "volume"), &volume) || volume < 0 ||
        !ParseInt64(Field(fields, columns, "ts_ns"), &ts_ns) || ts_ns <= 0 ||
        (options.max_ts_ns > 0 && ts_ns > options.max_ts_ns)) {
        return false;
    }

    StateSnapshot7D parsed;
    parsed.instrument_id = options.instrument_id;
    parsed.timeframe_minutes = timeframe_minutes;
    parsed.ts_ns = ts_ns;
    parsed.bar_volume = static_cast<double>(volume);
    parsed.has_bar = true;
    if (!ParseDouble(Field(fields, columns, "open"), &parsed.bar_open) ||
        !ParseDouble(Field(fields, columns, "high"), &parsed.bar_high) ||
        !ParseDouble(Field(fields, columns, "low"), &parsed.bar_low) ||
        !ParseDouble(Field(fields, columns, "close"), &parsed.bar_close) ||
        !ParseDouble(Field(fields, columns, "analysis_open"), &parsed.analysis_bar_open) ||
        !ParseDouble(Field(fields, columns, "analysis_high"), &parsed.analysis_bar_high) ||
        !ParseDouble(Field(fields, columns, "analysis_low"), &parsed.analysis_bar_low) ||
        !ParseDouble(Field(fields, columns, "analysis_close"), &parsed.analysis_bar_close) ||
        !ParseDouble(Field(fields, columns, "analysis_price_offset"),
                     &parsed.analysis_price_offset)) {
        return false;
    }
    if (parsed.bar_low > parsed.bar_high || parsed.bar_open < parsed.bar_low ||
        parsed.bar_open > parsed.bar_high || parsed.bar_close < parsed.bar_low ||
        parsed.bar_close > parsed.bar_high || parsed.analysis_bar_low > parsed.analysis_bar_high ||
        parsed.analysis_bar_open < parsed.analysis_bar_low ||
        parsed.analysis_bar_open > parsed.analysis_bar_high ||
        parsed.analysis_bar_close < parsed.analysis_bar_low ||
        parsed.analysis_bar_close > parsed.analysis_bar_high) {
        return false;
    }
    *state = std::move(parsed);
    return true;
}

bool ParseCanonicalState(const std::vector<std::string>& fields,
                         const std::unordered_map<std::string, std::size_t>& columns,
                         const CanonicalWarmupHistoryOptions& options, StateSnapshot7D* state) {
    bool complete = false;
    bool endpoint = false;
    bool eligible = false;
    bool volume_complete = false;
    bool conflict = false;
    bool recovery_replay = false;
    std::int32_t expected = 0;
    std::int32_t observed = 0;
    std::int64_t volume = 0;
    EpochNanos ts_ns = 0;
    EpochNanos period_end_ts_ns = 0;
    EpochNanos finalized_ts_ns = 0;
    if (!ParseFlag(Field(fields, columns, "is_complete"), &complete) ||
        !ParseFlag(Field(fields, columns, "is_session_endpoint"), &endpoint) ||
        !ParseFlag(Field(fields, columns, "strategy_eligible"), &eligible) ||
        !ParseFlag(Field(fields, columns, "volume_complete"), &volume_complete) ||
        !ParseFlag(Field(fields, columns, "has_conflict"), &conflict) ||
        !ParseFlag(Field(fields, columns, "is_recovery_replay"), &recovery_replay) ||
        !ParseInt32(Field(fields, columns, "expected_source_bars"), &expected) ||
        !ParseInt32(Field(fields, columns, "observed_source_bars"), &observed) ||
        !ParseInt64(Field(fields, columns, "volume"), &volume) ||
        !ParseInt64(Field(fields, columns, "ts_ns"), &ts_ns) ||
        !ParseInt64(Field(fields, columns, "period_end_ts_ns"), &period_end_ts_ns) ||
        !ParseInt64(Field(fields, columns, "finalized_ts_ns"), &finalized_ts_ns)) {
        return false;
    }
    if (!complete || endpoint || !eligible || !volume_complete || conflict || recovery_replay ||
        expected != options.timeframe_minutes || observed != expected || volume < 0 || ts_ns <= 0 ||
        period_end_ts_ns <= 0 || finalized_ts_ns < period_end_ts_ns ||
        (options.max_ts_ns > 0 && ts_ns > options.max_ts_ns)) {
        return false;
    }

    return ParseBaseState(fields, columns, options, options.timeframe_minutes, state);
}

struct LegacyBar {
    std::string minute;
    StateSnapshot7D state;
};

bool IsLeapYear(int year) { return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0); }

int DaysInMonth(int year, int month) {
    static constexpr std::array<int, 12> kDays = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month < 1 || month > 12) {
        return 0;
    }
    if (month == 2 && IsLeapYear(year)) {
        return 29;
    }
    return kDays[static_cast<std::size_t>(month - 1)];
}

bool ParseMinuteKey(std::string_view text, int* year, int* month, int* day, int* hour,
                    int* minute) {
    if (year == nullptr || month == nullptr || day == nullptr || hour == nullptr ||
        minute == nullptr || text.size() != 14 || text[8] != ' ' || text[11] != ':') {
        return false;
    }
    const std::array<std::size_t, 12> digit_positions = {0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 12, 13};
    for (const std::size_t position : digit_positions) {
        if (text[position] < '0' || text[position] > '9') {
            return false;
        }
    }
    const auto digit = [&](std::size_t position) { return text[position] - '0'; };
    *year = digit(0) * 1000 + digit(1) * 100 + digit(2) * 10 + digit(3);
    *month = digit(4) * 10 + digit(5);
    *day = digit(6) * 10 + digit(7);
    *hour = digit(9) * 10 + digit(10);
    *minute = digit(12) * 10 + digit(13);
    return *year > 0 && *month >= 1 && *month <= 12 && *day >= 1 &&
           *day <= DaysInMonth(*year, *month) && *hour >= 0 && *hour < 24 && *minute >= 0 &&
           *minute < 60;
}

bool AddMinutesToKey(std::string_view text, int delta, std::string* result) {
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    if (result == nullptr || delta < 0 ||
        !ParseMinuteKey(text, &year, &month, &day, &hour, &minute)) {
        return false;
    }
    minute += delta;
    hour += minute / 60;
    minute %= 60;
    while (hour >= 24) {
        hour -= 24;
        ++day;
        if (day > DaysInMonth(year, month)) {
            day = 1;
            ++month;
            if (month > 12) {
                month = 1;
                ++year;
            }
        }
    }
    char buffer[32]{};
    std::snprintf(buffer, sizeof(buffer), "%04d%02d%02d %02d:%02d", year, month, day, hour, minute);
    *result = buffer;
    return true;
}

bool ParseLegacyBar(const std::vector<std::string>& fields,
                    const std::unordered_map<std::string, std::size_t>& columns,
                    const CanonicalWarmupHistoryOptions& options, std::int32_t timeframe_minutes,
                    LegacyBar* bar) {
    if (bar == nullptr ||
        !ParseBaseState(fields, columns, options, timeframe_minutes, &bar->state)) {
        return false;
    }
    bar->minute = std::string(Field(fields, columns, "minute"));
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    return ParseMinuteKey(bar->minute, &year, &month, &day, &hour, &minute);
}

std::string LegacyFingerprint(const LegacyBar& bar) {
    return bar.minute + '|' + StateFingerprint(bar.state);
}

bool LoadOneMinuteBars(const fs::path& path, const CanonicalWarmupHistoryOptions& options,
                       std::unordered_map<std::string, LegacyBar>* bars) {
    if (bars == nullptr) {
        return false;
    }
    bars->clear();
    std::ifstream input(path);
    std::string line;
    if (!input.is_open() || !std::getline(input, line)) {
        return false;
    }
    const std::vector<std::string> header = SplitCsvLine(line);
    std::unordered_map<std::string, std::size_t> columns;
    for (std::size_t i = 0; i < header.size(); ++i) {
        columns[header[i]] = i;
    }
    if (!HasLegacyColumns(columns)) {
        return false;
    }
    const bool canonical = HasCanonicalColumns(columns);
    std::unordered_map<std::string, std::string> fingerprints;
    std::unordered_set<std::string> conflicted_minutes;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        const std::vector<std::string> fields = SplitCsvLine(line);
        LegacyBar bar;
        bool valid = false;
        if (canonical) {
            CanonicalWarmupHistoryOptions one_minute_options = options;
            one_minute_options.timeframe_minutes = 1;
            valid = ParseCanonicalState(fields, columns, one_minute_options, &bar.state);
            bar.minute = std::string(Field(fields, columns, "minute"));
            int year = 0;
            int month = 0;
            int day = 0;
            int hour = 0;
            int minute = 0;
            valid = valid && ParseMinuteKey(bar.minute, &year, &month, &day, &hour, &minute);
        } else {
            valid = ParseLegacyBar(fields, columns, options, 1, &bar);
        }
        if (!valid || conflicted_minutes.find(bar.minute) != conflicted_minutes.end()) {
            continue;
        }
        const std::string fingerprint = LegacyFingerprint(bar);
        const auto fingerprint_it = fingerprints.find(bar.minute);
        if (fingerprint_it == fingerprints.end()) {
            fingerprints.emplace(bar.minute, fingerprint);
            bars->emplace(bar.minute, std::move(bar));
        } else if (fingerprint_it->second != fingerprint) {
            bars->erase(bar.minute);
            fingerprints.erase(fingerprint_it);
            conflicted_minutes.insert(bar.minute);
        }
    }
    return true;
}

bool NearlyEqual(double lhs, double rhs) {
    const double scale = std::max({1.0, std::abs(lhs), std::abs(rhs)});
    return std::abs(lhs - rhs) <= std::numeric_limits<double>::epsilon() * 8.0 * scale;
}

bool ValidateLegacyFiveMinuteBar(
    const LegacyBar& target, const std::unordered_map<std::string, LegacyBar>& one_minute_bars) {
    constexpr std::size_t kSourceBarCount = 5;
    std::array<const LegacyBar*, kSourceBarCount> sources{};
    for (std::size_t i = 0; i < sources.size(); ++i) {
        std::string minute;
        if (!AddMinutesToKey(target.minute, static_cast<int>(i), &minute)) {
            return false;
        }
        const auto source_it = one_minute_bars.find(minute);
        if (source_it == one_minute_bars.end()) {
            return false;
        }
        sources[i] = &source_it->second;
        if (i > 0 && sources[i]->state.ts_ns <= sources[i - 1]->state.ts_ns) {
            return false;
        }
    }

    double high = sources.front()->state.bar_high;
    double low = sources.front()->state.bar_low;
    double analysis_high = sources.front()->state.analysis_bar_high;
    double analysis_low = sources.front()->state.analysis_bar_low;
    double volume = 0.0;
    for (const LegacyBar* source : sources) {
        high = std::max(high, source->state.bar_high);
        low = std::min(low, source->state.bar_low);
        analysis_high = std::max(analysis_high, source->state.analysis_bar_high);
        analysis_low = std::min(analysis_low, source->state.analysis_bar_low);
        volume += source->state.bar_volume;
        if (!NearlyEqual(source->state.analysis_price_offset, target.state.analysis_price_offset)) {
            return false;
        }
    }

    const StateSnapshot7D& first = sources.front()->state;
    const StateSnapshot7D& last = sources.back()->state;
    return target.state.ts_ns == last.ts_ns && NearlyEqual(target.state.bar_open, first.bar_open) &&
           NearlyEqual(target.state.bar_high, high) && NearlyEqual(target.state.bar_low, low) &&
           NearlyEqual(target.state.bar_close, last.bar_close) &&
           NearlyEqual(target.state.analysis_bar_open, first.analysis_bar_open) &&
           NearlyEqual(target.state.analysis_bar_high, analysis_high) &&
           NearlyEqual(target.state.analysis_bar_low, analysis_low) &&
           NearlyEqual(target.state.analysis_bar_close, last.analysis_bar_close) &&
           NearlyEqual(target.state.bar_volume, volume);
}

}  // namespace

bool LoadCanonicalWarmupHistory(const CanonicalWarmupHistoryOptions& options,
                                CanonicalWarmupHistoryResult* result, std::string* error) {
    if (result == nullptr) {
        SetError(error, "canonical warmup result pointer is null");
        return false;
    }
    *result = CanonicalWarmupHistoryResult{};
    if (options.market_data_root.empty() || options.product_id.empty() ||
        options.instrument_id.empty() || options.timeframe_minutes <= 0 || options.limit == 0) {
        SetError(error, "canonical warmup options are incomplete");
        return false;
    }

    std::error_code ec;
    const fs::path root(options.market_data_root);
    if (!fs::exists(root, ec)) {
        return true;
    }
    if (ec || !fs::is_directory(root, ec)) {
        SetError(error, "canonical warmup market data root is not a directory: " + root.string());
        return false;
    }

    std::vector<fs::path> files;
    for (fs::directory_iterator it(root, fs::directory_options::skip_permission_denied, ec), end;
         it != end; it.increment(ec)) {
        if (ec) {
            SetError(error, "failed to enumerate canonical warmup root: " + ec.message());
            return false;
        }
        if (!it->is_directory(ec) || it->path().filename().string().rfind("trading_day=", 0) != 0) {
            continue;
        }
        const fs::path path = it->path() / "varieties" / options.product_id / "market" /
                              ("bars_" + std::to_string(options.timeframe_minutes) + "m.csv");
        if (fs::is_regular_file(path, ec)) {
            files.push_back(path);
        }
    }
    std::sort(files.begin(), files.end());

    std::map<EpochNanos, StateSnapshot7D> accepted;
    std::unordered_map<EpochNanos, std::string> fingerprints;
    std::unordered_set<EpochNanos> conflicted_timestamps;
    for (const fs::path& path : files) {
        ++result->files_scanned;
        std::ifstream input(path);
        std::string line;
        if (!input.is_open() || !std::getline(input, line)) {
            continue;
        }
        const std::vector<std::string> header = SplitCsvLine(line);
        std::unordered_map<std::string, std::size_t> columns;
        for (std::size_t i = 0; i < header.size(); ++i) {
            columns[header[i]] = i;
        }
        const bool canonical = HasCanonicalColumns(columns);
        const bool legacy =
            !canonical && HasLegacyColumns(columns) && options.timeframe_minutes == 5;
        std::unordered_map<std::string, LegacyBar> one_minute_bars;
        if (!canonical && (!legacy || !LoadOneMinuteBars(path.parent_path() / "bars_1m.csv",
                                                         options, &one_minute_bars))) {
            continue;
        }
        while (std::getline(input, line)) {
            if (line.empty()) {
                continue;
            }
            ++result->rows_scanned;
            StateSnapshot7D state;
            const std::vector<std::string> fields = SplitCsvLine(line);
            bool valid = false;
            if (canonical) {
                valid = ParseCanonicalState(fields, columns, options, &state);
            } else {
                LegacyBar legacy_bar;
                valid = ParseLegacyBar(fields, columns, options, options.timeframe_minutes,
                                       &legacy_bar) &&
                        ValidateLegacyFiveMinuteBar(legacy_bar, one_minute_bars);
                if (valid) {
                    state = std::move(legacy_bar.state);
                    ++result->legacy_rows_validated;
                }
            }
            if (!valid) {
                ++result->rows_rejected;
                continue;
            }
            if (conflicted_timestamps.find(state.ts_ns) != conflicted_timestamps.end()) {
                ++result->rows_rejected;
                continue;
            }
            const std::string fingerprint = StateFingerprint(state);
            const auto fingerprint_it = fingerprints.find(state.ts_ns);
            if (fingerprint_it != fingerprints.end()) {
                if (fingerprint_it->second != fingerprint) {
                    accepted.erase(state.ts_ns);
                    fingerprints.erase(fingerprint_it);
                    conflicted_timestamps.insert(state.ts_ns);
                    ++result->conflicting_rows;
                    ++result->rows_rejected;
                }
                continue;
            }
            fingerprints.emplace(state.ts_ns, fingerprint);
            accepted.emplace(state.ts_ns, std::move(state));
        }
    }

    const std::size_t offset =
        accepted.size() > options.limit ? accepted.size() - options.limit : 0;
    std::size_t index = 0;
    for (auto& [ts_ns, state] : accepted) {
        (void)ts_ns;
        if (index++ >= offset) {
            result->states.push_back(std::move(state));
        }
    }
    result->rows_accepted = result->states.size();
    return true;
}

}  // namespace quant_hft
