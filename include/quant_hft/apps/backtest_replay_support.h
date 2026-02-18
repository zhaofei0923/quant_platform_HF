#pragma once

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <queue>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/backtest/parquet_data_feed.h"
#include "quant_hft/common/timestamp.h"
#include "quant_hft/services/market_state_detector.h"
#include "quant_hft/strategy/demo_live_strategy.h"

namespace quant_hft::apps {

namespace detail {

constexpr EpochNanos kNanosPerSecond = 1'000'000'000LL;
constexpr EpochNanos kNanosPerMillisecond = 1'000'000LL;
constexpr EpochNanos kNanosPerMinute = 60LL * kNanosPerSecond;

inline std::string ToLower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return text;
}

inline std::string Trim(std::string text) {
    const auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    while (!text.empty() && is_space(static_cast<unsigned char>(text.front()))) {
        text.erase(text.begin());
    }
    while (!text.empty() && is_space(static_cast<unsigned char>(text.back()))) {
        text.pop_back();
    }
    return text;
}

inline std::uint64_t Fnv1a64(std::uint64_t seed, std::string_view text) {
    std::uint64_t hash = seed;
    for (unsigned char ch : text) {
        hash ^= static_cast<std::uint64_t>(ch);
        hash *= 1099511628211ULL;
    }
    return hash;
}

inline std::string HexDigest64(std::uint64_t value) {
    std::ostringstream oss;
    oss << std::hex << std::setw(16) << std::setfill('0') << value;
    return oss.str();
}

inline std::string StableDigest(std::string_view text) {
    return HexDigest64(Fnv1a64(14695981039346656037ULL, text));
}

inline std::string GetArgAny(const ArgMap& args, std::initializer_list<const char*> keys,
                             const std::string& fallback = "") {
    for (const char* key : keys) {
        const auto it = args.find(key);
        if (it != args.end()) {
            return it->second;
        }
    }
    return fallback;
}

inline bool ParseBool(const std::string& raw, bool* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string normalized = ToLower(Trim(raw));
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

inline bool ParseInt64(const std::string& raw, std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string text = Trim(raw);
    if (text.empty()) {
        return false;
    }
    try {
        std::size_t parsed = 0;
        const std::int64_t value = std::stoll(text, &parsed);
        if (parsed != text.size()) {
            return false;
        }
        *out = value;
        return true;
    } catch (...) {
        return false;
    }
}

inline bool ParseDouble(const std::string& raw, double* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string text = Trim(raw);
    if (text.empty()) {
        return false;
    }
    try {
        std::size_t parsed = 0;
        const double value = std::stod(text, &parsed);
        if (parsed != text.size()) {
            return false;
        }
        *out = value;
        return true;
    } catch (...) {
        return false;
    }
}

inline std::string StripInlineComment(const std::string& line) {
    bool in_single_quote = false;
    bool in_double_quote = false;
    for (std::size_t index = 0; index < line.size(); ++index) {
        const char ch = line[index];
        if (ch == '\'' && !in_double_quote) {
            in_single_quote = !in_single_quote;
            continue;
        }
        if (ch == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
            continue;
        }
        if (ch == '#' && !in_single_quote && !in_double_quote) {
            return line.substr(0, index);
        }
    }
    return line;
}

inline bool LoadYamlScalarMap(const std::filesystem::path& path,
                              std::map<std::string, std::string>* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "yaml output is null";
        }
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open detector config file: " + path.string();
        }
        return false;
    }

    out->clear();
    std::vector<std::pair<int, std::string>> scope_stack;
    std::string line;
    while (std::getline(input, line)) {
        const std::string no_comment = StripInlineComment(line);
        const std::string trimmed = Trim(no_comment);
        if (trimmed.empty() || trimmed.front() == '-') {
            continue;
        }

        const auto first_non_space = no_comment.find_first_not_of(' ');
        const int indent =
            first_non_space == std::string::npos ? 0 : static_cast<int>(first_non_space);
        while (!scope_stack.empty() && indent <= scope_stack.back().first) {
            scope_stack.pop_back();
        }

        const std::size_t colon = trimmed.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        const std::string key = Trim(trimmed.substr(0, colon));
        std::string value = Trim(trimmed.substr(colon + 1));
        if (key.empty()) {
            continue;
        }
        if (value.empty()) {
            scope_stack.emplace_back(indent, key);
            continue;
        }
        if (value.size() >= 2 && ((value.front() == '"' && value.back() == '"') ||
                                  (value.front() == '\'' && value.back() == '\''))) {
            value = value.substr(1, value.size() - 2);
        }

        std::ostringstream full_key;
        for (const auto& scope : scope_stack) {
            full_key << scope.second << '.';
        }
        full_key << key;
        (*out)[full_key.str()] = value;
    }
    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading detector config file: " + path.string();
        }
        return false;
    }
    return true;
}

inline bool ResolveDetectorYamlValue(const std::map<std::string, std::string>& values,
                                     const std::string& field, std::string* out) {
    if (out == nullptr) {
        return false;
    }
    const std::array<std::string, 3> keys = {
        "market_state_detector." + field,
        "ctp.market_state_detector." + field,
        field,
    };
    for (const std::string& key : keys) {
        const auto it = values.find(key);
        if (it != values.end()) {
            *out = it->second;
            return true;
        }
    }
    return false;
}

inline bool LoadMarketStateDetectorConfigFile(const std::string& config_path,
                                              MarketStateDetectorConfig* out_config,
                                              std::string* error) {
    if (out_config == nullptr) {
        if (error != nullptr) {
            *error = "detector config output is null";
        }
        return false;
    }

    std::map<std::string, std::string> yaml_values;
    if (!LoadYamlScalarMap(config_path, &yaml_values, error)) {
        return false;
    }

    MarketStateDetectorConfig config;
    auto parse_int = [&](const std::string& field, int* target) -> bool {
        std::string raw;
        if (!ResolveDetectorYamlValue(yaml_values, field, &raw)) {
            return true;
        }
        std::int64_t parsed = 0;
        if (!ParseInt64(raw, &parsed)) {
            if (error != nullptr) {
                *error = "invalid detector_config " + field + ": " + raw;
            }
            return false;
        }
        if (parsed < std::numeric_limits<int>::min() || parsed > std::numeric_limits<int>::max()) {
            if (error != nullptr) {
                *error = "detector_config " + field + " is out of int range: " + raw;
            }
            return false;
        }
        *target = static_cast<int>(parsed);
        return true;
    };
    auto parse_double = [&](const std::string& field, double* target) -> bool {
        std::string raw;
        if (!ResolveDetectorYamlValue(yaml_values, field, &raw)) {
            return true;
        }
        double parsed = 0.0;
        if (!ParseDouble(raw, &parsed)) {
            if (error != nullptr) {
                *error = "invalid detector_config " + field + ": " + raw;
            }
            return false;
        }
        *target = parsed;
        return true;
    };
    auto parse_bool = [&](const std::string& field, bool* target) -> bool {
        std::string raw;
        if (!ResolveDetectorYamlValue(yaml_values, field, &raw)) {
            return true;
        }
        bool parsed = false;
        if (!ParseBool(raw, &parsed)) {
            if (error != nullptr) {
                *error = "invalid detector_config " + field + ": " + raw;
            }
            return false;
        }
        *target = parsed;
        return true;
    };

    if (!parse_int("adx_period", &config.adx_period) ||
        !parse_double("adx_strong_threshold", &config.adx_strong_threshold) ||
        !parse_double("adx_weak_lower", &config.adx_weak_lower) ||
        !parse_double("adx_weak_upper", &config.adx_weak_upper) ||
        !parse_int("kama_er_period", &config.kama_er_period) ||
        !parse_int("kama_fast_period", &config.kama_fast_period) ||
        !parse_int("kama_slow_period", &config.kama_slow_period) ||
        !parse_double("kama_er_strong", &config.kama_er_strong) ||
        !parse_double("kama_er_weak_lower", &config.kama_er_weak_lower) ||
        !parse_int("atr_period", &config.atr_period) ||
        !parse_double("atr_flat_ratio", &config.atr_flat_ratio) ||
        !parse_bool("require_adx_for_trend", &config.require_adx_for_trend) ||
        !parse_bool("use_kama_er", &config.use_kama_er) ||
        !parse_int("min_bars_for_flat", &config.min_bars_for_flat)) {
        return false;
    }

    try {
        (void)MarketStateDetector(config);
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = "invalid detector_config: " + std::string(ex.what());
        }
        return false;
    }

    *out_config = config;
    return true;
}

inline std::string NormalizeTradingDay(const std::string& raw) {
    std::string digits;
    digits.reserve(raw.size());
    for (unsigned char ch : raw) {
        if (std::isdigit(ch) != 0) {
            digits.push_back(static_cast<char>(ch));
        }
    }
    if (digits.size() != 8) {
        return "";
    }
    return digits;
}

inline bool BuildUtcTm(const std::string& normalized_day, int hour, int minute, int second,
                       std::tm* out_tm) {
    if (out_tm == nullptr || normalized_day.size() != 8) {
        return false;
    }
    try {
        const int year = std::stoi(normalized_day.substr(0, 4));
        const int month = std::stoi(normalized_day.substr(4, 2));
        const int day = std::stoi(normalized_day.substr(6, 2));
        std::tm tm = {};
        tm.tm_isdst = 0;
        tm.tm_year = year - 1900;
        tm.tm_mon = month - 1;
        tm.tm_mday = day;
        tm.tm_hour = hour;
        tm.tm_min = minute;
        tm.tm_sec = second;
        *out_tm = tm;
        return true;
    } catch (...) {
        return false;
    }
}

inline bool ParseTimeHms(const std::string& raw, int* hour, int* minute, int* second) {
    if (hour == nullptr || minute == nullptr || second == nullptr) {
        return false;
    }
    std::string digits;
    digits.reserve(raw.size());
    for (unsigned char ch : raw) {
        if (std::isdigit(ch) != 0) {
            digits.push_back(static_cast<char>(ch));
        }
    }
    if (digits.size() < 6) {
        return false;
    }
    try {
        *hour = std::stoi(digits.substr(0, 2));
        *minute = std::stoi(digits.substr(2, 2));
        *second = std::stoi(digits.substr(4, 2));
    } catch (...) {
        return false;
    }
    return *hour >= 0 && *hour <= 23 && *minute >= 0 && *minute <= 59 && *second >= 0 &&
           *second <= 60;
}

inline EpochNanos ToEpochNs(const std::string& trading_day, const std::string& update_time,
                            int update_millisec) {
    const std::string normalized_day = NormalizeTradingDay(trading_day);
    int hour = 0;
    int minute = 0;
    int second = 0;
    if (!ParseTimeHms(update_time, &hour, &minute, &second)) {
        return 0;
    }

    std::tm tm = {};
    if (!BuildUtcTm(normalized_day, hour, minute, second, &tm)) {
        return 0;
    }

    const std::time_t seconds = timegm(&tm);
    if (seconds < 0) {
        return 0;
    }

    const int millis = std::max(0, update_millisec);
    return static_cast<EpochNanos>(seconds) * kNanosPerSecond +
           static_cast<EpochNanos>(millis) * kNanosPerMillisecond;
}

inline std::string TradingDayFromEpochNs(EpochNanos ts_ns) {
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / kNanosPerSecond);
    std::tm tm = *gmtime(&seconds);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d");
    return oss.str();
}

inline std::string UpdateTimeFromEpochNs(EpochNanos ts_ns) {
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / kNanosPerSecond);
    std::tm tm = *gmtime(&seconds);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%H:%M:%S");
    return oss.str();
}

inline std::vector<std::string> SplitCsvLine(const std::string& line) {
    std::vector<std::string> cells;
    std::string current;
    bool in_quotes = false;
    for (char ch : line) {
        if (ch == '"') {
            in_quotes = !in_quotes;
            continue;
        }
        if (ch == ',' && !in_quotes) {
            cells.push_back(current);
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    cells.push_back(current);
    return cells;
}

inline std::vector<std::string> SplitCommaList(const std::string& raw) {
    std::vector<std::string> out;
    std::string current;
    for (const char ch : raw) {
        if (ch == ',') {
            const std::string trimmed = Trim(current);
            if (!trimmed.empty()) {
                out.push_back(trimmed);
            }
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    const std::string trimmed = Trim(current);
    if (!trimmed.empty()) {
        out.push_back(trimmed);
    }
    return out;
}

inline std::string FindCell(const std::map<std::string, std::size_t>& header_index,
                            const std::vector<std::string>& cells,
                            std::initializer_list<const char*> candidates) {
    for (const char* key : candidates) {
        const auto it = header_index.find(key);
        if (it != header_index.end() && it->second < cells.size()) {
            return cells[it->second];
        }
    }
    return "";
}

inline std::string InstrumentSymbolPrefix(const std::string& instrument_id) {
    std::string prefix;
    for (unsigned char ch : instrument_id) {
        if (std::isalpha(ch) == 0) {
            break;
        }
        prefix.push_back(static_cast<char>(std::tolower(ch)));
    }
    return prefix;
}

inline double Clamp01(double value) { return std::max(0.0, std::min(1.0, value)); }

inline std::string FormatDouble(double value) {
    std::ostringstream oss;
    oss << std::setprecision(12) << value;
    return oss.str();
}

inline bool WriteWalLine(std::ofstream* out, const std::string& line) {
    if (out == nullptr || !out->is_open()) {
        return true;
    }
    *out << line << '\n';
    return out->good();
}

inline std::size_t P95Index(std::size_t count) {
    if (count == 0) {
        return 0;
    }
    const double scaled = std::round(static_cast<double>(count - 1) * 0.95);
    const auto index = static_cast<std::size_t>(std::max(0.0, scaled));
    return std::min(index, count - 1);
}

inline double Mean(const std::vector<double>& values) {
    if (values.empty()) {
        return 0.0;
    }
    const double sum = std::accumulate(values.begin(), values.end(), 0.0);
    return sum / static_cast<double>(values.size());
}

inline bool ExtractJsonNumber(const std::string& json, const std::string& key, double* out_value) {
    if (out_value == nullptr) {
        return false;
    }
    const std::string quoted_key = "\"" + key + "\"";
    const std::size_t key_pos = json.find(quoted_key);
    if (key_pos == std::string::npos) {
        return false;
    }
    const std::size_t colon_pos = json.find(':', key_pos + quoted_key.size());
    if (colon_pos == std::string::npos) {
        return false;
    }

    std::size_t value_start = colon_pos + 1;
    while (value_start < json.size() &&
           std::isspace(static_cast<unsigned char>(json[value_start])) != 0) {
        ++value_start;
    }

    std::size_t value_end = value_start;
    while (value_end < json.size()) {
        const char ch = json[value_end];
        if ((ch >= '0' && ch <= '9') || ch == '-' || ch == '+' || ch == '.' || ch == 'e' ||
            ch == 'E') {
            ++value_end;
            continue;
        }
        break;
    }

    if (value_end <= value_start) {
        return false;
    }

    return ParseDouble(json.substr(value_start, value_end - value_start), out_value);
}

inline bool ExtractJsonString(const std::string& json, const std::string& key,
                              std::string* out_value) {
    if (out_value == nullptr) {
        return false;
    }
    const std::string quoted_key = "\"" + key + "\"";
    const std::size_t key_pos = json.find(quoted_key);
    if (key_pos == std::string::npos) {
        return false;
    }
    const std::size_t colon_pos = json.find(':', key_pos + quoted_key.size());
    if (colon_pos == std::string::npos) {
        return false;
    }

    std::size_t value_pos = colon_pos + 1;
    while (value_pos < json.size() &&
           std::isspace(static_cast<unsigned char>(json[value_pos])) != 0) {
        ++value_pos;
    }
    if (value_pos >= json.size() || json[value_pos] != '"') {
        return false;
    }
    ++value_pos;

    std::string value;
    value.reserve(32);
    bool escaped = false;
    while (value_pos < json.size()) {
        const char ch = json[value_pos++];
        if (escaped) {
            switch (ch) {
                case '"':
                    value.push_back('"');
                    break;
                case '\\':
                    value.push_back('\\');
                    break;
                case 'n':
                    value.push_back('\n');
                    break;
                case 'r':
                    value.push_back('\r');
                    break;
                case 't':
                    value.push_back('\t');
                    break;
                default:
                    value.push_back(ch);
                    break;
            }
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
            continue;
        }
        if (ch == '"') {
            *out_value = value;
            return true;
        }
        value.push_back(ch);
    }
    return false;
}

inline bool ExtractJsonBool(const std::string& json, const std::string& key, bool* out_value) {
    if (out_value == nullptr) {
        return false;
    }
    const std::string quoted_key = "\"" + key + "\"";
    const std::size_t key_pos = json.find(quoted_key);
    if (key_pos == std::string::npos) {
        return false;
    }
    const std::size_t colon_pos = json.find(':', key_pos + quoted_key.size());
    if (colon_pos == std::string::npos) {
        return false;
    }
    std::size_t value_start = colon_pos + 1;
    while (value_start < json.size() &&
           std::isspace(static_cast<unsigned char>(json[value_start])) != 0) {
        ++value_start;
    }
    if (value_start + 4 <= json.size() && json.compare(value_start, 4, "true") == 0) {
        *out_value = true;
        return true;
    }
    if (value_start + 5 <= json.size() && json.compare(value_start, 5, "false") == 0) {
        *out_value = false;
        return true;
    }
    return false;
}

}  // namespace detail

struct BacktestCliSpec {
    std::string csv_path;
    std::string dataset_root;
    std::string dataset_manifest;
    std::string detector_config_path;
    std::string engine_mode{"csv"};
    std::string rollover_mode{"strict"};
    std::string rollover_price_mode{"bbo"};
    double rollover_slippage_bps{0.0};
    std::vector<std::string> symbols;
    std::string start_date;
    std::string end_date;
    std::optional<std::int64_t> max_ticks;
    bool deterministic_fills{true};
    bool streaming{true};
    bool strict_parquet{true};
    std::string wal_path;
    std::string account_id{"sim-account"};
    std::string run_id;
    bool emit_state_snapshots{false};
    MarketStateDetectorConfig detector_config{};
};

struct ReplayTick {
    std::string trading_day;
    std::string instrument_id;
    std::string update_time;
    int update_millisec{0};
    EpochNanos ts_ns{0};
    double last_price{0.0};
    std::int64_t volume{0};
    double bid_price_1{0.0};
    std::int64_t bid_volume_1{0};
    double ask_price_1{0.0};
    std::int64_t ask_volume_1{0};
};

struct ReplayReport {
    std::int64_t ticks_read{0};
    std::int64_t scan_rows{0};
    std::int64_t scan_row_groups{0};
    std::int64_t io_bytes{0};
    bool early_stop_hit{false};
    std::int64_t bars_emitted{0};
    std::int64_t intents_emitted{0};
    std::string first_instrument;
    std::string last_instrument;
    std::int64_t instrument_count{0};
    std::vector<std::string> instrument_universe;
    EpochNanos first_ts_ns{0};
    EpochNanos last_ts_ns{0};
};

struct InstrumentPnlSnapshot {
    std::int32_t net_position{0};
    double avg_open_price{0.0};
    double realized_pnl{0.0};
    double unrealized_pnl{0.0};
    double last_price{0.0};
};

struct BacktestPerformanceSummary {
    double total_realized_pnl{0.0};
    double total_unrealized_pnl{0.0};
    double total_pnl{0.0};
    double max_equity{0.0};
    double min_equity{0.0};
    double max_drawdown{0.0};
    std::map<std::string, std::int64_t> order_status_counts;
};

struct RolloverEvent {
    std::string symbol;
    std::string from_instrument;
    std::string to_instrument;
    std::string mode;
    std::int32_t position{0};
    std::string direction;
    double from_price{0.0};
    double to_price{0.0};
    std::int32_t canceled_orders{0};
    std::string price_mode;
    double slippage_bps{0.0};
    EpochNanos ts_ns{0};
};

struct RolloverAction {
    std::string symbol;
    std::string action;
    std::string from_instrument;
    std::string to_instrument;
    std::int32_t position{0};
    std::string side;
    double price{0.0};
    std::string mode;
    std::string price_mode;
    double slippage_bps{0.0};
    std::int32_t canceled_orders{0};
    EpochNanos ts_ns{0};
};

struct DeterministicReplayReport {
    ReplayReport replay;
    std::int64_t intents_processed{0};
    std::int64_t order_events_emitted{0};
    std::int64_t wal_records{0};
    std::map<std::string, std::int64_t> instrument_bars;
    std::map<std::string, InstrumentPnlSnapshot> instrument_pnl;
    double total_realized_pnl{0.0};
    double total_unrealized_pnl{0.0};
    BacktestPerformanceSummary performance;
    std::vector<std::string> invariant_violations;
    std::vector<RolloverEvent> rollover_events;
    std::vector<RolloverAction> rollover_actions;
    double rollover_slippage_cost{0.0};
    std::int64_t rollover_canceled_orders{0};
};

struct BacktestCliResult {
    std::string run_id;
    std::string mode;
    std::string data_source;
    std::string engine_mode;
    std::string rollover_mode;
    BacktestCliSpec spec;
    std::string input_signature;
    std::string data_signature;
    ReplayReport replay;
    bool has_deterministic{false};
    DeterministicReplayReport deterministic;
};

struct BacktestSummary {
    std::int64_t intents_emitted{0};
    std::int64_t order_events{0};
    double total_pnl{0.0};
    double max_drawdown{0.0};
};

inline bool IsApproxEqual(double left, double right, double abs_tol = 1e-8, double rel_tol = 1e-6) {
    const double diff = std::fabs(left - right);
    if (diff <= abs_tol) {
        return true;
    }
    const double scale = std::max(std::fabs(left), std::fabs(right));
    return scale > 0.0 && (diff / scale) <= rel_tol;
}

inline bool ParseBacktestCliSpec(const ArgMap& args, BacktestCliSpec* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "spec output is null";
        }
        return false;
    }

    BacktestCliSpec spec;
    spec.csv_path = detail::GetArgAny(args, {"csv_path", "csv-path", "csv"});
    spec.dataset_root =
        detail::GetArgAny(args, {"dataset_root", "dataset-root", "parquet_root", "parquet-root"});
    spec.dataset_manifest = detail::GetArgAny(
        args, {"dataset_manifest", "dataset-manifest", "manifest_path", "manifest-path"});
    spec.detector_config_path = detail::GetArgAny(args, {"detector_config", "detector-config"});
    spec.engine_mode =
        detail::ToLower(detail::GetArgAny(args, {"engine_mode", "engine-mode"}, "csv"));
    spec.rollover_mode =
        detail::ToLower(detail::GetArgAny(args, {"rollover_mode", "rollover-mode"}, "strict"));
    spec.rollover_price_mode = detail::ToLower(
        detail::GetArgAny(args, {"rollover_price_mode", "rollover-price-mode"}, "bbo"));
    spec.start_date =
        detail::NormalizeTradingDay(detail::GetArgAny(args, {"start_date", "start-date"}));
    spec.end_date = detail::NormalizeTradingDay(detail::GetArgAny(args, {"end_date", "end-date"}));
    spec.wal_path = detail::GetArgAny(args, {"wal_path", "wal-path"});
    spec.account_id = detail::GetArgAny(args, {"account_id", "account-id"}, "sim-account");
    spec.run_id = detail::GetArgAny(args, {"run_id", "run-id"},
                                    "backtest-" + std::to_string(UnixEpochMillisNow()));
    spec.symbols = detail::SplitCommaList(detail::GetArgAny(args, {"symbols", "symbol"}));

    {
        const std::string raw =
            detail::GetArgAny(args, {"rollover_slippage_bps", "rollover-slippage-bps"}, "0");
        double parsed = 0.0;
        if (!detail::ParseDouble(raw, &parsed)) {
            if (error != nullptr) {
                *error = "invalid rollover_slippage_bps: " + raw;
            }
            return false;
        }
        spec.rollover_slippage_bps = parsed;
    }

    {
        const std::string raw_max_ticks = detail::GetArgAny(args, {"max_ticks", "max-ticks"});
        if (!raw_max_ticks.empty()) {
            std::int64_t parsed = 0;
            if (!detail::ParseInt64(raw_max_ticks, &parsed)) {
                if (error != nullptr) {
                    *error = "invalid max_ticks: " + raw_max_ticks;
                }
                return false;
            }
            if (parsed > 0) {
                spec.max_ticks = parsed;
            } else if (parsed < 0) {
                if (error != nullptr) {
                    *error = "max_ticks must be non-negative";
                }
                return false;
            }
        }
    }

    {
        const std::string raw_det =
            detail::GetArgAny(args, {"deterministic_fills", "deterministic-fills"}, "true");
        bool parsed = true;
        if (!detail::ParseBool(raw_det, &parsed)) {
            if (error != nullptr) {
                *error = "invalid deterministic_fills: " + raw_det;
            }
            return false;
        }
        spec.deterministic_fills = parsed;
    }

    {
        const std::string raw_emit =
            detail::GetArgAny(args, {"emit_state_snapshots", "emit-state-snapshots"}, "false");
        bool parsed = false;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_state_snapshots: " + raw_emit;
            }
            return false;
        }
        spec.emit_state_snapshots = parsed;
    }
    {
        const std::string raw_streaming =
            detail::GetArgAny(args, {"streaming", "streaming_mode", "streaming-mode"}, "true");
        bool parsed = true;
        if (!detail::ParseBool(raw_streaming, &parsed)) {
            if (error != nullptr) {
                *error = "invalid streaming: " + raw_streaming;
            }
            return false;
        }
        spec.streaming = parsed;
    }
    {
        const std::string raw_strict =
            detail::GetArgAny(args, {"strict_parquet", "strict-parquet"}, "true");
        bool parsed = true;
        if (!detail::ParseBool(raw_strict, &parsed)) {
            if (error != nullptr) {
                *error = "invalid strict_parquet: " + raw_strict;
            }
            return false;
        }
        spec.strict_parquet = parsed;
    }

    if (spec.engine_mode != "csv" && spec.engine_mode != "parquet" &&
        spec.engine_mode != "core_sim") {
        if (error != nullptr) {
            *error = "unsupported engine_mode: " + spec.engine_mode;
        }
        return false;
    }
    if (spec.rollover_mode != "strict" && spec.rollover_mode != "carry") {
        if (error != nullptr) {
            *error = "unsupported rollover_mode: " + spec.rollover_mode;
        }
        return false;
    }
    if (spec.rollover_price_mode != "bbo" && spec.rollover_price_mode != "mid" &&
        spec.rollover_price_mode != "last") {
        if (error != nullptr) {
            *error = "unsupported rollover_price_mode: " + spec.rollover_price_mode;
        }
        return false;
    }
    if (spec.rollover_slippage_bps < 0.0) {
        if (error != nullptr) {
            *error = "rollover_slippage_bps must be non-negative";
        }
        return false;
    }

    if (spec.engine_mode == "csv" && spec.csv_path.empty()) {
        if (error != nullptr) {
            *error = "csv_path is required when engine_mode=csv";
        }
        return false;
    }
    if (spec.engine_mode == "parquet" && spec.dataset_root.empty()) {
        if (error != nullptr) {
            *error = "dataset_root is required when engine_mode=parquet";
        }
        return false;
    }
    if (spec.engine_mode == "core_sim" && spec.dataset_root.empty() && spec.csv_path.empty()) {
        if (error != nullptr) {
            *error = "core_sim requires dataset_root or csv_path";
        }
        return false;
    }
    if (!spec.dataset_root.empty() && spec.dataset_manifest.empty()) {
        spec.dataset_manifest =
            (std::filesystem::path(spec.dataset_root) / "_manifest" / "partitions.jsonl").string();
    }
    if (!spec.detector_config_path.empty() &&
        !detail::LoadMarketStateDetectorConfigFile(spec.detector_config_path, &spec.detector_config,
                                                   error)) {
        return false;
    }

    *out = std::move(spec);
    return true;
}

inline std::string BuildInputSignature(const BacktestCliSpec& spec) {
    std::ostringstream symbols_stream;
    for (std::size_t index = 0; index < spec.symbols.size(); ++index) {
        if (index > 0) {
            symbols_stream << ',';
        }
        symbols_stream << spec.symbols[index];
    }

    std::ostringstream oss;
    oss << "csv_path=" << spec.csv_path << ';' << "dataset_root=" << spec.dataset_root << ';'
        << "dataset_manifest=" << spec.dataset_manifest << ';'
        << "detector_config_path=" << spec.detector_config_path << ';'
        << "detector_config.adx_period=" << spec.detector_config.adx_period << ';'
        << "detector_config.adx_strong_threshold="
        << detail::FormatDouble(spec.detector_config.adx_strong_threshold) << ';'
        << "detector_config.adx_weak_lower="
        << detail::FormatDouble(spec.detector_config.adx_weak_lower) << ';'
        << "detector_config.adx_weak_upper="
        << detail::FormatDouble(spec.detector_config.adx_weak_upper) << ';'
        << "detector_config.kama_er_period=" << spec.detector_config.kama_er_period << ';'
        << "detector_config.kama_fast_period=" << spec.detector_config.kama_fast_period << ';'
        << "detector_config.kama_slow_period=" << spec.detector_config.kama_slow_period << ';'
        << "detector_config.kama_er_strong="
        << detail::FormatDouble(spec.detector_config.kama_er_strong) << ';'
        << "detector_config.kama_er_weak_lower="
        << detail::FormatDouble(spec.detector_config.kama_er_weak_lower) << ';'
        << "detector_config.atr_period=" << spec.detector_config.atr_period << ';'
        << "detector_config.atr_flat_ratio="
        << detail::FormatDouble(spec.detector_config.atr_flat_ratio) << ';'
        << "detector_config.require_adx_for_trend="
        << (spec.detector_config.require_adx_for_trend ? "true" : "false") << ';'
        << "detector_config.use_kama_er=" << (spec.detector_config.use_kama_er ? "true" : "false")
        << ';' << "detector_config.min_bars_for_flat=" << spec.detector_config.min_bars_for_flat
        << ';' << "engine_mode=" << spec.engine_mode << ';'
        << "rollover_mode=" << spec.rollover_mode << ';'
        << "rollover_price_mode=" << spec.rollover_price_mode << ';'
        << "rollover_slippage_bps=" << detail::FormatDouble(spec.rollover_slippage_bps) << ';'
        << "symbols=" << symbols_stream.str() << ';'
        << "streaming=" << (spec.streaming ? "true" : "false") << ';'
        << "strict_parquet=" << (spec.strict_parquet ? "true" : "false") << ';'
        << "start_date=" << spec.start_date << ';' << "end_date=" << spec.end_date << ';'
        << "max_ticks="
        << (spec.max_ticks.has_value() ? std::to_string(spec.max_ticks.value()) : "null") << ';'
        << "deterministic_fills=" << (spec.deterministic_fills ? "true" : "false") << ';'
        << "wal_path=" << spec.wal_path << ';' << "account_id=" << spec.account_id << ';'
        << "run_id=" << spec.run_id << ';'
        << "emit_state_snapshots=" << (spec.emit_state_snapshots ? "true" : "false") << ';';
    return detail::StableDigest(oss.str());
}

inline std::string ComputeFileDigest(const std::filesystem::path& path, std::string* error) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open file for digest: " + path.string();
        }
        return "";
    }

    std::array<char, 1024 * 64> buffer{};
    std::uint64_t hash = 14695981039346656037ULL;
    while (input.good()) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const std::streamsize count = input.gcount();
        if (count <= 0) {
            break;
        }
        hash =
            detail::Fnv1a64(hash, std::string_view(buffer.data(), static_cast<std::size_t>(count)));
    }

    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading file for digest: " + path.string();
        }
        return "";
    }

    return detail::HexDigest64(hash);
}

inline std::string ComputeDatasetDigest(const std::filesystem::path& root,
                                        const std::string& start_date, const std::string& end_date,
                                        std::string* error) {
    if (!std::filesystem::exists(root)) {
        if (error != nullptr) {
            *error = "dataset root does not exist: " + root.string();
        }
        return "";
    }

    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const std::string ext = detail::ToLower(entry.path().extension().string());
        if (ext == ".parquet" || ext == ".csv") {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());

    std::uint64_t hash = 14695981039346656037ULL;
    hash = detail::Fnv1a64(hash, root.string());
    hash = detail::Fnv1a64(hash, start_date);
    hash = detail::Fnv1a64(hash, end_date);

    for (const auto& path : files) {
        const std::string relative = std::filesystem::relative(path, root).string();
        const auto stat = std::filesystem::status(path);
        if (stat.type() != std::filesystem::file_type::regular) {
            continue;
        }
        const auto size = std::filesystem::file_size(path);
        const auto mtime = std::filesystem::last_write_time(path).time_since_epoch().count();
        hash = detail::Fnv1a64(hash, relative);
        hash = detail::Fnv1a64(hash, std::to_string(size));
        hash = detail::Fnv1a64(hash, std::to_string(mtime));
    }

    if (error != nullptr) {
        error->clear();
    }
    return detail::HexDigest64(hash);
}

inline bool ParseCsvTick(const std::map<std::string, std::size_t>& header_index,
                         const std::vector<std::string>& cells, ReplayTick* out_tick) {
    if (out_tick == nullptr) {
        return false;
    }

    ReplayTick tick;
    tick.trading_day = detail::NormalizeTradingDay(detail::FindCell(
        header_index, cells, {"TradingDay", "trading_day", "ActionDay", "action_day"}));
    tick.instrument_id = detail::FindCell(header_index, cells,
                                          {"InstrumentID", "instrument_id", "symbol", "Symbol"});
    tick.update_time = detail::FindCell(header_index, cells, {"UpdateTime", "update_time"});

    {
        std::int64_t millis = 0;
        const std::string raw =
            detail::FindCell(header_index, cells, {"UpdateMillisec", "update_millisec"});
        if (!raw.empty()) {
            detail::ParseInt64(raw, &millis);
        }
        tick.update_millisec = static_cast<int>(std::max<std::int64_t>(0, millis));
    }

    {
        std::int64_t ts = 0;
        const std::string raw = detail::FindCell(header_index, cells, {"ts_ns", "TsNs", "ts"});
        if (!raw.empty() && detail::ParseInt64(raw, &ts)) {
            tick.ts_ns = ts;
            if (tick.trading_day.empty()) {
                tick.trading_day = detail::TradingDayFromEpochNs(ts);
            }
            if (tick.update_time.empty()) {
                tick.update_time = detail::UpdateTimeFromEpochNs(ts);
            }
        }
    }

    if (tick.ts_ns == 0) {
        tick.ts_ns = detail::ToEpochNs(tick.trading_day, tick.update_time, tick.update_millisec);
    }

    {
        const std::string raw = detail::FindCell(header_index, cells,
                                                 {"LastPrice", "last_price", "lastPrice", "close"});
        double value = 0.0;
        detail::ParseDouble(raw, &value);
        tick.last_price = value;
    }

    {
        const std::string raw = detail::FindCell(header_index, cells, {"Volume", "volume"});
        std::int64_t value = 0;
        detail::ParseInt64(raw, &value);
        tick.volume = value;
    }

    {
        const std::string raw =
            detail::FindCell(header_index, cells, {"BidPrice1", "bid_price1", "bid"});
        double value = 0.0;
        detail::ParseDouble(raw, &value);
        tick.bid_price_1 = value;
    }

    {
        const std::string raw =
            detail::FindCell(header_index, cells, {"BidVolume1", "bid_volume1"});
        std::int64_t value = 0;
        detail::ParseInt64(raw, &value);
        tick.bid_volume_1 = value;
    }

    {
        const std::string raw =
            detail::FindCell(header_index, cells, {"AskPrice1", "ask_price1", "ask"});
        double value = 0.0;
        detail::ParseDouble(raw, &value);
        tick.ask_price_1 = value;
    }

    {
        const std::string raw =
            detail::FindCell(header_index, cells, {"AskVolume1", "ask_volume1"});
        std::int64_t value = 0;
        detail::ParseInt64(raw, &value);
        tick.ask_volume_1 = value;
    }

    if (tick.instrument_id.empty() || tick.ts_ns <= 0) {
        return false;
    }

    if (tick.trading_day.empty()) {
        tick.trading_day = detail::TradingDayFromEpochNs(tick.ts_ns);
    }
    if (tick.update_time.empty()) {
        tick.update_time = detail::UpdateTimeFromEpochNs(tick.ts_ns);
    }

    *out_tick = std::move(tick);
    return true;
}

inline bool LoadCsvTicks(const BacktestCliSpec& spec, std::vector<ReplayTick>* out,
                         std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "csv tick output is null";
        }
        return false;
    }

    const std::filesystem::path path(spec.csv_path);
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open csv file: " + path.string();
        }
        return false;
    }

    std::string header_line;
    if (!std::getline(input, header_line)) {
        if (error != nullptr) {
            *error = "csv file is empty: " + path.string();
        }
        return false;
    }

    const auto headers = detail::SplitCsvLine(header_line);
    std::map<std::string, std::size_t> header_index;
    for (std::size_t i = 0; i < headers.size(); ++i) {
        header_index[headers[i]] = i;
    }
    std::unordered_set<std::string> instrument_filter;
    for (const std::string& symbol : spec.symbols) {
        if (!symbol.empty()) {
            instrument_filter.insert(symbol);
        }
    }

    out->clear();
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }

        ReplayTick tick;
        const auto cells = detail::SplitCsvLine(line);
        if (!ParseCsvTick(header_index, cells, &tick)) {
            continue;
        }

        if (!spec.start_date.empty()) {
            const std::string day = detail::NormalizeTradingDay(tick.trading_day);
            if (!day.empty() && day < spec.start_date) {
                continue;
            }
        }
        if (!spec.end_date.empty()) {
            const std::string day = detail::NormalizeTradingDay(tick.trading_day);
            if (!day.empty() && day > spec.end_date) {
                continue;
            }
        }
        if (!instrument_filter.empty() &&
            instrument_filter.find(tick.instrument_id) == instrument_filter.end()) {
            continue;
        }

        out->push_back(std::move(tick));
        if (spec.max_ticks.has_value() &&
            static_cast<std::int64_t>(out->size()) >= spec.max_ticks.value()) {
            break;
        }
    }

    std::sort(out->begin(), out->end(), [](const ReplayTick& left, const ReplayTick& right) {
        if (left.ts_ns != right.ts_ns) {
            return left.ts_ns < right.ts_ns;
        }
        return left.instrument_id < right.instrument_id;
    });
    return true;
}

inline bool BuildTimestampRange(const BacktestCliSpec& spec, Timestamp* out_start,
                                Timestamp* out_end, std::string* error) {
    if (out_start == nullptr || out_end == nullptr) {
        if (error != nullptr) {
            *error = "timestamp output is null";
        }
        return false;
    }

    try {
        if (spec.start_date.empty()) {
            *out_start = Timestamp(0);
        } else {
            const std::string text = spec.start_date.substr(0, 4) + "-" +
                                     spec.start_date.substr(4, 2) + "-" +
                                     spec.start_date.substr(6, 2) + " 00:00:00";
            *out_start = Timestamp::FromSql(text);
        }

        if (spec.end_date.empty()) {
            *out_end = Timestamp(4'102'444'799LL * detail::kNanosPerSecond);
        } else {
            const std::string text = spec.end_date.substr(0, 4) + "-" + spec.end_date.substr(4, 2) +
                                     "-" + spec.end_date.substr(6, 2) + " 23:59:59";
            *out_end = Timestamp::FromSql(text);
        }
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = ex.what();
        }
        return false;
    }

    return true;
}

inline bool ValidatePartitionMetaFile(const std::filesystem::path& meta_path, std::string* error) {
    std::ifstream input(meta_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open parquet meta file: " + meta_path.string();
        }
        return false;
    }

    bool has_min = false;
    bool has_max = false;
    bool has_rows = false;
    bool has_schema = false;
    bool has_fingerprint = false;
    std::string schema_version;

    std::string line;
    while (std::getline(input, line)) {
        const std::size_t split = line.find('=');
        if (split == std::string::npos) {
            continue;
        }
        const std::string key = detail::Trim(line.substr(0, split));
        const std::string value = detail::Trim(line.substr(split + 1));
        if (key == "min_ts_ns") {
            has_min = true;
        } else if (key == "max_ts_ns") {
            has_max = true;
        } else if (key == "row_count") {
            has_rows = true;
        } else if (key == "schema_version") {
            has_schema = true;
            schema_version = value;
        } else if (key == "source_csv_fingerprint") {
            has_fingerprint = true;
        }
    }

    if (!has_min || !has_max || !has_rows || !has_schema || !has_fingerprint) {
        if (error != nullptr) {
            *error = "parquet meta missing required fields: " + meta_path.string();
        }
        return false;
    }
    if (schema_version != "v2") {
        if (error != nullptr) {
            *error = "unsupported schema_version in meta: " + meta_path.string();
        }
        return false;
    }
    return true;
}

inline std::string SourceFilterFromSymbols(const std::vector<std::string>& symbols) {
    std::set<std::string> prefixes;
    for (const std::string& symbol : symbols) {
        const std::string prefix = detail::InstrumentSymbolPrefix(symbol);
        if (!prefix.empty()) {
            prefixes.insert(prefix);
        }
    }
    if (prefixes.size() == 1U) {
        return *prefixes.begin();
    }
    return "";
}

inline bool LoadParquetTicks(const BacktestCliSpec& spec, std::vector<ReplayTick>* out,
                             ReplayReport* report, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "parquet tick output is null";
        }
        return false;
    }
    if (report == nullptr) {
        if (error != nullptr) {
            *error = "parquet replay report is null";
        }
        return false;
    }

    const std::filesystem::path root(spec.dataset_root);
    if (!std::filesystem::exists(root)) {
        if (error != nullptr) {
            *error = "dataset_root does not exist: " + root.string();
        }
        return false;
    }

    Timestamp start;
    Timestamp end;
    if (!BuildTimestampRange(spec, &start, &end, error)) {
        return false;
    }

    ParquetDataFeed feed(root.string());
    std::filesystem::path manifest_path(spec.dataset_manifest);
    if (manifest_path.empty()) {
        manifest_path = root / "_manifest" / "partitions.jsonl";
    } else if (manifest_path.is_relative()) {
        if (!std::filesystem::exists(manifest_path)) {
            manifest_path = root / manifest_path;
        }
    }

    const bool manifest_exists = std::filesystem::exists(manifest_path);
    if (!manifest_exists && spec.strict_parquet) {
        if (error != nullptr) {
            *error =
                "missing parquet manifest, run csv_to_parquet_cli first: " + manifest_path.string();
        }
        return false;
    }
    if (manifest_exists) {
        std::string manifest_error;
        if (!feed.LoadManifestJsonl(manifest_path.string(), &manifest_error)) {
            if (error != nullptr) {
                *error = "failed to load parquet manifest: " + manifest_error;
            }
            return false;
        }
    }

    const std::string source_filter = SourceFilterFromSymbols(spec.symbols);
    const auto selected =
        feed.QueryPartitions(start.ToEpochNanos(), end.ToEpochNanos(), spec.symbols, source_filter);

    out->clear();
    std::vector<std::vector<ReplayTick>> streams;
    if (spec.streaming) {
        streams.reserve(selected.size());
    }

    ParquetScanMetrics totals;
    const std::vector<std::string> projected_columns = {
        "symbol",      "exchange",   "ts_ns",       "last_price", "last_volume", "bid_price1",
        "bid_volume1", "ask_price1", "ask_volume1", "volume",     "turnover",    "open_interest",
    };

    for (const ParquetPartitionMeta& partition : selected) {
        if (spec.strict_parquet) {
            const std::filesystem::path meta_path = partition.file_path + ".meta";
            if (!std::filesystem::exists(meta_path)) {
                if (error != nullptr) {
                    *error = "missing parquet meta sidecar: " + meta_path.string();
                }
                return false;
            }
            if (!ValidatePartitionMetaFile(meta_path, error)) {
                return false;
            }
        }

        std::int64_t partition_limit = -1;
        if (spec.max_ticks.has_value() && !spec.streaming) {
            partition_limit = std::max<std::int64_t>(
                0, spec.max_ticks.value() - static_cast<std::int64_t>(out->size()));
            if (partition_limit == 0) {
                totals.early_stop_hit = true;
                break;
            }
        }

        std::vector<Tick> partition_ticks;
        ParquetScanMetrics partition_metrics;
        if (!feed.LoadPartitionTicks(partition, start, end, projected_columns, &partition_ticks,
                                     &partition_metrics, partition_limit, error)) {
            return false;
        }

        totals.scan_rows += partition_metrics.scan_rows;
        totals.scan_row_groups += partition_metrics.scan_row_groups;
        totals.io_bytes += partition_metrics.io_bytes;
        totals.early_stop_hit = totals.early_stop_hit || partition_metrics.early_stop_hit;

        std::vector<ReplayTick> replay_ticks;
        replay_ticks.reserve(partition_ticks.size());
        for (const Tick& tick : partition_ticks) {
            ReplayTick replay_tick;
            replay_tick.trading_day = detail::TradingDayFromEpochNs(tick.ts_ns);
            replay_tick.instrument_id = tick.symbol;
            replay_tick.update_time = detail::UpdateTimeFromEpochNs(tick.ts_ns);
            replay_tick.update_millisec = static_cast<int>((tick.ts_ns % detail::kNanosPerSecond) /
                                                           detail::kNanosPerMillisecond);
            replay_tick.ts_ns = tick.ts_ns;
            replay_tick.last_price = tick.last_price;
            replay_tick.volume = tick.volume;
            replay_tick.bid_price_1 = tick.bid_price1;
            replay_tick.bid_volume_1 = tick.bid_volume1;
            replay_tick.ask_price_1 = tick.ask_price1;
            replay_tick.ask_volume_1 = tick.ask_volume1;
            replay_ticks.push_back(std::move(replay_tick));
        }

        if (spec.streaming) {
            streams.push_back(std::move(replay_ticks));
        } else {
            out->insert(out->end(), replay_ticks.begin(), replay_ticks.end());
            if (spec.max_ticks.has_value() &&
                static_cast<std::int64_t>(out->size()) >= spec.max_ticks.value()) {
                totals.early_stop_hit = true;
                break;
            }
        }
    }

    if (spec.streaming) {
        struct MergeNode {
            EpochNanos ts_ns{0};
            std::string instrument_id;
            std::size_t stream_index{0};
            std::size_t row_index{0};
        };
        struct MergeNodeCompare {
            bool operator()(const MergeNode& left, const MergeNode& right) const {
                if (left.ts_ns != right.ts_ns) {
                    return left.ts_ns > right.ts_ns;
                }
                if (left.instrument_id != right.instrument_id) {
                    return left.instrument_id > right.instrument_id;
                }
                return left.stream_index > right.stream_index;
            }
        };

        std::priority_queue<MergeNode, std::vector<MergeNode>, MergeNodeCompare> heap;
        for (std::size_t index = 0; index < streams.size(); ++index) {
            if (!streams[index].empty()) {
                heap.push(MergeNode{
                    .ts_ns = streams[index][0].ts_ns,
                    .instrument_id = streams[index][0].instrument_id,
                    .stream_index = index,
                    .row_index = 0,
                });
            }
        }

        while (!heap.empty()) {
            const MergeNode node = heap.top();
            heap.pop();

            out->push_back(streams[node.stream_index][node.row_index]);
            if (spec.max_ticks.has_value() &&
                static_cast<std::int64_t>(out->size()) >= spec.max_ticks.value()) {
                totals.early_stop_hit = true;
                break;
            }

            const std::size_t next_row = node.row_index + 1;
            if (next_row < streams[node.stream_index].size()) {
                const ReplayTick& next_tick = streams[node.stream_index][next_row];
                heap.push(MergeNode{
                    .ts_ns = next_tick.ts_ns,
                    .instrument_id = next_tick.instrument_id,
                    .stream_index = node.stream_index,
                    .row_index = next_row,
                });
            }
        }
    } else {
        std::sort(out->begin(), out->end(), [](const ReplayTick& left, const ReplayTick& right) {
            if (left.ts_ns != right.ts_ns) {
                return left.ts_ns < right.ts_ns;
            }
            return left.instrument_id < right.instrument_id;
        });
    }

    if (spec.max_ticks.has_value() &&
        static_cast<std::int64_t>(out->size()) > spec.max_ticks.value()) {
        out->resize(static_cast<std::size_t>(spec.max_ticks.value()));
        totals.early_stop_hit = true;
    }

    report->scan_rows += totals.scan_rows;
    report->scan_row_groups += totals.scan_row_groups;
    report->io_bytes += totals.io_bytes;
    report->early_stop_hit = report->early_stop_hit || totals.early_stop_hit;
    return true;
}

inline bool LoadTicksForSpec(const BacktestCliSpec& spec, std::vector<ReplayTick>* out,
                             std::string* out_data_source, ReplayReport* report,
                             std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "tick output is null";
        }
        return false;
    }
    if (out_data_source == nullptr) {
        if (error != nullptr) {
            *error = "data source output is null";
        }
        return false;
    }
    if (report == nullptr) {
        if (error != nullptr) {
            *error = "replay report output is null";
        }
        return false;
    }
    report->scan_rows = 0;
    report->scan_row_groups = 0;
    report->io_bytes = 0;
    report->early_stop_hit = false;

    if (spec.engine_mode == "parquet") {
        *out_data_source = "parquet";
        return LoadParquetTicks(spec, out, report, error);
    }

    if (spec.engine_mode == "core_sim") {
        if (!spec.dataset_root.empty()) {
            *out_data_source = "parquet";
            return LoadParquetTicks(spec, out, report, error);
        }
        *out_data_source = "csv";
        const bool ok = LoadCsvTicks(spec, out, error);
        if (ok && spec.max_ticks.has_value() &&
            static_cast<std::int64_t>(out->size()) >= spec.max_ticks.value()) {
            report->early_stop_hit = true;
        }
        return ok;
    }

    *out_data_source = "csv";
    const bool ok = LoadCsvTicks(spec, out, error);
    if (ok && spec.max_ticks.has_value() &&
        static_cast<std::int64_t>(out->size()) >= spec.max_ticks.value()) {
        report->early_stop_hit = true;
    }
    return ok;
}

inline StateSnapshot7D BuildStateSnapshotFromBar(const ReplayTick& first, const ReplayTick& last,
                                                 double high, double low, std::int64_t volume_delta,
                                                 EpochNanos ts_ns,
                                                 MarketStateDetector* detector = nullptr) {
    const double open_price = first.last_price;
    const double close_price = last.last_price;

    double trend_score = 0.0;
    if (std::fabs(open_price) > 1e-9) {
        trend_score = (close_price - open_price) / std::fabs(open_price);
    }

    const double volatility_score =
        (std::fabs(close_price) > 1e-9) ? ((high - low) / std::fabs(close_price)) : 0.0;
    const double liquidity_depth =
        std::max(0.0, static_cast<double>(last.bid_volume_1 + last.ask_volume_1 + volume_delta));
    const double liquidity_balance =
        static_cast<double>(std::min<std::int64_t>(last.bid_volume_1, last.ask_volume_1));

    StateSnapshot7D state;
    state.instrument_id = last.instrument_id;
    state.trend = {trend_score, detail::Clamp01(std::fabs(trend_score) * 10.0)};
    state.volatility = {volatility_score, detail::Clamp01(volatility_score * 5.0)};
    state.liquidity = {detail::Clamp01(liquidity_depth / 1000.0),
                       detail::Clamp01(liquidity_balance / 500.0)};
    state.sentiment = {0.0, 0.1};
    state.seasonality = {0.0, 0.1};
    state.pattern = {close_price > open_price ? 1.0 : (close_price < open_price ? -1.0 : 0.0),
                     close_price == open_price ? 0.2 : 0.7};
    state.event_drive = {0.0, 0.1};
    state.bar_open = open_price;
    state.bar_high = high;
    state.bar_low = low;
    state.bar_close = close_price;
    state.bar_volume = static_cast<double>(volume_delta);
    state.has_bar = true;
    if (detector != nullptr) {
        detector->Update(high, low, close_price);
        state.market_regime = detector->GetRegime();
    }
    state.ts_ns = ts_ns;
    return state;
}

struct PositionState {
    std::int32_t net_position{0};
    double avg_open_price{0.0};
    double realized_pnl{0.0};
};

inline void ApplyTrade(PositionState* state, Side side, std::int32_t volume, double fill_price) {
    if (state == nullptr || volume <= 0) {
        return;
    }

    const std::int32_t signed_qty = side == Side::kBuy ? volume : -volume;

    if (state->net_position == 0 || ((state->net_position > 0) == (signed_qty > 0))) {
        const std::int32_t current_abs = std::abs(state->net_position);
        const std::int32_t next_abs = current_abs + std::abs(signed_qty);
        if (next_abs > 0) {
            state->avg_open_price = (state->avg_open_price * static_cast<double>(current_abs) +
                                     fill_price * static_cast<double>(std::abs(signed_qty))) /
                                    static_cast<double>(next_abs);
        }
        state->net_position += signed_qty;
        return;
    }

    std::int32_t remaining = std::abs(signed_qty);
    if (state->net_position > 0) {
        const std::int32_t close_qty = std::min(state->net_position, remaining);
        state->realized_pnl +=
            (fill_price - state->avg_open_price) * static_cast<double>(close_qty);
        state->net_position -= close_qty;
        remaining -= close_qty;
    } else {
        const std::int32_t short_abs = std::abs(state->net_position);
        const std::int32_t close_qty = std::min(short_abs, remaining);
        state->realized_pnl +=
            (state->avg_open_price - fill_price) * static_cast<double>(close_qty);
        state->net_position += close_qty;
        remaining -= close_qty;
    }

    if (state->net_position == 0) {
        state->avg_open_price = 0.0;
    }

    if (remaining > 0) {
        state->net_position = signed_qty > 0 ? remaining : -remaining;
        state->avg_open_price = fill_price;
    }
}

inline double ComputeUnrealized(std::int32_t net_position, double avg_open_price,
                                double last_price) {
    if (net_position > 0) {
        return (last_price - avg_open_price) * static_cast<double>(net_position);
    }
    if (net_position < 0) {
        return (avg_open_price - last_price) * static_cast<double>(std::abs(net_position));
    }
    return 0.0;
}

inline double ComputeTotalEquity(const std::map<std::string, PositionState>& state_by_instrument,
                                 const std::map<std::string, double>& mark_price_by_instrument) {
    double total = 0.0;
    for (const auto& [instrument_id, state] : state_by_instrument) {
        double mark = state.avg_open_price;
        const auto it = mark_price_by_instrument.find(instrument_id);
        if (it != mark_price_by_instrument.end()) {
            mark = it->second;
        }
        total +=
            state.realized_pnl + ComputeUnrealized(state.net_position, state.avg_open_price, mark);
    }
    return total;
}

inline std::pair<double, double> ComputeRolloverPrice(Side side, double last_price,
                                                      double bid_price, double ask_price,
                                                      const std::string& price_mode,
                                                      double slippage_bps) {
    double base_price = last_price;
    if (price_mode == "last") {
        base_price = last_price;
    } else if (price_mode == "mid") {
        if (bid_price > 0.0 && ask_price > 0.0) {
            base_price = (bid_price + ask_price) * 0.5;
        }
    } else {
        if (side == Side::kBuy) {
            base_price = ask_price > 0.0 ? ask_price : last_price;
        } else {
            base_price = bid_price > 0.0 ? bid_price : last_price;
        }
    }

    const double slip = std::max(0.0, slippage_bps) * 0.0001 * std::max(0.0, base_price);
    const double price =
        side == Side::kBuy ? std::max(0.0, base_price + slip) : std::max(0.0, base_price - slip);
    return {price, slip};
}

inline std::vector<std::string> ValidateInvariants(
    const std::map<std::string, InstrumentPnlSnapshot>& pnl) {
    std::vector<std::string> violations;
    for (const auto& [instrument_id, snapshot] : pnl) {
        if (snapshot.net_position == 0 && std::fabs(snapshot.avg_open_price) > 1e-9) {
            violations.push_back(instrument_id + ": flat position must have zero avg_open_price");
        }
        if (snapshot.net_position != 0 && snapshot.avg_open_price <= 0.0) {
            violations.push_back(instrument_id +
                                 ": non-flat position must have positive avg_open_price");
        }
        if (snapshot.net_position == 0 && std::fabs(snapshot.unrealized_pnl) > 1e-9) {
            violations.push_back(instrument_id + ": flat position must have zero unrealized_pnl");
        }
    }
    return violations;
}

inline std::string SideToString(Side side) { return side == Side::kBuy ? "BUY" : "SELL"; }

inline bool RunBacktestSpec(const BacktestCliSpec& spec, BacktestCliResult* out,
                            std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "result output is null";
        }
        return false;
    }

    std::vector<ReplayTick> ticks;
    std::string data_source;
    ReplayReport replay;
    if (!LoadTicksForSpec(spec, &ticks, &data_source, &replay, error)) {
        return false;
    }

    std::string register_error;
    if (!RegisterDemoLiveStrategy(&register_error)) {
        if (error != nullptr) {
            *error = "failed to register demo strategy: " + register_error;
        }
        return false;
    }

    DemoLiveStrategy strategy;
    StrategyContext strategy_ctx;
    strategy_ctx.strategy_id = "demo";
    strategy_ctx.account_id = spec.account_id;
    strategy.Initialize(strategy_ctx);

    std::set<std::string> instrument_universe;

    std::map<std::string, PositionState> position_state;
    std::map<std::string, double> mark_price;
    std::map<std::string, MarketStateDetector> regime_detectors;
    std::map<std::string, std::int64_t> instrument_bars;
    std::map<std::string, std::int64_t> order_status_counts;
    std::vector<double> equity_points;

    std::map<std::string, std::string> symbol_active_contract;
    std::vector<RolloverEvent> rollover_events;
    std::vector<RolloverAction> rollover_actions;
    double rollover_slippage_cost = 0.0;
    std::int64_t rollover_canceled_orders = 0;

    std::int64_t intents_processed = 0;
    std::int64_t order_events = 0;
    std::int64_t wal_records = 0;
    std::int64_t wal_seq = 1;

    std::ofstream wal_out;
    if (spec.deterministic_fills && !spec.wal_path.empty()) {
        std::filesystem::create_directories(std::filesystem::path(spec.wal_path).parent_path());
        wal_out.open(spec.wal_path, std::ios::out | std::ios::trunc);
        if (!wal_out.is_open()) {
            if (error != nullptr) {
                *error = "unable to open wal file: " + spec.wal_path;
            }
            return false;
        }
    }

    const bool enable_rollover = spec.deterministic_fills && spec.engine_mode == "core_sim";

    auto handle_rollover = [&](const ReplayTick& tick) {
        const std::string symbol = detail::InstrumentSymbolPrefix(tick.instrument_id);
        if (symbol.empty()) {
            return;
        }

        const auto current_it = symbol_active_contract.find(symbol);
        if (current_it == symbol_active_contract.end()) {
            symbol_active_contract[symbol] = tick.instrument_id;
            return;
        }

        const std::string previous_contract = current_it->second;
        const std::string current_contract = tick.instrument_id;
        if (previous_contract == current_contract) {
            return;
        }

        PositionState& previous_state = position_state[previous_contract];
        const std::int32_t previous_position = std::abs(previous_state.net_position);
        if (previous_position == 0) {
            symbol_active_contract[symbol] = current_contract;
            return;
        }

        const std::int32_t canceled_orders = 0;
        rollover_canceled_orders += canceled_orders;

        std::string applied_mode = spec.rollover_mode;
        PositionState& next_state = position_state[current_contract];
        if (applied_mode == "carry" && next_state.net_position != 0) {
            applied_mode = "strict";
        }

        const std::string direction = previous_state.net_position > 0 ? "long" : "short";
        double from_price = tick.last_price;
        double to_price = tick.last_price;

        if (applied_mode == "strict") {
            const Side close_side = previous_state.net_position > 0 ? Side::kSell : Side::kBuy;
            const Side open_side = previous_state.net_position > 0 ? Side::kBuy : Side::kSell;

            const auto [close_price, close_slip] = ComputeRolloverPrice(
                close_side, tick.last_price, tick.bid_price_1, tick.ask_price_1,
                spec.rollover_price_mode, spec.rollover_slippage_bps);
            const auto [open_price, open_slip] =
                ComputeRolloverPrice(open_side, tick.last_price, tick.bid_price_1, tick.ask_price_1,
                                     spec.rollover_price_mode, spec.rollover_slippage_bps);
            from_price = close_price;
            to_price = open_price;

            ApplyTrade(&previous_state, close_side, previous_position, close_price);
            ApplyTrade(&next_state, open_side, previous_position, open_price);
            rollover_slippage_cost +=
                (close_slip + open_slip) * static_cast<double>(previous_position);

            RolloverAction close_action;
            close_action.symbol = symbol;
            close_action.action = "close";
            close_action.from_instrument = previous_contract;
            close_action.to_instrument = current_contract;
            close_action.position = previous_position;
            close_action.side = SideToString(close_side);
            close_action.price = close_price;
            close_action.mode = applied_mode;
            close_action.price_mode = spec.rollover_price_mode;
            close_action.slippage_bps = spec.rollover_slippage_bps;
            close_action.canceled_orders = canceled_orders;
            close_action.ts_ns = tick.ts_ns;

            RolloverAction open_action = close_action;
            open_action.action = "open";
            open_action.side = SideToString(open_side);
            open_action.price = open_price;

            rollover_actions.push_back(close_action);
            rollover_actions.push_back(open_action);

            if (wal_out.is_open()) {
                const std::string close_line =
                    "{\"seq\":" + std::to_string(wal_seq++) +
                    ",\"kind\":\"rollover\",\"action\":\"close\",\"symbol\":\"" +
                    JsonEscape(symbol) + "\",\"from_instrument\":\"" +
                    JsonEscape(previous_contract) + "\",\"to_instrument\":\"" +
                    JsonEscape(current_contract) +
                    "\",\"position\":" + std::to_string(previous_position) + "}";
                const std::string open_line =
                    "{\"seq\":" + std::to_string(wal_seq++) +
                    ",\"kind\":\"rollover\",\"action\":\"open\",\"symbol\":\"" +
                    JsonEscape(symbol) + "\",\"from_instrument\":\"" +
                    JsonEscape(previous_contract) + "\",\"to_instrument\":\"" +
                    JsonEscape(current_contract) +
                    "\",\"position\":" + std::to_string(previous_position) + "}";
                if (detail::WriteWalLine(&wal_out, close_line)) {
                    ++wal_records;
                }
                if (detail::WriteWalLine(&wal_out, open_line)) {
                    ++wal_records;
                }
            }
        } else {
            const double carry_price = mark_price.count(previous_contract) != 0
                                           ? mark_price[previous_contract]
                                           : tick.last_price;
            from_price = carry_price;
            to_price = carry_price;

            next_state.net_position = previous_state.net_position;
            next_state.avg_open_price = previous_state.avg_open_price;
            next_state.realized_pnl += previous_state.realized_pnl;

            previous_state.net_position = 0;
            previous_state.avg_open_price = 0.0;
            previous_state.realized_pnl = 0.0;

            RolloverAction action;
            action.symbol = symbol;
            action.action = "carry";
            action.from_instrument = previous_contract;
            action.to_instrument = current_contract;
            action.position = previous_position;
            action.side = "";
            action.price = carry_price;
            action.mode = applied_mode;
            action.price_mode = spec.rollover_price_mode;
            action.slippage_bps = spec.rollover_slippage_bps;
            action.canceled_orders = canceled_orders;
            action.ts_ns = tick.ts_ns;
            rollover_actions.push_back(action);

            if (wal_out.is_open()) {
                const std::string line =
                    "{\"seq\":" + std::to_string(wal_seq++) +
                    ",\"kind\":\"rollover\",\"action\":\"carry\",\"symbol\":\"" +
                    JsonEscape(symbol) + "\",\"from_instrument\":\"" +
                    JsonEscape(previous_contract) + "\",\"to_instrument\":\"" +
                    JsonEscape(current_contract) +
                    "\",\"position\":" + std::to_string(previous_position) + "}";
                if (detail::WriteWalLine(&wal_out, line)) {
                    ++wal_records;
                }
            }
        }

        RolloverEvent event;
        event.symbol = symbol;
        event.from_instrument = previous_contract;
        event.to_instrument = current_contract;
        event.mode = applied_mode;
        event.position = previous_position;
        event.direction = direction;
        event.from_price = from_price;
        event.to_price = to_price;
        event.canceled_orders = canceled_orders;
        event.price_mode = spec.rollover_price_mode;
        event.slippage_bps = spec.rollover_slippage_bps;
        event.ts_ns = tick.ts_ns;
        rollover_events.push_back(event);

        symbol_active_contract[symbol] = current_contract;
    };

    std::vector<ReplayTick> bucket;
    std::string active_instrument;
    std::int64_t active_minute = -1;

    auto process_bucket = [&]() {
        if (bucket.empty()) {
            return;
        }

        const ReplayTick& first = bucket.front();
        const ReplayTick& last = bucket.back();
        double high = first.last_price;
        double low = first.last_price;
        for (const ReplayTick& tick : bucket) {
            high = std::max(high, tick.last_price);
            low = std::min(low, tick.last_price);
        }
        const std::int64_t volume_delta = std::max<std::int64_t>(0, last.volume - first.volume);

        ++replay.bars_emitted;
        instrument_bars[last.instrument_id] += 1;
        mark_price[last.instrument_id] = last.last_price;

        auto [detector_it, inserted] =
            regime_detectors.try_emplace(last.instrument_id, spec.detector_config);
        (void)inserted;
        const StateSnapshot7D state = BuildStateSnapshotFromBar(
            first, last, high, low, volume_delta, last.ts_ns, &detector_it->second);
        std::vector<SignalIntent> intents = strategy.OnState(state);

        replay.intents_emitted += static_cast<std::int64_t>(intents.size());

        if (spec.deterministic_fills) {
            intents_processed += static_cast<std::int64_t>(intents.size());
            for (const SignalIntent& intent : intents) {
                const double fill_price = last.last_price;
                PositionState& pnl_state = position_state[intent.instrument_id];
                ApplyTrade(&pnl_state, intent.side, intent.volume, fill_price);

                order_events += 2;
                order_status_counts["ACCEPTED"] += 1;
                order_status_counts["FILLED"] += 1;

                if (wal_out.is_open()) {
                    const std::string accepted_line =
                        "{\"seq\":" + std::to_string(wal_seq++) +
                        ",\"kind\":\"order\",\"status\":1,\"instrument_id\":\"" +
                        JsonEscape(intent.instrument_id) + "\",\"trace_id\":\"" +
                        JsonEscape(intent.trace_id) +
                        "\",\"ts_ns\":" + std::to_string(intent.ts_ns) + "}";
                    const std::string filled_line =
                        "{\"seq\":" + std::to_string(wal_seq++) +
                        ",\"kind\":\"trade\",\"status\":3,\"instrument_id\":\"" +
                        JsonEscape(intent.instrument_id) + "\",\"trace_id\":\"" +
                        JsonEscape(intent.trace_id) +
                        "\",\"ts_ns\":" + std::to_string(intent.ts_ns) +
                        ",\"price\":" + detail::FormatDouble(fill_price) +
                        ",\"filled_volume\":" + std::to_string(intent.volume) + "}";
                    if (detail::WriteWalLine(&wal_out, accepted_line)) {
                        ++wal_records;
                    }
                    if (detail::WriteWalLine(&wal_out, filled_line)) {
                        ++wal_records;
                    }
                }
            }

            equity_points.push_back(ComputeTotalEquity(position_state, mark_price));
        }
    };

    for (const ReplayTick& tick : ticks) {
        if (replay.ticks_read == 0) {
            replay.first_instrument = tick.instrument_id;
            replay.first_ts_ns = tick.ts_ns;
        }
        replay.last_instrument = tick.instrument_id;
        replay.last_ts_ns = tick.ts_ns;
        ++replay.ticks_read;
        instrument_universe.insert(tick.instrument_id);

        if (enable_rollover) {
            handle_rollover(tick);
        }

        const std::int64_t minute_bucket = tick.ts_ns / detail::kNanosPerMinute;
        if (bucket.empty()) {
            bucket.push_back(tick);
            active_instrument = tick.instrument_id;
            active_minute = minute_bucket;
            continue;
        }

        if (tick.instrument_id == active_instrument && minute_bucket == active_minute) {
            bucket.push_back(tick);
            continue;
        }

        process_bucket();
        bucket.clear();
        bucket.push_back(tick);
        active_instrument = tick.instrument_id;
        active_minute = minute_bucket;
    }

    process_bucket();

    replay.instrument_count = static_cast<std::int64_t>(instrument_universe.size());
    replay.instrument_universe.assign(instrument_universe.begin(), instrument_universe.end());

    strategy.Shutdown();

    BacktestCliResult result;
    result.run_id = spec.run_id;
    result.mode = spec.deterministic_fills ? "deterministic" : "bar_replay";
    result.data_source = data_source;
    result.engine_mode = spec.engine_mode;
    result.rollover_mode = spec.rollover_mode;
    result.spec = spec;
    result.input_signature = BuildInputSignature(spec);

    if (data_source == "csv") {
        result.data_signature = ComputeFileDigest(spec.csv_path, error);
    } else {
        result.data_signature =
            ComputeDatasetDigest(spec.dataset_root, spec.start_date, spec.end_date, error);
    }
    if (result.data_signature.empty()) {
        return false;
    }

    if (!spec.deterministic_fills) {
        result.replay = replay;
        *out = std::move(result);
        return true;
    }

    std::map<std::string, InstrumentPnlSnapshot> instrument_pnl;
    double total_realized_pnl = 0.0;
    double total_unrealized_pnl = 0.0;
    for (const auto& [instrument_id, state] : position_state) {
        const auto mark_it = mark_price.find(instrument_id);
        const double last_price =
            mark_it != mark_price.end() ? mark_it->second : state.avg_open_price;
        const double unrealized =
            ComputeUnrealized(state.net_position, state.avg_open_price, last_price);

        InstrumentPnlSnapshot snapshot;
        snapshot.net_position = state.net_position;
        snapshot.avg_open_price = state.avg_open_price;
        snapshot.realized_pnl = state.realized_pnl;
        snapshot.unrealized_pnl = unrealized;
        snapshot.last_price = last_price;
        instrument_pnl[instrument_id] = snapshot;

        total_realized_pnl += snapshot.realized_pnl;
        total_unrealized_pnl += snapshot.unrealized_pnl;
    }

    double max_equity = 0.0;
    double min_equity = 0.0;
    double max_drawdown = 0.0;
    if (!equity_points.empty()) {
        max_equity = equity_points.front();
        min_equity = equity_points.front();
        double running_peak = equity_points.front();
        for (double equity : equity_points) {
            max_equity = std::max(max_equity, equity);
            min_equity = std::min(min_equity, equity);
            running_peak = std::max(running_peak, equity);
            max_drawdown = std::max(max_drawdown, running_peak - equity);
        }
    }

    DeterministicReplayReport deterministic;
    deterministic.replay = replay;
    deterministic.intents_processed = intents_processed;
    deterministic.order_events_emitted = order_events;
    deterministic.wal_records = wal_records;
    deterministic.instrument_bars = instrument_bars;
    deterministic.instrument_pnl = instrument_pnl;
    deterministic.total_realized_pnl = total_realized_pnl;
    deterministic.total_unrealized_pnl = total_unrealized_pnl;
    deterministic.performance.total_realized_pnl = total_realized_pnl;
    deterministic.performance.total_unrealized_pnl = total_unrealized_pnl;
    deterministic.performance.total_pnl = total_realized_pnl + total_unrealized_pnl;
    deterministic.performance.max_equity = max_equity;
    deterministic.performance.min_equity = min_equity;
    deterministic.performance.max_drawdown = max_drawdown;
    deterministic.performance.order_status_counts = order_status_counts;
    deterministic.invariant_violations = ValidateInvariants(instrument_pnl);
    deterministic.rollover_events = rollover_events;
    deterministic.rollover_actions = rollover_actions;
    deterministic.rollover_slippage_cost = rollover_slippage_cost;
    deterministic.rollover_canceled_orders = rollover_canceled_orders;

    result.replay = replay;
    result.has_deterministic = true;
    result.deterministic = deterministic;

    *out = std::move(result);
    return true;
}

inline BacktestSummary SummarizeBacktest(const BacktestCliResult& result) {
    BacktestSummary summary;
    summary.intents_emitted = result.replay.intents_emitted;
    if (result.has_deterministic) {
        summary.order_events = result.deterministic.order_events_emitted;
        summary.total_pnl = result.deterministic.performance.total_pnl;
        summary.max_drawdown = result.deterministic.performance.max_drawdown;
    }
    return summary;
}

inline std::string RenderBacktestMarkdown(const BacktestCliResult& result) {
    std::ostringstream md;
    md << "# Backtest Replay Result\n\n"
       << "## Metadata\n"
       << "- Run ID: `" << result.run_id << "`\n"
       << "- Mode: `" << result.mode << "`\n"
       << "- Input Signature: `" << result.input_signature << "`\n"
       << "- Data Signature: `" << result.data_signature << "`\n\n"
       << "## Replay Overview\n"
       << "- Ticks Read: `" << result.replay.ticks_read << "`\n"
       << "- Scan Rows: `" << result.replay.scan_rows << "`\n"
       << "- Scan Row Groups: `" << result.replay.scan_row_groups << "`\n"
       << "- IO Bytes: `" << result.replay.io_bytes << "`\n"
       << "- Early Stop Hit: `" << (result.replay.early_stop_hit ? "true" : "false") << "`\n"
       << "- Bars Emitted: `" << result.replay.bars_emitted << "`\n"
       << "- Intents Emitted: `" << result.replay.intents_emitted << "`\n"
       << "- Instrument Count: `" << result.replay.instrument_count << "`\n"
       << "- Instrument Universe: `";

    for (std::size_t i = 0; i < result.replay.instrument_universe.size(); ++i) {
        if (i > 0) {
            md << ',';
        }
        md << result.replay.instrument_universe[i];
    }
    md << "`\n"
       << "- Time Range (ns): `" << result.replay.first_ts_ns << ':' << result.replay.last_ts_ns
       << "`\n\n";

    if (result.has_deterministic) {
        md << "## Deterministic Summary\n"
           << "- Order Events: `" << result.deterministic.order_events_emitted << "`\n"
           << "- WAL Records: `" << result.deterministic.wal_records << "`\n"
           << "- Total PnL: `" << detail::FormatDouble(result.deterministic.performance.total_pnl)
           << "`\n"
           << "- Max Drawdown: `"
           << detail::FormatDouble(result.deterministic.performance.max_drawdown) << "`\n";
    }
    return md.str();
}

inline std::string RenderBacktestJson(const BacktestCliResult& result) {
    std::ostringstream json;
    json << "{\n"
         << "  \"run_id\": \"" << JsonEscape(result.run_id) << "\",\n"
         << "  \"mode\": \"" << JsonEscape(result.mode) << "\",\n"
         << "  \"data_source\": \"" << JsonEscape(result.data_source) << "\",\n"
         << "  \"engine_mode\": \"" << JsonEscape(result.engine_mode) << "\",\n"
         << "  \"rollover_mode\": \"" << JsonEscape(result.rollover_mode) << "\",\n"
         << "  \"metric_keys\": [\"total_pnl\", \"max_drawdown\", \"win_rate\", \"fill_rate\", "
            "\"capital_efficiency\"],\n"
         << "  \"spec\": {\n"
         << "    \"csv_path\": \"" << JsonEscape(result.spec.csv_path) << "\",\n"
         << "    \"dataset_root\": \"" << JsonEscape(result.spec.dataset_root) << "\",\n"
         << "    \"dataset_manifest\": \"" << JsonEscape(result.spec.dataset_manifest) << "\",\n"
         << "    \"detector_config\": \"" << JsonEscape(result.spec.detector_config_path) << "\",\n"
         << "    \"engine_mode\": \"" << JsonEscape(result.spec.engine_mode) << "\",\n"
         << "    \"rollover_mode\": \"" << JsonEscape(result.spec.rollover_mode) << "\",\n"
         << "    \"rollover_price_mode\": \"" << JsonEscape(result.spec.rollover_price_mode)
         << "\",\n"
         << "    \"rollover_slippage_bps\": "
         << detail::FormatDouble(result.spec.rollover_slippage_bps) << ",\n"
         << "    \"start_date\": \"" << JsonEscape(result.spec.start_date) << "\",\n"
         << "    \"end_date\": \"" << JsonEscape(result.spec.end_date) << "\",\n"
         << "    \"max_ticks\": ";

    if (result.spec.max_ticks.has_value()) {
        json << result.spec.max_ticks.value();
    } else {
        json << "null";
    }

    json << ",\n"
         << "    \"symbols\": [";
    for (std::size_t i = 0; i < result.spec.symbols.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << "\"" << JsonEscape(result.spec.symbols[i]) << "\"";
    }
    json << "],\n"
         << "    \"deterministic_fills\": " << (result.spec.deterministic_fills ? "true" : "false")
         << ",\n"
         << "    \"streaming\": " << (result.spec.streaming ? "true" : "false") << ",\n"
         << "    \"strict_parquet\": " << (result.spec.strict_parquet ? "true" : "false") << ",\n"
         << "    \"wal_path\": \"" << JsonEscape(result.spec.wal_path) << "\",\n"
         << "    \"account_id\": \"" << JsonEscape(result.spec.account_id) << "\",\n"
         << "    \"run_id\": \"" << JsonEscape(result.spec.run_id) << "\",\n"
         << "    \"market_state_detector\": {\n"
         << "      \"adx_period\": " << result.spec.detector_config.adx_period << ",\n"
         << "      \"adx_strong_threshold\": "
         << detail::FormatDouble(result.spec.detector_config.adx_strong_threshold) << ",\n"
         << "      \"adx_weak_lower\": "
         << detail::FormatDouble(result.spec.detector_config.adx_weak_lower) << ",\n"
         << "      \"adx_weak_upper\": "
         << detail::FormatDouble(result.spec.detector_config.adx_weak_upper) << ",\n"
         << "      \"kama_er_period\": " << result.spec.detector_config.kama_er_period << ",\n"
         << "      \"kama_fast_period\": " << result.spec.detector_config.kama_fast_period << ",\n"
         << "      \"kama_slow_period\": " << result.spec.detector_config.kama_slow_period << ",\n"
         << "      \"kama_er_strong\": "
         << detail::FormatDouble(result.spec.detector_config.kama_er_strong) << ",\n"
         << "      \"kama_er_weak_lower\": "
         << detail::FormatDouble(result.spec.detector_config.kama_er_weak_lower) << ",\n"
         << "      \"atr_period\": " << result.spec.detector_config.atr_period << ",\n"
         << "      \"atr_flat_ratio\": "
         << detail::FormatDouble(result.spec.detector_config.atr_flat_ratio) << ",\n"
         << "      \"require_adx_for_trend\": "
         << (result.spec.detector_config.require_adx_for_trend ? "true" : "false") << ",\n"
         << "      \"use_kama_er\": "
         << (result.spec.detector_config.use_kama_er ? "true" : "false") << ",\n"
         << "      \"min_bars_for_flat\": " << result.spec.detector_config.min_bars_for_flat << "\n"
         << "    },\n"
         << "    \"emit_state_snapshots\": "
         << (result.spec.emit_state_snapshots ? "true" : "false") << "\n"
         << "  },\n"
         << "  \"input_signature\": \"" << JsonEscape(result.input_signature) << "\",\n"
         << "  \"data_signature\": \"" << JsonEscape(result.data_signature) << "\",\n"
         << "  \"attribution\": {},\n"
         << "  \"risk_decomposition\": {},\n"
         << "  \"replay\": {\n"
         << "    \"ticks_read\": " << result.replay.ticks_read << ",\n"
         << "    \"scan_rows\": " << result.replay.scan_rows << ",\n"
         << "    \"scan_row_groups\": " << result.replay.scan_row_groups << ",\n"
         << "    \"io_bytes\": " << result.replay.io_bytes << ",\n"
         << "    \"early_stop_hit\": " << (result.replay.early_stop_hit ? "true" : "false") << ",\n"
         << "    \"bars_emitted\": " << result.replay.bars_emitted << ",\n"
         << "    \"intents_emitted\": " << result.replay.intents_emitted << ",\n"
         << "    \"first_instrument\": \"" << JsonEscape(result.replay.first_instrument) << "\",\n"
         << "    \"last_instrument\": \"" << JsonEscape(result.replay.last_instrument) << "\",\n"
         << "    \"instrument_count\": " << result.replay.instrument_count << ",\n"
         << "    \"instrument_universe\": [";

    for (std::size_t i = 0; i < result.replay.instrument_universe.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << "\"" << JsonEscape(result.replay.instrument_universe[i]) << "\"";
    }

    json << "],\n"
         << "    \"first_ts_ns\": " << result.replay.first_ts_ns << ",\n"
         << "    \"last_ts_ns\": " << result.replay.last_ts_ns << "\n"
         << "  }";

    if (result.has_deterministic) {
        json << ",\n"
             << "  \"deterministic\": {\n"
             << "    \"intents_processed\": " << result.deterministic.intents_processed << ",\n"
             << "    \"order_events_emitted\": " << result.deterministic.order_events_emitted
             << ",\n"
             << "    \"wal_records\": " << result.deterministic.wal_records << ",\n"
             << "    \"instrument_bars\": {";

        bool first_entry = true;
        for (const auto& [instrument_id, count] : result.deterministic.instrument_bars) {
            if (!first_entry) {
                json << ", ";
            }
            first_entry = false;
            json << "\"" << JsonEscape(instrument_id) << "\": " << count;
        }

        json << "},\n"
             << "    \"instrument_pnl\": {";

        bool first_instrument = true;
        for (const auto& [instrument_id, snapshot] : result.deterministic.instrument_pnl) {
            if (!first_instrument) {
                json << ", ";
            }
            first_instrument = false;
            json << "\"" << JsonEscape(instrument_id) << "\": {"
                 << "\"net_position\": " << snapshot.net_position << ", "
                 << "\"avg_open_price\": " << detail::FormatDouble(snapshot.avg_open_price) << ", "
                 << "\"realized_pnl\": " << detail::FormatDouble(snapshot.realized_pnl) << ", "
                 << "\"unrealized_pnl\": " << detail::FormatDouble(snapshot.unrealized_pnl) << ", "
                 << "\"last_price\": " << detail::FormatDouble(snapshot.last_price) << "}";
        }

        json << "},\n"
             << "    \"total_realized_pnl\": "
             << detail::FormatDouble(result.deterministic.total_realized_pnl) << ",\n"
             << "    \"total_unrealized_pnl\": "
             << detail::FormatDouble(result.deterministic.total_unrealized_pnl) << ",\n"
             << "    \"performance\": {\n"
             << "      \"total_realized_pnl\": "
             << detail::FormatDouble(result.deterministic.performance.total_realized_pnl) << ",\n"
             << "      \"total_unrealized_pnl\": "
             << detail::FormatDouble(result.deterministic.performance.total_unrealized_pnl) << ",\n"
             << "      \"total_pnl\": "
             << detail::FormatDouble(result.deterministic.performance.total_pnl) << ",\n"
             << "      \"max_equity\": "
             << detail::FormatDouble(result.deterministic.performance.max_equity) << ",\n"
             << "      \"min_equity\": "
             << detail::FormatDouble(result.deterministic.performance.min_equity) << ",\n"
             << "      \"max_drawdown\": "
             << detail::FormatDouble(result.deterministic.performance.max_drawdown) << ",\n"
             << "      \"order_status_counts\": {";

        bool first_status = true;
        for (const auto& [status, count] : result.deterministic.performance.order_status_counts) {
            if (!first_status) {
                json << ", ";
            }
            first_status = false;
            json << "\"" << JsonEscape(status) << "\": " << count;
        }

        json << "}\n"
             << "    },\n"
             << "    \"invariant_violations\": [";

        for (std::size_t i = 0; i < result.deterministic.invariant_violations.size(); ++i) {
            if (i > 0) {
                json << ", ";
            }
            json << "\"" << JsonEscape(result.deterministic.invariant_violations[i]) << "\"";
        }

        json << "],\n"
             << "    \"rollover_events\": [";

        for (std::size_t i = 0; i < result.deterministic.rollover_events.size(); ++i) {
            if (i > 0) {
                json << ", ";
            }
            const RolloverEvent& event = result.deterministic.rollover_events[i];
            json << "{" << "\"symbol\": \"" << JsonEscape(event.symbol) << "\", "
                 << "\"from_instrument\": \"" << JsonEscape(event.from_instrument) << "\", "
                 << "\"to_instrument\": \"" << JsonEscape(event.to_instrument) << "\", "
                 << "\"mode\": \"" << JsonEscape(event.mode) << "\", "
                 << "\"position\": " << event.position << ", " << "\"direction\": \""
                 << JsonEscape(event.direction) << "\", "
                 << "\"from_price\": " << detail::FormatDouble(event.from_price) << ", "
                 << "\"to_price\": " << detail::FormatDouble(event.to_price) << ", "
                 << "\"canceled_orders\": " << event.canceled_orders << ", " << "\"price_mode\": \""
                 << JsonEscape(event.price_mode) << "\", "
                 << "\"slippage_bps\": " << detail::FormatDouble(event.slippage_bps) << ", "
                 << "\"ts_ns\": " << event.ts_ns << "}";
        }

        json << "],\n"
             << "    \"rollover_actions\": [";

        for (std::size_t i = 0; i < result.deterministic.rollover_actions.size(); ++i) {
            if (i > 0) {
                json << ", ";
            }
            const RolloverAction& action = result.deterministic.rollover_actions[i];
            json << "{" << "\"symbol\": \"" << JsonEscape(action.symbol) << "\", "
                 << "\"action\": \"" << JsonEscape(action.action) << "\", "
                 << "\"from_instrument\": \"" << JsonEscape(action.from_instrument) << "\", "
                 << "\"to_instrument\": \"" << JsonEscape(action.to_instrument) << "\", "
                 << "\"position\": " << action.position << ", " << "\"side\": \""
                 << JsonEscape(action.side) << "\", "
                 << "\"price\": " << detail::FormatDouble(action.price) << ", " << "\"mode\": \""
                 << JsonEscape(action.mode) << "\", " << "\"price_mode\": \""
                 << JsonEscape(action.price_mode) << "\", "
                 << "\"slippage_bps\": " << detail::FormatDouble(action.slippage_bps) << ", "
                 << "\"canceled_orders\": " << action.canceled_orders << ", "
                 << "\"ts_ns\": " << action.ts_ns << "}";
        }

        json << "],\n"
             << "    \"rollover_slippage_cost\": "
             << detail::FormatDouble(result.deterministic.rollover_slippage_cost) << ",\n"
             << "    \"rollover_canceled_orders\": "
             << result.deterministic.rollover_canceled_orders << "\n"
             << "  }";
    }

    const BacktestSummary summary = SummarizeBacktest(result);
    json << ",\n"
         << "  \"summary\": {\n"
         << "    \"intents_emitted\": " << summary.intents_emitted << ",\n"
         << "    \"order_events\": " << summary.order_events << ",\n"
         << "    \"total_pnl\": " << detail::FormatDouble(summary.total_pnl) << ",\n"
         << "    \"max_drawdown\": " << detail::FormatDouble(summary.max_drawdown) << "\n"
         << "  }\n"
         << "}\n";
    return json.str();
}

}  // namespace quant_hft::apps
