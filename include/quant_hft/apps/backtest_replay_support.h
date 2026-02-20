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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/apps/backtest_metrics.h"
#include "quant_hft/backtest/indicator_trace_parquet_writer.h"
#include "quant_hft/backtest/parquet_data_feed.h"
#include "quant_hft/backtest/product_fee_config_loader.h"
#include "quant_hft/backtest/sub_strategy_indicator_trace_parquet_writer.h"
#include "quant_hft/common/timestamp.h"
#include "quant_hft/services/market_state_detector.h"
#include "quant_hft/strategy/composite_strategy.h"
#include "quant_hft/strategy/demo_live_strategy.h"
#include "quant_hft/strategy/strategy_main_config_loader.h"
#include "quant_hft/strategy/strategy_registry.h"

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

inline bool HasArgAny(const ArgMap& args, std::initializer_list<const char*> keys) {
    for (const char* key : keys) {
        if (args.find(key) != args.end()) {
            return true;
        }
    }
    return false;
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
    double initial_equity{1'000'000.0};
    std::string product_config_path;
    std::string strategy_main_config_path;
    std::string strategy_factory{"demo"};
    std::string strategy_composite_config;
    bool emit_state_snapshots{false};
    bool emit_indicator_trace{false};
    std::string indicator_trace_path;
    bool emit_sub_strategy_indicator_trace{false};
    std::string sub_strategy_indicator_trace_path;
    bool emit_trades{true};
    bool emit_orders{true};
    bool emit_position_history{false};
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
    double initial_equity{0.0};
    double final_equity{0.0};
    double total_commission{0.0};
    double total_pnl_after_cost{0.0};
    double max_margin_used{0.0};
    double final_margin_used{0.0};
    std::int64_t margin_clipped_orders{0};
    std::int64_t margin_rejected_orders{0};
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
    double initial_equity{0.0};
    double final_equity{0.0};
    BacktestCliSpec spec;
    std::string input_signature;
    std::string data_signature;
    bool indicator_trace_enabled{false};
    std::string indicator_trace_path;
    std::int64_t indicator_trace_rows{0};
    bool sub_strategy_indicator_trace_enabled{false};
    std::string sub_strategy_indicator_trace_path;
    std::int64_t sub_strategy_indicator_trace_rows{0};
    ReplayReport replay;
    bool has_deterministic{false};
    DeterministicReplayReport deterministic;
    std::string version{"2.0"};
    Parameters parameters;
    AdvancedSummary advanced_summary;
    std::vector<DailyPerformance> daily;
    std::vector<TradeRecord> trades;
    std::vector<OrderRecord> orders;
    std::vector<RegimePerformance> regime_performance;
    std::vector<PositionSnapshot> position_history;
    ExecutionQuality execution_quality;
    RiskMetrics risk_metrics;
    RollingMetrics rolling_metrics;
    MonteCarloResult monte_carlo;
    std::vector<FactorExposure> factor_exposure;
};

struct BacktestSummary {
    std::int64_t intents_emitted{0};
    std::int64_t order_events{0};
    double total_pnl{0.0};
    double max_drawdown{0.0};
};

bool ExportBacktestCsv(const BacktestCliResult& result, const std::string& out_dir,
                       std::string* error);

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
    const bool has_symbols = detail::HasArgAny(args, {"symbols", "symbol"});
    const bool has_start_date = detail::HasArgAny(args, {"start_date", "start-date"});
    const bool has_end_date = detail::HasArgAny(args, {"end_date", "end-date"});
    const bool has_strategy_factory =
        detail::HasArgAny(args, {"strategy_factory", "strategy-factory"});
    const bool has_strategy_composite_config =
        detail::HasArgAny(args, {"strategy_composite_config", "strategy-composite-config"});
    const bool has_initial_equity = detail::HasArgAny(args, {"initial_equity", "initial-equity"});
    const bool has_product_config_path =
        detail::HasArgAny(args, {"product_config_path", "product-config-path"});
    const bool has_max_loss_percent =
        detail::HasArgAny(args, {"max_loss_percent", "max-loss-percent"});

    if (has_max_loss_percent) {
        if (error != nullptr) {
            *error = "max_loss_percent has been removed; configure risk_per_trade_pct in each "
                     "sub strategy params";
        }
        return false;
    }

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
    spec.initial_equity = 1'000'000.0;
    spec.product_config_path =
        detail::GetArgAny(args, {"product_config_path", "product-config-path"});
    spec.strategy_main_config_path =
        detail::GetArgAny(args, {"strategy_main_config_path", "strategy-main-config-path"});
    spec.strategy_factory =
        detail::ToLower(detail::GetArgAny(args, {"strategy_factory", "strategy-factory"}, "demo"));
    spec.strategy_composite_config =
        detail::GetArgAny(args, {"strategy_composite_config", "strategy-composite-config"});
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
        const std::string raw_initial =
            detail::GetArgAny(args, {"initial_equity", "initial-equity"}, "1000000");
        double parsed = 0.0;
        if (!detail::ParseDouble(raw_initial, &parsed)) {
            if (error != nullptr) {
                *error = "invalid initial_equity: " + raw_initial;
            }
            return false;
        }
        spec.initial_equity = parsed;
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
        const std::string raw_emit =
            detail::GetArgAny(args, {"emit_indicator_trace", "emit-indicator-trace"}, "false");
        bool parsed = false;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_indicator_trace: " + raw_emit;
            }
            return false;
        }
        spec.emit_indicator_trace = parsed;
    }
    spec.indicator_trace_path =
        detail::GetArgAny(args, {"indicator_trace_path", "indicator-trace-path"});
    {
        const std::string raw_emit = detail::GetArgAny(
            args, {"emit_sub_strategy_indicator_trace", "emit-sub-strategy-indicator-trace"},
            "false");
        bool parsed = false;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_sub_strategy_indicator_trace: " + raw_emit;
            }
            return false;
        }
        spec.emit_sub_strategy_indicator_trace = parsed;
    }
    spec.sub_strategy_indicator_trace_path = detail::GetArgAny(
        args, {"sub_strategy_indicator_trace_path", "sub-strategy-indicator-trace-path"});
    {
        const std::string raw_emit =
            detail::GetArgAny(args, {"emit_trades", "emit-trades"}, "true");
        bool parsed = true;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_trades: " + raw_emit;
            }
            return false;
        }
        spec.emit_trades = parsed;
    }
    {
        const std::string raw_emit =
            detail::GetArgAny(args, {"emit_orders", "emit-orders"}, "true");
        bool parsed = true;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_orders: " + raw_emit;
            }
            return false;
        }
        spec.emit_orders = parsed;
    }
    {
        const std::string raw_emit = detail::GetArgAny(
            args, {"emit_position_history", "emit-position-history"}, "false");
        bool parsed = false;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_position_history: " + raw_emit;
            }
            return false;
        }
        spec.emit_position_history = parsed;
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
    if (!spec.strategy_main_config_path.empty()) {
        StrategyMainConfig main_config;
        if (!LoadStrategyMainConfig(spec.strategy_main_config_path, &main_config, error)) {
            return false;
        }
        if (main_config.run_type != "backtest") {
            if (error != nullptr) {
                *error = "strategy_main_config run_type must be backtest for backtest replay";
            }
            return false;
        }
        if (!has_initial_equity) {
            spec.initial_equity = main_config.backtest.initial_equity;
        }
        if (!has_symbols && !main_config.backtest.symbols.empty()) {
            spec.symbols = main_config.backtest.symbols;
        }
        if (!has_start_date && !main_config.backtest.start_date.empty()) {
            spec.start_date = detail::NormalizeTradingDay(main_config.backtest.start_date);
        }
        if (!has_end_date && !main_config.backtest.end_date.empty()) {
            spec.end_date = detail::NormalizeTradingDay(main_config.backtest.end_date);
        }
        if (!has_product_config_path && !main_config.backtest.product_config_path.empty()) {
            spec.product_config_path = main_config.backtest.product_config_path;
        }
        if (!has_strategy_factory) {
            spec.strategy_factory = "composite";
        }
        if (!has_strategy_composite_config) {
            spec.strategy_composite_config = spec.strategy_main_config_path;
        }
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
    if (!(spec.initial_equity > 0.0)) {
        if (error != nullptr) {
            *error = "initial_equity must be > 0";
        }
        return false;
    }
    if (spec.strategy_factory != "demo" && spec.strategy_factory != "composite") {
        if (error != nullptr) {
            *error = "unsupported strategy_factory: " + spec.strategy_factory;
        }
        return false;
    }
    if (spec.strategy_factory == "composite" && spec.strategy_composite_config.empty()) {
        if (error != nullptr) {
            *error = "strategy_composite_config is required when strategy_factory=composite";
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
        << "initial_equity=" << detail::FormatDouble(spec.initial_equity) << ';'
        << "product_config_path=" << spec.product_config_path << ';'
        << "strategy_main_config_path=" << spec.strategy_main_config_path << ';'
        << "strategy_factory=" << spec.strategy_factory << ';'
        << "strategy_composite_config=" << spec.strategy_composite_config << ';'
        << "emit_state_snapshots=" << (spec.emit_state_snapshots ? "true" : "false") << ';'
        << "emit_indicator_trace=" << (spec.emit_indicator_trace ? "true" : "false") << ';'
        << "indicator_trace_path=" << spec.indicator_trace_path << ';'
        << "emit_sub_strategy_indicator_trace="
        << (spec.emit_sub_strategy_indicator_trace ? "true" : "false") << ';'
        << "sub_strategy_indicator_trace_path=" << spec.sub_strategy_indicator_trace_path << ';'
        << "emit_trades=" << (spec.emit_trades ? "true" : "false") << ';'
        << "emit_orders=" << (spec.emit_orders ? "true" : "false") << ';'
        << "emit_position_history=" << (spec.emit_position_history ? "true" : "false") << ';';
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
    std::set<std::string> product_prefixes;
    for (const std::string& symbol : symbols) {
        const std::string trimmed = detail::Trim(symbol);
        if (trimmed.empty()) {
            continue;
        }
        bool has_digit = false;
        for (unsigned char ch : trimmed) {
            if (std::isdigit(ch) != 0) {
                has_digit = true;
                break;
            }
        }
        if (has_digit) {
            continue;
        }
        const std::string prefix = detail::InstrumentSymbolPrefix(trimmed);
        if (!prefix.empty()) {
            product_prefixes.insert(prefix);
        }
    }
    if (product_prefixes.size() == 1U) {
        return *product_prefixes.begin();
    }
    return "";
}

struct ParquetSymbolSelection {
    std::vector<std::string> instrument_ids;
    std::vector<std::string> product_symbols;
};

inline ParquetSymbolSelection BuildParquetSymbolSelection(const std::vector<std::string>& symbols) {
    ParquetSymbolSelection selection;
    std::set<std::string> instrument_ids;
    std::set<std::string> product_symbols;
    for (const std::string& symbol : symbols) {
        const std::string trimmed = detail::Trim(symbol);
        if (trimmed.empty()) {
            continue;
        }

        bool has_digit = false;
        for (unsigned char ch : trimmed) {
            if (std::isdigit(ch) != 0) {
                has_digit = true;
                break;
            }
        }

        if (has_digit) {
            instrument_ids.insert(trimmed);
            continue;
        }

        const std::string product = detail::InstrumentSymbolPrefix(trimmed);
        if (!product.empty()) {
            product_symbols.insert(product);
        }
    }

    selection.instrument_ids.assign(instrument_ids.begin(), instrument_ids.end());
    selection.product_symbols.assign(product_symbols.begin(), product_symbols.end());
    return selection;
}

inline std::vector<ParquetPartitionMeta> SelectParquetPartitionsForSymbols(
    ParquetDataFeed* feed, EpochNanos start_ts_ns, EpochNanos end_ts_ns,
    const std::vector<std::string>& symbols) {
    std::vector<ParquetPartitionMeta> selected;
    if (feed == nullptr) {
        return selected;
    }
    if (start_ts_ns > end_ts_ns) {
        return selected;
    }

    const ParquetSymbolSelection selection = BuildParquetSymbolSelection(symbols);
    std::unordered_set<std::string> seen_paths;
    auto append_unique = [&](const std::vector<ParquetPartitionMeta>& partitions) {
        for (const auto& partition : partitions) {
            if (seen_paths.insert(partition.file_path).second) {
                selected.push_back(partition);
            }
        }
    };

    if (selection.instrument_ids.empty() && selection.product_symbols.empty()) {
        append_unique(feed->QueryPartitions(start_ts_ns, end_ts_ns, std::vector<std::string>{},
                                            std::string{}));
    } else {
        if (!selection.instrument_ids.empty()) {
            append_unique(feed->QueryPartitions(start_ts_ns, end_ts_ns, selection.instrument_ids,
                                                std::string{}));
        }
        for (const std::string& product : selection.product_symbols) {
            append_unique(
                feed->QueryPartitions(start_ts_ns, end_ts_ns, std::vector<std::string>{}, product));
        }
    }

    std::sort(selected.begin(), selected.end(),
              [](const ParquetPartitionMeta& left, const ParquetPartitionMeta& right) {
                  if (left.min_ts_ns != right.min_ts_ns) {
                      return left.min_ts_ns < right.min_ts_ns;
                  }
                  return left.file_path < right.file_path;
              });
    return selected;
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

    const auto selected = SelectParquetPartitionsForSymbols(&feed, start.ToEpochNanos(),
                                                            end.ToEpochNanos(), spec.symbols);

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

inline double ComputeTotalPnl(const std::map<std::string, PositionState>& state_by_instrument,
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

inline double ComputeTotalEquity(double initial_equity,
                                 const std::map<std::string, PositionState>& state_by_instrument,
                                 const std::map<std::string, double>& mark_price_by_instrument,
                                 double total_commission) {
    return initial_equity + ComputeTotalPnl(state_by_instrument, mark_price_by_instrument) -
           total_commission;
}

inline double ComputeInstrumentMarginUsed(
    const std::string& instrument_id, const PositionState& state,
    const std::map<std::string, double>& mark_price_by_instrument,
    const ProductFeeBook& product_fee_book) {
    if (state.net_position == 0) {
        return 0.0;
    }
    const ProductFeeEntry* fee_entry = product_fee_book.Find(instrument_id);
    if (fee_entry == nullptr) {
        return 0.0;
    }
    double fill_price = state.avg_open_price;
    const auto mark_it = mark_price_by_instrument.find(instrument_id);
    if (mark_it != mark_price_by_instrument.end()) {
        fill_price = mark_it->second;
    }
    const Side side = state.net_position > 0 ? Side::kBuy : Side::kSell;
    return ProductFeeBook::ComputeRequiredMargin(*fee_entry, side, std::abs(state.net_position),
                                                 fill_price);
}

inline double ComputeTotalMarginUsed(
    const std::map<std::string, PositionState>& state_by_instrument,
    const std::map<std::string, double>& mark_price_by_instrument,
    const ProductFeeBook& product_fee_book) {
    double total = 0.0;
    for (const auto& [instrument_id, state] : state_by_instrument) {
        total += ComputeInstrumentMarginUsed(instrument_id, state, mark_price_by_instrument,
                                             product_fee_book);
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

inline std::string SideToTitleString(Side side) { return side == Side::kBuy ? "Buy" : "Sell"; }

inline std::string OffsetFlagToString(OffsetFlag offset) {
    switch (offset) {
        case OffsetFlag::kOpen:
            return "OPEN";
        case OffsetFlag::kClose:
            return "CLOSE";
        case OffsetFlag::kCloseToday:
            return "CLOSE_TODAY";
        case OffsetFlag::kCloseYesterday:
            return "CLOSE_YESTERDAY";
    }
    return "OPEN";
}

inline std::string OffsetFlagToTitleString(OffsetFlag offset) {
    switch (offset) {
        case OffsetFlag::kOpen:
            return "Open";
        case OffsetFlag::kClose:
            return "Close";
        case OffsetFlag::kCloseToday:
            return "CloseToday";
        case OffsetFlag::kCloseYesterday:
            return "CloseYesterday";
    }
    return "Open";
}

inline std::string OrderStatusToString(OrderStatus status) {
    switch (status) {
        case OrderStatus::kNew:
            return "NEW";
        case OrderStatus::kAccepted:
            return "ACCEPTED";
        case OrderStatus::kPartiallyFilled:
            return "PARTIALLY_FILLED";
        case OrderStatus::kFilled:
            return "FILLED";
        case OrderStatus::kCanceled:
            return "CANCELED";
        case OrderStatus::kRejected:
            return "REJECTED";
    }
    return "REJECTED";
}

inline std::string SignalTypeToString(SignalType signal_type) {
    switch (signal_type) {
        case SignalType::kOpen:
            return "kOpen";
        case SignalType::kClose:
            return "kClose";
        case SignalType::kStopLoss:
            return "kStopLoss";
        case SignalType::kTakeProfit:
            return "kTakeProfit";
        case SignalType::kForceClose:
            return "kForceClose";
    }
    return "kOpen";
}

inline std::string MarketRegimeToString(MarketRegime regime) {
    switch (regime) {
        case MarketRegime::kUnknown:
            return "kUnknown";
        case MarketRegime::kStrongTrend:
            return "kStrongTrend";
        case MarketRegime::kWeakTrend:
            return "kWeakTrend";
        case MarketRegime::kRanging:
            return "kRanging";
        case MarketRegime::kFlat:
            return "kFlat";
    }
    return "kUnknown";
}

inline std::string BuildDefaultIndicatorTracePath(const std::string& run_id) {
    return (std::filesystem::path("runtime") / "research" / "indicator_trace" /
            (run_id + ".parquet"))
        .string();
}

inline std::string BuildDefaultSubStrategyIndicatorTracePath(const std::string& run_id) {
    return (std::filesystem::path("runtime") / "research" / "sub_strategy_indicator_trace" /
            (run_id + ".parquet"))
        .string();
}

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
    if (!RegisterCompositeStrategy(&register_error)) {
        if (error != nullptr) {
            *error = "failed to register composite strategy: " + register_error;
        }
        return false;
    }

    std::unique_ptr<ILiveStrategy> strategy =
        StrategyRegistry::Instance().Create(spec.strategy_factory);
    if (strategy == nullptr) {
        if (error != nullptr) {
            *error = "strategy_factory not found: " + spec.strategy_factory;
        }
        return false;
    }
    StrategyContext strategy_ctx;
    strategy_ctx.strategy_id = spec.strategy_factory;
    strategy_ctx.account_id = spec.account_id;
    strategy_ctx.metadata["run_type"] = "backtest";
    strategy_ctx.metadata["strategy_factory"] = spec.strategy_factory;
    if (spec.strategy_factory == "composite") {
        strategy_ctx.metadata["composite_config_path"] = spec.strategy_composite_config;
    }
    try {
        strategy->Initialize(strategy_ctx);
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = std::string("strategy initialize failed: ") + ex.what();
        }
        return false;
    } catch (...) {
        if (error != nullptr) {
            *error = "strategy initialize failed: unknown exception";
        }
        return false;
    }

    CompositeStrategy* composite_strategy = dynamic_cast<CompositeStrategy*>(strategy.get());
    if (spec.emit_sub_strategy_indicator_trace && composite_strategy == nullptr) {
        if (error != nullptr) {
            *error =
                "emit_sub_strategy_indicator_trace requires strategy_factory=composite and a "
                "CompositeStrategy instance";
        }
        return false;
    }

    std::set<std::string> instrument_universe;

    std::map<std::string, PositionState> position_state;
    std::map<std::string, double> mark_price;
    std::map<std::string, MarketStateDetector> regime_detectors;
    std::map<std::string, std::int64_t> instrument_bars;
    std::map<std::string, std::int64_t> order_status_counts;
    std::vector<double> equity_points;
    std::vector<EquitySample> equity_history;
    std::vector<TradeRecord> trades;
    std::vector<OrderRecord> orders;
    std::vector<PositionSnapshot> position_history;
    std::int64_t trade_seq = 0;
    std::int64_t order_seq = 0;
    double total_commission = 0.0;
    double used_margin_total = 0.0;
    double max_margin_used = 0.0;
    std::int64_t margin_clipped_orders = 0;
    std::int64_t margin_rejected_orders = 0;
    if (spec.deterministic_fills) {
        equity_points.push_back(spec.initial_equity);
        if (!ticks.empty()) {
            EquitySample seed;
            seed.ts_ns = ticks.front().ts_ns;
            seed.trading_day = detail::NormalizeTradingDay(ticks.front().trading_day);
            if (seed.trading_day.empty()) {
                seed.trading_day = detail::TradingDayFromEpochNs(seed.ts_ns);
            }
            seed.equity = spec.initial_equity;
            seed.position_value = 0.0;
            seed.market_regime = "kUnknown";
            equity_history.push_back(std::move(seed));
        }
    }

    ProductFeeBook product_fee_book;
    bool has_product_fee = false;
    if (!spec.product_config_path.empty()) {
        if (!LoadProductFeeConfig(spec.product_config_path, &product_fee_book, error)) {
            return false;
        }
        has_product_fee = true;
        if (composite_strategy != nullptr) {
            std::unordered_map<std::string, double> multipliers;
            if (product_fee_book.ExportContractMultipliers(&multipliers)) {
                for (const auto& [instrument_id, multiplier] : multipliers) {
                    composite_strategy->SetBacktestContractMultiplier(instrument_id, multiplier);
                }
            }
        }
    }

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

    std::string indicator_trace_path = spec.indicator_trace_path;
    IndicatorTraceParquetWriter indicator_trace_writer;
    if (spec.emit_indicator_trace) {
        if (indicator_trace_path.empty()) {
            indicator_trace_path = BuildDefaultIndicatorTracePath(spec.run_id);
        }
        if (!indicator_trace_writer.Open(indicator_trace_path, error)) {
            return false;
        }
    }
    std::string sub_strategy_indicator_trace_path = spec.sub_strategy_indicator_trace_path;
    SubStrategyIndicatorTraceParquetWriter sub_strategy_indicator_trace_writer;
    if (spec.emit_sub_strategy_indicator_trace) {
        if (sub_strategy_indicator_trace_path.empty()) {
            sub_strategy_indicator_trace_path =
                BuildDefaultSubStrategyIndicatorTracePath(spec.run_id);
        }
        if (!sub_strategy_indicator_trace_writer.Open(sub_strategy_indicator_trace_path, error)) {
            return false;
        }
    }

    const bool enable_rollover = spec.deterministic_fills && spec.engine_mode == "core_sim";

    auto compute_position_value = [&]() {
        double total = 0.0;
        for (const auto& [instrument_id, state] : position_state) {
            if (state.net_position == 0) {
                continue;
            }
            const auto mark_it = mark_price.find(instrument_id);
            const double last_price =
                mark_it != mark_price.end() ? mark_it->second : state.avg_open_price;
            total += std::fabs(static_cast<double>(state.net_position)) * last_price;
        }
        return total;
    };

    auto record_position_snapshot = [&](const std::string& instrument_id, EpochNanos ts_ns) {
        if (!spec.emit_position_history) {
            return;
        }
        const auto state_it = position_state.find(instrument_id);
        if (state_it == position_state.end()) {
            return;
        }
        const PositionState& state = state_it->second;
        const auto mark_it = mark_price.find(instrument_id);
        const double last_price =
            mark_it != mark_price.end() ? mark_it->second : state.avg_open_price;

        PositionSnapshot snapshot;
        snapshot.timestamp_ns = ts_ns;
        snapshot.symbol = instrument_id;
        snapshot.net_position = state.net_position;
        snapshot.avg_price = state.avg_open_price;
        snapshot.unrealized_pnl =
            ComputeUnrealized(state.net_position, state.avg_open_price, last_price);
        position_history.push_back(std::move(snapshot));
    };

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

            const double prev_realized_before = previous_state.realized_pnl;
            const double next_realized_before = next_state.realized_pnl;
            ApplyTrade(&previous_state, close_side, previous_position, close_price);
            ApplyTrade(&next_state, open_side, previous_position, open_price);
            rollover_slippage_cost +=
                (close_slip + open_slip) * static_cast<double>(previous_position);
            const double close_realized_pnl = previous_state.realized_pnl - prev_realized_before;
            const double open_realized_pnl = next_state.realized_pnl - next_realized_before;

            if (spec.emit_orders) {
                OrderRecord close_order;
                close_order.order_id = "rollover-order-" + std::to_string(++order_seq);
                close_order.client_order_id = close_order.order_id;
                close_order.symbol = previous_contract;
                close_order.type = "Market";
                close_order.side = SideToTitleString(close_side);
                close_order.offset = "Close";
                close_order.price = close_price;
                close_order.volume = previous_position;
                close_order.status = "Filled";
                close_order.filled_volume = previous_position;
                close_order.avg_fill_price = close_price;
                close_order.created_at_ns = tick.ts_ns;
                close_order.last_update_ns = tick.ts_ns;
                close_order.strategy_id = "rollover";
                orders.push_back(std::move(close_order));

                OrderRecord open_order;
                open_order.order_id = "rollover-order-" + std::to_string(++order_seq);
                open_order.client_order_id = open_order.order_id;
                open_order.symbol = current_contract;
                open_order.type = "Market";
                open_order.side = SideToTitleString(open_side);
                open_order.offset = "Open";
                open_order.price = open_price;
                open_order.volume = previous_position;
                open_order.status = "Filled";
                open_order.filled_volume = previous_position;
                open_order.avg_fill_price = open_price;
                open_order.created_at_ns = tick.ts_ns;
                open_order.last_update_ns = tick.ts_ns;
                open_order.strategy_id = "rollover";
                orders.push_back(std::move(open_order));
            }

            if (spec.emit_trades) {
                TradeRecord close_trade;
                close_trade.trade_id = "rollover-trade-" + std::to_string(++trade_seq);
                close_trade.order_id = "rollover-order-close-" + std::to_string(trade_seq);
                close_trade.symbol = previous_contract;
                close_trade.exchange = "";
                close_trade.side = SideToTitleString(close_side);
                close_trade.offset = "Close";
                close_trade.volume = previous_position;
                close_trade.price = close_price;
                close_trade.timestamp_ns = tick.ts_ns;
                close_trade.commission = 0.0;
                close_trade.slippage = close_slip;
                close_trade.realized_pnl = close_realized_pnl;
                close_trade.strategy_id = "rollover";
                close_trade.signal_type = "rollover_close";
                close_trade.regime_at_entry = "rollover";
                trades.push_back(std::move(close_trade));

                TradeRecord open_trade;
                open_trade.trade_id = "rollover-trade-" + std::to_string(++trade_seq);
                open_trade.order_id = "rollover-order-open-" + std::to_string(trade_seq);
                open_trade.symbol = current_contract;
                open_trade.exchange = "";
                open_trade.side = SideToTitleString(open_side);
                open_trade.offset = "Open";
                open_trade.volume = previous_position;
                open_trade.price = open_price;
                open_trade.timestamp_ns = tick.ts_ns;
                open_trade.commission = 0.0;
                open_trade.slippage = open_slip;
                open_trade.realized_pnl = open_realized_pnl;
                open_trade.strategy_id = "rollover";
                open_trade.signal_type = "rollover_open";
                open_trade.regime_at_entry = "rollover";
                trades.push_back(std::move(open_trade));
            }

            record_position_snapshot(previous_contract, tick.ts_ns);
            record_position_snapshot(current_contract, tick.ts_ns);

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

    auto process_bucket = [&]() -> bool {
        if (bucket.empty()) {
            return true;
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

        if (spec.emit_indicator_trace) {
            IndicatorTraceRow row;
            row.instrument_id = state.instrument_id;
            row.ts_ns = state.ts_ns;
            row.bar_open = state.bar_open;
            row.bar_high = state.bar_high;
            row.bar_low = state.bar_low;
            row.bar_close = state.bar_close;
            row.bar_volume = state.bar_volume;
            row.kama = detector_it->second.GetKAMA();
            row.atr = detector_it->second.GetATR();
            row.adx = detector_it->second.GetADX();
            row.er = detector_it->second.GetKAMAER();
            row.market_regime = state.market_regime;
            if (!indicator_trace_writer.Append(row, error)) {
                return false;
            }
        }

        if (has_product_fee) {
            used_margin_total =
                ComputeTotalMarginUsed(position_state, mark_price, product_fee_book);
            max_margin_used = std::max(max_margin_used, used_margin_total);
            if (composite_strategy != nullptr) {
                const ProductFeeEntry* entry = product_fee_book.Find(state.instrument_id);
                if (entry != nullptr && entry->contract_multiplier > 0.0) {
                    composite_strategy->SetBacktestContractMultiplier(state.instrument_id,
                                                                      entry->contract_multiplier);
                }
            }
        }
        if (composite_strategy != nullptr) {
            const double equity = ComputeTotalEquity(spec.initial_equity, position_state,
                                                     mark_price, total_commission);
            composite_strategy->SetBacktestAccountSnapshot(equity, equity - spec.initial_equity);
        }
        std::vector<SignalIntent> intents = strategy->OnState(state);

        if (spec.emit_sub_strategy_indicator_trace) {
            if (composite_strategy == nullptr) {
                if (error != nullptr) {
                    *error =
                        "emit_sub_strategy_indicator_trace requires strategy_factory=composite";
                }
                return false;
            }
            const std::vector<CompositeAtomicTraceRow> atomic_trace_rows =
                composite_strategy->CollectAtomicIndicatorTrace();
            for (const auto& atomic_trace : atomic_trace_rows) {
                SubStrategyIndicatorTraceRow row;
                row.instrument_id = state.instrument_id;
                row.ts_ns = state.ts_ns;
                row.strategy_id = atomic_trace.strategy_id;
                row.strategy_type = atomic_trace.strategy_type;
                row.bar_open = state.bar_open;
                row.bar_high = state.bar_high;
                row.bar_low = state.bar_low;
                row.bar_close = state.bar_close;
                row.bar_volume = state.bar_volume;
                row.kama = atomic_trace.kama;
                row.atr = atomic_trace.atr;
                row.adx = atomic_trace.adx;
                row.er = atomic_trace.er;
                row.stop_loss_price = atomic_trace.stop_loss_price;
                row.take_profit_price = atomic_trace.take_profit_price;
                row.market_regime = state.market_regime;
                if (!sub_strategy_indicator_trace_writer.Append(row, error)) {
                    return false;
                }
            }
        }

        replay.intents_emitted += static_cast<std::int64_t>(intents.size());

        if (spec.deterministic_fills) {
            intents_processed += static_cast<std::int64_t>(intents.size());
            for (const SignalIntent& intent : intents) {
                const double fill_price = last.last_price;
                const std::string client_order_id =
                    intent.trace_id.empty()
                        ? ("det-order-" + std::to_string(intents_processed) + "-" +
                           intent.instrument_id + "-" + std::to_string(intent.ts_ns))
                        : intent.trace_id;
                const std::string order_id = "order-" + std::to_string(++order_seq);
                const ProductFeeEntry* fee_entry = nullptr;
                if (has_product_fee) {
                    fee_entry = product_fee_book.Find(intent.instrument_id);
                    if (fee_entry == nullptr) {
                        if (error != nullptr) {
                            *error = "missing product fee config for instrument_id: " +
                                     intent.instrument_id;
                        }
                        return false;
                    }
                }

                std::int32_t exec_volume = intent.volume;
                if (fee_entry != nullptr && intent.offset == OffsetFlag::kOpen && exec_volume > 0) {
                    const double account_equity = ComputeTotalEquity(
                        spec.initial_equity, position_state, mark_price, total_commission);
                    const double available_margin =
                        std::max(0.0, account_equity - used_margin_total);
                    const double per_lot_margin =
                        ProductFeeBook::ComputePerLotMargin(*fee_entry, intent.side, fill_price);
                    std::int32_t max_openable = 0;
                    if (std::isfinite(per_lot_margin) && per_lot_margin > 0.0) {
                        const double raw_openable = std::floor(available_margin / per_lot_margin);
                        if (std::isfinite(raw_openable) && raw_openable > 0.0) {
                            max_openable = static_cast<std::int32_t>(std::min<double>(
                                raw_openable, std::numeric_limits<std::int32_t>::max()));
                        }
                    }
                    if (max_openable < exec_volume) {
                        ++margin_clipped_orders;
                        exec_volume = std::max<std::int32_t>(0, max_openable);
                    }
                    if (exec_volume <= 0) {
                        ++margin_rejected_orders;
                        ++order_events;
                        order_status_counts["REJECTED"] += 1;
                        if (spec.emit_orders) {
                            OrderRecord rejected_order;
                            rejected_order.order_id = order_id;
                            rejected_order.client_order_id = client_order_id;
                            rejected_order.symbol = intent.instrument_id;
                            rejected_order.type = "Market";
                            rejected_order.side = SideToTitleString(intent.side);
                            rejected_order.offset = OffsetFlagToTitleString(intent.offset);
                            rejected_order.price = fill_price;
                            rejected_order.volume = intent.volume;
                            rejected_order.status = "Rejected";
                            rejected_order.filled_volume = 0;
                            rejected_order.avg_fill_price = 0.0;
                            rejected_order.created_at_ns = intent.ts_ns;
                            rejected_order.last_update_ns = intent.ts_ns;
                            rejected_order.strategy_id = intent.strategy_id;
                            rejected_order.cancel_reason = "margin_rejected";
                            orders.push_back(std::move(rejected_order));
                        }
                        continue;
                    }
                }

                if (exec_volume <= 0) {
                    continue;
                }

                PositionState& pnl_state = position_state[intent.instrument_id];
                const double realized_before = pnl_state.realized_pnl;
                ApplyTrade(&pnl_state, intent.side, exec_volume, fill_price);
                const double realized_delta = pnl_state.realized_pnl - realized_before;

                double commission = 0.0;
                if (fee_entry != nullptr) {
                    commission = ProductFeeBook::ComputeCommission(*fee_entry, intent.offset,
                                                                   exec_volume, fill_price);
                }
                total_commission += commission;
                if (has_product_fee) {
                    used_margin_total =
                        ComputeTotalMarginUsed(position_state, mark_price, product_fee_book);
                    max_margin_used = std::max(max_margin_used, used_margin_total);
                }

                order_events += 2;
                order_status_counts["ACCEPTED"] += 1;
                order_status_counts["FILLED"] += 1;

                OrderEvent filled_event;
                filled_event.account_id = spec.account_id;
                filled_event.strategy_id = intent.strategy_id;
                filled_event.client_order_id = client_order_id;
                filled_event.instrument_id = intent.instrument_id;
                filled_event.side = intent.side;
                filled_event.offset = intent.offset;
                filled_event.status = OrderStatus::kFilled;
                filled_event.total_volume = exec_volume;
                filled_event.filled_volume = exec_volume;
                filled_event.avg_fill_price = fill_price;
                filled_event.ts_ns = intent.ts_ns;
                strategy->OnOrderEvent(filled_event);

                if (spec.emit_orders) {
                    OrderRecord order;
                    order.order_id = order_id;
                    order.client_order_id = client_order_id;
                    order.symbol = intent.instrument_id;
                    order.type = "Market";
                    order.side = SideToTitleString(intent.side);
                    order.offset = OffsetFlagToTitleString(intent.offset);
                    order.price = fill_price;
                    order.volume = exec_volume;
                    order.status = "Filled";
                    order.filled_volume = exec_volume;
                    order.avg_fill_price = fill_price;
                    order.created_at_ns = intent.ts_ns;
                    order.last_update_ns = intent.ts_ns;
                    order.strategy_id = intent.strategy_id;
                    orders.push_back(std::move(order));
                }
                if (spec.emit_trades) {
                    double slippage = 0.0;
                    if (intent.limit_price > 0.0) {
                        slippage = intent.side == Side::kBuy ? fill_price - intent.limit_price
                                                             : intent.limit_price - fill_price;
                    }

                    TradeRecord trade;
                    trade.trade_id = "trade-" + std::to_string(++trade_seq);
                    trade.order_id = order_id;
                    trade.symbol = intent.instrument_id;
                    trade.exchange = "";
                    trade.side = SideToTitleString(intent.side);
                    trade.offset = OffsetFlagToTitleString(intent.offset);
                    trade.volume = exec_volume;
                    trade.price = fill_price;
                    trade.timestamp_ns = intent.ts_ns;
                    trade.commission = commission;
                    trade.slippage = slippage;
                    trade.realized_pnl = realized_delta;
                    trade.strategy_id = intent.strategy_id;
                    trade.signal_type = SignalTypeToString(intent.signal_type);
                    trade.regime_at_entry = MarketRegimeToString(state.market_regime);
                    trades.push_back(std::move(trade));
                }

                record_position_snapshot(intent.instrument_id, intent.ts_ns);

                if (wal_out.is_open()) {
                    const std::string accepted_line =
                        "{\"seq\":" + std::to_string(wal_seq++) +
                        ",\"kind\":\"order\",\"status\":1,\"instrument_id\":\"" +
                        JsonEscape(intent.instrument_id) + "\",\"trace_id\":\"" +
                        JsonEscape(client_order_id) +
                        "\",\"ts_ns\":" + std::to_string(intent.ts_ns) + "}";
                    const std::string filled_line =
                        "{\"seq\":" + std::to_string(wal_seq++) +
                        ",\"kind\":\"trade\",\"status\":3,\"instrument_id\":\"" +
                        JsonEscape(intent.instrument_id) + "\",\"trace_id\":\"" +
                        JsonEscape(client_order_id) +
                        "\",\"ts_ns\":" + std::to_string(intent.ts_ns) +
                        ",\"price\":" + detail::FormatDouble(fill_price) +
                        ",\"filled_volume\":" + std::to_string(exec_volume) + "}";
                    if (detail::WriteWalLine(&wal_out, accepted_line)) {
                        ++wal_records;
                    }
                    if (detail::WriteWalLine(&wal_out, filled_line)) {
                        ++wal_records;
                    }
                }
            }

            const double current_equity =
                ComputeTotalEquity(spec.initial_equity, position_state, mark_price, total_commission);
            equity_points.push_back(current_equity);
            EquitySample sample;
            sample.ts_ns = state.ts_ns;
            sample.trading_day = detail::NormalizeTradingDay(last.trading_day);
            if (sample.trading_day.empty()) {
                sample.trading_day = detail::TradingDayFromEpochNs(state.ts_ns);
            }
            sample.equity = current_equity;
            sample.position_value = compute_position_value();
            sample.market_regime = MarketRegimeToString(state.market_regime);
            equity_history.push_back(std::move(sample));
        }
        return true;
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

        if (!process_bucket()) {
            return false;
        }
        bucket.clear();
        bucket.push_back(tick);
        active_instrument = tick.instrument_id;
        active_minute = minute_bucket;
    }

    if (!process_bucket()) {
        return false;
    }

    if (spec.emit_indicator_trace && !indicator_trace_writer.Close(error)) {
        return false;
    }
    if (spec.emit_sub_strategy_indicator_trace &&
        !sub_strategy_indicator_trace_writer.Close(error)) {
        return false;
    }

    replay.instrument_count = static_cast<std::int64_t>(instrument_universe.size());
    replay.instrument_universe.assign(instrument_universe.begin(), instrument_universe.end());

    strategy->Shutdown();

    BacktestCliResult result;
    result.run_id = spec.run_id;
    result.mode = spec.deterministic_fills ? "deterministic" : "bar_replay";
    result.data_source = data_source;
    result.engine_mode = spec.engine_mode;
    result.rollover_mode = spec.rollover_mode;
    result.initial_equity = spec.initial_equity;
    result.final_equity = spec.initial_equity;
    result.spec = spec;
    result.spec.strategy_factory = spec.strategy_factory;
    result.spec.strategy_composite_config = spec.strategy_composite_config;
    result.spec.indicator_trace_path = indicator_trace_path;
    result.spec.sub_strategy_indicator_trace_path = sub_strategy_indicator_trace_path;
    BacktestCliSpec signature_spec = spec;
    signature_spec.indicator_trace_path = indicator_trace_path;
    signature_spec.sub_strategy_indicator_trace_path = sub_strategy_indicator_trace_path;
    result.input_signature = BuildInputSignature(signature_spec);
    result.indicator_trace_enabled = spec.emit_indicator_trace;
    result.indicator_trace_path = indicator_trace_path;
    result.indicator_trace_rows = indicator_trace_writer.rows_written();
    result.sub_strategy_indicator_trace_enabled = spec.emit_sub_strategy_indicator_trace;
    result.sub_strategy_indicator_trace_path = sub_strategy_indicator_trace_path;
    result.sub_strategy_indicator_trace_rows = sub_strategy_indicator_trace_writer.rows_written();

    if (data_source == "csv") {
        result.data_signature = ComputeFileDigest(spec.csv_path, error);
    } else {
        result.data_signature =
            ComputeDatasetDigest(spec.dataset_root, spec.start_date, spec.end_date, error);
    }
    if (result.data_signature.empty()) {
        return false;
    }

    result.parameters.start_date = spec.start_date;
    result.parameters.end_date = spec.end_date;
    result.parameters.initial_capital = spec.initial_equity;
    result.parameters.engine_mode = spec.engine_mode;
    result.parameters.rollover_mode = spec.rollover_mode;
    result.parameters.strategy_factory = spec.strategy_factory;
    if (spec.emit_trades) {
        result.trades = trades;
    }
    if (spec.emit_orders) {
        result.orders = orders;
    }
    if (spec.emit_position_history) {
        result.position_history = position_history;
    }
    result.daily = ComputeDailyMetrics(equity_history, result.trades, spec.initial_equity);
    result.risk_metrics = ComputeRiskMetrics(result.daily);
    result.execution_quality = ComputeExecutionQuality(result.orders, result.trades);
    result.rolling_metrics = ComputeRollingMetrics(result.daily, 63);
    result.regime_performance = ComputeRegimePerformance(result.trades);
    result.advanced_summary =
        ComputeAdvancedSummary(result.daily, result.trades, result.risk_metrics);

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
    deterministic.performance.initial_equity = spec.initial_equity;
    deterministic.performance.final_equity =
        equity_points.empty() ? spec.initial_equity : equity_points.back();
    deterministic.performance.total_commission = total_commission;
    deterministic.performance.total_pnl_after_cost =
        deterministic.performance.total_pnl - deterministic.performance.total_commission;
    deterministic.performance.max_margin_used = max_margin_used;
    deterministic.performance.final_margin_used =
        has_product_fee ? ComputeTotalMarginUsed(position_state, mark_price, product_fee_book)
                        : 0.0;
    deterministic.performance.margin_clipped_orders = margin_clipped_orders;
    deterministic.performance.margin_rejected_orders = margin_rejected_orders;
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
    result.final_equity = deterministic.performance.final_equity;

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

    md << "\n## HF Standard Summary\n"
       << "- Version: `" << result.version << "`\n"
       << "- Daily Rows: `" << result.daily.size() << "`\n"
       << "- Trades Rows: `" << result.trades.size() << "`\n"
       << "- Orders Rows: `" << result.orders.size() << "`\n"
       << "- Position Snapshot Rows: `" << result.position_history.size() << "`\n"
       << "- Emit Trades: `" << (result.spec.emit_trades ? "true" : "false") << "`\n"
       << "- Emit Orders: `" << (result.spec.emit_orders ? "true" : "false") << "`\n"
       << "- Emit Position History: `" << (result.spec.emit_position_history ? "true" : "false")
       << "`\n"
       << "- VaR95 (%): `" << detail::FormatDouble(result.risk_metrics.var_95) << "`\n"
       << "- ES95 (%): `" << detail::FormatDouble(result.risk_metrics.expected_shortfall_95)
       << "`\n"
       << "- Fill Rate: `"
       << detail::FormatDouble(result.execution_quality.limit_order_fill_rate) << "`\n"
       << "- Cancel Rate: `" << detail::FormatDouble(result.execution_quality.cancel_rate)
       << "`\n";
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
         << "  \"initial_equity\": " << detail::FormatDouble(result.initial_equity) << ",\n"
         << "  \"final_equity\": " << detail::FormatDouble(result.final_equity) << ",\n"
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
         << "    \"initial_equity\": " << detail::FormatDouble(result.spec.initial_equity) << ",\n"
         << "    \"product_config_path\": \"" << JsonEscape(result.spec.product_config_path)
         << "\",\n"
         << "    \"strategy_main_config_path\": \""
         << JsonEscape(result.spec.strategy_main_config_path) << "\",\n"
         << "    \"strategy_factory\": \"" << JsonEscape(result.spec.strategy_factory) << "\",\n"
         << "    \"strategy_composite_config\": \""
         << JsonEscape(result.spec.strategy_composite_config) << "\",\n"
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
         << (result.spec.emit_state_snapshots ? "true" : "false") << ",\n"
         << "    \"emit_indicator_trace\": "
         << (result.spec.emit_indicator_trace ? "true" : "false") << ",\n"
         << "    \"indicator_trace_path\": \"" << JsonEscape(result.spec.indicator_trace_path)
         << "\",\n"
         << "    \"emit_sub_strategy_indicator_trace\": "
         << (result.spec.emit_sub_strategy_indicator_trace ? "true" : "false") << ",\n"
         << "    \"sub_strategy_indicator_trace_path\": \""
         << JsonEscape(result.spec.sub_strategy_indicator_trace_path) << "\",\n"
         << "    \"emit_trades\": " << (result.spec.emit_trades ? "true" : "false") << ",\n"
         << "    \"emit_orders\": " << (result.spec.emit_orders ? "true" : "false") << ",\n"
         << "    \"emit_position_history\": "
         << (result.spec.emit_position_history ? "true" : "false") << "\n"
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
         << "  },\n"
         << "  \"indicator_trace\": {\n"
         << "    \"enabled\": " << (result.indicator_trace_enabled ? "true" : "false") << ",\n"
         << "    \"path\": \"" << JsonEscape(result.indicator_trace_path) << "\",\n"
         << "    \"rows\": " << result.indicator_trace_rows << "\n"
         << "  },\n"
         << "  \"sub_strategy_indicator_trace\": {\n"
         << "    \"enabled\": " << (result.sub_strategy_indicator_trace_enabled ? "true" : "false")
         << ",\n"
         << "    \"path\": \"" << JsonEscape(result.sub_strategy_indicator_trace_path) << "\",\n"
         << "    \"rows\": " << result.sub_strategy_indicator_trace_rows << "\n"
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
             << "      \"initial_equity\": "
             << detail::FormatDouble(result.deterministic.performance.initial_equity) << ",\n"
             << "      \"final_equity\": "
             << detail::FormatDouble(result.deterministic.performance.final_equity) << ",\n"
             << "      \"total_commission\": "
             << detail::FormatDouble(result.deterministic.performance.total_commission) << ",\n"
             << "      \"total_pnl_after_cost\": "
             << detail::FormatDouble(result.deterministic.performance.total_pnl_after_cost) << ",\n"
             << "      \"max_margin_used\": "
             << detail::FormatDouble(result.deterministic.performance.max_margin_used) << ",\n"
             << "      \"final_margin_used\": "
             << detail::FormatDouble(result.deterministic.performance.final_margin_used) << ",\n"
             << "      \"margin_clipped_orders\": "
             << result.deterministic.performance.margin_clipped_orders << ",\n"
             << "      \"margin_rejected_orders\": "
             << result.deterministic.performance.margin_rejected_orders << ",\n"
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
         << "  },\n"
         << "  \"hf_standard\": {\n"
         << "    \"version\": \"" << JsonEscape(result.version) << "\",\n"
         << "    \"parameters\": {\n"
         << "      \"start_date\": \"" << JsonEscape(result.parameters.start_date) << "\",\n"
         << "      \"end_date\": \"" << JsonEscape(result.parameters.end_date) << "\",\n"
         << "      \"initial_capital\": "
         << detail::FormatDouble(result.parameters.initial_capital) << ",\n"
         << "      \"engine_mode\": \"" << JsonEscape(result.parameters.engine_mode) << "\",\n"
         << "      \"rollover_mode\": \"" << JsonEscape(result.parameters.rollover_mode) << "\",\n"
         << "      \"strategy_factory\": \""
         << JsonEscape(result.parameters.strategy_factory) << "\"\n"
         << "    },\n"
         << "    \"metadata\": {\n"
         << "      \"emit_trades\": " << (result.spec.emit_trades ? "true" : "false") << ",\n"
         << "      \"emit_orders\": " << (result.spec.emit_orders ? "true" : "false") << ",\n"
         << "      \"emit_position_history\": "
         << (result.spec.emit_position_history ? "true" : "false") << ",\n"
         << "      \"position_sampling\": \"on_trade\"\n"
         << "    },\n"
         << "    \"advanced_summary\": {\n"
         << "      \"rolling_sharpe_3m_last\": "
         << detail::FormatDouble(result.advanced_summary.rolling_sharpe_3m_last) << ",\n"
         << "      \"rolling_max_dd_3m_last\": "
         << detail::FormatDouble(result.advanced_summary.rolling_max_dd_3m_last) << ",\n"
         << "      \"information_ratio\": "
         << detail::FormatDouble(result.advanced_summary.information_ratio) << ",\n"
         << "      \"beta\": " << detail::FormatDouble(result.advanced_summary.beta) << ",\n"
         << "      \"alpha\": " << detail::FormatDouble(result.advanced_summary.alpha) << ",\n"
         << "      \"tail_ratio\": "
         << detail::FormatDouble(result.advanced_summary.tail_ratio) << ",\n"
         << "      \"gain_to_pain_ratio\": "
         << detail::FormatDouble(result.advanced_summary.gain_to_pain_ratio) << ",\n"
         << "      \"avg_win_loss_duration_ratio\": "
         << detail::FormatDouble(result.advanced_summary.avg_win_loss_duration_ratio) << ",\n"
         << "      \"profit_factor\": "
         << detail::FormatDouble(result.advanced_summary.profit_factor) << "\n"
         << "    },\n"
         << "    \"execution_quality\": {\n"
         << "      \"limit_order_fill_rate\": "
         << detail::FormatDouble(result.execution_quality.limit_order_fill_rate) << ",\n"
         << "      \"avg_wait_time_ms\": "
         << detail::FormatDouble(result.execution_quality.avg_wait_time_ms) << ",\n"
         << "      \"cancel_rate\": "
         << detail::FormatDouble(result.execution_quality.cancel_rate) << ",\n"
         << "      \"slippage_mean\": "
         << detail::FormatDouble(result.execution_quality.slippage_mean) << ",\n"
         << "      \"slippage_std\": "
         << detail::FormatDouble(result.execution_quality.slippage_std) << ",\n"
         << "      \"slippage_percentiles\": [";
    for (std::size_t i = 0; i < result.execution_quality.slippage_percentiles.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << detail::FormatDouble(result.execution_quality.slippage_percentiles[i]);
    }
    json << "]\n"
         << "    },\n"
         << "    \"risk_metrics\": {\n"
         << "      \"var_95\": " << detail::FormatDouble(result.risk_metrics.var_95) << ",\n"
         << "      \"expected_shortfall_95\": "
         << detail::FormatDouble(result.risk_metrics.expected_shortfall_95) << ",\n"
         << "      \"ulcer_index\": " << detail::FormatDouble(result.risk_metrics.ulcer_index)
         << ",\n"
         << "      \"recovery_factor\": "
         << detail::FormatDouble(result.risk_metrics.recovery_factor) << ",\n"
         << "      \"tail_loss\": " << detail::FormatDouble(result.risk_metrics.tail_loss) << "\n"
         << "    },\n"
         << "    \"rolling_metrics\": {\n"
         << "      \"rolling_sharpe_3m\": [";
    for (std::size_t i = 0; i < result.rolling_metrics.rolling_sharpe_3m.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << detail::FormatDouble(result.rolling_metrics.rolling_sharpe_3m[i]);
    }
    json << "],\n"
         << "      \"rolling_max_dd_3m\": [";
    for (std::size_t i = 0; i < result.rolling_metrics.rolling_max_dd_3m.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << detail::FormatDouble(result.rolling_metrics.rolling_max_dd_3m[i]);
    }
    json << "]\n"
         << "    },\n"
         << "    \"daily\": [";
    for (std::size_t i = 0; i < result.daily.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const DailyPerformance& row = result.daily[i];
        json << "{\"date\":\"" << JsonEscape(row.date) << "\",\"capital\":"
             << detail::FormatDouble(row.capital) << ",\"daily_return_pct\":"
             << detail::FormatDouble(row.daily_return_pct) << ",\"cumulative_return_pct\":"
             << detail::FormatDouble(row.cumulative_return_pct) << ",\"drawdown_pct\":"
             << detail::FormatDouble(row.drawdown_pct) << ",\"position_value\":"
             << detail::FormatDouble(row.position_value) << ",\"trades_count\":" << row.trades_count
             << ",\"turnover\":" << detail::FormatDouble(row.turnover) << ",\"market_regime\":\""
             << JsonEscape(row.market_regime) << "\"}";
    }
    json << "],\n"
         << "    \"trades\": [";
    for (std::size_t i = 0; i < result.trades.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const TradeRecord& row = result.trades[i];
        json << "{\"trade_id\":\"" << JsonEscape(row.trade_id) << "\",\"order_id\":\""
             << JsonEscape(row.order_id) << "\",\"symbol\":\"" << JsonEscape(row.symbol)
             << "\",\"exchange\":\"" << JsonEscape(row.exchange) << "\",\"side\":\""
             << JsonEscape(row.side) << "\",\"offset\":\"" << JsonEscape(row.offset)
             << "\",\"volume\":" << row.volume << ",\"price\":"
             << detail::FormatDouble(row.price) << ",\"timestamp_ns\":" << row.timestamp_ns
             << ",\"commission\":" << detail::FormatDouble(row.commission) << ",\"slippage\":"
             << detail::FormatDouble(row.slippage) << ",\"realized_pnl\":"
             << detail::FormatDouble(row.realized_pnl) << ",\"strategy_id\":\""
             << JsonEscape(row.strategy_id) << "\",\"signal_type\":\""
             << JsonEscape(row.signal_type) << "\",\"regime_at_entry\":\""
             << JsonEscape(row.regime_at_entry) << "\"}";
    }
    json << "],\n"
         << "    \"orders\": [";
    for (std::size_t i = 0; i < result.orders.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const OrderRecord& row = result.orders[i];
        json << "{\"order_id\":\"" << JsonEscape(row.order_id) << "\",\"client_order_id\":\""
             << JsonEscape(row.client_order_id) << "\",\"symbol\":\"" << JsonEscape(row.symbol)
             << "\",\"type\":\"" << JsonEscape(row.type) << "\",\"side\":\""
             << JsonEscape(row.side) << "\",\"offset\":\"" << JsonEscape(row.offset)
             << "\",\"price\":" << detail::FormatDouble(row.price) << ",\"volume\":" << row.volume
             << ",\"status\":\"" << JsonEscape(row.status) << "\",\"filled_volume\":"
             << row.filled_volume << ",\"avg_fill_price\":"
             << detail::FormatDouble(row.avg_fill_price) << ",\"created_at_ns\":"
             << row.created_at_ns << ",\"last_update_ns\":" << row.last_update_ns
             << ",\"strategy_id\":\"" << JsonEscape(row.strategy_id) << "\",\"cancel_reason\":\""
             << JsonEscape(row.cancel_reason) << "\"}";
    }
    json << "],\n"
         << "    \"regime_performance\": [";
    for (std::size_t i = 0; i < result.regime_performance.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const RegimePerformance& row = result.regime_performance[i];
        json << "{\"regime\":\"" << JsonEscape(row.regime) << "\",\"total_days\":"
             << row.total_days << ",\"trades_count\":" << row.trades_count << ",\"win_rate\":"
             << detail::FormatDouble(row.win_rate) << ",\"average_return_pct\":"
             << detail::FormatDouble(row.average_return_pct) << ",\"total_pnl\":"
             << detail::FormatDouble(row.total_pnl) << ",\"sharpe\":"
             << detail::FormatDouble(row.sharpe) << ",\"max_drawdown_pct\":"
             << detail::FormatDouble(row.max_drawdown_pct) << "}";
    }
    json << "],\n"
         << "    \"position_history\": [";
    for (std::size_t i = 0; i < result.position_history.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const PositionSnapshot& row = result.position_history[i];
        json << "{\"timestamp_ns\":" << row.timestamp_ns << ",\"symbol\":\""
             << JsonEscape(row.symbol) << "\",\"net_position\":" << row.net_position
             << ",\"avg_price\":" << detail::FormatDouble(row.avg_price)
             << ",\"unrealized_pnl\":" << detail::FormatDouble(row.unrealized_pnl) << "}";
    }
    json << "],\n"
         << "    \"monte_carlo\": {\n"
         << "      \"simulations\": " << result.monte_carlo.simulations << ",\n"
         << "      \"mean_final_capital\": "
         << detail::FormatDouble(result.monte_carlo.mean_final_capital) << ",\n"
         << "      \"ci_95_lower\": " << detail::FormatDouble(result.monte_carlo.ci_95_lower)
         << ",\n"
         << "      \"ci_95_upper\": " << detail::FormatDouble(result.monte_carlo.ci_95_upper)
         << ",\n"
         << "      \"prob_loss\": " << detail::FormatDouble(result.monte_carlo.prob_loss) << ",\n"
         << "      \"max_drawdown_95\": "
         << detail::FormatDouble(result.monte_carlo.max_drawdown_95) << "\n"
         << "    },\n"
         << "    \"factor_exposure\": [";
    for (std::size_t i = 0; i < result.factor_exposure.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const FactorExposure& row = result.factor_exposure[i];
        json << "{\"factor\":\"" << JsonEscape(row.factor) << "\",\"exposure\":"
             << detail::FormatDouble(row.exposure) << ",\"t_stat\":"
             << detail::FormatDouble(row.t_stat) << "}";
    }
    json << "]\n"
         << "  }\n"
         << "}\n";
    return json.str();
}

}  // namespace quant_hft::apps
