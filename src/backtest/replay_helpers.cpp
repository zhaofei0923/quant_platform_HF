#include "quant_hft/backtest/replay_runtime.h"

namespace quant_hft::backtest {
namespace detail {

std::string ToLower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return text;
}

std::string Trim(std::string text) {
    const auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    while (!text.empty() && is_space(static_cast<unsigned char>(text.front()))) {
        text.erase(text.begin());
    }
    while (!text.empty() && is_space(static_cast<unsigned char>(text.back()))) {
        text.pop_back();
    }
    return text;
}

std::string StripUtf8Bom(std::string text) {
    if (text.size() >= 3 && static_cast<unsigned char>(text[0]) == 0xEF &&
        static_cast<unsigned char>(text[1]) == 0xBB &&
        static_cast<unsigned char>(text[2]) == 0xBF) {
        text.erase(0, 3);
    }
    return text;
}

std::string NormalizeCsvHeaderName(std::string text) { return Trim(StripUtf8Bom(std::move(text))); }

std::uint64_t Fnv1a64(std::uint64_t seed, std::string_view text) {
    std::uint64_t hash = seed;
    for (unsigned char ch : text) {
        hash ^= static_cast<std::uint64_t>(ch);
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string HexDigest64(std::uint64_t value) {
    std::ostringstream oss;
    oss << std::hex << std::setw(16) << std::setfill('0') << value;
    return oss.str();
}

std::string StableDigest(std::string_view text) {
    return HexDigest64(Fnv1a64(14695981039346656037ULL, text));
}

std::string GetArgAny(const ArgMap& args, std::initializer_list<const char*> keys,
                      const std::string& fallback) {
    for (const char* key : keys) {
        const auto it = args.find(key);
        if (it != args.end()) {
            return it->second;
        }
    }
    return fallback;
}

bool HasArgAny(const ArgMap& args, std::initializer_list<const char*> keys) {
    for (const char* key : keys) {
        if (args.find(key) != args.end()) {
            return true;
        }
    }
    return false;
}

bool ParseBool(const std::string& raw, bool* out) {
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

bool ParseInt64(const std::string& raw, std::int64_t* out) {
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

bool ParseDouble(const std::string& raw, double* out) {
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

std::string StripInlineComment(const std::string& line) {
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

bool LoadYamlScalarMap(const std::filesystem::path& path, std::map<std::string, std::string>* out,
                       std::string* error) {
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

bool ResolveDetectorYamlValue(const std::map<std::string, std::string>& values,
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

bool ApplyDetectorConfigField(MarketStateDetectorConfig* config, const std::string& field,
                              const std::string& raw, const std::string& context,
                              std::string* error) {
    if (config == nullptr) {
        if (error != nullptr) {
            *error = "detector_config output is null";
        }
        return false;
    }
    const auto parse_int = [&](int* target) -> bool {
        std::int64_t parsed = 0;
        if (!ParseInt64(raw, &parsed)) {
            if (error != nullptr) {
                *error = "invalid detector_config " + context + ": " + raw;
            }
            return false;
        }
        if (parsed < std::numeric_limits<int>::min() || parsed > std::numeric_limits<int>::max()) {
            if (error != nullptr) {
                *error = "detector_config " + context + " is out of int range: " + raw;
            }
            return false;
        }
        *target = static_cast<int>(parsed);
        return true;
    };
    const auto parse_double = [&](double* target) -> bool {
        double parsed = 0.0;
        if (!ParseDouble(raw, &parsed)) {
            if (error != nullptr) {
                *error = "invalid detector_config " + context + ": " + raw;
            }
            return false;
        }
        *target = parsed;
        return true;
    };
    const auto parse_bool = [&](bool* target) -> bool {
        bool parsed = false;
        if (!ParseBool(raw, &parsed)) {
            if (error != nullptr) {
                *error = "invalid detector_config " + context + ": " + raw;
            }
            return false;
        }
        *target = parsed;
        return true;
    };

    if (field == "adx_period") {
        return parse_int(&config->adx_period);
    }
    if (field == "adx_strong_threshold") {
        return parse_double(&config->adx_strong_threshold);
    }
    if (field == "adx_weak_lower") {
        return parse_double(&config->adx_weak_lower);
    }
    if (field == "adx_weak_upper") {
        return parse_double(&config->adx_weak_upper);
    }
    if (field == "kama_er_period") {
        return parse_int(&config->kama_er_period);
    }
    if (field == "kama_fast_period") {
        return parse_int(&config->kama_fast_period);
    }
    if (field == "kama_slow_period") {
        return parse_int(&config->kama_slow_period);
    }
    if (field == "kama_er_strong") {
        return parse_double(&config->kama_er_strong);
    }
    if (field == "kama_er_weak_lower") {
        return parse_double(&config->kama_er_weak_lower);
    }
    if (field == "atr_period") {
        return parse_int(&config->atr_period);
    }
    if (field == "atr_flat_ratio" || field == "min_bars_for_flat") {
        // Deprecated ATR flat-gate keys are accepted as no-ops for legacy configs.
        return true;
    }
    if (field == "require_adx_for_trend") {
        return parse_bool(&config->require_adx_for_trend);
    }
    if (field == "use_kama_er") {
        return parse_bool(&config->use_kama_er);
    }

    if (error != nullptr) {
        *error = "unknown detector_config field: " + context;
    }
    return false;
}

bool LoadMarketStateDetectorConfigFile(const std::string& config_path,
                                       MarketStateDetectorConfig* out_config,
                                       MarketStateDetectorConfigByProduct* out_by_product,
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
    for (const char* field : kMarketStateDetectorFields) {
        std::string raw;
        if (ResolveDetectorYamlValue(yaml_values, field, &raw) &&
            !ApplyDetectorConfigField(&config, field, raw, field, error)) {
            return false;
        }
    }

    try {
        (void)MarketStateDetector(config);
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = "invalid detector_config: " + std::string(ex.what());
        }
        return false;
    }

    MarketStateDetectorConfigByProduct by_product;
    const std::array<std::string, 2> prefixes = {"market_state_detector_by_product.",
                                                 "ctp.market_state_detector_by_product."};
    for (const auto& [key, raw] : yaml_values) {
        std::string rest;
        for (const std::string& prefix : prefixes) {
            if (key.rfind(prefix, 0) == 0) {
                rest = key.substr(prefix.size());
                break;
            }
        }
        if (rest.empty()) {
            continue;
        }
        const std::size_t dot = rest.find('.');
        if (dot == std::string::npos || dot == 0 || dot + 1 >= rest.size()) {
            if (error != nullptr) {
                *error = "invalid detector_config market_state_detector_by_product key: " + key;
            }
            return false;
        }
        const std::string raw_product = rest.substr(0, dot);
        const std::string product_id = NormalizeMarketStateProductId(raw_product);
        if (product_id.empty()) {
            if (error != nullptr) {
                *error = "invalid detector_config market_state_detector_by_product product: " +
                         raw_product;
            }
            return false;
        }
        const std::string field = rest.substr(dot + 1);
        auto [it, inserted] = by_product.try_emplace(product_id, config);
        (void)inserted;
        if (!ApplyDetectorConfigField(
                &it->second, field, raw,
                "market_state_detector_by_product." + product_id + "." + field, error)) {
            return false;
        }
    }
    for (const auto& [product_id, product_config] : by_product) {
        try {
            (void)MarketStateDetector(product_config);
        } catch (const std::exception& ex) {
            if (error != nullptr) {
                *error = "invalid detector_config_by_product " + product_id + ": " + ex.what();
            }
            return false;
        }
    }

    *out_config = config;
    if (out_by_product != nullptr) {
        *out_by_product = std::move(by_product);
    }
    return true;
}

bool LoadMarketStateDetectorConfigFile(const std::string& config_path,
                                       MarketStateDetectorConfig* out_config, std::string* error) {
    MarketStateDetectorConfigByProduct ignored;
    return LoadMarketStateDetectorConfigFile(config_path, out_config, &ignored, error);
}

std::string NormalizeTradingDay(const std::string& raw) {
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

bool BuildUtcTm(const std::string& normalized_day, int hour, int minute, int second,
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

bool ParseTimeHms(const std::string& raw, int* hour, int* minute, int* second) {
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

bool IsDaySessionTick(const std::string& update_time) {
    int hour = 0;
    int minute = 0;
    int second = 0;
    if (!ParseTimeHms(update_time, &hour, &minute, &second)) {
        return false;
    }
    (void)minute;
    (void)second;
    return hour >= 9 && hour < 15;
}

EpochNanos ToEpochNs(const std::string& trading_day, const std::string& update_time,
                     int update_millisec) {
    const std::string normalized_day = NormalizeTradingDay(trading_day);
    int hour = 0;
    int minute = 0;
    int second = 0;
    if (!ParseTimeHms(update_time, &hour, &minute, &second)) {
        return 0;
    }

    std::string effective_day = normalized_day;
    // CTP-style futures ticks use trading_day for both day and night sessions.
    // For night-session times, map to previous calendar day to preserve chronology.
    if (hour >= 18 && !normalized_day.empty()) {
        std::tm midnight_tm = {};
        if (BuildUtcTm(normalized_day, 0, 0, 0, &midnight_tm)) {
            const std::time_t midnight_seconds = timegm(&midnight_tm);
            if (midnight_seconds > 0) {
                const std::time_t previous_day_seconds = midnight_seconds - 24 * 60 * 60;
                const std::tm previous_day_tm = *gmtime(&previous_day_seconds);
                std::ostringstream day_oss;
                day_oss << std::put_time(&previous_day_tm, "%Y%m%d");
                effective_day = day_oss.str();
            }
        }
    }

    std::tm tm = {};
    if (!BuildUtcTm(effective_day, hour, minute, second, &tm)) {
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

std::string TradingDayFromEpochNs(EpochNanos ts_ns) {
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / kNanosPerSecond);
    std::tm tm = *gmtime(&seconds);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d");
    return oss.str();
}

std::string UpdateTimeFromEpochNs(EpochNanos ts_ns) {
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / kNanosPerSecond);
    std::tm tm = *gmtime(&seconds);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%H:%M:%S");
    return oss.str();
}

std::string DateTimeFromEpochNs(EpochNanos ts_ns) {
    if (ts_ns <= 0) {
        return "";
    }
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / kNanosPerSecond);
    std::tm tm = *gmtime(&seconds);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

std::string TickDateTimeFromTickFields(const std::string& trading_day,
                                       const std::string& update_time, int update_millisec,
                                       EpochNanos fallback_ts_ns) {
    (void)update_millisec;
    const std::string normalized_day = NormalizeTradingDay(trading_day);
    int hour = 0;
    int minute = 0;
    int second = 0;
    if (!normalized_day.empty() && ParseTimeHms(update_time, &hour, &minute, &second)) {
        (void)second;
        std::ostringstream oss;
        oss << normalized_day.substr(0, 4) << '-' << normalized_day.substr(4, 2) << '-'
            << normalized_day.substr(6, 2) << ' ' << std::setw(2) << std::setfill('0') << hour
            << ':' << std::setw(2) << std::setfill('0') << minute;
        return oss.str();
    }

    if (fallback_ts_ns <= 0) {
        return "";
    }
    const std::string fallback_day = TradingDayFromEpochNs(fallback_ts_ns);
    const std::string fallback_time = UpdateTimeFromEpochNs(fallback_ts_ns);
    const std::string fallback_minute =
        fallback_time.size() >= 5 ? fallback_time.substr(0, 5) : fallback_time;
    if (fallback_day.size() != 8) {
        return fallback_minute;
    }
    return fallback_day.substr(0, 4) + "-" + fallback_day.substr(4, 2) + "-" +
           fallback_day.substr(6, 2) + " " + fallback_minute;
}

std::string ShiftTradingDay(const std::string& trading_day, int day_offset) {
    const std::string normalized_day = NormalizeTradingDay(trading_day);
    if (normalized_day.empty()) {
        return "";
    }
    if (day_offset == 0) {
        return normalized_day;
    }

    std::tm midnight_tm = {};
    if (!BuildUtcTm(normalized_day, 0, 0, 0, &midnight_tm)) {
        return normalized_day;
    }
    const std::time_t midnight_seconds = timegm(&midnight_tm);
    if (midnight_seconds < 0) {
        return normalized_day;
    }

    const std::time_t shifted_seconds =
        midnight_seconds + static_cast<std::time_t>(day_offset) * 24 * 60 * 60;
    const std::tm shifted_tm = *gmtime(&shifted_seconds);
    std::ostringstream oss;
    oss << std::put_time(&shifted_tm, "%Y%m%d");
    return oss.str();
}

std::string DeriveActionDayFromTradingDayAndUpdateTime(const std::string& trading_day,
                                                       const std::string& update_time) {
    const std::string normalized_day = NormalizeTradingDay(trading_day);
    if (normalized_day.empty()) {
        return "";
    }

    int hour = 0;
    int minute = 0;
    int second = 0;
    if (!ParseTimeHms(update_time, &hour, &minute, &second)) {
        return normalized_day;
    }
    if (hour >= 20) {
        return ShiftTradingDay(normalized_day, -1);
    }
    return normalized_day;
}

std::string ResolveActionDay(const std::string& trading_day, const std::string& action_day,
                             const std::string& update_time) {
    const std::string normalized_action_day = NormalizeTradingDay(action_day);
    if (!normalized_action_day.empty()) {
        return normalized_action_day;
    }
    return DeriveActionDayFromTradingDayAndUpdateTime(trading_day, update_time);
}

std::string DateTimeFromDayAndUpdateTime(const std::string& day, const std::string& update_time) {
    const std::string normalized_day = NormalizeTradingDay(day);
    int hour = 0;
    int minute = 0;
    int second = 0;
    if (normalized_day.size() != 8 || !ParseTimeHms(update_time, &hour, &minute, &second)) {
        return "";
    }
    std::ostringstream oss;
    oss << normalized_day.substr(0, 4) << '-' << normalized_day.substr(4, 2) << '-'
        << normalized_day.substr(6, 2) << ' ' << std::setw(2) << std::setfill('0') << hour << ':'
        << std::setw(2) << std::setfill('0') << minute << ':' << std::setw(2) << std::setfill('0')
        << second;
    return oss.str();
}

std::string LocalDateTimeFromTradingDayAndUpdateTime(const std::string& trading_day,
                                                     const std::string& action_day,
                                                     const std::string& update_time,
                                                     EpochNanos fallback_ts_ns) {
    const std::string resolved_action_day = ResolveActionDay(trading_day, action_day, update_time);
    const std::string display = DateTimeFromDayAndUpdateTime(resolved_action_day, update_time);
    if (!display.empty()) {
        return display;
    }
    return DateTimeFromEpochNs(fallback_ts_ns);
}

BarAggregator& SharedSessionResolver() {
    static BarAggregator resolver([] {
        BarAggregatorConfig config;
        config.filter_non_trading_ticks = false;
        config.is_backtest_mode = true;
        return config;
    }());
    return resolver;
}

int ResolveSessionOrderForOutput(const std::string& exchange, const std::string& symbol,
                                 const std::string& update_time) {
    std::string resolved_update_time = update_time;
    if (resolved_update_time.empty()) {
        return std::numeric_limits<int>::max();
    }
    return SharedSessionResolver().ResolveSessionOrder(exchange, symbol, resolved_update_time);
}

std::string ResolveSessionKey(const std::string& exchange_id, const std::string& instrument_id,
                              const std::string& update_time) {
    const std::string exchange =
        exchange_id.empty() ? SharedSessionResolver().InferExchangeId(instrument_id) : exchange_id;
    return SharedSessionResolver().ResolveSessionKey(exchange, instrument_id, update_time);
}

std::string BuildReplayMinuteKey(const std::string& trading_day, const std::string& update_time) {
    const std::string normalized_day = NormalizeTradingDay(trading_day);
    if (normalized_day.empty() || update_time.size() < 5) {
        return "";
    }
    return normalized_day + " " + update_time.substr(0, 5);
}

std::string BuildReplayBarContextKey(const std::string& instrument_id,
                                     const std::string& minute_key) {
    if (instrument_id.empty() || minute_key.empty()) {
        return "";
    }
    return instrument_id + "|" + minute_key;
}

std::filesystem::path AlternateProductConfigCandidate(
    const std::filesystem::path& configured_path) {
    const std::string filename = ToLower(configured_path.filename().string());
    if (filename == "instrument_info.json") {
        return configured_path.parent_path() / "products_info.yaml";
    }
    if (filename == "products_info.yaml" || filename == "products_info.yml") {
        return configured_path.parent_path() / "instrument_info.json";
    }
    return {};
}

void AppendUniqueProductConfigCandidate(std::vector<std::filesystem::path>* candidates,
                                        const std::filesystem::path& candidate) {
    if (candidates == nullptr || candidate.empty()) {
        return;
    }
    const std::filesystem::path normalized = candidate.lexically_normal();
    if (std::find(candidates->begin(), candidates->end(), normalized) == candidates->end()) {
        candidates->push_back(normalized);
    }
}

bool ResolveBacktestProductConfigPath(const std::string& configured_path,
                                      const std::string& strategy_main_config_path,
                                      std::string* out_path, std::string* error) {
    if (out_path == nullptr) {
        if (error != nullptr) {
            *error = "product config output path is null";
        }
        return false;
    }

    std::vector<std::filesystem::path> candidates;
    std::filesystem::path repo_root;
    std::filesystem::path source_file = std::filesystem::path(__FILE__);
    for (int i = 0; i < 3 && source_file.has_parent_path(); ++i) {
        source_file = source_file.parent_path();
    }
    if (!source_file.empty()) {
        repo_root = source_file;
    }

    auto append_repo_relative_candidate = [&](const std::filesystem::path& candidate) {
        if (candidate.empty() || candidate.is_absolute() || repo_root.empty()) {
            return;
        }
        AppendUniqueProductConfigCandidate(&candidates, repo_root / candidate);
    };

    if (!configured_path.empty()) {
        const std::filesystem::path configured(configured_path);
        AppendUniqueProductConfigCandidate(&candidates, configured);
        AppendUniqueProductConfigCandidate(&candidates,
                                           AlternateProductConfigCandidate(configured));
        append_repo_relative_candidate(configured);
        append_repo_relative_candidate(AlternateProductConfigCandidate(configured));
    } else {
        AppendUniqueProductConfigCandidate(&candidates, "configs/strategies/instrument_info.json");
        AppendUniqueProductConfigCandidate(&candidates, "configs/strategies/products_info.yaml");
        append_repo_relative_candidate("configs/strategies/instrument_info.json");
        append_repo_relative_candidate("configs/strategies/products_info.yaml");
        if (!strategy_main_config_path.empty()) {
            const std::filesystem::path main_config_parent =
                std::filesystem::path(strategy_main_config_path).parent_path();
            AppendUniqueProductConfigCandidate(&candidates,
                                               main_config_parent / "instrument_info.json");
            AppendUniqueProductConfigCandidate(&candidates,
                                               main_config_parent / "products_info.yaml");
        }
    }

    for (const auto& candidate : candidates) {
        std::error_code ec;
        const bool exists = std::filesystem::exists(candidate, ec);
        if (ec || !exists) {
            continue;
        }
        std::ifstream input(candidate);
        if (!input.is_open()) {
            if (error != nullptr) {
                *error = "unable to read product config: " + candidate.string();
            }
            return false;
        }
        *out_path = candidate.string();
        return true;
    }

    if (!configured_path.empty()) {
        if (error != nullptr) {
            if (candidates.size() >= 2) {
                *error = "product config is required: missing " + candidates[0].string() + " and " +
                         candidates[1].string();
            } else {
                *error = "product config is required: missing " + configured_path;
            }
        }
        return false;
    }

    out_path->clear();
    return true;
}

bool ResolveTradingSessionsConfigPathForParquetBacktest(std::string* out_path, std::string* error) {
    if (out_path == nullptr) {
        if (error != nullptr) {
            *error = "trading sessions config output is null";
        }
        return false;
    }

    const char* env_path = std::getenv("TRADING_SESSIONS_CONFIG_PATH");
    std::vector<std::filesystem::path> candidates;
    if (env_path != nullptr && std::string(env_path).size() > 0) {
        candidates.emplace_back(env_path);
    } else {
        candidates.emplace_back("configs/trading_sessions.yaml");

        // Also try the repository-root relative path when tests run under build directories.
        std::filesystem::path source_file = std::filesystem::path(__FILE__);
        for (int i = 0; i < 3 && source_file.has_parent_path(); ++i) {
            source_file = source_file.parent_path();
        }
        if (!source_file.empty()) {
            const std::filesystem::path repo_relative =
                source_file / "configs" / "trading_sessions.yaml";
            if (std::find(candidates.begin(), candidates.end(), repo_relative) ==
                candidates.end()) {
                candidates.push_back(repo_relative);
            }
        }
    }

    std::string last_missing_path;
    for (const auto& candidate : candidates) {
        std::error_code ec;
        const bool exists = std::filesystem::exists(candidate, ec);
        if (ec || !exists) {
            last_missing_path = candidate.string();
            continue;
        }
        std::ifstream input(candidate);
        if (!input.is_open()) {
            if (error != nullptr) {
                *error =
                    "unable to read trading sessions config for backtest: " + candidate.string();
            }
            return false;
        }
        *out_path = candidate.string();
        return true;
    }

    if (error != nullptr) {
        if (candidates.size() == 1) {
            *error = "trading sessions config is required for backtest: " + candidates[0].string();
        } else {
            *error = "trading sessions config is required for backtest: missing " +
                     candidates[0].string() + " and " + candidates[1].string();
        }
    }
    (void)last_missing_path;
    return false;
}

std::vector<std::string> SplitCsvLine(const std::string& line) {
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

std::vector<std::string> SplitCommaList(const std::string& raw) {
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

std::string FindCell(const std::map<std::string, std::size_t>& header_index,
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

std::string InstrumentSymbolPrefix(const std::string& instrument_id) {
    std::string prefix;
    for (unsigned char ch : instrument_id) {
        if (std::isalpha(ch) == 0) {
            break;
        }
        prefix.push_back(static_cast<char>(std::tolower(ch)));
    }
    return prefix;
}

double Clamp01(double value) { return std::max(0.0, std::min(1.0, value)); }

std::string FormatDouble(double value) {
    std::ostringstream oss;
    oss << std::setprecision(12) << value;
    return oss.str();
}

bool WriteWalLine(std::ofstream* out, const std::string& line) {
    if (out == nullptr || !out->is_open()) {
        return true;
    }
    *out << line << '\n';
    return out->good();
}

bool ParseTraceOutputFormat(const std::string& raw, std::string* out_value) {
    if (out_value == nullptr) {
        return false;
    }
    const std::string normalized = ToLower(Trim(raw));
    if (normalized.empty() || normalized == "csv") {
        *out_value = "csv";
        return true;
    }
    if (normalized == "parquet" || normalized == "both") {
        *out_value = normalized;
        return true;
    }
    return false;
}

bool TraceOutputWritesCsv(const std::string& format) {
    const std::string normalized = ToLower(Trim(format));
    return normalized.empty() || normalized == "csv" || normalized == "both";
}

bool TraceOutputWritesParquet(const std::string& format) {
    const std::string normalized = ToLower(Trim(format));
    return normalized == "parquet" || normalized == "both";
}

std::filesystem::path TraceBasePath(std::filesystem::path path) {
    path.replace_extension();
    return path;
}

TraceOutputPaths ResolveTraceOutputPaths(const std::string& requested_path,
                                         const std::string& default_base_path,
                                         const std::string& format) {
    const std::filesystem::path base = requested_path.empty()
                                           ? std::filesystem::path(default_base_path)
                                           : TraceBasePath(std::filesystem::path(requested_path));
    TraceOutputPaths paths;
    if (TraceOutputWritesCsv(format)) {
        std::filesystem::path csv_path = base;
        csv_path.replace_extension(".csv");
        paths.csv_path = csv_path.string();
        paths.primary_path = paths.csv_path;
    }
    if (TraceOutputWritesParquet(format)) {
        std::filesystem::path parquet_path = base;
        parquet_path.replace_extension(".parquet");
        paths.parquet_path = parquet_path.string();
        if (paths.primary_path.empty()) {
            paths.primary_path = paths.parquet_path;
        }
    }
    return paths;
}

std::size_t P95Index(std::size_t count) {
    if (count == 0) {
        return 0;
    }
    const double scaled = std::round(static_cast<double>(count - 1) * 0.95);
    const auto index = static_cast<std::size_t>(std::max(0.0, scaled));
    return std::min(index, count - 1);
}

double Mean(const std::vector<double>& values) {
    if (values.empty()) {
        return 0.0;
    }
    const double sum = std::accumulate(values.begin(), values.end(), 0.0);
    return sum / static_cast<double>(values.size());
}

bool ExtractJsonNumber(const std::string& json, const std::string& key, double* out_value) {
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

bool ExtractJsonString(const std::string& json, const std::string& key, std::string* out_value) {
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

bool ExtractJsonBool(const std::string& json, const std::string& key, bool* out_value) {
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

bool TrackReplayBarTickContext(const ReplayTick& tick,
                               std::unordered_map<std::string, ReplayBarTickContext>* contexts,
                               std::string* error) {
    if (contexts == nullptr) {
        if (error != nullptr) {
            *error = "replay bar context map is null";
        }
        return false;
    }
    const std::string minute_key = BuildReplayMinuteKey(tick.trading_day, tick.update_time);
    const std::string context_key = BuildReplayBarContextKey(tick.instrument_id, minute_key);
    if (context_key.empty()) {
        if (error != nullptr) {
            *error = "invalid replay tick context key for instrument: " + tick.instrument_id;
        }
        return false;
    }

    auto& context = (*contexts)[context_key];
    if (!context.initialized) {
        context.first_tick = tick;
        context.last_tick = tick;
        context.initialized = true;
        return true;
    }
    context.last_tick = tick;
    return true;
}

bool ConsumeReplayBarTickContext(const BarSnapshot& bar,
                                 std::unordered_map<std::string, ReplayBarTickContext>* contexts,
                                 ReplayBarTickContext* out_context, std::string* error) {
    if (contexts == nullptr || out_context == nullptr) {
        if (error != nullptr) {
            *error = "replay bar context output is null";
        }
        return false;
    }
    const std::string context_key = BuildReplayBarContextKey(bar.instrument_id, bar.minute);
    const auto it = contexts->find(context_key);
    if (it == contexts->end() || !it->second.initialized) {
        if (error != nullptr) {
            *error = "missing replay bar context for " + context_key;
        }
        return false;
    }
    *out_context = it->second;
    contexts->erase(it);
    return true;
}

bool ParseReplayMinuteValue(const std::string& minute_key, std::string* trading_day,
                            int* minute_of_day) {
    if (trading_day == nullptr || minute_of_day == nullptr || minute_key.size() < 14) {
        return false;
    }
    if (minute_key[8] != ' ' || minute_key[11] != ':') {
        return false;
    }
    for (int i = 0; i < 8; ++i) {
        if (std::isdigit(static_cast<unsigned char>(minute_key[i])) == 0) {
            return false;
        }
    }
    if (std::isdigit(static_cast<unsigned char>(minute_key[9])) == 0 ||
        std::isdigit(static_cast<unsigned char>(minute_key[10])) == 0 ||
        std::isdigit(static_cast<unsigned char>(minute_key[12])) == 0 ||
        std::isdigit(static_cast<unsigned char>(minute_key[13])) == 0) {
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

std::string FormatReplayMinuteValue(const std::string& trading_day, int minute_of_day) {
    if (trading_day.size() != 8 || minute_of_day < 0 || minute_of_day >= 24 * 60) {
        return "";
    }
    const int hour = minute_of_day / 60;
    const int minute = minute_of_day % 60;
    std::ostringstream oss;
    oss << trading_day << ' ' << std::setw(2) << std::setfill('0') << hour << ':' << std::setw(2)
        << std::setfill('0') << minute;
    return oss.str();
}

EpochNanos ReplayMinuteStartEpochNs(const std::string& minute_key) {
    std::string trading_day;
    int minute_of_day = 0;
    if (!ParseReplayMinuteValue(minute_key, &trading_day, &minute_of_day)) {
        return 0;
    }
    const int hour = minute_of_day / 60;
    const int minute = minute_of_day % 60;
    std::ostringstream update_time;
    update_time << std::setw(2) << std::setfill('0') << hour << ':' << std::setw(2)
                << std::setfill('0') << minute << ":00";
    return ToEpochNs(trading_day, update_time.str(), 0);
}

std::string ReplayMinuteToDisplayDateTime(const std::string& minute_key,
                                          const std::string& action_day) {
    std::string trading_day;
    int minute_of_day = 0;
    if (!ParseReplayMinuteValue(minute_key, &trading_day, &minute_of_day)) {
        return "";
    }
    const std::string display_day =
        NormalizeTradingDay(action_day).empty() ? trading_day : NormalizeTradingDay(action_day);
    if (display_day.size() != 8) {
        return "";
    }
    const int hour = minute_of_day / 60;
    const int minute = minute_of_day % 60;
    std::ostringstream oss;
    oss << display_day.substr(0, 4) << '-' << display_day.substr(4, 2) << '-'
        << display_day.substr(6, 2) << ' ' << std::setw(2) << std::setfill('0') << hour << ':'
        << std::setw(2) << std::setfill('0') << minute;
    return oss.str();
}

}  // namespace detail

bool IsApproxEqual(double left, double right, double abs_tol, double rel_tol) {
    const double diff = std::fabs(left - right);
    if (diff <= abs_tol) {
        return true;
    }
    const double scale = std::max(std::fabs(left), std::fabs(right));
    return scale > 0.0 && (diff / scale) <= rel_tol;
}

std::string DefaultBacktestStrategyId(const std::string& config_path, std::size_t index) {
    std::filesystem::path path(config_path);
    std::string stem = path.stem().string();
    if (stem.empty()) {
        stem = "strategy_" + std::to_string(index);
    }
    return stem;
}

void AppendUniqueStrings(const std::vector<std::string>& values, std::vector<std::string>* out) {
    if (out == nullptr) {
        return;
    }
    std::set<std::string> seen(out->begin(), out->end());
    for (const std::string& value : values) {
        if (value.empty() || !seen.insert(value).second) {
            continue;
        }
        out->push_back(value);
    }
}

std::string PrimaryStrategyMainConfigPath(const BacktestCliSpec& spec) {
    if (!spec.strategy_main_config_path.empty()) {
        return spec.strategy_main_config_path;
    }
    for (const BacktestStrategyConfig& config : spec.strategy_configs) {
        if (!config.strategy_main_config_path.empty()) {
            return config.strategy_main_config_path;
        }
    }
    return "";
}

StateSnapshot7D BuildStateSnapshotFromBar(const ReplayTick& /*first*/, const ReplayTick& last,
                                          const BarSnapshot& bar, EpochNanos ts_ns,
                                          std::int32_t timeframe_minutes,
                                          MarketStateDetector* detector) {
    const double analysis_open = std::isfinite(bar.analysis_open) ? bar.analysis_open : bar.open;
    const double analysis_high = std::isfinite(bar.analysis_high) ? bar.analysis_high : bar.high;
    const double analysis_low = std::isfinite(bar.analysis_low) ? bar.analysis_low : bar.low;
    const double analysis_close =
        std::isfinite(bar.analysis_close) ? bar.analysis_close : bar.close;

    double trend_score = 0.0;
    if (std::fabs(analysis_open) > 1e-9) {
        trend_score = (analysis_close - analysis_open) / std::fabs(analysis_open);
    }

    const double volatility_score =
        (std::fabs(analysis_close) > 1e-9)
            ? ((analysis_high - analysis_low) / std::fabs(analysis_close))
            : 0.0;
    const double liquidity_depth =
        std::max(0.0, static_cast<double>(last.bid_volume_1 + last.ask_volume_1 + bar.volume));
    const double liquidity_balance =
        static_cast<double>(std::min<std::int64_t>(last.bid_volume_1, last.ask_volume_1));

    StateSnapshot7D state;
    state.instrument_id = last.instrument_id;
    state.timeframe_minutes = timeframe_minutes > 0 ? timeframe_minutes : 1;
    state.trend = {trend_score, detail::Clamp01(std::fabs(trend_score) * 10.0)};
    state.volatility = {volatility_score, detail::Clamp01(volatility_score * 5.0)};
    state.liquidity = {detail::Clamp01(liquidity_depth / 1000.0),
                       detail::Clamp01(liquidity_balance / 500.0)};
    state.sentiment = {0.0, 0.1};
    state.seasonality = {0.0, 0.1};
    state.pattern = {
        analysis_close > analysis_open ? 1.0 : (analysis_close < analysis_open ? -1.0 : 0.0),
        analysis_close == analysis_open ? 0.2 : 0.7};
    state.event_drive = {0.0, 0.1};
    state.bar_open = bar.open;
    state.bar_high = bar.high;
    state.bar_low = bar.low;
    state.bar_close = bar.close;
    state.analysis_bar_open = analysis_open;
    state.analysis_bar_high = analysis_high;
    state.analysis_bar_low = analysis_low;
    state.analysis_bar_close = analysis_close;
    state.analysis_price_offset = bar.analysis_price_offset;
    state.bar_volume = static_cast<double>(bar.volume);
    state.has_bar = true;
    if (detector != nullptr) {
        detector->Update(analysis_high, analysis_low, analysis_close);
        PopulateMarketStateDiagnostics(*detector, &state);
    }
    state.ts_ns = ts_ns;
    return state;
}

StateSnapshot7D BuildStateSnapshotFromBar(const ReplayTick& first, const ReplayTick& last,
                                          double high, double low, std::int64_t volume_delta,
                                          EpochNanos ts_ns, std::int32_t timeframe_minutes,
                                          MarketStateDetector* detector) {
    BarSnapshot bar;
    bar.instrument_id = last.instrument_id;
    bar.open = first.last_price;
    bar.high = high;
    bar.low = low;
    bar.close = last.last_price;
    bar.analysis_open = bar.open;
    bar.analysis_high = bar.high;
    bar.analysis_low = bar.low;
    bar.analysis_close = bar.close;
    bar.analysis_price_offset = 0.0;
    bar.volume = volume_delta;
    bar.ts_ns = ts_ns;
    return BuildStateSnapshotFromBar(first, last, bar, ts_ns, timeframe_minutes, detector);
}

double NormalizeContractMultiplier(double contract_multiplier) {
    return std::isfinite(contract_multiplier) && contract_multiplier > 0.0 ? contract_multiplier
                                                                           : 1.0;
}

double ResolveContractMultiplier(const ProductFeeEntry* fee_entry) {
    if (fee_entry == nullptr) {
        return 1.0;
    }
    return NormalizeContractMultiplier(fee_entry->contract_multiplier);
}

double ResolveContractMultiplier(const ProductFeeBook* product_fee_book,
                                 const std::string& instrument_id) {
    if (product_fee_book == nullptr) {
        return 1.0;
    }
    return ResolveContractMultiplier(product_fee_book->Find(instrument_id));
}

void ApplyTrade(PositionState* state, Side side, std::int32_t volume, double fill_price,
                double contract_multiplier) {
    if (state == nullptr || volume <= 0) {
        return;
    }

    const double effective_multiplier = NormalizeContractMultiplier(contract_multiplier);
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
        state->realized_pnl += (fill_price - state->avg_open_price) *
                               static_cast<double>(close_qty) * effective_multiplier;
        state->net_position -= close_qty;
        remaining -= close_qty;
    } else {
        const std::int32_t short_abs = std::abs(state->net_position);
        const std::int32_t close_qty = std::min(short_abs, remaining);
        state->realized_pnl += (state->avg_open_price - fill_price) *
                               static_cast<double>(close_qty) * effective_multiplier;
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

double ComputeUnrealized(std::int32_t net_position, double avg_open_price, double last_price,
                         double contract_multiplier) {
    const double effective_multiplier = NormalizeContractMultiplier(contract_multiplier);
    if (net_position > 0) {
        return (last_price - avg_open_price) * static_cast<double>(net_position) *
               effective_multiplier;
    }
    if (net_position < 0) {
        return (avg_open_price - last_price) * static_cast<double>(std::abs(net_position)) *
               effective_multiplier;
    }
    return 0.0;
}

double ComputeTotalPnl(const std::map<std::string, PositionState>& state_by_instrument,
                       const std::map<std::string, double>& mark_price_by_instrument,
                       const ProductFeeBook* product_fee_book) {
    double total = 0.0;
    for (const auto& [instrument_id, state] : state_by_instrument) {
        double mark = state.avg_open_price;
        const auto it = mark_price_by_instrument.find(instrument_id);
        if (it != mark_price_by_instrument.end()) {
            mark = it->second;
        }
        total += state.realized_pnl +
                 ComputeUnrealized(state.net_position, state.avg_open_price, mark,
                                   ResolveContractMultiplier(product_fee_book, instrument_id));
    }
    return total;
}

double ComputeTotalEquity(double initial_equity,
                          const std::map<std::string, PositionState>& state_by_instrument,
                          const std::map<std::string, double>& mark_price_by_instrument,
                          double total_commission, const ProductFeeBook* product_fee_book) {
    return initial_equity +
           ComputeTotalPnl(state_by_instrument, mark_price_by_instrument, product_fee_book) -
           total_commission;
}

double ComputeInstrumentMarginUsed(const std::string& instrument_id, const PositionState& state,
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

double ComputeTotalMarginUsed(const std::map<std::string, PositionState>& state_by_instrument,
                              const std::map<std::string, double>& mark_price_by_instrument,
                              const ProductFeeBook& product_fee_book) {
    double total = 0.0;
    for (const auto& [instrument_id, state] : state_by_instrument) {
        total += ComputeInstrumentMarginUsed(instrument_id, state, mark_price_by_instrument,
                                             product_fee_book);
    }
    return total;
}

std::pair<double, double> ComputeRolloverPrice(Side side, double last_price, double bid_price,
                                               double ask_price, const std::string& price_mode,
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

std::vector<std::string> ValidateInvariants(
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

std::string SideToString(Side side) { return side == Side::kBuy ? "BUY" : "SELL"; }

std::string SideToTitleString(Side side) { return side == Side::kBuy ? "Buy" : "Sell"; }

std::string OffsetFlagToString(OffsetFlag offset) {
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

std::string OffsetFlagToTitleString(OffsetFlag offset) {
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

std::string OrderStatusToString(OrderStatus status) {
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

std::string SignalTypeToString(SignalType signal_type) {
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

std::string MarketRegimeToString(MarketRegime regime) {
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

std::string BuildDefaultIndicatorTraceBasePath(const std::string& run_id) {
    return (std::filesystem::path("runtime") / "research" / "indicator_trace" / run_id).string();
}

std::string BuildDefaultIndicatorTracePath(const std::string& run_id, const std::string& format) {
    return detail::ResolveTraceOutputPaths("", BuildDefaultIndicatorTraceBasePath(run_id), format)
        .primary_path;
}

std::string BuildDefaultSubStrategyIndicatorTraceBasePath(const std::string& run_id) {
    return (std::filesystem::path("runtime") / "research" / "sub_strategy_indicator_trace" / run_id)
        .string();
}

std::string BuildDefaultSubStrategyIndicatorTracePath(const std::string& run_id,
                                                      const std::string& format) {
    return detail::ResolveTraceOutputPaths(
               "", BuildDefaultSubStrategyIndicatorTraceBasePath(run_id), format)
        .primary_path;
}

}  // namespace quant_hft::backtest
