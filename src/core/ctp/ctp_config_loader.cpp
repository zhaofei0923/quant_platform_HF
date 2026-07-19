#include "quant_hft/core/ctp_config_loader.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace quant_hft {
namespace {

std::string Trim(std::string value) {
    const auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
    value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        return value.substr(1, value.size() - 2);
    }
    return value;
}

std::string Lowercase(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

std::unordered_map<std::string, std::string> LoadSimpleYaml(const std::string& path,
                                                            std::string* error) {
    std::unordered_map<std::string, std::string> kv;
    std::ifstream in(path);
    if (!in.is_open()) {
        if (error != nullptr) {
            *error = "unable to open config: " + path;
        }
        return kv;
    }

    std::string line;
    std::vector<std::pair<std::size_t, std::string>> section_stack;
    while (std::getline(in, line)) {
        const auto hash = line.find('#');
        if (hash != std::string::npos) {
            line = line.substr(0, hash);
        }
        const auto first_non_space = line.find_first_not_of(" \t");
        if (first_non_space == std::string::npos) {
            continue;
        }
        const std::size_t indent = first_non_space;
        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed == "ctp:") {
            continue;
        }

        while (!section_stack.empty() && indent <= section_stack.back().first) {
            section_stack.pop_back();
        }

        const auto pos = trimmed.find(':');
        if (pos == std::string::npos) {
            continue;
        }

        const auto key = Trim(trimmed.substr(0, pos));
        const bool is_section = !trimmed.empty() && trimmed.back() == ':';
        if (is_section) {
            if (key != "ctp") {
                section_stack.emplace_back(indent, key);
            }
            continue;
        }

        auto value = Trim(trimmed.substr(pos + 1));
        if (!key.empty()) {
            std::string full_key;
            for (const auto& section : section_stack) {
                if (!full_key.empty()) {
                    full_key.push_back('.');
                }
                full_key += section.second;
            }
            if (!full_key.empty()) {
                full_key.push_back('.');
            }
            full_key += key;
            kv[full_key] = ResolveEnvVars(value);
        }
    }
    return kv;
}

bool ParseBoolValue(const std::string& value, bool* out) {
    if (out == nullptr) {
        return false;
    }
    const auto normalized = Lowercase(Trim(value));
    if (normalized == "true" || normalized == "1" || normalized == "yes") {
        *out = true;
        return true;
    }
    if (normalized == "false" || normalized == "0" || normalized == "no") {
        *out = false;
        return true;
    }
    return false;
}

bool ParseIntValue(const std::string& value, int* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        *out = std::stoi(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseDoubleValue(const std::string& value, double* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        *out = std::stod(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool IsValidLogLevel(const std::string& value) {
    const auto normalized = Lowercase(Trim(value));
    return normalized == "debug" || normalized == "info" || normalized == "warn" ||
           normalized == "warning" || normalized == "error";
}

bool IsValidLogSink(const std::string& value) {
    const auto normalized = Lowercase(Trim(value));
    return normalized == "stdout" || normalized == "stderr";
}

void SetOptionalInt(const std::unordered_map<std::string, std::string>& kv, const char* key,
                    int* target, std::string* error) {
    const auto it = kv.find(key);
    if (it == kv.end()) {
        return;
    }
    int parsed = 0;
    if (!ParseIntValue(it->second, &parsed)) {
        if (error != nullptr) {
            *error = std::string("invalid integer for key: ") + key;
        }
        return;
    }
    *target = parsed;
}

void SetOptionalDouble(const std::unordered_map<std::string, std::string>& kv, const char* key,
                       double* target, std::string* error) {
    const auto it = kv.find(key);
    if (it == kv.end()) {
        return;
    }
    double parsed = 0.0;
    if (!ParseDoubleValue(it->second, &parsed)) {
        if (error != nullptr) {
            *error = std::string("invalid double for key: ") + key;
        }
        return;
    }
    *target = parsed;
}

bool IsValidHhmm(int value) {
    if (value < 0 || value > 2359) {
        return false;
    }
    const int minute = value % 100;
    return minute >= 0 && minute < 60;
}

std::vector<std::string> SplitCsvList(const std::string& raw) {
    std::vector<std::string> values;
    std::size_t start = 0;
    while (start <= raw.size()) {
        const auto end = raw.find(',', start);
        const auto item =
            end == std::string::npos ? raw.substr(start) : raw.substr(start, end - start);
        const auto trimmed = Trim(item);
        if (!trimmed.empty()) {
            values.push_back(trimmed);
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return values;
}

bool ParseExecutionMode(const std::string& raw, ExecutionMode* out) {
    if (out == nullptr) {
        return false;
    }
    const auto normalized = Lowercase(Trim(raw));
    if (normalized.empty() || normalized == "direct") {
        *out = ExecutionMode::kDirect;
        return true;
    }
    if (normalized == "sliced") {
        *out = ExecutionMode::kSliced;
        return true;
    }
    return false;
}

bool ParseExecutionAlgo(const std::string& raw, ExecutionAlgo* out) {
    if (out == nullptr) {
        return false;
    }
    const auto normalized = Lowercase(Trim(raw));
    if (normalized.empty() || normalized == "direct") {
        *out = ExecutionAlgo::kDirect;
        return true;
    }
    if (normalized == "sliced") {
        *out = ExecutionAlgo::kSliced;
        return true;
    }
    if (normalized == "twap") {
        *out = ExecutionAlgo::kTwap;
        return true;
    }
    if (normalized == "vwap_lite" || normalized == "vwap-lite" || normalized == "vwap") {
        *out = ExecutionAlgo::kVwapLite;
        return true;
    }
    return false;
}

bool ParseExecutionPriceMode(const std::string& raw, ExecutionPriceMode* out) {
    if (out == nullptr) {
        return false;
    }
    const auto normalized = Lowercase(Trim(raw));
    if (normalized.empty() || normalized == "signal_limit" || normalized == "signal-limit" ||
        normalized == "limit") {
        *out = ExecutionPriceMode::kSignalLimit;
        return true;
    }
    if (normalized == "marketable_limit" || normalized == "marketable-limit" ||
        normalized == "market") {
        *out = ExecutionPriceMode::kMarketableLimit;
        return true;
    }
    return false;
}

constexpr std::array<const char*, 12> kMarketStateDetectorFields = {
    "adx_period",         "adx_strong_threshold", "adx_weak_lower",        "adx_weak_upper",
    "kama_er_period",     "kama_fast_period",     "kama_slow_period",      "kama_er_strong",
    "kama_er_weak_lower", "atr_period",           "require_adx_for_trend", "use_kama_er",
};

bool ApplyMarketStateDetectorField(MarketStateDetectorConfig* detector, const std::string& field,
                                   const std::string& raw_value, const std::string& key,
                                   std::string* error) {
    if (detector == nullptr) {
        if (error != nullptr) {
            *error = "market_state_detector output is null";
        }
        return false;
    }
    const auto parse_int = [&](int* target) -> bool {
        if (!ParseIntValue(raw_value, target)) {
            if (error != nullptr) {
                *error = "invalid integer for key: " + key;
            }
            return false;
        }
        return true;
    };
    const auto parse_double = [&](double* target) -> bool {
        if (!ParseDoubleValue(raw_value, target)) {
            if (error != nullptr) {
                *error = "invalid double for key: " + key;
            }
            return false;
        }
        return true;
    };
    const auto parse_bool = [&](bool* target) -> bool {
        if (!ParseBoolValue(raw_value, target)) {
            if (error != nullptr) {
                *error = "invalid bool for key: " + key;
            }
            return false;
        }
        return true;
    };

    if (field == "adx_period") {
        return parse_int(&detector->adx_period);
    }
    if (field == "adx_strong_threshold") {
        return parse_double(&detector->adx_strong_threshold);
    }
    if (field == "adx_weak_lower") {
        return parse_double(&detector->adx_weak_lower);
    }
    if (field == "adx_weak_upper") {
        return parse_double(&detector->adx_weak_upper);
    }
    if (field == "kama_er_period") {
        return parse_int(&detector->kama_er_period);
    }
    if (field == "kama_fast_period") {
        return parse_int(&detector->kama_fast_period);
    }
    if (field == "kama_slow_period") {
        return parse_int(&detector->kama_slow_period);
    }
    if (field == "kama_er_strong") {
        return parse_double(&detector->kama_er_strong);
    }
    if (field == "kama_er_weak_lower") {
        return parse_double(&detector->kama_er_weak_lower);
    }
    if (field == "atr_period") {
        return parse_int(&detector->atr_period);
    }
    if (field == "atr_flat_ratio" || field == "min_bars_for_flat") {
        // Deprecated ATR flat-gate keys are accepted as no-ops for legacy configs.
        return true;
    }
    if (field == "require_adx_for_trend") {
        return parse_bool(&detector->require_adx_for_trend);
    }
    if (field == "use_kama_er") {
        return parse_bool(&detector->use_kama_er);
    }

    if (error != nullptr) {
        *error = "unknown market_state_detector key: " + key;
    }
    return false;
}

}  // namespace

bool CtpConfigLoader::LoadFromYaml(const std::string& path, CtpFileConfig* config,
                                   std::string* error) {
    if (config == nullptr) {
        if (error != nullptr) {
            *error = "output config pointer is null";
        }
        return false;
    }

    std::string load_error;
    const auto kv = LoadSimpleYaml(path, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }

    CtpFileConfig loaded;
    loaded.runtime.flow_path = "./ctp_flow";

    const auto environment_it = kv.find("environment");
    const auto environment = environment_it == kv.end() ? "sim" : Lowercase(environment_it->second);
    if (environment == "sim" || environment == "simnow") {
        loaded.runtime.environment = CtpEnvironment::kSimNow;
    } else if (environment == "prod" || environment == "production") {
        loaded.runtime.environment = CtpEnvironment::kProduction;
    } else {
        if (error != nullptr) {
            *error = "invalid environment: " + environment;
        }
        return false;
    }

    const auto production_mode_it = kv.find("is_production_mode");
    if (production_mode_it == kv.end()) {
        if (error != nullptr) {
            *error = "is_production_mode must be explicitly configured";
        }
        return false;
    }
    if (!ParseBoolValue(production_mode_it->second, &loaded.runtime.is_production_mode)) {
        if (error != nullptr) {
            *error = "invalid bool value for is_production_mode";
        }
        return false;
    }

    loaded.runtime.enable_real_api = false;
    if (const auto enable_real_api_it = kv.find("enable_real_api");
        enable_real_api_it != kv.end()) {
        if (!ParseBoolValue(enable_real_api_it->second, &loaded.runtime.enable_real_api)) {
            if (error != nullptr) {
                *error = "invalid bool value for enable_real_api";
            }
            return false;
        }
    }

    loaded.runtime.enable_terminal_auth = true;
    if (const auto terminal_auth_it = kv.find("enable_terminal_auth");
        terminal_auth_it != kv.end()) {
        if (!ParseBoolValue(terminal_auth_it->second, &loaded.runtime.enable_terminal_auth)) {
            if (error != nullptr) {
                *error = "invalid bool value for enable_terminal_auth";
            }
            return false;
        }
    }
    loaded.runtime.settlement_confirm_required = true;
    if (const auto settlement_confirm_it = kv.find("settlement_confirm_required");
        settlement_confirm_it != kv.end()) {
        if (!ParseBoolValue(settlement_confirm_it->second,
                            &loaded.runtime.settlement_confirm_required)) {
            if (error != nullptr) {
                *error = "invalid bool value for settlement_confirm_required";
            }
            return false;
        }
    }

    loaded.runtime.metrics_enabled = false;
    if (const auto metrics_enabled_it = kv.find("metrics_enabled");
        metrics_enabled_it != kv.end()) {
        if (!ParseBoolValue(metrics_enabled_it->second, &loaded.runtime.metrics_enabled)) {
            if (error != nullptr) {
                *error = "invalid bool value for metrics_enabled";
            }
            return false;
        }
    }

    SetOptionalInt(kv, "metrics_port", &loaded.runtime.metrics_port, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.metrics_port <= 0) {
        if (error != nullptr) {
            *error = "metrics_port must be > 0";
        }
        return false;
    }

    auto get_value = [&](const std::string& key) -> std::string {
        const auto it = kv.find(key);
        if (it == kv.end()) {
            return "";
        }
        return ResolveEnvVars(it->second);
    };

    loaded.runtime.md_front = get_value("market_front");
    if (loaded.runtime.md_front.empty()) {
        loaded.runtime.md_front = get_value("md_front");
    }
    loaded.runtime.td_front = get_value("trader_front");
    if (loaded.runtime.td_front.empty()) {
        loaded.runtime.td_front = get_value("td_front");
    }
    loaded.runtime.flow_path =
        get_value("flow_path").empty() ? loaded.runtime.flow_path : get_value("flow_path");
    loaded.runtime.broker_id = get_value("broker_id");
    loaded.runtime.user_id = get_value("user_id");
    loaded.runtime.investor_id = get_value("investor_id");
    if (loaded.runtime.investor_id.empty()) {
        loaded.runtime.investor_id = loaded.runtime.user_id;
    }
    loaded.runtime.app_id = get_value("app_id");
    loaded.runtime.auth_code = get_value("auth_code");
    if (!get_value("log_level").empty()) {
        if (!IsValidLogLevel(get_value("log_level"))) {
            if (error != nullptr) {
                *error = "invalid log_level, expected one of: debug/info/warn/error";
            }
            return false;
        }
        loaded.runtime.log_level = Lowercase(get_value("log_level"));
        if (loaded.runtime.log_level == "warning") {
            loaded.runtime.log_level = "warn";
        }
    }
    if (!get_value("log_sink").empty()) {
        if (!IsValidLogSink(get_value("log_sink"))) {
            if (error != nullptr) {
                *error = "invalid log_sink, expected one of: stdout/stderr";
            }
            return false;
        }
        loaded.runtime.log_sink = Lowercase(get_value("log_sink"));
    }
    loaded.runtime.last_login_time = get_value("last_login_time");
    loaded.runtime.reserve_info = get_value("reserve_info");
    if (!get_value("offset_apply_src").empty()) {
        loaded.runtime.offset_apply_src = get_value("offset_apply_src").front();
    }

    SetOptionalInt(kv, "connect_timeout_ms", &loaded.runtime.connect_timeout_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    SetOptionalInt(kv, "reconnect_max_attempts", &loaded.runtime.reconnect_max_attempts,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    SetOptionalInt(kv, "reconnect_initial_backoff_ms", &loaded.runtime.reconnect_initial_backoff_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    SetOptionalInt(kv, "reconnect_max_backoff_ms", &loaded.runtime.reconnect_max_backoff_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    SetOptionalInt(kv, "gateway_reconnect_cycle_cooldown_ms",
                   &loaded.runtime.reconnect_cycle_cooldown_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    SetOptionalInt(kv, "recovery_quiet_period_ms", &loaded.runtime.recovery_quiet_period_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }

    loaded.query_rate_limit_qps = 10;
    SetOptionalInt(kv, "query_rate_limit_qps", &loaded.query_rate_limit_qps, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    loaded.runtime.query_rate_per_sec = loaded.query_rate_limit_qps;
    SetOptionalInt(kv, "query_rate_per_sec", &loaded.runtime.query_rate_per_sec, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.query_rate_per_sec <= 0) {
        if (error != nullptr) {
            *error = "query_rate_per_sec must be > 0";
        }
        return false;
    }
    loaded.query_rate_limit_qps = loaded.runtime.query_rate_per_sec;

    loaded.runtime.settlement_query_rate_per_sec = 2;
    SetOptionalInt(kv, "settlement_query_rate_per_sec",
                   &loaded.runtime.settlement_query_rate_per_sec, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.settlement_query_rate_per_sec <= 0) {
        if (error != nullptr) {
            *error = "settlement_query_rate_per_sec must be > 0";
        }
        return false;
    }

    loaded.runtime.order_insert_rate_per_sec = 50;
    SetOptionalInt(kv, "order_insert_rate_per_sec", &loaded.runtime.order_insert_rate_per_sec,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.order_insert_rate_per_sec <= 0) {
        if (error != nullptr) {
            *error = "order_insert_rate_per_sec must be > 0";
        }
        return false;
    }

    loaded.runtime.order_cancel_rate_per_sec = 50;
    SetOptionalInt(kv, "order_cancel_rate_per_sec", &loaded.runtime.order_cancel_rate_per_sec,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.order_cancel_rate_per_sec <= 0) {
        if (error != nullptr) {
            *error = "order_cancel_rate_per_sec must be > 0";
        }
        return false;
    }

    loaded.runtime.order_bucket_capacity = 20;
    SetOptionalInt(kv, "order_bucket_capacity", &loaded.runtime.order_bucket_capacity, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.order_bucket_capacity <= 0) {
        if (error != nullptr) {
            *error = "order_bucket_capacity must be > 0";
        }
        return false;
    }

    loaded.runtime.cancel_bucket_capacity = 20;
    SetOptionalInt(kv, "cancel_bucket_capacity", &loaded.runtime.cancel_bucket_capacity,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.cancel_bucket_capacity <= 0) {
        if (error != nullptr) {
            *error = "cancel_bucket_capacity must be > 0";
        }
        return false;
    }

    loaded.runtime.query_bucket_capacity = 5;
    SetOptionalInt(kv, "query_bucket_capacity", &loaded.runtime.query_bucket_capacity, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.query_bucket_capacity <= 0) {
        if (error != nullptr) {
            *error = "query_bucket_capacity must be > 0";
        }
        return false;
    }

    loaded.runtime.settlement_query_bucket_capacity = 2;
    SetOptionalInt(kv, "settlement_query_bucket_capacity",
                   &loaded.runtime.settlement_query_bucket_capacity, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.settlement_query_bucket_capacity <= 0) {
        if (error != nullptr) {
            *error = "settlement_query_bucket_capacity must be > 0";
        }
        return false;
    }

    loaded.runtime.settlement_retry_max = 3;
    SetOptionalInt(kv, "settlement_retry_max", &loaded.runtime.settlement_retry_max, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.settlement_retry_max <= 0) {
        if (error != nullptr) {
            *error = "settlement_retry_max must be > 0";
        }
        return false;
    }

    loaded.runtime.settlement_retry_backoff_initial_ms = 1000;
    SetOptionalInt(kv, "settlement_retry_backoff_initial_ms",
                   &loaded.runtime.settlement_retry_backoff_initial_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.settlement_retry_backoff_initial_ms <= 0) {
        if (error != nullptr) {
            *error = "settlement_retry_backoff_initial_ms must be > 0";
        }
        return false;
    }

    loaded.runtime.settlement_retry_backoff_max_ms = 5000;
    SetOptionalInt(kv, "settlement_retry_backoff_max_ms",
                   &loaded.runtime.settlement_retry_backoff_max_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.settlement_retry_backoff_max_ms <
        loaded.runtime.settlement_retry_backoff_initial_ms) {
        if (error != nullptr) {
            *error =
                "settlement_retry_backoff_max_ms must be >= settlement_retry_backoff_initial_ms";
        }
        return false;
    }

    loaded.runtime.settlement_running_stale_timeout_ms = 300000;
    SetOptionalInt(kv, "settlement_running_stale_timeout_ms",
                   &loaded.runtime.settlement_running_stale_timeout_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.settlement_running_stale_timeout_ms <= 0) {
        if (error != nullptr) {
            *error = "settlement_running_stale_timeout_ms must be > 0";
        }
        return false;
    }

    loaded.runtime.settlement_shadow_enabled = false;
    if (const auto settlement_shadow_it = kv.find("settlement_shadow_enabled");
        settlement_shadow_it != kv.end()) {
        if (!ParseBoolValue(settlement_shadow_it->second,
                            &loaded.runtime.settlement_shadow_enabled)) {
            if (error != nullptr) {
                *error = "invalid bool value for settlement_shadow_enabled";
            }
            return false;
        }
    }

    loaded.runtime.cancel_retry_max = 3;
    SetOptionalInt(kv, "cancel_retry_max", &loaded.runtime.cancel_retry_max, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.cancel_retry_max <= 0) {
        if (error != nullptr) {
            *error = "cancel_retry_max must be > 0";
        }
        return false;
    }

    loaded.runtime.cancel_retry_base_ms = 1000;
    SetOptionalInt(kv, "cancel_retry_base_ms", &loaded.runtime.cancel_retry_base_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.cancel_retry_base_ms <= 0) {
        if (error != nullptr) {
            *error = "cancel_retry_base_ms must be > 0";
        }
        return false;
    }

    loaded.runtime.cancel_retry_max_delay_ms = 5000;
    SetOptionalInt(kv, "cancel_retry_max_delay_ms", &loaded.runtime.cancel_retry_max_delay_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.cancel_retry_max_delay_ms < loaded.runtime.cancel_retry_base_ms) {
        if (error != nullptr) {
            *error = "cancel_retry_max_delay_ms must be >= cancel_retry_base_ms";
        }
        return false;
    }

    loaded.runtime.cancel_wait_ack_timeout_ms = 1200;
    SetOptionalInt(kv, "cancel_wait_ack_timeout_ms", &loaded.runtime.cancel_wait_ack_timeout_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.cancel_wait_ack_timeout_ms <= 0) {
        if (error != nullptr) {
            *error = "cancel_wait_ack_timeout_ms must be > 0";
        }
        return false;
    }

    loaded.runtime.breaker_failure_threshold = 5;
    SetOptionalInt(kv, "breaker_failure_threshold", &loaded.runtime.breaker_failure_threshold,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    loaded.runtime.breaker_timeout_ms = 1000;
    SetOptionalInt(kv, "breaker_timeout_ms", &loaded.runtime.breaker_timeout_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    loaded.runtime.breaker_half_open_timeout_ms = 5000;
    SetOptionalInt(kv, "breaker_half_open_timeout_ms", &loaded.runtime.breaker_half_open_timeout_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }

    if (const auto strategy_breaker_it = kv.find("breaker_strategy_enabled");
        strategy_breaker_it != kv.end()) {
        if (!ParseBoolValue(strategy_breaker_it->second,
                            &loaded.runtime.breaker_strategy_enabled)) {
            if (error != nullptr) {
                *error = "invalid bool value for breaker_strategy_enabled";
            }
            return false;
        }
    }
    if (const auto account_breaker_it = kv.find("breaker_account_enabled");
        account_breaker_it != kv.end()) {
        if (!ParseBoolValue(account_breaker_it->second, &loaded.runtime.breaker_account_enabled)) {
            if (error != nullptr) {
                *error = "invalid bool value for breaker_account_enabled";
            }
            return false;
        }
    }
    if (const auto system_breaker_it = kv.find("breaker_system_enabled");
        system_breaker_it != kv.end()) {
        if (!ParseBoolValue(system_breaker_it->second, &loaded.runtime.breaker_system_enabled)) {
            if (error != nullptr) {
                *error = "invalid bool value for breaker_system_enabled";
            }
            return false;
        }
    }

    loaded.runtime.audit_hot_days = 7;
    SetOptionalInt(kv, "audit_hot_days", &loaded.runtime.audit_hot_days, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    loaded.runtime.audit_cold_days = 180;
    SetOptionalInt(kv, "audit_cold_days", &loaded.runtime.audit_cold_days, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }

    loaded.runtime.kafka_bootstrap_servers = get_value("kafka_bootstrap_servers");
    loaded.runtime.kafka_topic_ticks = get_value("kafka_topic_ticks");
    if (loaded.runtime.kafka_topic_ticks.empty()) {
        loaded.runtime.kafka_topic_ticks = "market.ticks.v1";
    }
    loaded.runtime.clickhouse_dsn = get_value("clickhouse_dsn");
    loaded.market_data_recording.enabled = false;
    if (const auto it = kv.find("market_data_recording_enabled"); it != kv.end()) {
        if (!ParseBoolValue(it->second, &loaded.market_data_recording.enabled)) {
            if (error != nullptr) {
                *error = "market_data_recording_enabled must be bool";
            }
            return false;
        }
    }
    loaded.market_data_recording.output_dir = "runtime/market_data";
    if (const auto output_dir = get_value("market_data_recording_dir"); !output_dir.empty()) {
        loaded.market_data_recording.output_dir = output_dir;
    }
    loaded.market_data_recording.run_id = get_value("market_data_recording_run_id");
    loaded.market_data_recording.flush_each_write = false;
    if (const auto it = kv.find("market_data_recording_flush_each_write"); it != kv.end()) {
        if (!ParseBoolValue(it->second, &loaded.market_data_recording.flush_each_write)) {
            if (error != nullptr) {
                *error = "market_data_recording_flush_each_write must be bool";
            }
            return false;
        }
    }
    loaded.market_data_recording.partition_by_product = false;
    if (const auto it = kv.find("market_data_recording_partition_by_product"); it != kv.end()) {
        if (!ParseBoolValue(it->second, &loaded.market_data_recording.partition_by_product)) {
            if (error != nullptr) {
                *error = "market_data_recording_partition_by_product must be bool";
            }
            return false;
        }
    }
    loaded.market_data_recording.write_global_copy = false;
    if (const auto it = kv.find("market_data_recording_write_global_copy"); it != kv.end()) {
        if (!ParseBoolValue(it->second, &loaded.market_data_recording.write_global_copy)) {
            if (error != nullptr) {
                *error = "market_data_recording_write_global_copy must be bool";
            }
            return false;
        }
    }
    if (loaded.market_data_recording.enabled && loaded.market_data_recording.output_dir.empty()) {
        if (error != nullptr) {
            *error = "market_data_recording_dir must not be empty when enabled";
        }
        return false;
    }
    if (loaded.query_rate_limit_qps <= 0) {
        if (error != nullptr) {
            *error = "query_rate_limit_qps must be > 0";
        }
        return false;
    }

    loaded.account_query_interval_ms = 2000;
    SetOptionalInt(kv, "account_query_interval_ms", &loaded.account_query_interval_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.account_query_interval_ms <= 0) {
        if (error != nullptr) {
            *error = "account_query_interval_ms must be > 0";
        }
        return false;
    }

    loaded.position_query_interval_ms = 2000;
    SetOptionalInt(kv, "position_query_interval_ms", &loaded.position_query_interval_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.position_query_interval_ms <= 0) {
        if (error != nullptr) {
            *error = "position_query_interval_ms must be > 0";
        }
        return false;
    }

    loaded.instrument_query_interval_ms = 30000;
    SetOptionalInt(kv, "instrument_query_interval_ms", &loaded.instrument_query_interval_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.instrument_query_interval_ms <= 0) {
        if (error != nullptr) {
            *error = "instrument_query_interval_ms must be > 0";
        }
        return false;
    }

    loaded.runtime.query_retry_backoff_ms = 200;
    SetOptionalInt(kv, "query_retry_backoff_ms", &loaded.runtime.query_retry_backoff_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.runtime.query_retry_backoff_ms < 0) {
        if (error != nullptr) {
            *error = "query_retry_backoff_ms must be >= 0";
        }
        return false;
    }

    loaded.instruments = SplitCsvList(get_value("instruments"));
    loaded.product_ids = SplitCsvList(get_value("product_ids"));
    loaded.active_contract_mode = Lowercase(Trim(get_value("active_contract_mode")));
    if (loaded.active_contract_mode.empty()) {
        loaded.active_contract_mode = "static";
    }
    if (loaded.active_contract_mode != "static" &&
        loaded.active_contract_mode != "dominant_open_interest") {
        if (error != nullptr) {
            *error = "active_contract_mode must be one of: static|dominant_open_interest";
        }
        return false;
    }
    loaded.dominant_contract_wait_ms = 5000;
    SetOptionalInt(kv, "dominant_contract_wait_ms", &loaded.dominant_contract_wait_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.dominant_contract_wait_ms <= 0) {
        if (error != nullptr) {
            *error = "dominant_contract_wait_ms must be > 0";
        }
        return false;
    }
    loaded.dominant_contract_recheck_interval_ms = 0;
    SetOptionalInt(kv, "dominant_contract_recheck_interval_ms",
                   &loaded.dominant_contract_recheck_interval_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.dominant_contract_recheck_interval_ms < 0) {
        if (error != nullptr) {
            *error = "dominant_contract_recheck_interval_ms must be >= 0";
        }
        return false;
    }

    loaded.dominant_contract_min_lead_ratio = 0.1;
    SetOptionalDouble(kv, "dominant_contract_min_lead_ratio",
                      &loaded.dominant_contract_min_lead_ratio, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.dominant_contract_min_lead_ratio < 0.0) {
        if (error != nullptr) {
            *error = "dominant_contract_min_lead_ratio must be >= 0";
        }
        return false;
    }

    loaded.dominant_contract_min_lead_windows = 3;
    SetOptionalInt(kv, "dominant_contract_min_lead_windows",
                   &loaded.dominant_contract_min_lead_windows, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.dominant_contract_min_lead_windows <= 0) {
        if (error != nullptr) {
            *error = "dominant_contract_min_lead_windows must be > 0";
        }
        return false;
    }

    loaded.dominant_contract_min_hold_ms = 0;
    SetOptionalInt(kv, "dominant_contract_min_hold_ms", &loaded.dominant_contract_min_hold_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.dominant_contract_min_hold_ms < 0) {
        if (error != nullptr) {
            *error = "dominant_contract_min_hold_ms must be >= 0";
        }
        return false;
    }

    loaded.dominant_contract_switch_mode =
        Lowercase(Trim(get_value("dominant_contract_switch_mode")));
    if (loaded.dominant_contract_switch_mode.empty()) {
        loaded.dominant_contract_switch_mode = "startup_only";
    }
    if (loaded.dominant_contract_switch_mode != "startup_only" &&
        loaded.dominant_contract_switch_mode != "dry_run" &&
        loaded.dominant_contract_switch_mode != "flat_only") {
        if (error != nullptr) {
            *error = "dominant_contract_switch_mode must be one of: startup_only|dry_run|flat_only";
        }
        return false;
    }
    loaded.strategy_ids = SplitCsvList(get_value("strategy_ids"));
    loaded.run_type = loaded.runtime.environment == CtpEnvironment::kSimNow ? "sim" : "live";
    if (const auto run_type_it = kv.find("run_type"); run_type_it != kv.end()) {
        loaded.run_type = Lowercase(Trim(run_type_it->second));
    }
    if (loaded.run_type != "live" && loaded.run_type != "sim" && loaded.run_type != "backtest") {
        if (error != nullptr) {
            *error = "run_type must be one of: live|sim|backtest";
        }
        return false;
    }
    const auto strategy_factory_it = kv.find("strategy_factory");
    loaded.strategy_factory =
        strategy_factory_it == kv.end() ? "demo" : Trim(strategy_factory_it->second);
    if (loaded.strategy_factory.empty()) {
        if (error != nullptr) {
            *error = "strategy_factory must not be empty";
        }
        return false;
    }
    const auto strategy_composite_config_it = kv.find("strategy_composite_config");
    loaded.strategy_composite_config = strategy_composite_config_it == kv.end()
                                           ? std::string()
                                           : Trim(strategy_composite_config_it->second);
    const auto resolve_composite_path = [&](const std::string& raw_path) -> std::string {
        std::filesystem::path composite_path(raw_path);
        if (composite_path.is_relative()) {
            if (!std::filesystem::exists(composite_path)) {
                const auto quant_root = GetEnvOrDefault("QUANT_ROOT", "");
                if (!quant_root.empty()) {
                    const std::filesystem::path quant_root_candidate =
                        std::filesystem::path(quant_root) / composite_path;
                    if (std::filesystem::exists(quant_root_candidate)) {
                        composite_path = quant_root_candidate;
                    }
                }
            }
            if (!std::filesystem::exists(composite_path)) {
                const std::filesystem::path config_dir = std::filesystem::path(path).parent_path();
                composite_path = config_dir / composite_path;
            }
        }
        return composite_path.lexically_normal().string();
    };
    if (!loaded.strategy_composite_config.empty()) {
        loaded.strategy_composite_config = resolve_composite_path(loaded.strategy_composite_config);
    }
    loaded.strategy_composite_config_map.clear();
    constexpr const char* kCompositeMapPrefix = "strategy_composite_config_map.";
    for (const auto& [key, value] : kv) {
        if (key.rfind(kCompositeMapPrefix, 0) != 0) {
            continue;
        }
        const std::string strategy_id =
            Trim(key.substr(std::char_traits<char>::length(kCompositeMapPrefix)));
        const std::string config_path = Trim(value);
        if (strategy_id.empty() || config_path.empty()) {
            if (error != nullptr) {
                *error =
                    "strategy_composite_config_map entries require non-empty strategy id and path";
            }
            return false;
        }
        loaded.strategy_composite_config_map[strategy_id] = resolve_composite_path(config_path);
    }
    if (Lowercase(loaded.strategy_factory) == "composite" &&
        loaded.strategy_composite_config.empty() && loaded.strategy_composite_config_map.empty()) {
        if (error != nullptr) {
            *error =
                "strategy_composite_config or strategy_composite_config_map is required when "
                "strategy_factory=composite";
        }
        return false;
    }
    if (kv.find("strategy_poll_interval_ms") != kv.end()) {
        if (error != nullptr) {
            *error = "strategy_poll_interval_ms is removed; use strategy_queue_capacity";
        }
        return false;
    }
    loaded.strategy_queue_capacity = 8192;
    SetOptionalInt(kv, "strategy_queue_capacity", &loaded.strategy_queue_capacity, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.strategy_queue_capacity <= 0) {
        if (error != nullptr) {
            *error = "strategy_queue_capacity must be > 0";
        }
        return false;
    }
    loaded.strategy_state_persist_enabled = false;
    if (const auto it = kv.find("strategy_state_persist_enabled"); it != kv.end()) {
        if (!ParseBoolValue(it->second, &loaded.strategy_state_persist_enabled)) {
            if (error != nullptr) {
                *error = "strategy_state_persist_enabled must be bool";
            }
            return false;
        }
    }
    loaded.strategy_state_backend = "redis";
    if (const auto it = kv.find("strategy_state_backend"); it != kv.end()) {
        loaded.strategy_state_backend = Lowercase(Trim(it->second));
        if (loaded.strategy_state_backend != "redis" && loaded.strategy_state_backend != "file") {
            if (error != nullptr) {
                *error = "strategy_state_backend must be redis or file";
            }
            return false;
        }
    }
    loaded.strategy_state_snapshot_interval_ms = 60'000;
    SetOptionalInt(kv, "strategy_state_snapshot_interval_ms",
                   &loaded.strategy_state_snapshot_interval_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.strategy_state_snapshot_interval_ms < 0) {
        if (error != nullptr) {
            *error = "strategy_state_snapshot_interval_ms must be >= 0";
        }
        return false;
    }
    loaded.strategy_state_ttl_seconds = 86'400;
    SetOptionalInt(kv, "strategy_state_ttl_seconds", &loaded.strategy_state_ttl_seconds,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.strategy_state_ttl_seconds < 0) {
        if (error != nullptr) {
            *error = "strategy_state_ttl_seconds must be >= 0";
        }
        return false;
    }
    loaded.strategy_state_key_prefix = "strategy_state";
    if (const auto it = kv.find("strategy_state_key_prefix"); it != kv.end()) {
        loaded.strategy_state_key_prefix = Trim(it->second);
        if (loaded.strategy_state_key_prefix.empty()) {
            if (error != nullptr) {
                *error = "strategy_state_key_prefix must not be empty";
            }
            return false;
        }
    }
    loaded.strategy_state_file_dir = "runtime/trading/state";
    if (const auto it = kv.find("strategy_state_file_dir"); it != kv.end()) {
        loaded.strategy_state_file_dir = Trim(it->second);
        if (loaded.strategy_state_file_dir.empty()) {
            if (error != nullptr) {
                *error = "strategy_state_file_dir must not be empty";
            }
            return false;
        }
    }
    loaded.strategy_metrics_emit_interval_ms = 1'000;
    SetOptionalInt(kv, "strategy_metrics_emit_interval_ms",
                   &loaded.strategy_metrics_emit_interval_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.strategy_metrics_emit_interval_ms < 0) {
        if (error != nullptr) {
            *error = "strategy_metrics_emit_interval_ms must be >= 0";
        }
        return false;
    }
    loaded.account_id = get_value("account_id");
    if (loaded.account_id.empty()) {
        loaded.account_id = loaded.runtime.user_id;
    }

    loaded.execution.mode = ExecutionMode::kDirect;
    if (!ParseExecutionMode(get_value("execution_mode"), &loaded.execution.mode)) {
        if (error != nullptr) {
            *error = "execution_mode must be direct or sliced";
        }
        return false;
    }
    loaded.execution.algo = loaded.execution.mode == ExecutionMode::kSliced
                                ? ExecutionAlgo::kSliced
                                : ExecutionAlgo::kDirect;
    const auto execution_algo_raw = get_value("execution_algo");
    if (!execution_algo_raw.empty() &&
        !ParseExecutionAlgo(execution_algo_raw, &loaded.execution.algo)) {
        if (error != nullptr) {
            *error = "execution_algo must be one of direct|sliced|twap|vwap_lite";
        }
        return false;
    }
    const auto execution_price_mode_raw = get_value("execution_price_mode");
    if (!ParseExecutionPriceMode(execution_price_mode_raw, &loaded.execution.price_mode)) {
        if (error != nullptr) {
            *error = "execution_price_mode must be signal_limit or marketable_limit";
        }
        return false;
    }
    loaded.execution.slice_size = 1;
    SetOptionalInt(kv, "slice_size", &loaded.execution.slice_size, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.execution.slice_size <= 0) {
        if (error != nullptr) {
            *error = "slice_size must be > 0";
        }
        return false;
    }
    loaded.execution.slice_interval_ms = 200;
    SetOptionalInt(kv, "slice_interval_ms", &loaded.execution.slice_interval_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.execution.slice_interval_ms < 0) {
        if (error != nullptr) {
            *error = "slice_interval_ms must be >= 0";
        }
        return false;
    }
    loaded.execution.twap_duration_ms = 0;
    SetOptionalInt(kv, "twap_duration_ms", &loaded.execution.twap_duration_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.execution.twap_duration_ms < 0) {
        if (error != nullptr) {
            *error = "twap_duration_ms must be >= 0";
        }
        return false;
    }
    loaded.execution.vwap_lookback_bars = 20;
    SetOptionalInt(kv, "vwap_lookback_bars", &loaded.execution.vwap_lookback_bars, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.execution.vwap_lookback_bars <= 0) {
        if (error != nullptr) {
            *error = "vwap_lookback_bars must be > 0";
        }
        return false;
    }
    loaded.execution.throttle_reject_ratio = 0.0;
    SetOptionalDouble(kv, "throttle_reject_ratio", &loaded.execution.throttle_reject_ratio,
                      &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.execution.throttle_reject_ratio < 0.0 ||
        loaded.execution.throttle_reject_ratio > 1.0) {
        if (error != nullptr) {
            *error = "throttle_reject_ratio must be in [0, 1]";
        }
        return false;
    }
    loaded.execution.preferred_venue = get_value("preferred_venue");
    if (loaded.execution.preferred_venue.empty()) {
        loaded.execution.preferred_venue = get_value("execution_preferred_venue");
    }
    if (loaded.execution.preferred_venue.empty()) {
        loaded.execution.preferred_venue = "SIM";
    }
    loaded.execution.participation_rate_limit = 1.0;
    SetOptionalDouble(kv, "participation_rate_limit", &loaded.execution.participation_rate_limit,
                      &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.execution.participation_rate_limit <= 0.0 ||
        loaded.execution.participation_rate_limit > 1.0) {
        if (error != nullptr) {
            *error = "participation_rate_limit must be in (0, 1]";
        }
        return false;
    }
    loaded.execution.impact_cost_bps = 0.0;
    SetOptionalDouble(kv, "impact_cost_bps", &loaded.execution.impact_cost_bps, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.execution.impact_cost_bps < 0.0) {
        if (error != nullptr) {
            *error = "impact_cost_bps must be >= 0";
        }
        return false;
    }
    loaded.execution.cancel_after_ms = 0;
    SetOptionalInt(kv, "cancel_after_ms", &loaded.execution.cancel_after_ms, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.execution.cancel_after_ms < 0) {
        if (error != nullptr) {
            *error = "cancel_after_ms must be >= 0";
        }
        return false;
    }
    loaded.execution.cancel_check_interval_ms = 200;
    SetOptionalInt(kv, "cancel_check_interval_ms", &loaded.execution.cancel_check_interval_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.execution.cancel_check_interval_ms <= 0) {
        if (error != nullptr) {
            *error = "cancel_check_interval_ms must be > 0";
        }
        return false;
    }

    if (!get_value("session_gate_enabled").empty() &&
        !ParseBoolValue(get_value("session_gate_enabled"),
                        &loaded.execution.session_gate_enabled)) {
        if (error != nullptr) {
            *error = "invalid bool value for session_gate_enabled";
        }
        return false;
    }
    SetOptionalInt(kv, "max_signal_age_ms", &loaded.execution.max_signal_age_ms, &load_error);
    SetOptionalInt(kv, "max_market_tick_age_ms", &loaded.execution.max_market_tick_age_ms,
                   &load_error);
    SetOptionalInt(kv, "open_session_end_guard_ms", &loaded.execution.open_session_end_guard_ms,
                   &load_error);
    SetOptionalInt(kv, "recovery_query_timeout_ms", &loaded.execution.recovery_query_timeout_ms,
                   &load_error);
    SetOptionalInt(kv, "open_reenable_stability_ms", &loaded.execution.open_reenable_stability_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.execution.max_signal_age_ms <= 0 || loaded.execution.max_market_tick_age_ms <= 0 ||
        loaded.execution.open_session_end_guard_ms < 0 ||
        loaded.execution.recovery_query_timeout_ms <= 0 ||
        loaded.execution.open_reenable_stability_ms < 0) {
        if (error != nullptr) {
            *error = "execution freshness/recovery configuration is invalid";
        }
        return false;
    }

    SetOptionalInt(kv, "market_bar_allowed_lateness_ms", &loaded.market_bar.allowed_lateness_ms,
                   &load_error);
    SetOptionalInt(kv, "market_bar_poll_interval_ms", &loaded.market_bar.poll_interval_ms,
                   &load_error);
    SetOptionalInt(kv, "market_bar_checkpoint_interval_ms",
                   &loaded.market_bar.checkpoint_interval_ms, &load_error);
    SetOptionalInt(kv, "market_event_delay_hard_ms", &loaded.market_bar.event_delay_hard_ms,
                   &load_error);
    if (!get_value("require_complete_timeframe_bar").empty() &&
        !ParseBoolValue(get_value("require_complete_timeframe_bar"),
                        &loaded.market_bar.require_complete_timeframe_bar)) {
        load_error = "invalid bool value for require_complete_timeframe_bar";
    }
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.market_bar.allowed_lateness_ms < 0 || loaded.market_bar.poll_interval_ms <= 0 ||
        loaded.market_bar.checkpoint_interval_ms <= 0 ||
        loaded.market_bar.event_delay_hard_ms <= 0) {
        if (error != nullptr) {
            *error = "market bar timing configuration is invalid";
        }
        return false;
    }

    loaded.risk.default_max_order_volume = 200;
    SetOptionalInt(kv, "risk_default_max_order_volume", &loaded.risk.default_max_order_volume,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.risk.default_max_order_volume <= 0) {
        if (error != nullptr) {
            *error = "risk_default_max_order_volume must be > 0";
        }
        return false;
    }
    loaded.risk.default_max_order_notional = 1'000'000.0;
    SetOptionalDouble(kv, "risk_default_max_order_notional",
                      &loaded.risk.default_max_order_notional, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.risk.default_max_order_notional <= 0.0) {
        if (error != nullptr) {
            *error = "risk_default_max_order_notional must be > 0";
        }
        return false;
    }
    loaded.risk.default_max_active_orders = 0;
    SetOptionalInt(kv, "risk_default_max_active_orders", &loaded.risk.default_max_active_orders,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.risk.default_max_active_orders < 0) {
        if (error != nullptr) {
            *error = "risk_default_max_active_orders must be >= 0";
        }
        return false;
    }
    loaded.risk.default_max_position_notional = 0.0;
    SetOptionalDouble(kv, "risk_default_max_position_notional",
                      &loaded.risk.default_max_position_notional, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.risk.default_max_position_notional < 0.0) {
        if (error != nullptr) {
            *error = "risk_default_max_position_notional must be >= 0";
        }
        return false;
    }
    loaded.risk.sim_subaccount_enabled = false;
    const auto sim_subaccount_enabled = get_value("risk_sim_subaccount_enabled");
    if (!sim_subaccount_enabled.empty() &&
        !ParseBoolValue(sim_subaccount_enabled, &loaded.risk.sim_subaccount_enabled)) {
        if (error != nullptr) {
            *error = "invalid bool value for risk_sim_subaccount_enabled";
        }
        return false;
    }
    loaded.risk.sim_subaccount_id = get_value("risk_sim_subaccount_id");
    loaded.risk.sim_subaccount_initial_equity = 0.0;
    SetOptionalDouble(kv, "risk_sim_subaccount_initial_equity",
                      &loaded.risk.sim_subaccount_initial_equity, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    loaded.risk.sim_subaccount_max_margin = 0.0;
    SetOptionalDouble(kv, "risk_sim_subaccount_max_margin", &loaded.risk.sim_subaccount_max_margin,
                      &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    loaded.risk.sim_subaccount_order_margin_rate = 1.0;
    SetOptionalDouble(kv, "risk_sim_subaccount_order_margin_rate",
                      &loaded.risk.sim_subaccount_order_margin_rate, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    loaded.risk.sim_subaccount_contract_multiplier = 1.0;
    SetOptionalDouble(kv, "risk_sim_subaccount_contract_multiplier",
                      &loaded.risk.sim_subaccount_contract_multiplier, &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.risk.sim_subaccount_enabled) {
        if (loaded.risk.sim_subaccount_initial_equity <= 0.0) {
            if (error != nullptr) {
                *error = "risk_sim_subaccount_initial_equity must be > 0 when enabled";
            }
            return false;
        }
        if (loaded.risk.sim_subaccount_max_margin <= 0.0) {
            loaded.risk.sim_subaccount_max_margin = loaded.risk.sim_subaccount_initial_equity;
        }
    }
    if (loaded.risk.sim_subaccount_initial_equity < 0.0 ||
        loaded.risk.sim_subaccount_max_margin < 0.0) {
        if (error != nullptr) {
            *error = "risk_sim_subaccount_initial_equity/max_margin must be >= 0";
        }
        return false;
    }
    if (loaded.risk.sim_subaccount_order_margin_rate < 0.0) {
        if (error != nullptr) {
            *error = "risk_sim_subaccount_order_margin_rate must be >= 0";
        }
        return false;
    }
    if (loaded.risk.sim_subaccount_contract_multiplier <= 0.0) {
        if (error != nullptr) {
            *error = "risk_sim_subaccount_contract_multiplier must be > 0";
        }
        return false;
    }
    loaded.risk.default_max_cancel_count = 0;
    SetOptionalInt(kv, "risk_default_max_cancel_count", &loaded.risk.default_max_cancel_count,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.risk.default_max_cancel_count < 0) {
        if (error != nullptr) {
            *error = "risk_default_max_cancel_count must be >= 0";
        }
        return false;
    }
    loaded.risk.default_max_cancel_ratio = 0.0;
    SetOptionalDouble(kv, "risk_default_max_cancel_ratio", &loaded.risk.default_max_cancel_ratio,
                      &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.risk.default_max_cancel_ratio < 0.0) {
        if (error != nullptr) {
            *error = "risk_default_max_cancel_ratio must be >= 0";
        }
        return false;
    }
    loaded.risk.default_rule_group = get_value("risk_default_rule_group");
    if (loaded.risk.default_rule_group.empty()) {
        loaded.risk.default_rule_group = "default";
    }
    loaded.risk.default_rule_version = get_value("risk_default_rule_version");
    if (loaded.risk.default_rule_version.empty()) {
        loaded.risk.default_rule_version = "v1";
    }
    loaded.risk.default_policy_id = get_value("risk_default_policy_id");
    if (loaded.risk.default_policy_id.empty()) {
        loaded.risk.default_policy_id = "policy.global";
    }
    loaded.risk.default_policy_scope = get_value("risk_default_policy_scope");
    if (loaded.risk.default_policy_scope.empty()) {
        loaded.risk.default_policy_scope = "global";
    }
    loaded.risk.default_decision_tags = get_value("risk_default_decision_tags");

    const auto rule_groups = SplitCsvList(get_value("risk_rule_groups"));
    for (const auto& group : rule_groups) {
        RiskRuleConfig rule;
        rule.rule_group = group;
        rule.rule_id = get_value("risk_rule_" + group + "_id");
        if (rule.rule_id.empty()) {
            rule.rule_id = "risk." + group;
        }
        rule.rule_version = get_value("risk_rule_" + group + "_version");
        if (rule.rule_version.empty()) {
            rule.rule_version = loaded.risk.default_rule_version;
        }
        rule.policy_id = get_value("risk_rule_" + group + "_policy_id");
        if (rule.policy_id.empty()) {
            rule.policy_id = loaded.risk.default_policy_id;
        }
        rule.policy_scope = get_value("risk_rule_" + group + "_policy_scope");
        if (rule.policy_scope.empty()) {
            rule.policy_scope = loaded.risk.default_policy_scope;
        }
        rule.decision_tags = get_value("risk_rule_" + group + "_decision_tags");
        if (rule.decision_tags.empty()) {
            rule.decision_tags = loaded.risk.default_decision_tags;
        }

        rule.account_id = get_value("risk_rule_" + group + "_account_id");
        if (rule.account_id == "*") {
            rule.account_id.clear();
        }
        rule.instrument_id = get_value("risk_rule_" + group + "_instrument_id");
        if (rule.instrument_id == "*") {
            rule.instrument_id.clear();
        }
        rule.exchange_id = get_value("risk_rule_" + group + "_exchange_id");
        if (rule.exchange_id == "*") {
            rule.exchange_id.clear();
        }

        const auto start_hhmm = get_value("risk_rule_" + group + "_start_hhmm");
        if (!start_hhmm.empty()) {
            if (!ParseIntValue(start_hhmm, &rule.window_start_hhmm) ||
                !IsValidHhmm(rule.window_start_hhmm)) {
                if (error != nullptr) {
                    *error = "invalid risk rule start_hhmm for group: " + group;
                }
                return false;
            }
        }
        const auto end_hhmm = get_value("risk_rule_" + group + "_end_hhmm");
        if (!end_hhmm.empty()) {
            if (!ParseIntValue(end_hhmm, &rule.window_end_hhmm) ||
                !IsValidHhmm(rule.window_end_hhmm)) {
                if (error != nullptr) {
                    *error = "invalid risk rule end_hhmm for group: " + group;
                }
                return false;
            }
        }

        const auto max_vol = get_value("risk_rule_" + group + "_max_order_volume");
        if (!max_vol.empty()) {
            if (!ParseIntValue(max_vol, &rule.max_order_volume) || rule.max_order_volume <= 0) {
                if (error != nullptr) {
                    *error = "invalid risk rule max_order_volume for group: " + group;
                }
                return false;
            }
        } else {
            rule.max_order_volume = loaded.risk.default_max_order_volume;
        }

        const auto max_notional = get_value("risk_rule_" + group + "_max_order_notional");
        if (!max_notional.empty()) {
            if (!ParseDoubleValue(max_notional, &rule.max_order_notional) ||
                rule.max_order_notional <= 0.0) {
                if (error != nullptr) {
                    *error = "invalid risk rule max_order_notional for group: " + group;
                }
                return false;
            }
        } else {
            rule.max_order_notional = loaded.risk.default_max_order_notional;
        }
        const auto max_active_orders = get_value("risk_rule_" + group + "_max_active_orders");
        if (!max_active_orders.empty()) {
            if (!ParseIntValue(max_active_orders, &rule.max_active_orders) ||
                rule.max_active_orders < 0) {
                if (error != nullptr) {
                    *error = "invalid risk rule max_active_orders for group: " + group;
                }
                return false;
            }
        } else {
            rule.max_active_orders = loaded.risk.default_max_active_orders;
        }
        const auto max_position_notional =
            get_value("risk_rule_" + group + "_max_position_notional");
        if (!max_position_notional.empty()) {
            if (!ParseDoubleValue(max_position_notional, &rule.max_position_notional) ||
                rule.max_position_notional < 0.0) {
                if (error != nullptr) {
                    *error = "invalid risk rule max_position_notional for group: " + group;
                }
                return false;
            }
        } else {
            rule.max_position_notional = loaded.risk.default_max_position_notional;
        }
        const auto max_cancel_count = get_value("risk_rule_" + group + "_max_cancel_count");
        if (!max_cancel_count.empty()) {
            if (!ParseIntValue(max_cancel_count, &rule.max_cancel_count) ||
                rule.max_cancel_count < 0) {
                if (error != nullptr) {
                    *error = "invalid risk rule max_cancel_count for group: " + group;
                }
                return false;
            }
        } else {
            rule.max_cancel_count = loaded.risk.default_max_cancel_count;
        }
        const auto max_cancel_ratio = get_value("risk_rule_" + group + "_max_cancel_ratio");
        if (!max_cancel_ratio.empty()) {
            if (!ParseDoubleValue(max_cancel_ratio, &rule.max_cancel_ratio) ||
                rule.max_cancel_ratio < 0.0) {
                if (error != nullptr) {
                    *error = "invalid risk rule max_cancel_ratio for group: " + group;
                }
                return false;
            }
        } else {
            rule.max_cancel_ratio = loaded.risk.default_max_cancel_ratio;
        }

        loaded.risk.rules.push_back(std::move(rule));
    }

    {
        MarketStateDetectorConfig detector = loaded.market_state_detector;
        for (const char* field : kMarketStateDetectorFields) {
            std::string raw = get_value(std::string("market_state_detector.") + field);
            if (raw.empty()) {
                raw = get_value(field);
            }
            if (!raw.empty() &&
                !ApplyMarketStateDetectorField(
                    &detector, field, raw, std::string("market_state_detector.") + field, error)) {
                return false;
            }
        }

        try {
            (void)MarketStateDetector(detector);
        } catch (const std::invalid_argument& ex) {
            if (error != nullptr) {
                *error = std::string("invalid market_state_detector config: ") + ex.what();
            }
            return false;
        }
        loaded.market_state_detector = detector;

        MarketStateDetectorConfigByProduct by_product;
        constexpr std::string_view kPrefix = "market_state_detector_by_product.";
        for (const auto& [key, raw] : kv) {
            if (key.rfind(kPrefix, 0) != 0) {
                continue;
            }
            const std::string rest = key.substr(kPrefix.size());
            const std::size_t dot = rest.find('.');
            if (dot == std::string::npos || dot == 0 || dot + 1 >= rest.size()) {
                if (error != nullptr) {
                    *error = "invalid market_state_detector_by_product key: " + key;
                }
                return false;
            }
            const std::string raw_product = rest.substr(0, dot);
            const std::string product_id = NormalizeMarketStateProductId(raw_product);
            if (product_id.empty()) {
                if (error != nullptr) {
                    *error = "invalid market_state_detector_by_product product: " + raw_product;
                }
                return false;
            }
            const std::string field = rest.substr(dot + 1);
            auto [it, inserted] = by_product.try_emplace(product_id, detector);
            (void)inserted;
            if (!ApplyMarketStateDetectorField(&it->second, field, raw, key, error)) {
                return false;
            }
        }
        for (const auto& [product_id, product_detector] : by_product) {
            try {
                (void)MarketStateDetector(product_detector);
            } catch (const std::invalid_argument& ex) {
                if (error != nullptr) {
                    *error = "invalid market_state_detector_by_product config for product " +
                             product_id + ": " + ex.what();
                }
                return false;
            }
        }
        loaded.market_state_detector_by_product = std::move(by_product);
    }

    loaded.runtime.password = get_value("password");
    if (loaded.runtime.password.empty()) {
        std::string password_env = get_value("password_env");
        if (password_env.empty()) {
            password_env = "CTP_SIM_PASSWORD";
        }
        const char* value = std::getenv(password_env.c_str());
        if (value != nullptr) {
            loaded.runtime.password = value;
        }
        if (loaded.runtime.password.empty()) {
            if (error != nullptr) {
                *error =
                    "password is missing; set password or environment variable " + password_env;
            }
            return false;
        }
    }

    std::string validation_error;
    if (!CtpConfigValidator::Validate(loaded.runtime, &validation_error)) {
        if (error != nullptr) {
            *error = "ctp config validation failed: " + validation_error;
        }
        return false;
    }

    *config = std::move(loaded);
    return true;
}

}  // namespace quant_hft
