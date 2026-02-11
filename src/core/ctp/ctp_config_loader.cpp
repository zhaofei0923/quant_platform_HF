#include "quant_hft/core/ctp_config_loader.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <string>
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
    std::transform(value.begin(),
                   value.end(),
                   value.begin(),
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
    while (std::getline(in, line)) {
        const auto hash = line.find('#');
        if (hash != std::string::npos) {
            line = line.substr(0, hash);
        }
        line = Trim(line);
        if (line.empty() || line == "ctp:") {
            continue;
        }

        const auto pos = line.find(':');
        if (pos == std::string::npos) {
            continue;
        }

        const auto key = Trim(line.substr(0, pos));
        auto value = Trim(line.substr(pos + 1));
        if (!key.empty()) {
            kv[key] = value;
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

void SetOptionalInt(const std::unordered_map<std::string, std::string>& kv,
                    const char* key,
                    int* target,
                    std::string* error) {
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

void SetOptionalDouble(const std::unordered_map<std::string, std::string>& kv,
                       const char* key,
                       double* target,
                       std::string* error) {
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
        const auto item = end == std::string::npos ? raw.substr(start) : raw.substr(start, end - start);
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

}  // namespace

bool CtpConfigLoader::LoadFromYaml(const std::string& path,
                                   CtpFileConfig* config,
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

    auto get_value = [&](const std::string& key) -> std::string {
        const auto it = kv.find(key);
        if (it == kv.end()) {
            return "";
        }
        return it->second;
    };

    loaded.runtime.md_front = get_value("market_front");
    if (loaded.runtime.md_front.empty()) {
        loaded.runtime.md_front = get_value("md_front");
    }
    loaded.runtime.td_front = get_value("trader_front");
    if (loaded.runtime.td_front.empty()) {
        loaded.runtime.td_front = get_value("td_front");
    }
    loaded.runtime.flow_path = get_value("flow_path").empty() ? loaded.runtime.flow_path
                                                               : get_value("flow_path");
    loaded.runtime.broker_id = get_value("broker_id");
    loaded.runtime.user_id = get_value("user_id");
    loaded.runtime.investor_id = get_value("investor_id");
    if (loaded.runtime.investor_id.empty()) {
        loaded.runtime.investor_id = loaded.runtime.user_id;
    }
    loaded.runtime.app_id = get_value("app_id");
    loaded.runtime.auth_code = get_value("auth_code");
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
    SetOptionalInt(kv,
                   "reconnect_max_attempts",
                   &loaded.runtime.reconnect_max_attempts,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    SetOptionalInt(kv,
                   "reconnect_initial_backoff_ms",
                   &loaded.runtime.reconnect_initial_backoff_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    SetOptionalInt(kv,
                   "reconnect_max_backoff_ms",
                   &loaded.runtime.reconnect_max_backoff_ms,
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
    if (loaded.query_rate_limit_qps <= 0) {
        if (error != nullptr) {
            *error = "query_rate_limit_qps must be > 0";
        }
        return false;
    }

    loaded.instruments = SplitCsvList(get_value("instruments"));
    loaded.strategy_ids = SplitCsvList(get_value("strategy_ids"));
    loaded.strategy_poll_interval_ms = 200;
    SetOptionalInt(kv,
                   "strategy_poll_interval_ms",
                   &loaded.strategy_poll_interval_ms,
                   &load_error);
    if (!load_error.empty()) {
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }
    if (loaded.strategy_poll_interval_ms <= 0) {
        if (error != nullptr) {
            *error = "strategy_poll_interval_ms must be > 0";
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

    loaded.risk.default_max_order_volume = 200;
    SetOptionalInt(kv,
                   "risk_default_max_order_volume",
                   &loaded.risk.default_max_order_volume,
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
    SetOptionalDouble(kv,
                      "risk_default_max_order_notional",
                      &loaded.risk.default_max_order_notional,
                      &load_error);
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
    loaded.risk.default_rule_group = get_value("risk_default_rule_group");
    if (loaded.risk.default_rule_group.empty()) {
        loaded.risk.default_rule_group = "default";
    }
    loaded.risk.default_rule_version = get_value("risk_default_rule_version");
    if (loaded.risk.default_rule_version.empty()) {
        loaded.risk.default_rule_version = "v1";
    }

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

        rule.account_id = get_value("risk_rule_" + group + "_account_id");
        if (rule.account_id == "*") {
            rule.account_id.clear();
        }
        rule.instrument_id = get_value("risk_rule_" + group + "_instrument_id");
        if (rule.instrument_id == "*") {
            rule.instrument_id.clear();
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

        loaded.risk.rules.push_back(std::move(rule));
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
                *error = "password is missing; set password or environment variable " +
                         password_env;
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
