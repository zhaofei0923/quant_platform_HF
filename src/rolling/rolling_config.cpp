#include "quant_hft/rolling/rolling_config.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace quant_hft::rolling {
namespace {

std::string Trim(std::string text) {
    auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    while (!text.empty() && is_space(static_cast<unsigned char>(text.front()))) {
        text.erase(text.begin());
    }
    while (!text.empty() && is_space(static_cast<unsigned char>(text.back()))) {
        text.pop_back();
    }
    return text;
}

std::string ToLower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return text;
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

bool ParseInt(const std::string& raw, int* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string text = Trim(raw);
    if (text.empty()) {
        return false;
    }
    try {
        std::size_t parsed = 0;
        const int value = std::stoi(text, &parsed);
        if (parsed != text.size()) {
            return false;
        }
        *out = value;
        return true;
    } catch (...) {
        return false;
    }
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

std::string Unquote(const std::string& raw) {
    std::string text = Trim(raw);
    if (text.size() >= 2 && ((text.front() == '"' && text.back() == '"') ||
                             (text.front() == '\'' && text.back() == '\''))) {
        text = text.substr(1, text.size() - 2);
    }
    return text;
}

std::optional<std::string> GetString(const std::map<std::string, std::string>& values,
                                     const std::string& key) {
    const auto it = values.find(key);
    if (it == values.end()) {
        return std::nullopt;
    }
    return Unquote(it->second);
}

std::string GetStringOr(const std::map<std::string, std::string>& values,
                        const std::string& key,
                        const std::string& fallback) {
    const auto value = GetString(values, key);
    return value.has_value() ? value.value() : fallback;
}

bool LoadYamlScalarMap(const std::filesystem::path& path,
                       std::map<std::string, std::string>* out,
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
            *error = "unable to open rolling config: " + path.string();
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

        std::ostringstream full_key;
        for (const auto& scope : scope_stack) {
            full_key << scope.second << '.';
        }
        full_key << key;
        (*out)[full_key.str()] = value;
    }

    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading rolling config: " + path.string();
        }
        return false;
    }
    return true;
}

std::vector<std::string> ParseInlineList(std::string raw) {
    raw = Trim(raw);
    if (raw.size() < 2 || raw.front() != '[' || raw.back() != ']') {
        return {};
    }
    raw = raw.substr(1, raw.size() - 2);

    std::vector<std::string> out;
    std::string current;
    bool in_single_quote = false;
    bool in_double_quote = false;
    for (char ch : raw) {
        if (ch == '\'' && !in_double_quote) {
            in_single_quote = !in_single_quote;
            current.push_back(ch);
            continue;
        }
        if (ch == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
            current.push_back(ch);
            continue;
        }
        if (ch == ',' && !in_single_quote && !in_double_quote) {
            const std::string value = Unquote(current);
            if (!value.empty()) {
                out.push_back(value);
            }
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    const std::string value = Unquote(current);
    if (!value.empty()) {
        out.push_back(value);
    }
    return out;
}

std::string NormalizeTradingDay(const std::string& raw) {
    std::string digits;
    digits.reserve(raw.size());
    for (char ch : raw) {
        if (std::isdigit(static_cast<unsigned char>(ch)) != 0) {
            digits.push_back(ch);
        }
    }
    if (digits.size() != 8) {
        return "";
    }
    return digits;
}

std::filesystem::path ResolvePath(const std::filesystem::path& config_dir,
                                  const std::string& raw,
                                  bool prefer_existing_raw = true) {
    if (raw.empty()) {
        return {};
    }
    std::filesystem::path path = raw;
    if (path.is_absolute()) {
        return std::filesystem::absolute(path).lexically_normal();
    }

    if (prefer_existing_raw) {
        std::error_code ec;
        if (std::filesystem::exists(path, ec)) {
            return std::filesystem::absolute(path).lexically_normal();
        }
        if (!path.parent_path().empty() && std::filesystem::exists(path.parent_path(), ec)) {
            return std::filesystem::absolute(path).lexically_normal();
        }
    }

    return std::filesystem::absolute(config_dir / path).lexically_normal();
}

bool RequirePositive(const char* name, int value, std::string* error) {
    if (value > 0) {
        return true;
    }
    if (error != nullptr) {
        *error = std::string(name) + " must be > 0";
    }
    return false;
}

}  // namespace

bool LoadRollingConfig(const std::string& yaml_path, RollingConfig* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "rolling config output is null";
        }
        return false;
    }

    const std::filesystem::path config_path = std::filesystem::absolute(yaml_path).lexically_normal();
    const std::filesystem::path config_dir = config_path.parent_path();

    std::map<std::string, std::string> values;
    if (!LoadYamlScalarMap(config_path, &values, error)) {
        return false;
    }

    RollingConfig config;
    config.config_path = config_path;
    config.config_dir = config_dir;

    config.mode = ToLower(GetStringOr(values, "mode", config.mode));

    config.backtest_base.engine_mode = ToLower(GetStringOr(values, "backtest_base.engine_mode",
                                                           config.backtest_base.engine_mode));
    config.backtest_base.dataset_root = GetStringOr(values, "backtest_base.dataset_root", "");
    config.backtest_base.dataset_manifest =
        GetStringOr(values, "backtest_base.dataset_manifest", "");
    config.backtest_base.strategy_factory =
        ToLower(GetStringOr(values, "backtest_base.strategy_factory", config.backtest_base.strategy_factory));
    config.backtest_base.strategy_composite_config =
        GetStringOr(values, "backtest_base.strategy_composite_config", "");

    config.backtest_base.rollover_mode =
        ToLower(GetStringOr(values, "backtest_base.rollover_mode", config.backtest_base.rollover_mode));
    config.backtest_base.rollover_price_mode = ToLower(GetStringOr(
        values, "backtest_base.rollover_price_mode", config.backtest_base.rollover_price_mode));

    if (const auto symbols_raw = GetString(values, "backtest_base.symbols"); symbols_raw.has_value()) {
        config.backtest_base.symbols = ParseInlineList(symbols_raw.value());
    }

    if (const auto raw = GetString(values, "backtest_base.max_ticks"); raw.has_value() && !raw->empty()) {
        std::int64_t parsed = 0;
        if (!ParseInt64(*raw, &parsed) || parsed <= 0) {
            if (error != nullptr) {
                *error = "backtest_base.max_ticks must be a positive integer";
            }
            return false;
        }
        config.backtest_base.max_ticks = parsed;
    }

    if (const auto raw = GetString(values, "backtest_base.deterministic_fills"); raw.has_value()) {
        if (!ParseBool(*raw, &config.backtest_base.deterministic_fills)) {
            if (error != nullptr) {
                *error = "invalid backtest_base.deterministic_fills";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "backtest_base.strict_parquet"); raw.has_value()) {
        if (!ParseBool(*raw, &config.backtest_base.strict_parquet)) {
            if (error != nullptr) {
                *error = "invalid backtest_base.strict_parquet";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "backtest_base.emit_trades"); raw.has_value()) {
        if (!ParseBool(*raw, &config.backtest_base.emit_trades)) {
            if (error != nullptr) {
                *error = "invalid backtest_base.emit_trades";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "backtest_base.emit_orders"); raw.has_value()) {
        if (!ParseBool(*raw, &config.backtest_base.emit_orders)) {
            if (error != nullptr) {
                *error = "invalid backtest_base.emit_orders";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "backtest_base.emit_position_history"); raw.has_value()) {
        if (!ParseBool(*raw, &config.backtest_base.emit_position_history)) {
            if (error != nullptr) {
                *error = "invalid backtest_base.emit_position_history";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "backtest_base.rollover_slippage_bps"); raw.has_value()) {
        if (!ParseDouble(*raw, &config.backtest_base.rollover_slippage_bps)) {
            if (error != nullptr) {
                *error = "invalid backtest_base.rollover_slippage_bps";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "backtest_base.initial_equity"); raw.has_value()) {
        if (!ParseDouble(*raw, &config.backtest_base.initial_equity)) {
            if (error != nullptr) {
                *error = "invalid backtest_base.initial_equity";
            }
            return false;
        }
    }

    config.window.type = ToLower(GetStringOr(values, "window.type", config.window.type));
    config.window.start_date = NormalizeTradingDay(GetStringOr(values, "window.start_date", ""));
    config.window.end_date = NormalizeTradingDay(GetStringOr(values, "window.end_date", ""));

    if (const auto raw = GetString(values, "window.train_length_days"); raw.has_value()) {
        if (!ParseInt(*raw, &config.window.train_length_days)) {
            if (error != nullptr) {
                *error = "invalid window.train_length_days";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "window.test_length_days"); raw.has_value()) {
        if (!ParseInt(*raw, &config.window.test_length_days)) {
            if (error != nullptr) {
                *error = "invalid window.test_length_days";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "window.step_days"); raw.has_value()) {
        if (!ParseInt(*raw, &config.window.step_days)) {
            if (error != nullptr) {
                *error = "invalid window.step_days";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "window.min_train_days"); raw.has_value()) {
        if (!ParseInt(*raw, &config.window.min_train_days)) {
            if (error != nullptr) {
                *error = "invalid window.min_train_days";
            }
            return false;
        }
    }

    config.optimization.algorithm =
        ToLower(GetStringOr(values, "optimization.algorithm", config.optimization.algorithm));
    config.optimization.metric =
        GetStringOr(values, "optimization.metric", config.optimization.metric);
    config.optimization.param_space = GetStringOr(values, "optimization.param_space", "");
    config.optimization.target_sub_config_path =
        GetStringOr(values, "optimization.target_sub_config_path", "");

    if (const auto raw = GetString(values, "optimization.maximize"); raw.has_value()) {
        if (!ParseBool(*raw, &config.optimization.maximize)) {
            if (error != nullptr) {
                *error = "invalid optimization.maximize";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "optimization.max_trials"); raw.has_value()) {
        if (!ParseInt(*raw, &config.optimization.max_trials)) {
            if (error != nullptr) {
                *error = "invalid optimization.max_trials";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "optimization.parallel"); raw.has_value()) {
        if (!ParseInt(*raw, &config.optimization.parallel)) {
            if (error != nullptr) {
                *error = "invalid optimization.parallel";
            }
            return false;
        }
    }

    config.output.report_json = GetStringOr(values, "output.report_json", "");
    config.output.report_md = GetStringOr(values, "output.report_md", "");
    config.output.best_params_dir = GetStringOr(values, "output.best_params_dir", "");
    if (const auto raw = GetString(values, "output.keep_temp_files"); raw.has_value()) {
        if (!ParseBool(*raw, &config.output.keep_temp_files)) {
            if (error != nullptr) {
                *error = "invalid output.keep_temp_files";
            }
            return false;
        }
    }
    if (const auto raw = GetString(values, "output.window_parallel"); raw.has_value()) {
        if (!ParseInt(*raw, &config.output.window_parallel)) {
            if (error != nullptr) {
                *error = "invalid output.window_parallel";
            }
            return false;
        }
    }

    if (config.backtest_base.engine_mode != "parquet") {
        if (error != nullptr) {
            *error = "backtest_base.engine_mode must be parquet";
        }
        return false;
    }
    if (config.backtest_base.dataset_root.empty()) {
        if (error != nullptr) {
            *error = "backtest_base.dataset_root is required";
        }
        return false;
    }

    config.backtest_base.dataset_root =
        ResolvePath(config_dir, config.backtest_base.dataset_root).string();
    if (!std::filesystem::exists(config.backtest_base.dataset_root)) {
        if (error != nullptr) {
            *error = "dataset_root does not exist: " + config.backtest_base.dataset_root;
        }
        return false;
    }

    if (config.backtest_base.dataset_manifest.empty()) {
        config.backtest_base.dataset_manifest =
            (std::filesystem::path(config.backtest_base.dataset_root) / "_manifest" /
             "partitions.jsonl")
                .string();
    } else {
        config.backtest_base.dataset_manifest =
            ResolvePath(config_dir, config.backtest_base.dataset_manifest).string();
    }

    if (!std::filesystem::exists(config.backtest_base.dataset_manifest)) {
        if (error != nullptr) {
            *error = "dataset_manifest does not exist: " + config.backtest_base.dataset_manifest;
        }
        return false;
    }

    if (config.backtest_base.strategy_factory.empty()) {
        config.backtest_base.strategy_factory = "composite";
    }

    if (config.backtest_base.strategy_factory == "composite") {
        if (config.backtest_base.strategy_composite_config.empty()) {
            if (error != nullptr) {
                *error = "backtest_base.strategy_composite_config is required when strategy_factory=composite";
            }
            return false;
        }
        config.backtest_base.strategy_composite_config =
            ResolvePath(config_dir, config.backtest_base.strategy_composite_config).string();
        if (!std::filesystem::exists(config.backtest_base.strategy_composite_config)) {
            if (error != nullptr) {
                *error = "strategy_composite_config does not exist: " +
                         config.backtest_base.strategy_composite_config;
            }
            return false;
        }
    }

    if (config.window.type != "rolling" && config.window.type != "expanding") {
        if (error != nullptr) {
            *error = "window.type must be rolling or expanding";
        }
        return false;
    }
    if (config.window.start_date.empty() || config.window.end_date.empty()) {
        if (error != nullptr) {
            *error = "window.start_date and window.end_date are required in YYYYMMDD format";
        }
        return false;
    }
    if (config.window.start_date > config.window.end_date) {
        if (error != nullptr) {
            *error = "window.start_date must be <= window.end_date";
        }
        return false;
    }

    if (!RequirePositive("window.train_length_days", config.window.train_length_days, error) ||
        !RequirePositive("window.test_length_days", config.window.test_length_days, error) ||
        !RequirePositive("window.step_days", config.window.step_days, error) ||
        !RequirePositive("window.min_train_days", config.window.min_train_days, error)) {
        return false;
    }

    if (config.mode != "fixed_params" && config.mode != "rolling_optimize") {
        if (error != nullptr) {
            *error = "mode must be fixed_params or rolling_optimize";
        }
        return false;
    }

    if (!RequirePositive("output.window_parallel", config.output.window_parallel, error)) {
        return false;
    }

    if (config.output.report_json.empty() || config.output.report_md.empty()) {
        if (error != nullptr) {
            *error = "output.report_json and output.report_md are required";
        }
        return false;
    }

    config.output.report_json = ResolvePath(config_dir, config.output.report_json).string();
    config.output.report_md = ResolvePath(config_dir, config.output.report_md).string();
    if (!config.output.best_params_dir.empty()) {
        config.output.best_params_dir = ResolvePath(config_dir, config.output.best_params_dir).string();
    }

    if (config.mode == "rolling_optimize") {
        if (config.optimization.algorithm != "grid") {
            if (error != nullptr) {
                *error = "rolling_optimize currently supports optimization.algorithm=grid only";
            }
            return false;
        }
        if (!RequirePositive("optimization.max_trials", config.optimization.max_trials, error) ||
            !RequirePositive("optimization.parallel", config.optimization.parallel, error)) {
            return false;
        }
        if (config.optimization.param_space.empty()) {
            if (error != nullptr) {
                *error = "optimization.param_space is required when mode=rolling_optimize";
            }
            return false;
        }
        config.optimization.param_space = ResolvePath(config_dir, config.optimization.param_space).string();
        if (!std::filesystem::exists(config.optimization.param_space)) {
            if (error != nullptr) {
                *error = "optimization.param_space does not exist: " + config.optimization.param_space;
            }
            return false;
        }
        if (!config.optimization.target_sub_config_path.empty()) {
            config.optimization.target_sub_config_path =
                ResolvePath(config_dir, config.optimization.target_sub_config_path).string();
        }
    }

    *out = std::move(config);
    return true;
}

}  // namespace quant_hft::rolling

