#include "quant_hft/strategy/strategy_main_config_loader.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "quant_hft/core/simple_json.h"
#include "quant_hft/strategy/composite_config_loader.h"

namespace quant_hft {
namespace {

std::string Trim(const std::string& input) {
    std::size_t begin = 0;
    while (begin < input.size() && std::isspace(static_cast<unsigned char>(input[begin])) != 0) {
        ++begin;
    }
    std::size_t end = input.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(input[end - 1])) != 0) {
        --end;
    }
    return input.substr(begin, end - begin);
}

std::string ToLower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return text;
}

std::string StripInlineComment(const std::string& input) {
    bool in_single_quote = false;
    bool in_double_quote = false;
    for (std::size_t i = 0; i < input.size(); ++i) {
        const char ch = input[i];
        if (ch == '\'' && !in_double_quote) {
            in_single_quote = !in_single_quote;
            continue;
        }
        if (ch == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
            continue;
        }
        if (ch == '#' && !in_single_quote && !in_double_quote) {
            return input.substr(0, i);
        }
    }
    return input;
}

std::string Unquote(std::string text) {
    text = Trim(text);
    if (text.size() >= 2 && ((text.front() == '"' && text.back() == '"') ||
                             (text.front() == '\'' && text.back() == '\''))) {
        return text.substr(1, text.size() - 2);
    }
    return text;
}

bool ParseKeyValue(const std::string& line, std::string* out_key, std::string* out_value) {
    if (out_key == nullptr || out_value == nullptr) {
        return false;
    }
    const std::size_t pos = line.find(':');
    if (pos == std::string::npos) {
        return false;
    }
    *out_key = Trim(line.substr(0, pos));
    *out_value = Unquote(line.substr(pos + 1));
    return !out_key->empty();
}

bool ParseBoolText(const std::string& value, bool* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string normalized = ToLower(Trim(value));
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

bool ParseDoubleText(const std::string& value, double* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string text = Trim(value);
    if (text.empty()) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const double parsed = std::stod(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseStringList(const std::string& value, std::vector<std::string>* out) {
    if (out == nullptr) {
        return false;
    }
    out->clear();
    const std::string trimmed = Trim(value);
    if (trimmed.empty()) {
        return true;
    }
    if (trimmed.size() < 2 || trimmed.front() != '[' || trimmed.back() != ']') {
        return false;
    }
    const std::string body = trimmed.substr(1, trimmed.size() - 2);
    std::size_t start = 0;
    while (start <= body.size()) {
        const std::size_t comma = body.find(',', start);
        std::string piece =
            comma == std::string::npos ? body.substr(start) : body.substr(start, comma - start);
        piece = Unquote(piece);
        if (!piece.empty()) {
            out->push_back(piece);
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return true;
}

bool ParseBacktestYamlKey(StrategyMainBacktestConfig* backtest, const std::string& key,
                          const std::string& value, std::string* error) {
    if (backtest == nullptr) {
        return false;
    }
    if (key == "initial_equity") {
        if (!ParseDoubleText(value, &backtest->initial_equity)) {
            if (error != nullptr) {
                *error = "invalid backtest.initial_equity";
            }
            return false;
        }
        return true;
    }
    if (key == "max_loss_percent") {
        if (error != nullptr) {
            *error = "backtest.max_loss_percent has been removed; configure risk_per_trade_pct in "
                     "each sub strategy params";
        }
        return false;
    }
    if (key == "symbols") {
        if (!ParseStringList(value, &backtest->symbols)) {
            if (error != nullptr) {
                *error = "invalid backtest.symbols list";
            }
            return false;
        }
        return true;
    }
    if (key == "start_date") {
        backtest->start_date = value;
        return true;
    }
    if (key == "end_date") {
        backtest->end_date = value;
        return true;
    }
    if (key == "product_config_path") {
        backtest->product_config_path = value;
        return true;
    }
    if (error != nullptr) {
        *error = "unsupported backtest field: " + key;
    }
    return false;
}

bool LoadMainYaml(const std::filesystem::path& path, StrategyMainConfig* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "strategy main config output is null";
        }
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open strategy main config: " + path.string();
        }
        return false;
    }

    StrategyMainConfig config;
    std::string section;
    int section_indent = 0;
    std::string raw_line;
    int line_no = 0;
    while (std::getline(input, raw_line)) {
        ++line_no;
        const std::string stripped = StripInlineComment(raw_line);
        const std::size_t first_non_space = stripped.find_first_not_of(' ');
        if (first_non_space == std::string::npos) {
            continue;
        }
        const int indent = static_cast<int>(first_non_space);
        const std::string text = Trim(stripped);
        if (text.empty()) {
            continue;
        }

        std::string key;
        std::string value;
        if (!ParseKeyValue(text, &key, &value)) {
            if (error != nullptr) {
                *error = "line " + std::to_string(line_no) + ": invalid key/value entry";
            }
            return false;
        }

        if (indent == 0) {
            section.clear();
            if (key == "run_type") {
                config.run_type = value;
                continue;
            }
            if (key == "market_state_mode") {
                bool parsed = true;
                if (!ParseBoolText(value, &parsed)) {
                    if (error != nullptr) {
                        *error =
                            "line " + std::to_string(line_no) + ": invalid market_state_mode bool";
                    }
                    return false;
                }
                config.market_state_mode = parsed;
                continue;
            }
            if (key == "backtest") {
                if (!value.empty()) {
                    if (error != nullptr) {
                        *error =
                            "line " + std::to_string(line_no) + ": backtest must be YAML section";
                    }
                    return false;
                }
                section = "backtest";
                section_indent = indent;
                continue;
            }
            if (key == "composite") {
                if (!value.empty()) {
                    if (error != nullptr) {
                        *error =
                            "line " + std::to_string(line_no) + ": composite must be YAML section";
                    }
                    return false;
                }
                section = "composite";
                section_indent = indent;
                continue;
            }
            if (error != nullptr) {
                *error =
                    "line " + std::to_string(line_no) + ": unsupported top-level field: " + key;
            }
            return false;
        }

        if (section == "backtest" && indent > section_indent) {
            if (!ParseBacktestYamlKey(&config.backtest, key, value, error)) {
                if (error != nullptr && error->find("line ") != 0) {
                    *error = "line " + std::to_string(line_no) + ": " + *error;
                }
                return false;
            }
            continue;
        }
    }
    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading strategy main config: " + path.string();
        }
        return false;
    }

    *out = std::move(config);
    return true;
}

bool LoadMainJson(const std::filesystem::path& path, StrategyMainConfig* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "strategy main config output is null";
        }
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open strategy main config: " + path.string();
        }
        return false;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading strategy main config: " + path.string();
        }
        return false;
    }

    simple_json::Value root;
    if (!simple_json::Parse(buffer.str(), &root, error)) {
        return false;
    }
    if (!root.IsObject()) {
        if (error != nullptr) {
            *error = "strategy main json root must be object";
        }
        return false;
    }

    StrategyMainConfig config;
    for (const auto& [key, value] : root.object_value) {
        if (key == "run_type") {
            if (!value.IsString()) {
                if (error != nullptr) {
                    *error = "run_type must be string";
                }
                return false;
            }
            config.run_type = value.string_value;
            continue;
        }
        if (key == "market_state_mode") {
            if (!value.IsBool()) {
                if (error != nullptr) {
                    *error = "market_state_mode must be bool";
                }
                return false;
            }
            config.market_state_mode = value.bool_value;
            continue;
        }
        if (key == "backtest") {
            if (!value.IsObject()) {
                if (error != nullptr) {
                    *error = "backtest must be object";
                }
                return false;
            }
            for (const auto& [backtest_key, backtest_value] : value.object_value) {
                if (backtest_key == "initial_equity") {
                    if (!backtest_value.IsNumber()) {
                        if (error != nullptr) {
                            *error = "backtest.initial_equity must be number";
                        }
                        return false;
                    }
                    config.backtest.initial_equity = backtest_value.number_value;
                    continue;
                }
                if (backtest_key == "max_loss_percent") {
                    if (error != nullptr) {
                        *error = "backtest.max_loss_percent has been removed; configure "
                                 "risk_per_trade_pct in each sub strategy params";
                    }
                    return false;
                }
                if (backtest_key == "symbols") {
                    if (!backtest_value.IsArray()) {
                        if (error != nullptr) {
                            *error = "backtest.symbols must be array";
                        }
                        return false;
                    }
                    config.backtest.symbols.clear();
                    for (const auto& item : backtest_value.array_value) {
                        if (!item.IsString()) {
                            if (error != nullptr) {
                                *error = "backtest.symbols elements must be strings";
                            }
                            return false;
                        }
                        config.backtest.symbols.push_back(item.string_value);
                    }
                    continue;
                }
                if (backtest_key == "start_date") {
                    if (!backtest_value.IsString()) {
                        if (error != nullptr) {
                            *error = "backtest.start_date must be string";
                        }
                        return false;
                    }
                    config.backtest.start_date = backtest_value.string_value;
                    continue;
                }
                if (backtest_key == "end_date") {
                    if (!backtest_value.IsString()) {
                        if (error != nullptr) {
                            *error = "backtest.end_date must be string";
                        }
                        return false;
                    }
                    config.backtest.end_date = backtest_value.string_value;
                    continue;
                }
                if (backtest_key == "product_config_path") {
                    if (!backtest_value.IsString()) {
                        if (error != nullptr) {
                            *error = "backtest.product_config_path must be string";
                        }
                        return false;
                    }
                    config.backtest.product_config_path = backtest_value.string_value;
                    continue;
                }
                if (error != nullptr) {
                    *error = "unsupported backtest field: " + backtest_key;
                }
                return false;
            }
            continue;
        }
        if (key == "composite") {
            continue;
        }
        if (error != nullptr) {
            *error = "unsupported top-level field: " + key;
        }
        return false;
    }

    *out = std::move(config);
    return true;
}

bool ValidateRunType(const std::string& run_type) {
    const std::string normalized = ToLower(Trim(run_type));
    return normalized == "live" || normalized == "sim" || normalized == "backtest";
}

}  // namespace

bool LoadStrategyMainConfig(const std::string& path, StrategyMainConfig* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "strategy main config output is null";
        }
        return false;
    }
    const std::filesystem::path config_path(path);
    const std::string ext = ToLower(config_path.extension().string());

    StrategyMainConfig config;
    if (ext == ".json") {
        if (!LoadMainJson(config_path, &config, error)) {
            return false;
        }
    } else {
        if (!LoadMainYaml(config_path, &config, error)) {
            return false;
        }
    }

    if (!LoadCompositeStrategyDefinition(path, &config.composite, error)) {
        return false;
    }

    config.composite.run_type = config.run_type;
    config.composite.market_state_mode = config.market_state_mode;

    if (!ValidateRunType(config.run_type)) {
        if (error != nullptr) {
            *error = "run_type must be one of live|sim|backtest";
        }
        return false;
    }
    if (!(config.backtest.initial_equity > 0.0)) {
        if (error != nullptr) {
            *error = "backtest.initial_equity must be > 0";
        }
        return false;
    }
    if (!config.backtest.product_config_path.empty()) {
        std::filesystem::path resolved(config.backtest.product_config_path);
        if (resolved.is_relative()) {
            resolved = config_path.parent_path() / resolved;
        }
        config.backtest.product_config_path = resolved.lexically_normal().string();
    }

    *out = std::move(config);
    return true;
}

}  // namespace quant_hft
