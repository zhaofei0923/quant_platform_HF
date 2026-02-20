#include "quant_hft/strategy/composite_config_loader.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "quant_hft/core/simple_json.h"

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

bool ParseMergeRule(const std::string& value, SignalMergeRule* out) {
    if (out == nullptr) {
        return false;
    }
    if (value == "kPriority") {
        *out = SignalMergeRule::kPriority;
        return true;
    }
    return false;
}

bool ParseMarketRegime(const std::string& token, MarketRegime* out) {
    if (out == nullptr) {
        return false;
    }
    if (token == "kUnknown") {
        *out = MarketRegime::kUnknown;
        return true;
    }
    if (token == "kStrongTrend") {
        *out = MarketRegime::kStrongTrend;
        return true;
    }
    if (token == "kWeakTrend") {
        *out = MarketRegime::kWeakTrend;
        return true;
    }
    if (token == "kRanging") {
        *out = MarketRegime::kRanging;
        return true;
    }
    if (token == "kFlat") {
        *out = MarketRegime::kFlat;
        return true;
    }
    return false;
}

bool ParseMarketRegimeList(const std::string& value, std::vector<MarketRegime>* out) {
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
        const std::string piece = Trim(
            comma == std::string::npos ? body.substr(start) : body.substr(start, comma - start));
        if (!piece.empty()) {
            MarketRegime regime = MarketRegime::kUnknown;
            if (!ParseMarketRegime(piece, &regime)) {
                return false;
            }
            out->push_back(regime);
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return true;
}

bool ParseBoolText(const std::string& value, bool* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string normalized = ToLower(Trim(value));
    if (normalized == "true" || normalized == "1" || normalized == "yes" || normalized == "on") {
        *out = true;
        return true;
    }
    if (normalized == "false" || normalized == "0" || normalized == "no" || normalized == "off") {
        *out = false;
        return true;
    }
    return false;
}

std::string FormatLineError(int line_no, const std::string& reason) {
    std::ostringstream oss;
    oss << "line " << line_no << ": " << reason;
    return oss.str();
}

bool ValidateCurrentStrategy(SubStrategyDefinition* strategy, int strategy_line,
                             std::string* error) {
    if (strategy == nullptr) {
        return true;
    }
    if (strategy->id.empty()) {
        if (error != nullptr) {
            *error = FormatLineError(strategy_line, "strategy id is required");
        }
        return false;
    }
    if (strategy->type.empty()) {
        if (error != nullptr) {
            *error = FormatLineError(strategy_line, "strategy type is required");
        }
        return false;
    }
    return true;
}

bool ParseAtomicParamsYaml(const std::filesystem::path& path, AtomicParams* out,
                           std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "params output is null";
        }
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open strategy config: " + path.string();
        }
        return false;
    }

    out->clear();
    bool in_params = false;
    int params_indent = 0;
    std::string raw_line;
    int line_no = 0;
    while (std::getline(input, raw_line)) {
        ++line_no;
        const std::string text = Trim(StripInlineComment(raw_line));
        if (text.empty()) {
            continue;
        }
        const std::size_t first_non_space = raw_line.find_first_not_of(' ');
        const int indent =
            first_non_space == std::string::npos ? 0 : static_cast<int>(first_non_space);

        std::string key;
        std::string value;
        if (!ParseKeyValue(text, &key, &value)) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "invalid key/value entry");
            }
            return false;
        }

        if (!in_params) {
            if (key != "params") {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "unsupported_field: " + key);
                }
                return false;
            }
            if (!value.empty()) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "params must be a YAML section");
                }
                return false;
            }
            in_params = true;
            params_indent = indent;
            continue;
        }

        if (indent <= params_indent) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "unexpected field outside params section");
            }
            return false;
        }
        (*out)[key] = value;
    }

    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading strategy config: " + path.string();
        }
        return false;
    }
    if (!in_params) {
        if (error != nullptr) {
            *error = "line 1: expected top-level key `params:`";
        }
        return false;
    }
    return true;
}

bool ParseAtomicParamsJson(const std::filesystem::path& path, AtomicParams* out,
                           std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "params output is null";
        }
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open strategy config: " + path.string();
        }
        return false;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading strategy config: " + path.string();
        }
        return false;
    }

    simple_json::Value root;
    if (!simple_json::Parse(buffer.str(), &root, error)) {
        return false;
    }
    if (!root.IsObject()) {
        if (error != nullptr) {
            *error = "strategy json config root must be an object";
        }
        return false;
    }
    const simple_json::Value* params = root.Find("params");
    if (params == nullptr || !params->IsObject()) {
        if (error != nullptr) {
            *error = "strategy json config requires object field `params`";
        }
        return false;
    }

    out->clear();
    for (const auto& [key, value] : params->object_value) {
        if (value.IsObject() || value.IsArray()) {
            if (error != nullptr) {
                *error = "strategy param `" + key + "` must be a scalar value";
            }
            return false;
        }
        (*out)[key] = value.ToString();
    }
    return true;
}

bool LoadAtomicParamsFile(const std::filesystem::path& path, AtomicParams* out,
                          std::string* error) {
    const std::string lowered = ToLower(path.extension().string());
    if (lowered == ".json") {
        return ParseAtomicParamsJson(path, out, error);
    }
    return ParseAtomicParamsYaml(path, out, error);
}

bool ResolveStrategyExternalConfig(const std::filesystem::path& base_dir,
                                   SubStrategyDefinition* strategy, int line_no,
                                   std::string* error) {
    if (strategy == nullptr || strategy->config_path.empty()) {
        return true;
    }
    if (!strategy->params.empty()) {
        if (error != nullptr) {
            *error = FormatLineError(line_no, "config_path and params cannot be used together");
        }
        return false;
    }
    std::filesystem::path resolved(strategy->config_path);
    if (resolved.is_relative()) {
        resolved = base_dir / resolved;
    }
    resolved = resolved.lexically_normal();
    if (!LoadAtomicParamsFile(resolved, &strategy->params, error)) {
        return false;
    }
    strategy->config_path = resolved.string();
    return true;
}

bool ApplyStrategyField(SubStrategyDefinition* strategy, const std::string& key,
                        const std::string& value, int line_no, std::string* error) {
    if (strategy == nullptr) {
        return false;
    }
    if (key == "id") {
        strategy->id = value;
        return true;
    }
    if (key == "type") {
        if (value == "KamaTrendOpening" || value == "TrendOpening" ||
            value == "TrailingStopLoss" || value == "ATRStopLoss" ||
            value == "ATRTakeProfit" || value == "TimeFilter" ||
            value == "MaxPositionRiskControl") {
            if (error != nullptr) {
                *error = FormatLineError(
                    line_no, "legacy strategy type `" + value +
                                 "` is not supported in Composite V2; use complete sub strategy "
                                 "types such as `KamaTrendStrategy` or `TrendStrategy`");
            }
            return false;
        }
        strategy->type = value;
        return true;
    }
    if (key == "enabled") {
        bool parsed = true;
        if (!ParseBoolText(value, &parsed)) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "invalid enabled bool");
            }
            return false;
        }
        strategy->enabled = parsed;
        return true;
    }
    if (key == "config_path") {
        strategy->config_path = value;
        return true;
    }
    if (key == "entry_market_regimes") {
        if (!ParseMarketRegimeList(value, &strategy->entry_market_regimes)) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "invalid entry_market_regimes list");
            }
            return false;
        }
        return true;
    }
    if (key == "market_regimes") {
        if (error != nullptr) {
            *error = FormatLineError(
                line_no,
                "field `market_regimes` has been removed; use `entry_market_regimes`");
        }
        return false;
    }
    if (error != nullptr) {
        *error = FormatLineError(line_no, "unsupported strategy field: " + key);
    }
    return false;
}

bool LoadCompositeYaml(const std::filesystem::path& path, CompositeStrategyDefinition* out,
                       std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "composite definition output is null";
        }
        return false;
    }

    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open composite config: " + path.string();
        }
        return false;
    }

    CompositeStrategyDefinition definition;
    bool saw_composite_root = false;
    bool in_composite = false;
    const int composite_indent = 0;

    std::vector<SubStrategyDefinition>* active_section = nullptr;
    SubStrategyDefinition* current_strategy = nullptr;
    int current_strategy_line = 0;
    bool in_params = false;
    int params_indent = 0;

    auto reset_item_state = [&]() {
        in_params = false;
        params_indent = 0;
    };
    auto reset_section_state = [&]() {
        active_section = nullptr;
        current_strategy = nullptr;
        current_strategy_line = 0;
        reset_item_state();
    };
    const auto resolve_section =
        [&](const std::string& key) -> std::vector<SubStrategyDefinition>* {
        if (key == "sub_strategies") {
            return &definition.sub_strategies;
        }
        return nullptr;
    };

    std::string raw_line;
    int line_no = 0;
    while (std::getline(input, raw_line)) {
        ++line_no;
        const std::string no_comment = StripInlineComment(raw_line);
        const std::size_t first_non_space = no_comment.find_first_not_of(' ');
        if (first_non_space == std::string::npos) {
            continue;
        }
        const int indent = static_cast<int>(first_non_space);
        const std::string text = Trim(no_comment);
        if (text.empty()) {
            continue;
        }

        if (!in_composite) {
            if (indent == 0 && text == "composite:") {
                saw_composite_root = true;
                in_composite = true;
            }
            continue;
        }

        if (indent <= composite_indent) {
            break;
        }

        if (indent <= 2 &&
            !ValidateCurrentStrategy(current_strategy, current_strategy_line, error)) {
            return false;
        }
        if (indent <= 2) {
            reset_section_state();
        } else if (in_params && indent <= params_indent) {
            in_params = false;
        }

        if (indent == 2) {
            std::string key;
            std::string value;
            if (!ParseKeyValue(text, &key, &value)) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "invalid key/value entry");
                }
                return false;
            }
            if (key == "merge_rule") {
                if (!ParseMergeRule(value, &definition.merge_rule)) {
                    if (error != nullptr) {
                        *error = FormatLineError(line_no, "unsupported merge_rule: " + value);
                    }
                    return false;
                }
                continue;
            }
            if (key == "run_type") {
                if (value.empty()) {
                    if (error != nullptr) {
                        *error = FormatLineError(line_no, "run_type must not be empty");
                    }
                    return false;
                }
                definition.run_type = value;
                continue;
            }
            if (key == "market_state_mode") {
                bool parsed = true;
                if (!ParseBoolText(value, &parsed)) {
                    if (error != nullptr) {
                        *error = FormatLineError(line_no, "invalid market_state_mode bool");
                    }
                    return false;
                }
                definition.market_state_mode = parsed;
                continue;
            }
            if (key == "opening_strategies" || key == "stop_loss_strategies" ||
                key == "take_profit_strategies" || key == "time_filters" ||
                key == "risk_control_strategies") {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "legacy section `" + key +
                                                          "` is no longer supported; use "
                                                          "`sub_strategies`");
                }
                return false;
            }
            auto* section = resolve_section(key);
            if (section != nullptr) {
                if (!value.empty()) {
                    if (error != nullptr) {
                        *error = FormatLineError(line_no, key + " must be a YAML section");
                    }
                    return false;
                }
                active_section = section;
                current_strategy = nullptr;
                current_strategy_line = 0;
                reset_item_state();
                continue;
            }
            if (error != nullptr) {
                *error = FormatLineError(line_no, "unsupported_field: " + key);
            }
            return false;
        }

        if (active_section == nullptr) {
            if (error != nullptr) {
                *error =
                    FormatLineError(line_no, "unexpected nested field outside strategy section");
            }
            return false;
        }

        if (in_params) {
            std::string key;
            std::string value;
            if (!ParseKeyValue(text, &key, &value)) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "invalid params entry");
                }
                return false;
            }
            current_strategy->params[key] = value;
            continue;
        }

        if (text.rfind("- ", 0) == 0) {
            if (!ValidateCurrentStrategy(current_strategy, current_strategy_line, error)) {
                return false;
            }
            active_section->push_back({});
            current_strategy = &active_section->back();
            current_strategy_line = line_no;
            reset_item_state();

            const std::string tail = Trim(text.substr(2));
            if (tail.empty()) {
                continue;
            }
            std::string key;
            std::string value;
            if (!ParseKeyValue(tail, &key, &value)) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "invalid strategy list item");
                }
                return false;
            }
            if (key == "params") {
                if (!value.empty()) {
                    if (error != nullptr) {
                        *error = FormatLineError(line_no, "params must be a YAML section");
                    }
                    return false;
                }
                in_params = true;
                params_indent = indent;
                continue;
            }
            if (!ApplyStrategyField(current_strategy, key, value, line_no, error)) {
                return false;
            }
            continue;
        }

        if (current_strategy == nullptr) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "expected `-` strategy list item");
            }
            return false;
        }

        std::string key;
        std::string value;
        if (!ParseKeyValue(text, &key, &value)) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "invalid strategy field");
            }
            return false;
        }
        if (key == "params") {
            if (!value.empty()) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "params must be a YAML section");
                }
                return false;
            }
            in_params = true;
            params_indent = indent;
            continue;
        }
        if (!ApplyStrategyField(current_strategy, key, value, line_no, error)) {
            return false;
        }
    }

    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading composite config: " + path.string();
        }
        return false;
    }
    if (!saw_composite_root) {
        if (error != nullptr) {
            *error = "line 1: expected top-level key `composite:`";
        }
        return false;
    }
    if (!ValidateCurrentStrategy(current_strategy, current_strategy_line, error)) {
        return false;
    }

    const std::filesystem::path base_dir = path.parent_path();
    auto apply_section = [&](std::vector<SubStrategyDefinition>* section) -> bool {
        if (section == nullptr) {
            return true;
        }
        for (auto& strategy : *section) {
            if (!ResolveStrategyExternalConfig(base_dir, &strategy, 0, error)) {
                return false;
            }
        }
        return true;
    };
    if (!apply_section(&definition.sub_strategies)) {
        return false;
    }

    *out = std::move(definition);
    return true;
}

bool ParseMarketRegimeJsonArray(const simple_json::Value& array_value,
                                std::vector<MarketRegime>* out, std::string* error) {
    if (out == nullptr || !array_value.IsArray()) {
        if (error != nullptr) {
            *error = "entry_market_regimes must be an array";
        }
        return false;
    }
    out->clear();
    for (const auto& item : array_value.array_value) {
        if (!item.IsString()) {
            if (error != nullptr) {
                *error = "entry_market_regimes elements must be strings";
            }
            return false;
        }
        MarketRegime regime = MarketRegime::kUnknown;
        if (!ParseMarketRegime(item.string_value, &regime)) {
            if (error != nullptr) {
                *error = "invalid market_regime token: " + item.string_value;
            }
            return false;
        }
        out->push_back(regime);
    }
    return true;
}

bool ParseStrategyJsonObject(const simple_json::Value& item, SubStrategyDefinition* out,
                             std::string* error) {
    if (out == nullptr) {
        return false;
    }
    if (!item.IsObject()) {
        if (error != nullptr) {
            *error = "strategy entry must be an object";
        }
        return false;
    }

    SubStrategyDefinition parsed;
    for (const auto& [key, value] : item.object_value) {
        if (key == "id") {
            if (!value.IsString()) {
                if (error != nullptr) {
                    *error = "strategy id must be string";
                }
                return false;
            }
            parsed.id = value.string_value;
            continue;
        }
        if (key == "type") {
            if (!value.IsString()) {
                if (error != nullptr) {
                    *error = "strategy type must be string";
                }
                return false;
            }
            if (value.string_value == "KamaTrendOpening" || value.string_value == "TrendOpening" ||
                value.string_value == "TrailingStopLoss" ||
                value.string_value == "ATRStopLoss" || value.string_value == "ATRTakeProfit" ||
                value.string_value == "TimeFilter" ||
                value.string_value == "MaxPositionRiskControl") {
                if (error != nullptr) {
                    *error =
                        "legacy strategy type `" + value.string_value +
                        "` is not supported in Composite V2; use complete sub strategy types such "
                        "as `KamaTrendStrategy` or `TrendStrategy`";
                }
                return false;
            }
            parsed.type = value.string_value;
            continue;
        }
        if (key == "enabled") {
            if (!value.IsBool()) {
                if (error != nullptr) {
                    *error = "strategy enabled must be bool";
                }
                return false;
            }
            parsed.enabled = value.bool_value;
            continue;
        }
        if (key == "config_path") {
            if (!value.IsString()) {
                if (error != nullptr) {
                    *error = "strategy config_path must be string";
                }
                return false;
            }
            parsed.config_path = value.string_value;
            continue;
        }
        if (key == "entry_market_regimes") {
            if (!ParseMarketRegimeJsonArray(value, &parsed.entry_market_regimes, error)) {
                return false;
            }
            continue;
        }
        if (key == "market_regimes") {
            if (error != nullptr) {
                *error = "field `market_regimes` has been removed; use `entry_market_regimes`";
            }
            return false;
        }
        if (key == "params") {
            if (!value.IsObject()) {
                if (error != nullptr) {
                    *error = "strategy params must be object";
                }
                return false;
            }
            for (const auto& [param_key, param_value] : value.object_value) {
                if (param_value.IsObject() || param_value.IsArray()) {
                    if (error != nullptr) {
                        *error = "strategy param `" + param_key + "` must be scalar";
                    }
                    return false;
                }
                parsed.params[param_key] = param_value.ToString();
            }
            continue;
        }
        if (error != nullptr) {
            *error = "unsupported strategy field: " + key;
        }
        return false;
    }

    if (parsed.id.empty() || parsed.type.empty()) {
        if (error != nullptr) {
            *error = "strategy id and type are required";
        }
        return false;
    }
    *out = std::move(parsed);
    return true;
}

bool ParseStrategySectionJson(const simple_json::Value& value,
                              std::vector<SubStrategyDefinition>* out, std::string* error) {
    if (out == nullptr) {
        return false;
    }
    if (!value.IsArray()) {
        if (error != nullptr) {
            *error = "strategy section must be array";
        }
        return false;
    }
    out->clear();
    for (const auto& item : value.array_value) {
        SubStrategyDefinition strategy;
        if (!ParseStrategyJsonObject(item, &strategy, error)) {
            return false;
        }
        out->push_back(std::move(strategy));
    }
    return true;
}

bool LoadCompositeJson(const std::filesystem::path& path, CompositeStrategyDefinition* out,
                       std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "composite definition output is null";
        }
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open composite config: " + path.string();
        }
        return false;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading composite config: " + path.string();
        }
        return false;
    }

    simple_json::Value root;
    if (!simple_json::Parse(buffer.str(), &root, error)) {
        return false;
    }
    if (!root.IsObject()) {
        if (error != nullptr) {
            *error = "composite json root must be object";
        }
        return false;
    }
    const simple_json::Value* composite = root.Find("composite");
    if (composite == nullptr) {
        composite = &root;
    }
    if (!composite->IsObject()) {
        if (error != nullptr) {
            *error = "json field `composite` must be object";
        }
        return false;
    }

    CompositeStrategyDefinition definition;
    for (const auto& [key, value] : composite->object_value) {
        if (key == "merge_rule") {
            if (!value.IsString() || !ParseMergeRule(value.string_value, &definition.merge_rule)) {
                if (error != nullptr) {
                    *error = "unsupported merge_rule";
                }
                return false;
            }
            continue;
        }
        if (key == "run_type") {
            if (!value.IsString() || value.string_value.empty()) {
                if (error != nullptr) {
                    *error = "run_type must be non-empty string";
                }
                return false;
            }
            definition.run_type = value.string_value;
            continue;
        }
        if (key == "market_state_mode") {
            if (!value.IsBool()) {
                if (error != nullptr) {
                    *error = "market_state_mode must be bool";
                }
                return false;
            }
            definition.market_state_mode = value.bool_value;
            continue;
        }
        if (key == "sub_strategies") {
            if (!ParseStrategySectionJson(value, &definition.sub_strategies, error)) {
                return false;
            }
            continue;
        }
        if (key == "opening_strategies" || key == "stop_loss_strategies" ||
            key == "take_profit_strategies" || key == "time_filters" ||
            key == "risk_control_strategies") {
            if (error != nullptr) {
                *error = "legacy section `" + key + "` is no longer supported; use `sub_strategies`";
            }
            return false;
        }
        if (error != nullptr) {
            *error = "unsupported_field: " + key;
        }
        return false;
    }

    const std::filesystem::path base_dir = path.parent_path();
    auto apply_section = [&](std::vector<SubStrategyDefinition>* section) -> bool {
        if (section == nullptr) {
            return true;
        }
        for (auto& strategy : *section) {
            if (!ResolveStrategyExternalConfig(base_dir, &strategy, 0, error)) {
                return false;
            }
        }
        return true;
    };
    if (!apply_section(&definition.sub_strategies)) {
        return false;
    }

    *out = std::move(definition);
    return true;
}

}  // namespace

bool LoadCompositeStrategyDefinition(const std::string& path, CompositeStrategyDefinition* out,
                                     std::string* error) {
    const std::filesystem::path config_path(path);
    const std::string lowered = ToLower(config_path.extension().string());
    if (lowered == ".json") {
        return LoadCompositeJson(config_path, out, error);
    }
    return LoadCompositeYaml(config_path, out, error);
}

}  // namespace quant_hft
