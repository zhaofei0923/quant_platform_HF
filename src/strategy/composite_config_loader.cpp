#include "quant_hft/strategy/composite_config_loader.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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

std::string FormatLineError(int line_no, const std::string& reason) {
    std::ostringstream oss;
    oss << "line " << line_no << ": " << reason;
    return oss.str();
}

bool ValidateCurrentStrategy(AtomicStrategyDefinition* strategy, int strategy_line,
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

}  // namespace

bool LoadCompositeStrategyDefinition(const std::string& path, CompositeStrategyDefinition* out,
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
            *error = "unable to open composite config: " + path;
        }
        return false;
    }

    CompositeStrategyDefinition definition;
    bool saw_composite_root = false;

    std::vector<AtomicStrategyDefinition>* active_section = nullptr;
    AtomicStrategyDefinition* current_strategy = nullptr;
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
        [&](const std::string& key) -> std::vector<AtomicStrategyDefinition>* {
        if (key == "opening_strategies") {
            return &definition.opening_strategies;
        }
        if (key == "stop_loss_strategies") {
            return &definition.stop_loss_strategies;
        }
        if (key == "take_profit_strategies") {
            return &definition.take_profit_strategies;
        }
        if (key == "time_filters") {
            return &definition.time_filters;
        }
        if (key == "risk_control_strategies") {
            return &definition.risk_control_strategies;
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

        if (!saw_composite_root) {
            if (indent != 0 || text != "composite:") {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "expected top-level key `composite:`");
                }
                return false;
            }
            saw_composite_root = true;
            continue;
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
            if (key == "id") {
                current_strategy->id = value;
                continue;
            }
            if (key == "type") {
                current_strategy->type = value;
                continue;
            }
            if (key == "market_regimes") {
                if (!ParseMarketRegimeList(value, &current_strategy->market_regimes)) {
                    if (error != nullptr) {
                        *error = FormatLineError(line_no, "invalid market_regimes list");
                    }
                    return false;
                }
                continue;
            }
            if (error != nullptr) {
                *error = FormatLineError(line_no, "unsupported strategy field: " + key);
            }
            return false;
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
        if (key == "id") {
            current_strategy->id = value;
            continue;
        }
        if (key == "type") {
            current_strategy->type = value;
            continue;
        }
        if (key == "market_regimes") {
            if (!ParseMarketRegimeList(value, &current_strategy->market_regimes)) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "invalid market_regimes list");
                }
                return false;
            }
            continue;
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
        if (error != nullptr) {
            *error = FormatLineError(line_no, "unsupported strategy field: " + key);
        }
        return false;
    }

    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading composite config: " + path;
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

    *out = std::move(definition);
    return true;
}

}  // namespace quant_hft
