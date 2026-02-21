#include "quant_hft/optim/parameter_space.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace quant_hft::optim {
namespace {

struct ParameterDraft {
    std::string name;
    std::string type_text;
    std::vector<std::string> values;
    std::vector<std::string> range;
    std::optional<double> step;
    int line_no{0};
};

std::string Trim(const std::string& text) {
    std::size_t begin = 0;
    while (begin < text.size() && std::isspace(static_cast<unsigned char>(text[begin])) != 0) {
        ++begin;
    }
    std::size_t end = text.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1])) != 0) {
        --end;
    }
    return text.substr(begin, end - begin);
}

std::string ToLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::string Unquote(std::string value) {
    value = Trim(value);
    if (value.size() >= 2 && ((value.front() == '"' && value.back() == '"') ||
                              (value.front() == '\'' && value.back() == '\''))) {
        return value.substr(1, value.size() - 2);
    }
    return value;
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

bool ParseKeyValue(const std::string& text, std::string* out_key, std::string* out_value) {
    if (out_key == nullptr || out_value == nullptr) {
        return false;
    }
    const std::size_t pos = text.find(':');
    if (pos == std::string::npos) {
        return false;
    }
    *out_key = Trim(text.substr(0, pos));
    *out_value = Trim(text.substr(pos + 1));
    return !out_key->empty();
}

bool ParseBool(const std::string& text, bool* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string lowered = ToLower(Trim(text));
    if (lowered == "true" || lowered == "1" || lowered == "yes" || lowered == "on") {
        *out = true;
        return true;
    }
    if (lowered == "false" || lowered == "0" || lowered == "no" || lowered == "off") {
        *out = false;
        return true;
    }
    return false;
}

bool ParseDouble(const std::string& text, double* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string trimmed = Trim(text);
    if (trimmed.empty()) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const double value = std::stod(trimmed, &consumed);
        if (consumed != trimmed.size()) {
            return false;
        }
        *out = value;
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseInt(const std::string& text, int* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string trimmed = Trim(text);
    if (trimmed.empty()) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const long value = std::stol(trimmed, &consumed);
        if (consumed != trimmed.size()) {
            return false;
        }
        if (value < std::numeric_limits<int>::min() || value > std::numeric_limits<int>::max()) {
            return false;
        }
        *out = static_cast<int>(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseBracketTokens(const std::string& text, std::vector<std::string>* out_tokens) {
    if (out_tokens == nullptr) {
        return false;
    }
    out_tokens->clear();
    const std::string trimmed = Trim(text);
    if (trimmed.size() < 2 || trimmed.front() != '[' || trimmed.back() != ']') {
        return false;
    }
    const std::string body = trimmed.substr(1, trimmed.size() - 2);
    std::string current;
    bool in_single_quote = false;
    bool in_double_quote = false;
    for (char ch : body) {
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
            out_tokens->push_back(Trim(current));
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    if (!current.empty() || !body.empty()) {
        out_tokens->push_back(Trim(current));
    }
    return true;
}

bool ParseParameterType(const std::string& text, ParameterType* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string lowered = ToLower(Trim(text));
    if (lowered == "int") {
        *out = ParameterType::kInt;
        return true;
    }
    if (lowered == "double") {
        *out = ParameterType::kDouble;
        return true;
    }
    if (lowered == "string") {
        *out = ParameterType::kString;
        return true;
    }
    if (lowered == "enum") {
        *out = ParameterType::kEnum;
        return true;
    }
    return false;
}

bool ParseParamValue(const std::string& token, ParameterType type, ParamValue* out, std::string* error,
                     int line_no, const std::string& key) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "line " + std::to_string(line_no) + ": internal null output for " + key;
        }
        return false;
    }
    if (type == ParameterType::kInt) {
        int value = 0;
        if (!ParseInt(token, &value)) {
            if (error != nullptr) {
                *error = "line " + std::to_string(line_no) + ": invalid int value for " + key;
            }
            return false;
        }
        *out = value;
        return true;
    }
    if (type == ParameterType::kDouble) {
        double value = 0.0;
        if (!ParseDouble(token, &value)) {
            if (error != nullptr) {
                *error = "line " + std::to_string(line_no) + ": invalid double value for " + key;
            }
            return false;
        }
        *out = value;
        return true;
    }

    *out = Unquote(token);
    return true;
}

int DefaultParallel() {
    const unsigned int hw = std::thread::hardware_concurrency();
    if (hw == 0) {
        return 1;
    }
    return static_cast<int>(std::min<unsigned int>(4U, hw));
}

bool FinalizeParameterDraft(const ParameterDraft& draft, ParameterDef* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "line " + std::to_string(draft.line_no) + ": internal null parameter output";
        }
        return false;
    }
    if (draft.name.empty()) {
        if (error != nullptr) {
            *error = "line " + std::to_string(draft.line_no) + ": parameter name is required";
        }
        return false;
    }
    if (draft.type_text.empty()) {
        if (error != nullptr) {
            *error = "line " + std::to_string(draft.line_no) + ": parameter type is required";
        }
        return false;
    }

    ParameterType type = ParameterType::kString;
    if (!ParseParameterType(draft.type_text, &type)) {
        if (error != nullptr) {
            *error = "line " + std::to_string(draft.line_no) + ": unsupported parameter type: " +
                     draft.type_text;
        }
        return false;
    }

    if (!draft.values.empty() && !draft.range.empty()) {
        if (error != nullptr) {
            *error = "line " + std::to_string(draft.line_no) +
                     ": parameter cannot define both values and range";
        }
        return false;
    }
    if (draft.values.empty() && draft.range.empty()) {
        if (error != nullptr) {
            *error = "line " + std::to_string(draft.line_no) +
                     ": parameter must define values or range";
        }
        return false;
    }

    ParameterDef result;
    result.name = draft.name;
    result.type = type;

    if (!draft.values.empty()) {
        if (draft.step.has_value()) {
            if (error != nullptr) {
                *error = "line " + std::to_string(draft.line_no) +
                         ": step is only allowed with range";
            }
            return false;
        }
        for (const std::string& token : draft.values) {
            ParamValue value;
            if (!ParseParamValue(token, type, &value, error, draft.line_no, "values")) {
                return false;
            }
            result.values.push_back(std::move(value));
        }
    }

    if (!draft.range.empty()) {
        if (draft.range.size() != 2) {
            if (error != nullptr) {
                *error = "line " + std::to_string(draft.line_no) + ": range must have 2 elements";
            }
            return false;
        }
        if (type == ParameterType::kString || type == ParameterType::kEnum) {
            if (error != nullptr) {
                *error = "line " + std::to_string(draft.line_no) +
                         ": string/enum type does not support range";
            }
            return false;
        }

        ParamValue min_value;
        ParamValue max_value;
        if (!ParseParamValue(draft.range[0], type, &min_value, error, draft.line_no, "range")) {
            return false;
        }
        if (!ParseParamValue(draft.range[1], type, &max_value, error, draft.line_no, "range")) {
            return false;
        }

        if (type == ParameterType::kInt) {
            const int min_int = std::get<int>(min_value);
            const int max_int = std::get<int>(max_value);
            if (max_int < min_int) {
                if (error != nullptr) {
                    *error = "line " + std::to_string(draft.line_no) + ": range max must be >= min";
                }
                return false;
            }
        } else if (type == ParameterType::kDouble) {
            const double min_double = std::get<double>(min_value);
            const double max_double = std::get<double>(max_value);
            if (max_double < min_double) {
                if (error != nullptr) {
                    *error = "line " + std::to_string(draft.line_no) + ": range max must be >= min";
                }
                return false;
            }
        }

        result.min = min_value;
        result.max = max_value;

        double step = draft.step.value_or(1.0);
        if (!(step > 0.0)) {
            if (error != nullptr) {
                *error = "line " + std::to_string(draft.line_no) + ": step must be > 0";
            }
            return false;
        }
        if (type == ParameterType::kInt) {
            const double rounded = std::round(step);
            if (std::fabs(step - rounded) > 1e-9) {
                if (error != nullptr) {
                    *error = "line " + std::to_string(draft.line_no) +
                             ": int parameter step must be an integer";
                }
                return false;
            }
        }
        result.step = step;
    }

    *out = std::move(result);
    return true;
}

std::string FormatLineError(int line_no, const std::string& message) {
    return "line " + std::to_string(line_no) + ": " + message;
}

bool SetOptimizationField(const std::string& key,
                          const std::string& value,
                          OptimizationConfig* config,
                          std::string* error,
                          int line_no) {
    if (config == nullptr) {
        if (error != nullptr) {
            *error = FormatLineError(line_no, "internal null optimization config");
        }
        return false;
    }
    if (key == "algorithm") {
        config->algorithm = ToLower(Unquote(value));
        return true;
    }
    if (key == "metric_path" || key == "metric") {
        config->metric_path = Unquote(value);
        return true;
    }
    if (key == "maximize") {
        bool parsed = true;
        if (!ParseBool(value, &parsed)) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "invalid maximize bool");
            }
            return false;
        }
        config->maximize = parsed;
        return true;
    }
    if (key == "max_trials") {
        int parsed = 0;
        if (!ParseInt(value, &parsed)) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "invalid max_trials int");
            }
            return false;
        }
        config->max_trials = parsed;
        return true;
    }
    if (key == "parallel" || key == "batch_size") {
        int parsed = 0;
        if (!ParseInt(value, &parsed)) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "invalid parallel int");
            }
            return false;
        }
        config->batch_size = parsed;
        return true;
    }
    if (key == "output_json") {
        config->output_json = Unquote(value);
        return true;
    }
    if (key == "output_md") {
        config->output_md = Unquote(value);
        return true;
    }
    if (key == "best_params_yaml") {
        config->best_params_yaml = Unquote(value);
        return true;
    }

    if (error != nullptr) {
        *error = FormatLineError(line_no, "unsupported optimization field: " + key);
    }
    return false;
}

bool SetParameterDraftField(ParameterDraft* draft,
                            const std::string& key,
                            const std::string& value,
                            int line_no,
                            std::string* error) {
    if (draft == nullptr) {
        if (error != nullptr) {
            *error = FormatLineError(line_no, "internal null parameter draft");
        }
        return false;
    }

    if (key == "name") {
        draft->name = Unquote(value);
        return true;
    }
    if (key == "type") {
        draft->type_text = Unquote(value);
        return true;
    }
    if (key == "range") {
        std::vector<std::string> tokens;
        if (!ParseBracketTokens(value, &tokens) || tokens.size() != 2) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "range must be [min, max]");
            }
            return false;
        }
        draft->range = std::move(tokens);
        return true;
    }
    if (key == "values") {
        std::vector<std::string> tokens;
        if (!ParseBracketTokens(value, &tokens)) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "values must be [a, b, c]");
            }
            return false;
        }
        draft->values = std::move(tokens);
        return true;
    }
    if (key == "step") {
        double parsed = 0.0;
        if (!ParseDouble(value, &parsed)) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "invalid step");
            }
            return false;
        }
        draft->step = parsed;
        return true;
    }

    if (error != nullptr) {
        *error = FormatLineError(line_no, "unsupported parameter field: " + key);
    }
    return false;
}

}  // namespace

bool LoadParameterSpace(const std::string& yaml_path, ParameterSpace* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "parameter space output is null";
        }
        return false;
    }

    std::ifstream input(yaml_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open optimization config: " + yaml_path;
        }
        return false;
    }

    ParameterSpace space;
    space.optimization.batch_size = DefaultParallel();

    std::string section;
    ParameterDraft current_param;
    bool has_current_param = false;

    auto finalize_param = [&](int line_no) -> bool {
        if (!has_current_param) {
            return true;
        }
        ParameterDef def;
        if (!FinalizeParameterDraft(current_param, &def, error)) {
            return false;
        }
        space.parameters.push_back(std::move(def));
        has_current_param = false;
        current_param = ParameterDraft{};
        current_param.line_no = line_no;
        return true;
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

        if (indent == 0) {
            if (section == "parameters") {
                if (!finalize_param(line_no)) {
                    return false;
                }
            }

            std::string key;
            std::string value;
            if (!ParseKeyValue(text, &key, &value)) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "invalid top-level key/value");
                }
                return false;
            }

            if (value.empty()) {
                if (key == "backtest_args" || key == "optimization" || key == "parameters") {
                    section = key;
                    continue;
                }
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "unsupported section: " + key);
                }
                return false;
            }

            section.clear();
            if (key == "backtest_cli_path") {
                space.backtest_cli_path = Unquote(value);
                continue;
            }
            if (key == "composite_config_path") {
                space.composite_config_path = Unquote(value);
                continue;
            }
            if (key == "target_sub_config_path") {
                space.target_sub_config_path = Unquote(value);
                continue;
            }

            if (error != nullptr) {
                *error = FormatLineError(line_no, "unsupported top-level field: " + key);
            }
            return false;
        }

        if (section == "backtest_args") {
            if (indent < 2) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "invalid indentation for backtest_args");
                }
                return false;
            }
            std::string key;
            std::string value;
            if (!ParseKeyValue(text, &key, &value) || value.empty()) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "invalid backtest_args entry");
                }
                return false;
            }
            space.backtest_args[key] = Unquote(value);
            continue;
        }

        if (section == "optimization") {
            if (indent < 2) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "invalid indentation for optimization");
                }
                return false;
            }
            std::string key;
            std::string value;
            if (!ParseKeyValue(text, &key, &value) || value.empty()) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "invalid optimization entry");
                }
                return false;
            }
            if (!SetOptimizationField(key, value, &space.optimization, error, line_no)) {
                return false;
            }
            continue;
        }

        if (section == "parameters") {
            if (indent == 2 && text.rfind('-', 0) == 0) {
                if (!finalize_param(line_no)) {
                    return false;
                }
                has_current_param = true;
                current_param = ParameterDraft{};
                current_param.line_no = line_no;

                const std::string remainder = Trim(text.substr(1));
                if (!remainder.empty()) {
                    std::string key;
                    std::string value;
                    if (!ParseKeyValue(remainder, &key, &value) || value.empty()) {
                        if (error != nullptr) {
                            *error = FormatLineError(line_no, "invalid parameter list item");
                        }
                        return false;
                    }
                    if (!SetParameterDraftField(&current_param, key, value, line_no, error)) {
                        return false;
                    }
                }
                continue;
            }

            if (indent >= 4) {
                if (!has_current_param) {
                    if (error != nullptr) {
                        *error = FormatLineError(line_no, "parameter field without list item");
                    }
                    return false;
                }
                std::string key;
                std::string value;
                if (!ParseKeyValue(text, &key, &value) || value.empty()) {
                    if (error != nullptr) {
                        *error = FormatLineError(line_no, "invalid parameter field");
                    }
                    return false;
                }
                if (!SetParameterDraftField(&current_param, key, value, line_no, error)) {
                    return false;
                }
                continue;
            }

            if (error != nullptr) {
                *error = FormatLineError(line_no, "invalid parameters indentation");
            }
            return false;
        }

        if (error != nullptr) {
            *error = FormatLineError(line_no, "field is not inside a known section");
        }
        return false;
    }

    if (section == "parameters" && !finalize_param(line_no + 1)) {
        return false;
    }

    if (space.composite_config_path.empty()) {
        if (error != nullptr) {
            *error = "composite_config_path is required";
        }
        return false;
    }
    if (space.target_sub_config_path.empty()) {
        if (error != nullptr) {
            *error = "target_sub_config_path is required";
        }
        return false;
    }
    if (space.parameters.empty()) {
        if (error != nullptr) {
            *error = "parameters list must not be empty";
        }
        return false;
    }

    space.optimization.algorithm = ToLower(Trim(space.optimization.algorithm));
    if (space.optimization.algorithm.empty()) {
        space.optimization.algorithm = "grid";
    }
    if (space.optimization.algorithm != "grid" && space.optimization.algorithm != "random") {
        if (error != nullptr) {
            *error = "unsupported optimization.algorithm: " + space.optimization.algorithm;
        }
        return false;
    }
    if (space.optimization.metric_path.empty()) {
        space.optimization.metric_path = "hf_standard.profit_factor";
    }
    if (space.optimization.max_trials <= 0) {
        if (error != nullptr) {
            *error = "optimization.max_trials must be > 0";
        }
        return false;
    }
    if (space.optimization.batch_size <= 0) {
        if (error != nullptr) {
            *error = "optimization.parallel must be > 0";
        }
        return false;
    }

    const auto engine_mode_it = space.backtest_args.find("engine_mode");
    if (engine_mode_it == space.backtest_args.end()) {
        space.backtest_args["engine_mode"] = "parquet";
    } else if (ToLower(Trim(engine_mode_it->second)) != "parquet") {
        if (error != nullptr) {
            *error = "backtest_args.engine_mode must be parquet under parquet-only policy";
        }
        return false;
    }
    if (space.backtest_args.find("dataset_root") == space.backtest_args.end()) {
        if (error != nullptr) {
            *error = "backtest_args.dataset_root is required under parquet-only policy";
        }
        return false;
    }

    *out = std::move(space);
    return true;
}

}  // namespace quant_hft::optim
