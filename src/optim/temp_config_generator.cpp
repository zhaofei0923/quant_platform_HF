#include "quant_hft/optim/temp_config_generator.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "quant_hft/strategy/composite_config_loader.h"

namespace quant_hft::optim {
namespace {

using quant_hft::AtomicParams;
using quant_hft::CompositeStrategyDefinition;
using quant_hft::MarketRegime;
using quant_hft::SignalMergeRule;
using quant_hft::SubStrategyDefinition;

std::string ToScalarString(const ParamValue& value) {
    if (std::holds_alternative<int>(value)) {
        return std::to_string(std::get<int>(value));
    }
    if (std::holds_alternative<double>(value)) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(10) << std::get<double>(value);
        std::string text = oss.str();
        while (!text.empty() && text.back() == '0') {
            text.pop_back();
        }
        if (!text.empty() && text.back() == '.') {
            text.push_back('0');
        }
        return text.empty() ? "0.0" : text;
    }
    return std::get<std::string>(value);
}

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

bool LoadYamlScalarMapLocal(const std::filesystem::path& path, std::map<std::string, std::string>* out,
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
            *error = "unable to open yaml file: " + path.string();
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
            *error = "failed reading yaml file: " + path.string();
        }
        return false;
    }
    return true;
}

bool NeedsYamlQuote(const std::string& value) {
    if (value.empty()) {
        return true;
    }
    if (std::isspace(static_cast<unsigned char>(value.front())) != 0 ||
        std::isspace(static_cast<unsigned char>(value.back())) != 0) {
        return true;
    }
    for (char ch : value) {
        if (ch == ':' || ch == '#' || ch == '[' || ch == ']' || ch == '{' || ch == '}' ||
            ch == ',' || ch == '"' || ch == '\'' || std::isspace(static_cast<unsigned char>(ch)) != 0) {
            return true;
        }
    }
    return false;
}

std::string YamlScalar(const std::string& value) {
    if (!NeedsYamlQuote(value)) {
        return value;
    }
    std::string escaped;
    escaped.reserve(value.size() + 2);
    escaped.push_back('\'');
    for (char ch : value) {
        if (ch == '\'') {
            escaped.push_back('\'');
            escaped.push_back('\'');
        } else {
            escaped.push_back(ch);
        }
    }
    escaped.push_back('\'');
    return escaped;
}

std::string MergeRuleToString(SignalMergeRule rule) {
    switch (rule) {
        case SignalMergeRule::kPriority:
        default:
            return "kPriority";
    }
}

std::string MarketRegimeToString(MarketRegime regime) {
    switch (regime) {
        case MarketRegime::kStrongTrend:
            return "kStrongTrend";
        case MarketRegime::kWeakTrend:
            return "kWeakTrend";
        case MarketRegime::kRanging:
            return "kRanging";
        case MarketRegime::kFlat:
            return "kFlat";
        case MarketRegime::kUnknown:
        default:
            return "kUnknown";
    }
}

std::filesystem::path AbsolutePathFrom(const std::filesystem::path& base_dir,
                                       const std::filesystem::path& raw_path) {
    std::filesystem::path resolved = raw_path;
    if (resolved.is_relative()) {
        resolved = base_dir / resolved;
    }
    return std::filesystem::absolute(resolved).lexically_normal();
}

bool LoadAtomicParamsFromYaml(const std::filesystem::path& path, AtomicParams* out,
                              std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "params output is null";
        }
        return false;
    }
    std::map<std::string, std::string> scalars;
    if (!LoadYamlScalarMapLocal(path, &scalars, error)) {
        return false;
    }
    out->clear();
    for (const auto& [key, value] : scalars) {
        constexpr const char* kPrefix = "params.";
        if (key.rfind(kPrefix, 0) != 0) {
            continue;
        }
        const std::string param_key = key.substr(std::string(kPrefix).size());
        if (param_key.empty() || param_key.find('.') != std::string::npos) {
            continue;
        }
        (*out)[param_key] = value;
    }
    if (out->empty()) {
        if (error != nullptr) {
            *error = "params section is empty in: " + path.string();
        }
        return false;
    }
    return true;
}

void WriteAtomicParams(std::ostream& out, const AtomicParams& params, int indent_spaces) {
    std::vector<std::string> keys;
    keys.reserve(params.size());
    for (const auto& [key, value] : params) {
        (void)value;
        keys.push_back(key);
    }
    std::sort(keys.begin(), keys.end());

    const std::string indent(static_cast<std::size_t>(indent_spaces), ' ');
    out << indent << "params:\n";
    for (const std::string& key : keys) {
        out << indent << "  " << key << ": " << YamlScalar(params.at(key)) << "\n";
    }
}

void WriteOverrides(std::ostream& out, const SubStrategyDefinition& strategy, int indent_spaces) {
    const bool has_backtest = !strategy.overrides.backtest_params.empty();
    const bool has_sim = !strategy.overrides.sim_params.empty();
    const bool has_live = !strategy.overrides.live_params.empty();
    if (!has_backtest && !has_sim && !has_live) {
        return;
    }

    const std::string indent(static_cast<std::size_t>(indent_spaces), ' ');
    out << indent << "overrides:\n";

    const auto write_mode = [&](const std::string& mode, const AtomicParams& params) {
        if (params.empty()) {
            return;
        }
        out << indent << "  " << mode << ":\n";
        WriteAtomicParams(out, params, indent_spaces + 4);
    };

    write_mode("backtest", strategy.overrides.backtest_params);
    write_mode("sim", strategy.overrides.sim_params);
    write_mode("live", strategy.overrides.live_params);
}

bool WriteSubStrategyYaml(const std::filesystem::path& path, const AtomicParams& params,
                          std::string* error) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "unable to write temp sub strategy yaml: " + path.string();
        }
        return false;
    }
    WriteAtomicParams(out, params, 0);
    return true;
}

bool WriteCompositeYaml(const std::filesystem::path& path,
                        const CompositeStrategyDefinition& definition,
                        std::size_t target_index,
                        const std::filesystem::path& target_sub_yaml,
                        const std::filesystem::path& composite_base_dir,
                        std::string* error) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "unable to write temp composite yaml: " + path.string();
        }
        return false;
    }

    out << "composite:\n";
    out << "  merge_rule: " << MergeRuleToString(definition.merge_rule) << "\n";
    out << "  enable_non_backtest: " << (definition.enable_non_backtest ? "true" : "false") << "\n";
    out << "  market_state_mode: " << (definition.market_state_mode ? "true" : "false") << "\n";
    out << "  sub_strategies:\n";

    for (std::size_t i = 0; i < definition.sub_strategies.size(); ++i) {
        const SubStrategyDefinition& strategy = definition.sub_strategies[i];
        out << "    - id: " << YamlScalar(strategy.id) << "\n";
        out << "      enabled: " << (strategy.enabled ? "true" : "false") << "\n";
        out << "      type: " << YamlScalar(strategy.type) << "\n";

        if (i == target_index) {
            out << "      config_path: " << YamlScalar(target_sub_yaml.string()) << "\n";
        } else if (!strategy.config_path.empty()) {
            const std::filesystem::path resolved =
                AbsolutePathFrom(composite_base_dir, std::filesystem::path(strategy.config_path));
            out << "      config_path: " << YamlScalar(resolved.string()) << "\n";
        } else {
            WriteAtomicParams(out, strategy.params, 6);
        }

        if (!strategy.entry_market_regimes.empty()) {
            out << "      entry_market_regimes: [";
            for (std::size_t r = 0; r < strategy.entry_market_regimes.size(); ++r) {
                if (r > 0) {
                    out << ", ";
                }
                out << MarketRegimeToString(strategy.entry_market_regimes[r]);
            }
            out << "]\n";
        }

        WriteOverrides(out, strategy, 6);
    }

    return true;
}

std::string MakeUniqueSuffix() {
    const auto now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
    return std::to_string(now_ns);
}

}  // namespace

bool GenerateTrialConfig(const TrialConfigRequest& request,
                         TrialConfigArtifacts* out,
                         std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "trial config output is null";
        }
        return false;
    }

    const std::filesystem::path composite_path =
        std::filesystem::absolute(request.composite_config_path).lexically_normal();
    const std::filesystem::path composite_base_dir = composite_path.parent_path();

    CompositeStrategyDefinition definition;
    if (!LoadCompositeStrategyDefinition(composite_path.string(), &definition, error)) {
        return false;
    }

    const std::filesystem::path target_sub_abs =
        AbsolutePathFrom(composite_base_dir, request.target_sub_config_path).lexically_normal();

    std::size_t target_index = definition.sub_strategies.size();
    for (std::size_t i = 0; i < definition.sub_strategies.size(); ++i) {
        const SubStrategyDefinition& strategy = definition.sub_strategies[i];
        if (strategy.config_path.empty()) {
            continue;
        }
        const std::filesystem::path strategy_path =
            AbsolutePathFrom(composite_base_dir, std::filesystem::path(strategy.config_path)).lexically_normal();
        if (strategy_path == target_sub_abs) {
            target_index = i;
            break;
        }
    }

    if (target_index >= definition.sub_strategies.size()) {
        if (error != nullptr) {
            *error = "target_sub_config_path not found in composite.sub_strategies: " +
                     target_sub_abs.string();
        }
        return false;
    }

    AtomicParams target_params = definition.sub_strategies[target_index].params;
    if (target_params.empty()) {
        if (!LoadAtomicParamsFromYaml(target_sub_abs, &target_params, error)) {
            return false;
        }
    }
    for (const auto& [key, value] : request.param_overrides) {
        target_params[key] = ToScalarString(value);
    }

    const std::string trial_id = request.trial_id.empty() ? "trial" : request.trial_id;
    const std::filesystem::path work_dir =
        std::filesystem::temp_directory_path() /
        ("quant_hft_optim_" + trial_id + "_" + MakeUniqueSuffix());
    std::error_code ec;
    std::filesystem::create_directories(work_dir, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "unable to create temp work dir: " + work_dir.string() + ": " + ec.message();
        }
        return false;
    }

    const std::filesystem::path sub_yaml = work_dir / "target_sub_strategy.yaml";
    if (!WriteSubStrategyYaml(sub_yaml, target_params, error)) {
        return false;
    }

    const std::filesystem::path composite_yaml = work_dir / "composite.yaml";
    if (!WriteCompositeYaml(composite_yaml, definition, target_index, sub_yaml, composite_base_dir,
                            error)) {
        return false;
    }

    out->working_dir = work_dir;
    out->composite_config_path = composite_yaml;
    out->sub_config_path = sub_yaml;
    return true;
}

}  // namespace quant_hft::optim
