#include "quant_hft/optim/result_analyzer.h"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/core/simple_json.h"

namespace quant_hft::optim {
namespace {

using quant_hft::apps::JsonEscape;
using quant_hft::apps::WriteTextFile;
using quant_hft::simple_json::Value;

std::string FormatDouble(double value) {
    std::ostringstream oss;
    oss << std::setprecision(12) << value;
    return oss.str();
}

std::string ParamValueToString(const ParamValue& value) {
    if (std::holds_alternative<int>(value)) {
        return std::to_string(std::get<int>(value));
    }
    if (std::holds_alternative<double>(value)) {
        return FormatDouble(std::get<double>(value));
    }
    return std::get<std::string>(value);
}

std::string ParamValueToJson(const ParamValue& value) {
    if (std::holds_alternative<int>(value)) {
        return std::to_string(std::get<int>(value));
    }
    if (std::holds_alternative<double>(value)) {
        return FormatDouble(std::get<double>(value));
    }
    return "\"" + JsonEscape(std::get<std::string>(value)) + "\"";
}

std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::string current;
    for (char ch : path) {
        if (ch == '.') {
            if (!current.empty()) {
                parts.push_back(current);
                current.clear();
            }
            continue;
        }
        current.push_back(ch);
    }
    if (!current.empty()) {
        parts.push_back(current);
    }
    return parts;
}

bool ParseDoubleStrict(const std::string& text, double* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const double value = std::stod(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = value;
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<std::pair<std::string, ParamValue>> SortedParams(const ParamValueMap& params) {
    std::vector<std::pair<std::string, ParamValue>> sorted;
    sorted.reserve(params.values.size());
    for (const auto& [key, value] : params.values) {
        sorted.emplace_back(key, value);
    }
    std::sort(sorted.begin(), sorted.end(), [](const auto& left, const auto& right) {
        return left.first < right.first;
    });
    return sorted;
}

}  // namespace

std::string ResultAnalyzer::ResolveMetricPathAlias(const std::string& metric_path) {
    static const std::map<std::string, std::string> kAliases = {
        {"hf_standard.profit_factor", "hf_standard.advanced_summary.profit_factor"},
        {"profit_factor", "hf_standard.advanced_summary.profit_factor"},
        {"summary.total_pnl", "summary.total_pnl"},
        {"total_pnl", "summary.total_pnl"},
        {"max_drawdown", "summary.max_drawdown"},
    };
    const auto it = kAliases.find(metric_path);
    return it == kAliases.end() ? metric_path : it->second;
}

double ResultAnalyzer::ExtractMetricFromJson(const std::string& json_path,
                                             const std::string& metric_path,
                                             std::string* error) {
    std::ifstream input(json_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open trial json: " + json_path;
        }
        return 0.0;
    }

    std::ostringstream buffer;
    buffer << input.rdbuf();

    Value root;
    if (!quant_hft::simple_json::Parse(buffer.str(), &root, error)) {
        return 0.0;
    }

    const std::string resolved_path = ResolveMetricPathAlias(metric_path);
    const std::vector<std::string> segments = SplitPath(resolved_path);
    if (segments.empty()) {
        if (error != nullptr) {
            *error = "metric path is empty";
        }
        return 0.0;
    }

    const Value* current = &root;
    for (const std::string& segment : segments) {
        if (!current->IsObject()) {
            if (error != nullptr) {
                *error = "metric path segment `" + segment + "` is not an object parent";
            }
            return 0.0;
        }
        current = current->Find(segment);
        if (current == nullptr) {
            if (error != nullptr) {
                *error = "metric path segment not found: " + segment;
            }
            return 0.0;
        }
    }

    if (current->IsNumber()) {
        return current->number_value;
    }
    if (current->IsString()) {
        double value = 0.0;
        if (!ParseDoubleStrict(current->string_value, &value)) {
            if (error != nullptr) {
                *error = "metric path does not resolve to numeric value";
            }
            return 0.0;
        }
        return value;
    }

    if (error != nullptr) {
        *error = "metric path does not resolve to numeric value";
    }
    return 0.0;
}

OptimizationReport ResultAnalyzer::Analyze(const std::vector<Trial>& trials,
                                           const OptimizationConfig& config,
                                           bool interrupted) {
    OptimizationReport report;
    report.algorithm = config.algorithm;
    report.metric_path = ResolveMetricPathAlias(config.metric_path);
    report.maximize = config.maximize;
    report.interrupted = interrupted;
    report.trials = trials;
    report.total_trials = static_cast<int>(trials.size());

    bool has_best = false;
    Trial best;
    double best_so_far = config.maximize ? -std::numeric_limits<double>::infinity()
                                         : std::numeric_limits<double>::infinity();

    for (const Trial& trial : trials) {
        if (trial.status == "completed") {
            ++report.completed_trials;
            report.all_objectives.push_back(trial.objective);

            const bool better = !has_best ||
                                (config.maximize ? (trial.objective > best.objective)
                                                 : (trial.objective < best.objective));
            if (better) {
                best = trial;
                has_best = true;
            }
            best_so_far = config.maximize ? std::max(best_so_far, trial.objective)
                                          : std::min(best_so_far, trial.objective);
        } else {
            ++report.failed_trials;
            report.all_objectives.push_back(0.0);
        }

        if (has_best) {
            report.convergence_curve.push_back(best_so_far);
        } else {
            report.convergence_curve.push_back(0.0);
        }
    }

    if (has_best) {
        report.best_trial = best;
    } else {
        report.best_trial.status = "failed";
        report.best_trial.error_msg = "no completed trial";
    }

    return report;
}

bool ResultAnalyzer::WriteReport(const OptimizationReport& report,
                                 const std::string& json_path,
                                 const std::string& md_path,
                                 std::string* error) {
    std::ostringstream json;
    json << "{\n"
         << "  \"algorithm\": \"" << JsonEscape(report.algorithm) << "\",\n"
         << "  \"metric_path\": \"" << JsonEscape(report.metric_path) << "\",\n"
         << "  \"maximize\": " << (report.maximize ? "true" : "false") << ",\n"
         << "  \"interrupted\": " << (report.interrupted ? "true" : "false") << ",\n"
         << "  \"total_trials\": " << report.total_trials << ",\n"
         << "  \"completed_trials\": " << report.completed_trials << ",\n"
         << "  \"failed_trials\": " << report.failed_trials << ",\n"
         << "  \"best_trial\": {\n"
         << "    \"trial_id\": \"" << JsonEscape(report.best_trial.trial_id) << "\",\n"
         << "    \"status\": \"" << JsonEscape(report.best_trial.status) << "\",\n"
         << "    \"objective\": " << FormatDouble(report.best_trial.objective) << ",\n"
         << "    \"result_json_path\": \"" << JsonEscape(report.best_trial.result_json_path)
         << "\",\n"
         << "    \"working_dir\": \"" << JsonEscape(report.best_trial.working_dir) << "\",\n"
         << "    \"error_msg\": \"" << JsonEscape(report.best_trial.error_msg) << "\",\n"
         << "    \"params\": {";

    const auto best_params = SortedParams(report.best_trial.params);
    for (std::size_t i = 0; i < best_params.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << "\"" << JsonEscape(best_params[i].first) << "\": "
             << ParamValueToJson(best_params[i].second);
    }
    json << "}\n"
         << "  },\n"
         << "  \"all_objectives\": [";
    for (std::size_t i = 0; i < report.all_objectives.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << FormatDouble(report.all_objectives[i]);
    }
    json << "],\n"
         << "  \"convergence_curve\": [";
    for (std::size_t i = 0; i < report.convergence_curve.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << FormatDouble(report.convergence_curve[i]);
    }
    json << "],\n"
         << "  \"trials\": [\n";

    for (std::size_t i = 0; i < report.trials.size(); ++i) {
        const Trial& trial = report.trials[i];
        json << "    {\n"
             << "      \"trial_id\": \"" << JsonEscape(trial.trial_id) << "\",\n"
             << "      \"status\": \"" << JsonEscape(trial.status) << "\",\n"
             << "      \"objective\": " << FormatDouble(trial.objective) << ",\n"
             << "      \"elapsed_sec\": " << FormatDouble(trial.elapsed_sec) << ",\n"
             << "      \"result_json_path\": \"" << JsonEscape(trial.result_json_path)
             << "\",\n"
             << "      \"working_dir\": \"" << JsonEscape(trial.working_dir) << "\",\n"
             << "      \"error_msg\": \"" << JsonEscape(trial.error_msg) << "\",\n"
             << "      \"params\": {";

        const auto params = SortedParams(trial.params);
        for (std::size_t p = 0; p < params.size(); ++p) {
            if (p > 0) {
                json << ", ";
            }
            json << "\"" << JsonEscape(params[p].first) << "\": "
                 << ParamValueToJson(params[p].second);
        }

        json << "}\n"
             << "    }";
        if (i + 1 < report.trials.size()) {
            json << ',';
        }
        json << "\n";
    }

    json << "  ]\n"
         << "}\n";

    if (!WriteTextFile(json_path, json.str(), error)) {
        return false;
    }

    std::ostringstream md;
    md << "# 参数优化报告\n\n"
       << "- 算法: `" << report.algorithm << "`\n"
       << "- 指标: `" << report.metric_path << "`\n"
       << "- 优化方向: `" << (report.maximize ? "maximize" : "minimize") << "`\n"
       << "- 总试验数: `" << report.total_trials << "`\n"
       << "- 成功: `" << report.completed_trials << "`\n"
       << "- 失败: `" << report.failed_trials << "`\n"
       << "- 中断: `" << (report.interrupted ? "true" : "false") << "`\n\n"
       << "## 最优试验\n\n"
       << "- trial_id: `" << report.best_trial.trial_id << "`\n"
       << "- objective: `" << FormatDouble(report.best_trial.objective) << "`\n"
       << "- status: `" << report.best_trial.status << "`\n"
       << "- result_json_path: `" << report.best_trial.result_json_path << "`\n"
       << "- working_dir: `" << report.best_trial.working_dir << "`\n\n"
       << "## 最优参数\n\n";

    for (const auto& [key, value] : SortedParams(report.best_trial.params)) {
        md << "- " << key << ": `" << ParamValueToString(value) << "`\n";
    }

    md << "\n## 失败样本\n\n";
    for (const Trial& trial : report.trials) {
        if (trial.status == "completed") {
            continue;
        }
        md << "- " << trial.trial_id << ": " << trial.error_msg;
        if (!trial.working_dir.empty()) {
            md << " (`" << trial.working_dir << "`)";
        }
        md << "\n";
    }

    return WriteTextFile(md_path, md.str(), error);
}

bool ResultAnalyzer::WriteBestParamsYaml(const ParamValueMap& best_params,
                                         const std::string& output_path,
                                         std::string* error) {
    std::ostringstream yaml;
    yaml << "params:\n";
    const auto sorted = SortedParams(best_params);
    for (const auto& [key, value] : sorted) {
        yaml << "  " << key << ": " << ParamValueToString(value) << "\n";
    }
    return WriteTextFile(output_path, yaml.str(), error);
}

}  // namespace quant_hft::optim
