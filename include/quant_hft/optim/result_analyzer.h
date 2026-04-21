#pragma once

#include <string>
#include <vector>

#include "quant_hft/optim/optimization_algorithm.h"

namespace quant_hft::optim {

struct OptimizationReport {
    std::string task_id;
    std::string started_at;
    std::string finished_at;
    double wall_clock_sec{0.0};
    std::string algorithm;
    std::string metric_path;
    std::vector<OptimizationObjective> objectives;
    bool maximize{true};
    int total_trials{0};
    int completed_trials{0};
    int failed_trials{0};
    ConstraintStats constraint_stats;
    bool interrupted{false};
    std::vector<Trial> trials;
    Trial best_trial;
    std::vector<double> convergence_curve;
    std::vector<double> all_objectives;
};

class ResultAnalyzer {
   public:
    static bool ParseOptimizationConstraint(const std::string& expression,
                                            OptimizationConstraint* out_constraint,
                                            std::string* error);

    static std::string ResolveMetricPathAlias(const std::string& metric_path);

    static double ExtractMetricFromJsonText(const std::string& json_text,
                                            const std::string& metric_path,
                                            std::string* error);

    static double ExtractMetricFromJson(const std::string& json_path,
                                        const std::string& metric_path,
                                        std::string* error);

    static double ComputeObjectiveFromJsonText(const std::string& json_text,
                                               const OptimizationConfig& config,
                                               std::string* error);

    static double ComputeObjectiveFromJson(const std::string& json_path,
                                           const OptimizationConfig& config,
                                           std::string* error);

    static bool ExtractTrialMetricsFromJson(const std::string& json_path,
                                            TrialMetricsSnapshot* out_metrics,
                                            std::string* error);

    static bool ExtractTrialMetricsFromJsonText(const std::string& json_text,
                                                TrialMetricsSnapshot* out_metrics,
                                                std::string* error);

    static bool EvaluateConstraintsFromJson(const std::string& json_path,
                                            const OptimizationConfig& config,
                                            std::vector<std::string>* violations,
                                            std::string* error);

    static bool EvaluateConstraintsFromJsonText(const std::string& json_text,
                                                const OptimizationConfig& config,
                                                std::vector<std::string>* violations,
                                                std::string* error);

    static OptimizationReport Analyze(const std::vector<Trial>& trials,
                                      const OptimizationConfig& config,
                                      bool interrupted);

    static std::string DefaultTop10InSamplePath(const std::string& json_path,
                                                const std::string& md_path);

    static bool WriteReport(const OptimizationReport& report,
                            const std::string& json_path,
                            const std::string& md_path,
                            std::string* error);

    static bool WriteHeatmaps(const OptimizationReport& report,
                              const ParameterSpace& space,
                              const std::string& output_dir,
                              std::string* error);

    static bool WriteTop10InSampleMarkdown(const OptimizationReport& report,
                                           const std::string& output_path,
                                           std::string* error);

    static bool WriteBestParamsYaml(const ParamValueMap& best_params,
                                    const std::string& output_path,
                                    std::string* error);
};

}  // namespace quant_hft::optim
