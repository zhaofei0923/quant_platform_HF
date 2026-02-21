#pragma once

#include <string>
#include <vector>

#include "quant_hft/optim/optimization_algorithm.h"

namespace quant_hft::optim {

struct OptimizationReport {
    std::string algorithm;
    std::string metric_path;
    bool maximize{true};
    int total_trials{0};
    int completed_trials{0};
    int failed_trials{0};
    bool interrupted{false};
    std::vector<Trial> trials;
    Trial best_trial;
    std::vector<double> convergence_curve;
    std::vector<double> all_objectives;
};

class ResultAnalyzer {
   public:
    static std::string ResolveMetricPathAlias(const std::string& metric_path);

    static double ExtractMetricFromJsonText(const std::string& json_text,
                                            const std::string& metric_path,
                                            std::string* error);

    static double ExtractMetricFromJson(const std::string& json_path,
                                        const std::string& metric_path,
                                        std::string* error);

    static OptimizationReport Analyze(const std::vector<Trial>& trials,
                                      const OptimizationConfig& config,
                                      bool interrupted);

    static bool WriteReport(const OptimizationReport& report,
                            const std::string& json_path,
                            const std::string& md_path,
                            std::string* error);

    static bool WriteBestParamsYaml(const ParamValueMap& best_params,
                                    const std::string& output_path,
                                    std::string* error);
};

}  // namespace quant_hft::optim
