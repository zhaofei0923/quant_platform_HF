#pragma once

#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace quant_hft::optim {

using ParamValue = std::variant<int, double, std::string>;

struct ParamValueMap {
    std::unordered_map<std::string, ParamValue> values;
};

struct Trial {
    std::string trial_id;
    ParamValueMap params;
    double objective{0.0};
    std::string result_json_path;
    std::string status{"pending"};
    std::string error_msg;
    double elapsed_sec{0.0};
    std::string working_dir;
};

struct OptimizationConfig {
    std::string algorithm{"grid"};
    std::string metric_path{"hf_standard.profit_factor"};
    bool maximize{true};
    int max_trials{100};
    int batch_size{1};
    std::string output_json{"runtime/optim/optimization_report.json"};
    std::string output_md{"runtime/optim/optimization_report.md"};
    std::string best_params_yaml{"runtime/optim/best_params.yaml"};
};

class ParameterSpace;

class IOptimizationAlgorithm {
   public:
    virtual ~IOptimizationAlgorithm() = default;

    virtual void Initialize(const ParameterSpace& space, const OptimizationConfig& config) = 0;
    virtual std::vector<ParamValueMap> GetNextBatch(int batch_size) = 0;
    virtual void AddTrialResult(const Trial& trial) = 0;
    virtual bool IsFinished() const = 0;
    virtual std::vector<Trial> GetAllTrials() const = 0;
    virtual Trial GetBestTrial() const = 0;
};

}  // namespace quant_hft::optim
