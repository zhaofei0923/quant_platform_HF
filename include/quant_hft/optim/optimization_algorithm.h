#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace quant_hft::optim {

using ParamValue = std::variant<int, double, std::string>;

struct ParamValueMap {
    std::unordered_map<std::string, ParamValue> values;
};

struct TrialMetricsSnapshot {
    std::optional<double> total_pnl;
    std::optional<double> max_drawdown;
    std::optional<double> max_drawdown_pct;
    std::optional<double> annualized_return_pct;
    std::optional<double> sharpe_ratio;
    std::optional<double> calmar_ratio;
    std::optional<double> profit_factor;
    std::optional<double> win_rate_pct;
    std::optional<int> total_trades;
    std::optional<double> expectancy_r;
};

struct Trial {
    std::string trial_id;
    ParamValueMap params;
    double objective{0.0};
    std::string result_json_path;
    std::string stdout_log_path;
    std::string stderr_log_path;
    std::string status{"pending"};
    std::string error_msg;
    double elapsed_sec{0.0};
    std::string working_dir;
    std::string archived_artifact_dir;
    TrialMetricsSnapshot metrics;
    std::string metrics_error;
};

struct OptimizationObjective {
    std::string metric_path;
    double weight{1.0};
    bool maximize{true};
    bool scale_by_initial_equity{false};
};

enum class ConstraintOperator {
    kLess,
    kLessEqual,
    kGreater,
    kGreaterEqual,
    kEqual,
    kNotEqual,
};

struct OptimizationConstraint {
    std::string raw_expression;
    std::string metric_name;
    std::string metric_path;
    ConstraintOperator op{ConstraintOperator::kLess};
    double threshold{0.0};
};

struct ConstraintStats {
    int total_violations{0};
    std::vector<std::string> violated_trials;
};

struct OptimizationConfig {
    std::string algorithm{"grid"};
    std::string metric_path{"hf_standard.profit_factor"};
    std::vector<OptimizationObjective> objectives;
    bool maximize{true};
    int max_trials{100};
    std::optional<std::uint64_t> random_seed;
    int batch_size{1};
    int preserve_top_k_trials{0};
    bool export_heatmap{false};
    std::vector<OptimizationConstraint> constraints;
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
