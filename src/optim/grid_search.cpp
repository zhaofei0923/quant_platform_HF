#include "quant_hft/optim/grid_search.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <utility>

namespace quant_hft::optim {
namespace {

std::vector<ParamValue> BuildValuesForParam(const ParameterDef& param) {
    if (!param.values.empty()) {
        return param.values;
    }

    std::vector<ParamValue> values;
    if (!param.min.has_value() || !param.max.has_value()) {
        return values;
    }

    const double step = param.step.value_or(1.0);
    if (!(step > 0.0)) {
        throw std::runtime_error("grid search step must be > 0");
    }

    if (param.type == ParameterType::kInt) {
        const int min_value = std::get<int>(param.min.value());
        const int max_value = std::get<int>(param.max.value());
        const int step_int = static_cast<int>(std::round(step));
        if (step_int <= 0) {
            throw std::runtime_error("grid search int step must be > 0");
        }
        for (int value = min_value; value <= max_value; value += step_int) {
            values.emplace_back(value);
            if (value > max_value - step_int) {
                break;
            }
        }
        if (values.empty() || std::get<int>(values.back()) != max_value) {
            values.emplace_back(max_value);
        }
        return values;
    }

    if (param.type == ParameterType::kDouble) {
        const double min_value = std::get<double>(param.min.value());
        const double max_value = std::get<double>(param.max.value());
        constexpr double kEps = 1e-9;
        for (double value = min_value; value <= max_value + kEps; value += step) {
            const double clamped = (value > max_value && value < max_value + kEps) ? max_value : value;
            values.emplace_back(clamped);
            if (value > max_value - step) {
                break;
            }
        }
        if (values.empty()) {
            values.emplace_back(max_value);
        } else {
            const double last_value = std::get<double>(values.back());
            if (std::fabs(last_value - max_value) > kEps) {
                values.emplace_back(max_value);
            }
        }
        return values;
    }

    return values;
}

}  // namespace

void GridSearch::Initialize(const ParameterSpace& space, const OptimizationConfig& config) {
    config_ = config;
    all_combinations_.clear();
    results_.clear();
    next_index_ = 0;

    ParamValueMap current;
    GenerateCombinations(space.parameters, 0, std::move(current), &all_combinations_);

    if (config_.max_trials > 0 && static_cast<int>(all_combinations_.size()) > config_.max_trials) {
        all_combinations_.resize(static_cast<std::size_t>(config_.max_trials));
    }
}

std::vector<ParamValueMap> GridSearch::GetNextBatch(int batch_size) {
    if (batch_size <= 0) {
        batch_size = 1;
    }
    const std::size_t begin = next_index_;
    const std::size_t end =
        std::min(next_index_ + static_cast<std::size_t>(batch_size), all_combinations_.size());
    next_index_ = end;
    return std::vector<ParamValueMap>(all_combinations_.begin() + static_cast<std::ptrdiff_t>(begin),
                                      all_combinations_.begin() + static_cast<std::ptrdiff_t>(end));
}

void GridSearch::AddTrialResult(const Trial& trial) { results_.push_back(trial); }

bool GridSearch::IsFinished() const { return next_index_ >= all_combinations_.size(); }

std::vector<Trial> GridSearch::GetAllTrials() const { return results_; }

Trial GridSearch::GetBestTrial() const {
    Trial best;
    bool has_best = false;
    for (const Trial& trial : results_) {
        if (trial.status != "completed") {
            continue;
        }
        if (!has_best) {
            best = trial;
            has_best = true;
            continue;
        }
        const bool better = config_.maximize ? (trial.objective > best.objective)
                                             : (trial.objective < best.objective);
        if (better) {
            best = trial;
        }
    }
    if (!has_best) {
        best.status = "failed";
        best.error_msg = "no completed trial";
    }
    return best;
}

void GridSearch::GenerateCombinations(const std::vector<ParameterDef>& params,
                                      std::size_t index,
                                      ParamValueMap current,
                                      std::vector<ParamValueMap>* out) {
    if (out == nullptr) {
        return;
    }
    if (index >= params.size()) {
        out->push_back(std::move(current));
        return;
    }

    const ParameterDef& param = params[index];
    const std::vector<ParamValue> values = BuildValuesForParam(param);
    for (const ParamValue& value : values) {
        ParamValueMap next = current;
        next.values[param.name] = value;
        GenerateCombinations(params, index + 1, std::move(next), out);
    }
}

}  // namespace quant_hft::optim
