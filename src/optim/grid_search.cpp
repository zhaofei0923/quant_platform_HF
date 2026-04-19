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
    results_.clear();
    parameter_names_.clear();
    values_by_param_.clear();
    current_indices_.clear();
    total_trials_ = 0;
    emitted_trials_ = 0;

    parameter_names_.reserve(space.parameters.size());
    values_by_param_.reserve(space.parameters.size());
    for (const ParameterDef& param : space.parameters) {
        std::vector<ParamValue> values = BuildValuesForParam(param);
        if (values.empty()) {
            return;
        }
        parameter_names_.push_back(param.name);
        values_by_param_.push_back(std::move(values));
    }

    current_indices_.assign(values_by_param_.size(), 0);
    total_trials_ = CountPlannedTrials(values_by_param_, config_);
}

std::vector<ParamValueMap> GridSearch::GetNextBatch(int batch_size) {
    if (batch_size <= 0) {
        batch_size = 1;
    }

    const std::size_t remaining = total_trials_ > emitted_trials_ ? total_trials_ - emitted_trials_ : 0;
    const std::size_t count =
        std::min<std::size_t>(static_cast<std::size_t>(batch_size), remaining);

    std::vector<ParamValueMap> batch;
    batch.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        batch.push_back(BuildCurrentCombination());
        ++emitted_trials_;
        if (emitted_trials_ < total_trials_) {
            AdvanceIndices();
        }
    }
    return batch;
}

void GridSearch::AddTrialResult(const Trial& trial) { results_.push_back(trial); }

bool GridSearch::IsFinished() const { return emitted_trials_ >= total_trials_; }

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

std::size_t GridSearch::CountPlannedTrials(const std::vector<std::vector<ParamValue>>& values_by_param,
                                           const OptimizationConfig& config) {
    const bool has_limit = config.max_trials > 0;
    const std::size_t limit = has_limit ? static_cast<std::size_t>(config.max_trials) : 0U;

    std::size_t total = 1;
    for (const auto& values : values_by_param) {
        if (values.empty()) {
            return 0;
        }

        const std::size_t count = values.size();
        if (has_limit && total > limit / count) {
            return limit;
        }
        if (!has_limit && total > std::numeric_limits<std::size_t>::max() / count) {
            throw std::runtime_error(
                "grid search parameter space overflow; set max_trials to cap the search space");
        }
        total *= count;
    }

    if (has_limit) {
        total = std::min(total, limit);
    }
    return total;
}

ParamValueMap GridSearch::BuildCurrentCombination() const {
    ParamValueMap combination;
    combination.values.reserve(parameter_names_.size());

    for (std::size_t i = 0; i < parameter_names_.size(); ++i) {
        combination.values.emplace(parameter_names_[i], values_by_param_[i][current_indices_[i]]);
    }
    return combination;
}

void GridSearch::AdvanceIndices() {
    if (current_indices_.empty()) {
        return;
    }

    for (std::size_t index = current_indices_.size(); index-- > 0;) {
        if (current_indices_[index] + 1 < values_by_param_[index].size()) {
            ++current_indices_[index];
            for (std::size_t reset = index + 1; reset < current_indices_.size(); ++reset) {
                current_indices_[reset] = 0;
            }
            return;
        }
    }
}

}  // namespace quant_hft::optim
