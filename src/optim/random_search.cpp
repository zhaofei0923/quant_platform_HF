#include "quant_hft/optim/random_search.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <limits>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <type_traits>
#include <unordered_set>
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
        throw std::runtime_error("random search step must be > 0");
    }

    if (param.type == ParameterType::kInt) {
        const int min_value = std::get<int>(param.min.value());
        const int max_value = std::get<int>(param.max.value());
        const int step_int = static_cast<int>(std::round(step));
        if (step_int <= 0) {
            throw std::runtime_error("random search int step must be > 0");
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

std::optional<std::size_t> CountTotalCombinations(
    const std::vector<std::vector<ParamValue>>& values_by_param) {
    std::size_t total = 1;
    for (const auto& values : values_by_param) {
        if (values.empty()) {
            return 0U;
        }
        if (total > std::numeric_limits<std::size_t>::max() / values.size()) {
            return std::nullopt;
        }
        total *= values.size();
    }
    return total;
}

std::string ParamValueKey(const ParamValue& value) {
    std::ostringstream out;
    std::visit(
        [&out](const auto& typed_value) {
            using ValueType = std::decay_t<decltype(typed_value)>;
            if constexpr (std::is_same_v<ValueType, double>) {
                out << std::setprecision(17) << typed_value;
            } else {
                out << typed_value;
            }
        },
        value);
    return out.str();
}

std::string BuildCombinationKey(const std::vector<std::string>& parameter_names,
                                const ParamValueMap& combination) {
    std::ostringstream out;
    for (const std::string& name : parameter_names) {
        const auto it = combination.values.find(name);
        if (it == combination.values.end()) {
            continue;
        }
        out << name << '=' << ParamValueKey(it->second) << ';';
    }
    return out.str();
}

ParamValueMap BuildCombinationFromIndex(const std::vector<std::string>& parameter_names,
                                        const std::vector<std::vector<ParamValue>>& values_by_param,
                                        std::size_t linear_index) {
    ParamValueMap combination;
    combination.values.reserve(parameter_names.size());

    std::vector<std::size_t> indices(parameter_names.size(), 0);
    for (std::size_t i = parameter_names.size(); i-- > 0;) {
        const std::size_t bucket_size = values_by_param[i].size();
        indices[i] = linear_index % bucket_size;
        linear_index /= bucket_size;
    }

    for (std::size_t i = 0; i < parameter_names.size(); ++i) {
        combination.values.emplace(parameter_names[i], values_by_param[i][indices[i]]);
    }
    return combination;
}

ParamValueMap BuildRandomCombination(const std::vector<std::string>& parameter_names,
                                     const std::vector<std::vector<ParamValue>>& values_by_param,
                                     std::mt19937_64* rng) {
    ParamValueMap combination;
    combination.values.reserve(parameter_names.size());
    for (std::size_t i = 0; i < parameter_names.size(); ++i) {
        std::uniform_int_distribution<std::size_t> distribution(0, values_by_param[i].size() - 1);
        const std::size_t index = distribution(*rng);
        combination.values.emplace(parameter_names[i], values_by_param[i][index]);
    }
    return combination;
}

std::uint64_t DefaultRandomSeed() {
    return static_cast<std::uint64_t>(
        std::chrono::time_point_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now())
            .time_since_epoch()
            .count());
}

}  // namespace

void RandomSearch::Initialize(const ParameterSpace& space, const OptimizationConfig& config) {
    config_ = config;
    results_.clear();
    planned_combinations_.clear();
    emitted_trials_ = 0;
    rng_seed_ = config.random_seed.value_or(DefaultRandomSeed());

    std::vector<std::string> parameter_names;
    std::vector<std::vector<ParamValue>> values_by_param;
    parameter_names.reserve(space.parameters.size());
    values_by_param.reserve(space.parameters.size());
    for (const ParameterDef& param : space.parameters) {
        std::vector<ParamValue> values = BuildValuesForParam(param);
        if (values.empty()) {
            return;
        }
        parameter_names.push_back(param.name);
        values_by_param.push_back(std::move(values));
    }

    std::mt19937_64 rng(rng_seed_);
    const auto total_combinations = CountTotalCombinations(values_by_param);
    std::size_t planned_trials = static_cast<std::size_t>(config.max_trials);
    if (total_combinations.has_value()) {
        planned_trials = std::min(planned_trials, *total_combinations);
    }

    planned_combinations_.reserve(planned_trials);
    if (planned_trials == 0) {
        return;
    }

    if (total_combinations.has_value()) {
        if (*total_combinations <= planned_trials) {
            std::vector<std::size_t> indices(*total_combinations);
            std::iota(indices.begin(), indices.end(), 0U);
            std::shuffle(indices.begin(), indices.end(), rng);
            for (std::size_t index : indices) {
                planned_combinations_.push_back(
                    BuildCombinationFromIndex(parameter_names, values_by_param, index));
            }
            return;
        }

        std::unordered_set<std::size_t> sampled_indices;
        sampled_indices.reserve(planned_trials * 2U);
        std::uniform_int_distribution<std::size_t> distribution(0, *total_combinations - 1);
        while (planned_combinations_.size() < planned_trials) {
            const std::size_t index = distribution(rng);
            if (!sampled_indices.insert(index).second) {
                continue;
            }
            planned_combinations_.push_back(
                BuildCombinationFromIndex(parameter_names, values_by_param, index));
        }
        return;
    }

    std::unordered_set<std::string> seen_combinations;
    seen_combinations.reserve(planned_trials * 2U);
    while (planned_combinations_.size() < planned_trials) {
        ParamValueMap candidate = BuildRandomCombination(parameter_names, values_by_param, &rng);
        const std::string key = BuildCombinationKey(parameter_names, candidate);
        if (!seen_combinations.insert(key).second) {
            continue;
        }
        planned_combinations_.push_back(std::move(candidate));
    }
}

std::vector<ParamValueMap> RandomSearch::GetNextBatch(int batch_size) {
    if (batch_size <= 0) {
        batch_size = 1;
    }

    const std::size_t remaining = planned_combinations_.size() > emitted_trials_
                                      ? planned_combinations_.size() - emitted_trials_
                                      : 0U;
    const std::size_t count =
        std::min<std::size_t>(static_cast<std::size_t>(batch_size), remaining);

    std::vector<ParamValueMap> batch;
    batch.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        batch.push_back(planned_combinations_[emitted_trials_]);
        ++emitted_trials_;
    }
    return batch;
}

void RandomSearch::AddTrialResult(const Trial& trial) { results_.push_back(trial); }

bool RandomSearch::IsFinished() const { return emitted_trials_ >= planned_combinations_.size(); }

std::vector<Trial> RandomSearch::GetAllTrials() const { return results_; }

Trial RandomSearch::GetBestTrial() const {
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

}  // namespace quant_hft::optim
