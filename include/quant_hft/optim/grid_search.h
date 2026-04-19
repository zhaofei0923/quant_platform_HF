#pragma once

#include <cstddef>
#include <vector>

#include "quant_hft/optim/optimization_algorithm.h"
#include "quant_hft/optim/parameter_space.h"

namespace quant_hft::optim {

class GridSearch : public IOptimizationAlgorithm {
   public:
    void Initialize(const ParameterSpace& space, const OptimizationConfig& config) override;
    std::vector<ParamValueMap> GetNextBatch(int batch_size) override;
    void AddTrialResult(const Trial& trial) override;
    bool IsFinished() const override;
    std::vector<Trial> GetAllTrials() const override;
    Trial GetBestTrial() const override;

   private:
    static std::size_t CountPlannedTrials(const std::vector<std::vector<ParamValue>>& values_by_param,
                                          const OptimizationConfig& config);

    ParamValueMap BuildCurrentCombination() const;
    void AdvanceIndices();

    std::vector<std::string> parameter_names_;
    std::vector<std::vector<ParamValue>> values_by_param_;
    std::vector<std::size_t> current_indices_;
    std::size_t total_trials_{0};
    std::size_t emitted_trials_{0};
    std::vector<Trial> results_;
    OptimizationConfig config_;
};

}  // namespace quant_hft::optim
