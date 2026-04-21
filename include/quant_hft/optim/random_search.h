#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "quant_hft/optim/optimization_algorithm.h"
#include "quant_hft/optim/parameter_space.h"

namespace quant_hft::optim {

class RandomSearch : public IOptimizationAlgorithm {
   public:
    void Initialize(const ParameterSpace& space, const OptimizationConfig& config) override;
    std::vector<ParamValueMap> GetNextBatch(int batch_size) override;
    void AddTrialResult(const Trial& trial) override;
    bool IsFinished() const override;
    std::vector<Trial> GetAllTrials() const override;
    Trial GetBestTrial() const override;

   private:
    std::vector<ParamValueMap> planned_combinations_;
    std::size_t emitted_trials_{0};
    std::vector<Trial> results_;
    OptimizationConfig config_;
    std::uint64_t rng_seed_{0};
};

}  // namespace quant_hft::optim
