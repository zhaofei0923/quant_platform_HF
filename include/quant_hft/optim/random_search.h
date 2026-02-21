#pragma once

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
    std::vector<Trial> results_;
};

}  // namespace quant_hft::optim
