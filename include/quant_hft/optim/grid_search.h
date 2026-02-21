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
    static void GenerateCombinations(const std::vector<ParameterDef>& params,
                                     std::size_t index,
                                     ParamValueMap current,
                                     std::vector<ParamValueMap>* out);

    std::vector<ParamValueMap> all_combinations_;
    std::size_t next_index_{0};
    std::vector<Trial> results_;
    OptimizationConfig config_;
};

}  // namespace quant_hft::optim
