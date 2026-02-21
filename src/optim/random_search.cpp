#include "quant_hft/optim/random_search.h"

#include <stdexcept>

namespace quant_hft::optim {

void RandomSearch::Initialize(const ParameterSpace& /*space*/, const OptimizationConfig& /*config*/) {
    throw std::runtime_error("random search is not implemented in V1");
}

std::vector<ParamValueMap> RandomSearch::GetNextBatch(int /*batch_size*/) { return {}; }

void RandomSearch::AddTrialResult(const Trial& trial) { results_.push_back(trial); }

bool RandomSearch::IsFinished() const { return true; }

std::vector<Trial> RandomSearch::GetAllTrials() const { return results_; }

Trial RandomSearch::GetBestTrial() const {
    Trial trial;
    trial.status = "failed";
    trial.error_msg = "random search is not implemented in V1";
    return trial;
}

}  // namespace quant_hft::optim
