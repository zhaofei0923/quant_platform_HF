#pragma once

#include <functional>
#include <vector>

#include "quant_hft/optim/optimization_algorithm.h"

namespace quant_hft::optim {

class TaskScheduler {
   public:
    using TaskFunc = std::function<Trial(const ParamValueMap&)>;

    explicit TaskScheduler(int max_concurrent);

    int max_concurrent() const { return max_concurrent_; }

    std::vector<Trial> RunBatch(const std::vector<ParamValueMap>& params_batch,
                                const TaskFunc& task) const;

   private:
    int max_concurrent_{1};
};

}  // namespace quant_hft::optim
