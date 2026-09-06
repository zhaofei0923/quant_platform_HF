#pragma once

#include <cstdint>
#include <functional>
#include <vector>

#include "quant_hft/optim/optimization_algorithm.h"

namespace quant_hft::optim {

class TaskScheduler {
   public:
    using TaskFunc = std::function<Trial(const ParamValueMap&)>;

    explicit TaskScheduler(int max_concurrent, std::int64_t memory_budget_mb = 0,
                           std::int64_t per_task_memory_mb = 0);

    int max_concurrent() const { return max_concurrent_; }

    std::vector<Trial> RunBatch(const std::vector<ParamValueMap>& params_batch,
                                const TaskFunc& task) const;

   private:
    int max_concurrent_{1};
};

}  // namespace quant_hft::optim
