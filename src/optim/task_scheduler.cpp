#include "quant_hft/optim/task_scheduler.h"

#include <algorithm>
#include <deque>
#include <future>
#include <utility>
#include <vector>

namespace quant_hft::optim {
namespace {

struct ActiveTask {
    std::size_t index{0};
    std::future<Trial> future;
};

Trial ExecuteTaskSafely(const TaskScheduler::TaskFunc& task,
                        const ParamValueMap& params,
                        std::size_t index) {
    Trial trial;
    trial.trial_id = "trial_" + std::to_string(index);
    try {
        trial = task(params);
    } catch (const std::exception& ex) {
        trial.status = "failed";
        trial.error_msg = ex.what();
    } catch (...) {
        trial.status = "failed";
        trial.error_msg = "unknown task failure";
    }
    return trial;
}

Trial GetFutureResult(std::future<Trial>* future, std::size_t index) {
    Trial trial;
    trial.trial_id = "trial_" + std::to_string(index);
    try {
        trial = future->get();
    } catch (const std::exception& ex) {
        trial.status = "failed";
        trial.error_msg = ex.what();
    } catch (...) {
        trial.status = "failed";
        trial.error_msg = "unknown task failure";
    }
    return trial;
}

}  // namespace

TaskScheduler::TaskScheduler(int max_concurrent) {
    max_concurrent_ = std::max(1, max_concurrent);
}

std::vector<Trial> TaskScheduler::RunBatch(const std::vector<ParamValueMap>& params_batch,
                                           const TaskFunc& task) const {
    const std::size_t limit = static_cast<std::size_t>(std::max(1, max_concurrent_));

    if (limit == 1) {
        std::vector<Trial> results;
        results.reserve(params_batch.size());
        for (std::size_t index = 0; index < params_batch.size(); ++index) {
            results.push_back(ExecuteTaskSafely(task, params_batch[index], index));
        }
        return results;
    }

    std::deque<ActiveTask> active;

    std::vector<std::pair<std::size_t, Trial>> ordered_results;
    ordered_results.reserve(params_batch.size());

    for (std::size_t index = 0; index < params_batch.size(); ++index) {
        if (active.size() >= limit) {
            Trial completed = GetFutureResult(&active.front().future, active.front().index);
            ordered_results.emplace_back(active.front().index, std::move(completed));
            active.pop_front();
        }

        ActiveTask launched;
        launched.index = index;
        launched.future = std::async(std::launch::async, task, params_batch[index]);
        active.push_back(std::move(launched));
    }

    for (ActiveTask& task_item : active) {
        Trial completed = GetFutureResult(&task_item.future, task_item.index);
        ordered_results.emplace_back(task_item.index, std::move(completed));
    }

    std::sort(ordered_results.begin(), ordered_results.end(), [](const auto& left, const auto& right) {
        return left.first < right.first;
    });

    std::vector<Trial> results;
    results.reserve(ordered_results.size());
    for (auto& [index, trial] : ordered_results) {
        (void)index;
        results.push_back(std::move(trial));
    }
    return results;
}

}  // namespace quant_hft::optim
