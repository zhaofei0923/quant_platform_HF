#include "quant_hft/optim/task_scheduler.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

namespace quant_hft::optim {
namespace {

TEST(TaskSchedulerTest, RespectsMaxConcurrency) {
    TaskScheduler scheduler(2);

    std::vector<ParamValueMap> batch;
    for (int i = 0; i < 6; ++i) {
        ParamValueMap params;
        params.values["id"] = i;
        batch.push_back(params);
    }

    std::atomic<int> active{0};
    std::atomic<int> peak{0};

    const auto task = [&](const ParamValueMap& params) {
        Trial trial;
        trial.status = "completed";
        trial.trial_id = "t" + std::to_string(std::get<int>(params.values.at("id")));

        const int now = active.fetch_add(1) + 1;
        int expected = peak.load();
        while (now > expected && !peak.compare_exchange_weak(expected, now)) {
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        active.fetch_sub(1);
        return trial;
    };

    const std::vector<Trial> results = scheduler.RunBatch(batch, task);
    EXPECT_EQ(results.size(), batch.size());
    EXPECT_LE(peak.load(), 2);
}

TEST(TaskSchedulerTest, CapturesTaskExceptionsAsFailedTrials) {
    TaskScheduler scheduler(2);

    ParamValueMap ok;
    ok.values["id"] = 1;

    ParamValueMap bad;
    bad.values["id"] = 2;

    std::vector<ParamValueMap> batch{ok, bad};

    const auto task = [](const ParamValueMap& params) {
        const int id = std::get<int>(params.values.at("id"));
        if (id == 2) {
            throw std::runtime_error("boom");
        }
        Trial trial;
        trial.trial_id = "ok";
        trial.status = "completed";
        trial.objective = 1.0;
        return trial;
    };

    const std::vector<Trial> results = scheduler.RunBatch(batch, task);
    ASSERT_EQ(results.size(), 2U);
    EXPECT_EQ(results[0].status, "completed");
    EXPECT_EQ(results[1].status, "failed");
    EXPECT_NE(results[1].error_msg.find("boom"), std::string::npos);
}

}  // namespace
}  // namespace quant_hft::optim
