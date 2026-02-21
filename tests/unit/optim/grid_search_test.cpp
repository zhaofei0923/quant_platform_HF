#include "quant_hft/optim/grid_search.h"

#include <gtest/gtest.h>

namespace quant_hft::optim {
namespace {

ParameterDef MakeIntRange(const std::string& name, int min_value, int max_value, int step) {
    ParameterDef def;
    def.name = name;
    def.type = ParameterType::kInt;
    def.min = min_value;
    def.max = max_value;
    def.step = static_cast<double>(step);
    return def;
}

TEST(GridSearchTest, GeneratesCartesianProductWithMaxTrialsCutoff) {
    ParameterSpace space;
    space.parameters.push_back(MakeIntRange("a", 1, 3, 1));

    ParameterDef enum_param;
    enum_param.name = "b";
    enum_param.type = ParameterType::kEnum;
    enum_param.values = {std::string("x"), std::string("y")};
    space.parameters.push_back(enum_param);

    OptimizationConfig config;
    config.maximize = true;
    config.max_trials = 4;

    GridSearch search;
    search.Initialize(space, config);

    auto first = search.GetNextBatch(2);
    EXPECT_EQ(first.size(), 2U);
    EXPECT_FALSE(search.IsFinished());

    auto second = search.GetNextBatch(10);
    EXPECT_EQ(second.size(), 2U);
    EXPECT_TRUE(search.IsFinished());
}

TEST(GridSearchTest, SelectsBestTrialByDirection) {
    ParameterSpace space;
    space.parameters.push_back(MakeIntRange("a", 1, 2, 1));

    OptimizationConfig config;
    config.maximize = true;

    GridSearch search;
    search.Initialize(space, config);

    Trial low;
    low.trial_id = "low";
    low.status = "completed";
    low.objective = 1.5;

    Trial high;
    high.trial_id = "high";
    high.status = "completed";
    high.objective = 3.0;

    search.AddTrialResult(low);
    search.AddTrialResult(high);
    EXPECT_EQ(search.GetBestTrial().trial_id, "high");

    OptimizationConfig min_config;
    min_config.maximize = false;
    GridSearch min_search;
    min_search.Initialize(space, min_config);
    min_search.AddTrialResult(low);
    min_search.AddTrialResult(high);
    EXPECT_EQ(min_search.GetBestTrial().trial_id, "low");
}

}  // namespace
}  // namespace quant_hft::optim
