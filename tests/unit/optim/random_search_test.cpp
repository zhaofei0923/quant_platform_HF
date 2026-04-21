#include "quant_hft/optim/random_search.h"

#include <gtest/gtest.h>

#include <cmath>
#include <iomanip>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>

namespace quant_hft::optim {
namespace {

ParameterDef MakeDoubleRange(const std::string& name,
                             double min_value,
                             double max_value,
                             double step) {
    ParameterDef def;
    def.name = name;
    def.type = ParameterType::kDouble;
    def.min = min_value;
    def.max = max_value;
    def.step = step;
    return def;
}

ParameterDef MakeIntRange(const std::string& name, int min_value, int max_value, int step) {
    ParameterDef def;
    def.name = name;
    def.type = ParameterType::kInt;
    def.min = min_value;
    def.max = max_value;
    def.step = static_cast<double>(step);
    return def;
}

std::string ParamValueKey(const ParamValue& value) {
    std::ostringstream out;
    std::visit(
        [&out](const auto& typed_value) {
            using ValueType = std::decay_t<decltype(typed_value)>;
            if constexpr (std::is_same_v<ValueType, double>) {
                out << std::setprecision(17) << typed_value;
            } else {
                out << typed_value;
            }
        },
        value);
    return out.str();
}

std::string CombinationKey(const ParamValueMap& params) {
    std::set<std::string> keys;
    for (const auto& [name, value] : params.values) {
        keys.insert(name + "=" + ParamValueKey(value));
    }

    std::ostringstream out;
    for (const std::string& key : keys) {
        out << key << ';';
    }
    return out.str();
}

std::vector<std::string> BatchKeys(const std::vector<ParamValueMap>& batch) {
    std::vector<std::string> keys;
    keys.reserve(batch.size());
    for (const ParamValueMap& params : batch) {
        keys.push_back(CombinationKey(params));
    }
    return keys;
}

TEST(RandomSearchTest, SamplesDistinctValuesFromDiscreteDomains) {
    ParameterSpace space;
    space.parameters.push_back(MakeDoubleRange("ratio", 0.5, 1.0, 0.25));

    ParameterDef mode;
    mode.name = "mode";
    mode.type = ParameterType::kEnum;
    mode.values = {std::string("fast"), std::string("slow")};
    space.parameters.push_back(mode);

    OptimizationConfig config;
    config.max_trials = 4;
    config.random_seed = 123456ULL;

    RandomSearch search;
    search.Initialize(space, config);

    const auto batch = search.GetNextBatch(4);
    ASSERT_EQ(batch.size(), 4U);
    EXPECT_TRUE(search.IsFinished());

    std::set<std::string> unique_keys;
    for (const ParamValueMap& params : batch) {
        unique_keys.insert(CombinationKey(params));
        const double ratio = std::get<double>(params.values.at("ratio"));
        const std::string mode_value = std::get<std::string>(params.values.at("mode"));
        EXPECT_TRUE(std::fabs(ratio - 0.5) < 1e-9 || std::fabs(ratio - 0.75) < 1e-9 ||
                    std::fabs(ratio - 1.0) < 1e-9);
        EXPECT_TRUE(mode_value == "fast" || mode_value == "slow");
    }
    EXPECT_EQ(unique_keys.size(), batch.size());
}

TEST(RandomSearchTest, TruncatesWhenRequestedTrialsExceedSearchSpace) {
    ParameterSpace space;
    space.parameters.push_back(MakeIntRange("fast", 1, 3, 1));

    ParameterDef mode;
    mode.name = "mode";
    mode.type = ParameterType::kEnum;
    mode.values = {std::string("x"), std::string("y")};
    space.parameters.push_back(mode);

    OptimizationConfig config;
    config.max_trials = 20;
    config.random_seed = 42ULL;

    RandomSearch search;
    search.Initialize(space, config);

    const auto batch = search.GetNextBatch(20);
    EXPECT_EQ(batch.size(), 6U);
    EXPECT_TRUE(search.IsFinished());

    const auto keys = BatchKeys(batch);
    std::set<std::string> unique_keys(keys.begin(), keys.end());
    EXPECT_EQ(unique_keys.size(), batch.size());
}

TEST(RandomSearchTest, ProducesDeterministicSequenceForSameSeed) {
    ParameterSpace space;
    space.parameters.push_back(MakeIntRange("fast", 1, 4, 1));

    ParameterDef mode;
    mode.name = "mode";
    mode.type = ParameterType::kEnum;
    mode.values = {std::string("x"), std::string("y")};
    space.parameters.push_back(mode);

    OptimizationConfig config;
    config.max_trials = 5;
    config.random_seed = 20260420ULL;

    RandomSearch first;
    first.Initialize(space, config);
    const auto first_batch = first.GetNextBatch(5);

    RandomSearch second;
    second.Initialize(space, config);
    const auto second_batch = second.GetNextBatch(5);

    EXPECT_EQ(BatchKeys(first_batch), BatchKeys(second_batch));
}

TEST(RandomSearchTest, SelectsBestCompletedTrialByDirection) {
    ParameterSpace space;
    space.parameters.push_back(MakeIntRange("a", 1, 2, 1));

    OptimizationConfig config;
    config.maximize = true;
    config.max_trials = 2;
    config.random_seed = 9ULL;

    RandomSearch search;
    search.Initialize(space, config);

    Trial low;
    low.trial_id = "low";
    low.status = "completed";
    low.objective = 1.0;

    Trial failed;
    failed.trial_id = "failed";
    failed.status = "failed";
    failed.objective = 100.0;

    Trial high;
    high.trial_id = "high";
    high.status = "completed";
    high.objective = 3.0;

    search.AddTrialResult(low);
    search.AddTrialResult(failed);
    search.AddTrialResult(high);
    EXPECT_EQ(search.GetBestTrial().trial_id, "high");
}

}  // namespace
}  // namespace quant_hft::optim