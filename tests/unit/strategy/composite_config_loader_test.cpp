#include "quant_hft/strategy/composite_config_loader.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft {
namespace {

std::filesystem::path WriteTempCompositeConfig(const std::string& content) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_composite_config_loader_test_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

TEST(CompositeConfigLoaderTest, LoadsValidCompositeConfigYamlSubset) {
    const auto path = WriteTempCompositeConfig(
        "composite:\n"
        "  merge_rule: kPriority\n"
        "  opening_strategies:\n"
        "    - id: trend_open\n"
        "      type: TrendOpening\n"
        "      market_regimes: [kStrongTrend, kWeakTrend]\n"
        "      params:\n"
        "        volume: 1\n"
        "        kama_er_period: 10\n"
        "  stop_loss_strategies:\n"
        "    - id: atr_sl\n"
        "      type: ATRStopLoss\n"
        "      params:\n"
        "        atr_period: 14\n"
        "        atr_multiplier: 2.0\n"
        "  time_filters:\n"
        "    - id: night_filter\n"
        "      type: TimeFilter\n"
        "      params:\n"
        "        start_hour: 21\n"
        "        end_hour: 2\n"
        "        timezone: Asia/Shanghai\n");

    CompositeStrategyDefinition definition;
    std::string error;
    EXPECT_TRUE(LoadCompositeStrategyDefinition(path.string(), &definition, &error)) << error;
    EXPECT_EQ(definition.merge_rule, SignalMergeRule::kPriority);
    ASSERT_EQ(definition.opening_strategies.size(), 1U);
    EXPECT_EQ(definition.opening_strategies.front().id, "trend_open");
    EXPECT_EQ(definition.opening_strategies.front().type, "TrendOpening");
    ASSERT_EQ(definition.opening_strategies.front().market_regimes.size(), 2U);
    EXPECT_EQ(definition.opening_strategies.front().market_regimes[0], MarketRegime::kStrongTrend);
    EXPECT_EQ(definition.opening_strategies.front().market_regimes[1], MarketRegime::kWeakTrend);
    ASSERT_EQ(definition.time_filters.size(), 1U);
    EXPECT_EQ(definition.time_filters.front().params.at("timezone"), "Asia/Shanghai");

    std::filesystem::remove(path);
}

TEST(CompositeConfigLoaderTest, RejectsInvalidMergeRuleWithLineNumber) {
    const auto path = WriteTempCompositeConfig(
        "composite:\n"
        "  merge_rule: kUnknown\n");

    CompositeStrategyDefinition definition;
    std::string error;
    EXPECT_FALSE(LoadCompositeStrategyDefinition(path.string(), &definition, &error));
    EXPECT_NE(error.find("line"), std::string::npos);
    EXPECT_NE(error.find("merge_rule"), std::string::npos);

    std::filesystem::remove(path);
}

TEST(CompositeConfigLoaderTest, RejectsUnknownFieldWithLineNumber) {
    const auto path = WriteTempCompositeConfig(
        "composite:\n"
        "  merge_rule: kPriority\n"
        "  unsupported_field: true\n");

    CompositeStrategyDefinition definition;
    std::string error;
    EXPECT_FALSE(LoadCompositeStrategyDefinition(path.string(), &definition, &error));
    EXPECT_NE(error.find("line"), std::string::npos);
    EXPECT_NE(error.find("unsupported_field"), std::string::npos);

    std::filesystem::remove(path);
}

}  // namespace
}  // namespace quant_hft
