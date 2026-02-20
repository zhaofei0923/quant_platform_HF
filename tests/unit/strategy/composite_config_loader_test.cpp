#include "quant_hft/strategy/composite_config_loader.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft {
namespace {

std::filesystem::path WriteTempFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

std::filesystem::path MakeTempDir(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp));
    std::filesystem::create_directories(path);
    return path;
}

TEST(CompositeConfigLoaderTest, LoadsV2YamlSubStrategiesWithEnabledAndRegimeGate) {
    const std::filesystem::path root = MakeTempDir("quant_hft_composite_v2_yaml");
    WriteTempFile(root / "sub" / "kama.yaml",
                  "params:\n"
                  "  id: kama_1\n"
                  "  er_period: 10\n"
                  "  fast_period: 2\n"
                  "  slow_period: 30\n");
    WriteTempFile(root / "sub" / "trend.yaml",
                  "params:\n"
                  "  id: trend_1\n"
                  "  er_period: 10\n"
                  "  fast_period: 2\n"
                  "  slow_period: 30\n");

    const std::filesystem::path path =
        WriteTempFile(root / "composite.yaml",
                      "composite:\n"
                      "  merge_rule: kPriority\n"
                      "  sub_strategies:\n"
                      "    - id: kama_1\n"
                      "      enabled: true\n"
                      "      type: KamaTrendStrategy\n"
                      "      config_path: ./sub/kama.yaml\n"
                      "      entry_market_regimes: [kStrongTrend, kWeakTrend]\n"
                      "    - id: trend_1\n"
                      "      enabled: false\n"
                      "      type: TrendStrategy\n"
                      "      config_path: ./sub/trend.yaml\n");

    CompositeStrategyDefinition definition;
    std::string error;
    ASSERT_TRUE(LoadCompositeStrategyDefinition(path.string(), &definition, &error)) << error;
    ASSERT_EQ(definition.sub_strategies.size(), 2U);
    EXPECT_EQ(definition.sub_strategies[0].id, "kama_1");
    EXPECT_TRUE(definition.sub_strategies[0].enabled);
    EXPECT_EQ(definition.sub_strategies[0].type, "KamaTrendStrategy");
    ASSERT_EQ(definition.sub_strategies[0].entry_market_regimes.size(), 2U);
    EXPECT_EQ(definition.sub_strategies[0].entry_market_regimes[0], MarketRegime::kStrongTrend);
    EXPECT_EQ(definition.sub_strategies[0].entry_market_regimes[1], MarketRegime::kWeakTrend);
    EXPECT_EQ(definition.sub_strategies[1].id, "trend_1");
    EXPECT_FALSE(definition.sub_strategies[1].enabled);
    EXPECT_EQ(definition.sub_strategies[1].type, "TrendStrategy");
    EXPECT_EQ(definition.sub_strategies[1].params.at("er_period"), "10");

    std::filesystem::remove_all(root);
}

TEST(CompositeConfigLoaderTest, RejectsLegacyStrategySections) {
    const std::filesystem::path root = MakeTempDir("quant_hft_composite_v2_legacy_section");
    const std::filesystem::path path =
        WriteTempFile(root / "legacy.yaml",
                      "composite:\n"
                      "  merge_rule: kPriority\n"
                      "  opening_strategies:\n"
                      "    - id: legacy\n"
                      "      type: TrendOpening\n");

    CompositeStrategyDefinition definition;
    std::string error;
    EXPECT_FALSE(LoadCompositeStrategyDefinition(path.string(), &definition, &error));
    EXPECT_NE(error.find("opening_strategies"), std::string::npos);
    EXPECT_NE(error.find("sub_strategies"), std::string::npos);

    std::filesystem::remove_all(root);
}

TEST(CompositeConfigLoaderTest, RejectsLegacyMarketRegimesField) {
    const std::filesystem::path root = MakeTempDir("quant_hft_composite_v2_legacy_regimes");
    const std::filesystem::path path =
        WriteTempFile(root / "legacy_regime.yaml",
                      "composite:\n"
                      "  merge_rule: kPriority\n"
                      "  sub_strategies:\n"
                      "    - id: kama_1\n"
                      "      enabled: true\n"
                      "      type: KamaTrendStrategy\n"
                      "      params:\n"
                      "        id: kama_1\n"
                      "      market_regimes: [kStrongTrend]\n");

    CompositeStrategyDefinition definition;
    std::string error;
    EXPECT_FALSE(LoadCompositeStrategyDefinition(path.string(), &definition, &error));
    EXPECT_NE(error.find("market_regimes"), std::string::npos);
    EXPECT_NE(error.find("entry_market_regimes"), std::string::npos);

    std::filesystem::remove_all(root);
}

TEST(CompositeConfigLoaderTest, RejectsLegacyStrategyTypeNames) {
    const std::filesystem::path root = MakeTempDir("quant_hft_composite_v2_legacy_type");
    const std::filesystem::path path =
        WriteTempFile(root / "legacy_type.yaml",
                      "composite:\n"
                      "  merge_rule: kPriority\n"
                      "  sub_strategies:\n"
                      "    - id: kama_1\n"
                      "      enabled: true\n"
                      "      type: KamaTrendOpening\n"
                      "      params:\n"
                      "        id: kama_1\n");

    CompositeStrategyDefinition definition;
    std::string error;
    EXPECT_FALSE(LoadCompositeStrategyDefinition(path.string(), &definition, &error));
    EXPECT_NE(error.find("legacy strategy type"), std::string::npos);
    EXPECT_NE(error.find("KamaTrendStrategy"), std::string::npos);

    std::filesystem::remove_all(root);
}

TEST(CompositeConfigLoaderTest, LoadsV2JsonCompositeConfig) {
    const std::filesystem::path root = MakeTempDir("quant_hft_composite_v2_json");
    WriteTempFile(root / "sub" / "kama.json",
                  "{\n"
                  "  \"params\": {\n"
                  "    \"id\": \"kama_1\",\n"
                  "    \"er_period\": \"10\",\n"
                  "    \"fast_period\": \"2\",\n"
                  "    \"slow_period\": \"30\"\n"
                  "  }\n"
                  "}\n");
    const std::filesystem::path path =
        WriteTempFile(root / "composite.json",
                      "{\n"
                      "  \"composite\": {\n"
                      "    \"merge_rule\": \"kPriority\",\n"
                      "    \"sub_strategies\": [\n"
                      "      {\n"
                      "        \"id\": \"kama_1\",\n"
                      "        \"enabled\": true,\n"
                      "        \"type\": \"KamaTrendStrategy\",\n"
                      "        \"config_path\": \"./sub/kama.json\",\n"
                      "        \"entry_market_regimes\": [\"kStrongTrend\", \"kWeakTrend\"]\n"
                      "      }\n"
                      "    ]\n"
                      "  }\n"
                      "}\n");

    CompositeStrategyDefinition definition;
    std::string error;
    ASSERT_TRUE(LoadCompositeStrategyDefinition(path.string(), &definition, &error)) << error;
    ASSERT_EQ(definition.sub_strategies.size(), 1U);
    EXPECT_EQ(definition.sub_strategies[0].id, "kama_1");
    EXPECT_TRUE(definition.sub_strategies[0].enabled);
    EXPECT_EQ(definition.sub_strategies[0].type, "KamaTrendStrategy");
    ASSERT_EQ(definition.sub_strategies[0].entry_market_regimes.size(), 2U);
    EXPECT_EQ(definition.sub_strategies[0].params.at("er_period"), "10");

    std::filesystem::remove_all(root);
}

TEST(CompositeConfigLoaderTest, LoadsEnableNonBacktestAndOverridesFromYaml) {
    const std::filesystem::path root = MakeTempDir("quant_hft_composite_v2_yaml_overrides");
    WriteTempFile(root / "sub" / "kama.yaml",
                  "params:\n"
                  "  id: kama_1\n"
                  "  er_period: 10\n"
                  "  fast_period: 2\n"
                  "  slow_period: 30\n");

    const std::filesystem::path path =
        WriteTempFile(root / "composite.yaml",
                      "composite:\n"
                      "  merge_rule: kPriority\n"
                      "  enable_non_backtest: true\n"
                      "  sub_strategies:\n"
                      "    - id: kama_1\n"
                      "      enabled: true\n"
                      "      type: KamaTrendStrategy\n"
                      "      config_path: ./sub/kama.yaml\n"
                      "      overrides:\n"
                      "        backtest:\n"
                      "          params:\n"
                      "            take_profit_atr_multiplier: 20.0\n"
                      "        sim:\n"
                      "          params:\n"
                      "            default_volume: 2\n");

    CompositeStrategyDefinition definition;
    std::string error;
    ASSERT_TRUE(LoadCompositeStrategyDefinition(path.string(), &definition, &error)) << error;
    EXPECT_TRUE(definition.enable_non_backtest);
    ASSERT_EQ(definition.sub_strategies.size(), 1U);
    EXPECT_EQ(definition.sub_strategies[0].overrides.backtest_params.at("take_profit_atr_multiplier"),
              "20.0");
    EXPECT_EQ(definition.sub_strategies[0].overrides.sim_params.at("default_volume"), "2");
    EXPECT_TRUE(definition.sub_strategies[0].overrides.live_params.empty());

    std::filesystem::remove_all(root);
}

TEST(CompositeConfigLoaderTest, LoadsOverridesFromJson) {
    const std::filesystem::path root = MakeTempDir("quant_hft_composite_v2_json_overrides");
    WriteTempFile(root / "sub" / "trend.json",
                  "{\n"
                  "  \"params\": {\n"
                  "    \"id\": \"trend_1\",\n"
                  "    \"er_period\": \"10\",\n"
                  "    \"fast_period\": \"2\",\n"
                  "    \"slow_period\": \"30\"\n"
                  "  }\n"
                  "}\n");
    const std::filesystem::path path =
        WriteTempFile(root / "composite.json",
                      "{\n"
                      "  \"composite\": {\n"
                      "    \"merge_rule\": \"kPriority\",\n"
                      "    \"enable_non_backtest\": true,\n"
                      "    \"sub_strategies\": [\n"
                      "      {\n"
                      "        \"id\": \"trend_1\",\n"
                      "        \"enabled\": true,\n"
                      "        \"type\": \"TrendStrategy\",\n"
                      "        \"config_path\": \"./sub/trend.json\",\n"
                      "        \"overrides\": {\n"
                      "          \"live\": {\n"
                      "            \"params\": {\n"
                      "              \"risk_per_trade_pct\": \"0.02\"\n"
                      "            }\n"
                      "          }\n"
                      "        }\n"
                      "      }\n"
                      "    ]\n"
                      "  }\n"
                      "}\n");

    CompositeStrategyDefinition definition;
    std::string error;
    ASSERT_TRUE(LoadCompositeStrategyDefinition(path.string(), &definition, &error)) << error;
    EXPECT_TRUE(definition.enable_non_backtest);
    ASSERT_EQ(definition.sub_strategies.size(), 1U);
    EXPECT_EQ(definition.sub_strategies[0].overrides.live_params.at("risk_per_trade_pct"), "0.02");
    EXPECT_TRUE(definition.sub_strategies[0].overrides.backtest_params.empty());
    EXPECT_TRUE(definition.sub_strategies[0].overrides.sim_params.empty());

    std::filesystem::remove_all(root);
}

TEST(CompositeConfigLoaderTest, RejectsUnsupportedOverridesRunModeKey) {
    const std::filesystem::path root = MakeTempDir("quant_hft_composite_v2_override_invalid");
    const std::filesystem::path path =
        WriteTempFile(root / "invalid_overrides.yaml",
                      "composite:\n"
                      "  merge_rule: kPriority\n"
                      "  sub_strategies:\n"
                      "    - id: kama_1\n"
                      "      enabled: true\n"
                      "      type: KamaTrendStrategy\n"
                      "      params:\n"
                      "        id: kama_1\n"
                      "      overrides:\n"
                      "        paper:\n"
                      "          params:\n"
                      "            risk_per_trade_pct: 0.02\n");

    CompositeStrategyDefinition definition;
    std::string error;
    EXPECT_FALSE(LoadCompositeStrategyDefinition(path.string(), &definition, &error));
    EXPECT_NE(error.find("backtest|sim|live"), std::string::npos);

    std::filesystem::remove_all(root);
}

}  // namespace
}  // namespace quant_hft
