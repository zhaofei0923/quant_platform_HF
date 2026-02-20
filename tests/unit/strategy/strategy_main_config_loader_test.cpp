#include "quant_hft/strategy/strategy_main_config_loader.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft {
namespace {

std::filesystem::path MakeTempDir(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp));
    std::filesystem::create_directories(path);
    return path;
}

std::filesystem::path WriteFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

TEST(StrategyMainConfigLoaderTest, LoadsYamlMainConfigWithV2SubStrategies) {
    const std::filesystem::path root = MakeTempDir("quant_hft_strategy_main_v2_yaml");
    WriteFile(root / "sub" / "kama.yaml",
              "params:\n"
              "  id: kama_1\n"
              "  er_period: 10\n"
              "  fast_period: 2\n"
              "  slow_period: 30\n");
    WriteFile(root / "sub" / "trend.yaml",
              "params:\n"
              "  id: trend_1\n"
              "  er_period: 10\n"
              "  fast_period: 2\n"
              "  slow_period: 30\n");

    const std::filesystem::path main_path =
        WriteFile(root / "main_strategy.yaml",
                  "run_type: backtest\n"
                  "market_state_mode: true\n"
                  "backtest:\n"
                  "  initial_equity: 1000000\n"
                  "  symbols: [rb2405, ag2406]\n"
                  "  start_date: 20240101\n"
                  "  end_date: 20240131\n"
                  "  product_config_path: ./instrument_info.json\n"
                  "composite:\n"
                  "  merge_rule: kPriority\n"
                  "  sub_strategies:\n"
                  "    - id: kama_1\n"
                  "      enabled: true\n"
                  "      type: KamaTrendStrategy\n"
                  "      config_path: ./sub/kama.yaml\n"
                  "      entry_market_regimes: [kStrongTrend]\n"
                  "    - id: trend_1\n"
                  "      enabled: false\n"
                  "      type: TrendStrategy\n"
                  "      config_path: ./sub/trend.yaml\n");

    StrategyMainConfig config;
    std::string error;
    ASSERT_TRUE(LoadStrategyMainConfig(main_path.string(), &config, &error)) << error;
    EXPECT_EQ(config.run_type, "backtest");
    EXPECT_TRUE(config.market_state_mode);
    EXPECT_DOUBLE_EQ(config.backtest.initial_equity, 1000000.0);
    ASSERT_EQ(config.backtest.symbols.size(), 2U);
    EXPECT_EQ(config.backtest.symbols[0], "rb2405");
    EXPECT_EQ(config.backtest.symbols[1], "ag2406");
    EXPECT_EQ(config.backtest.start_date, "20240101");
    EXPECT_EQ(config.backtest.end_date, "20240131");
    EXPECT_NE(config.backtest.product_config_path.find("instrument_info.json"), std::string::npos);

    EXPECT_EQ(config.composite.run_type, "backtest");
    EXPECT_TRUE(config.composite.market_state_mode);
    ASSERT_EQ(config.composite.sub_strategies.size(), 2U);
    EXPECT_EQ(config.composite.sub_strategies[0].id, "kama_1");
    EXPECT_TRUE(config.composite.sub_strategies[0].enabled);
    EXPECT_EQ(config.composite.sub_strategies[0].type, "KamaTrendStrategy");
    EXPECT_EQ(config.composite.sub_strategies[0].params.at("er_period"), "10");
    EXPECT_EQ(config.composite.sub_strategies[1].id, "trend_1");
    EXPECT_FALSE(config.composite.sub_strategies[1].enabled);
    EXPECT_EQ(config.composite.sub_strategies[1].type, "TrendStrategy");
}

TEST(StrategyMainConfigLoaderTest, RejectsBacktestMaxLossPercentField) {
    const std::filesystem::path root = MakeTempDir("quant_hft_strategy_main_v2_max_loss");
    const std::filesystem::path main_path = WriteFile(root / "main_strategy.yaml",
                                                      "run_type: backtest\n"
                                                      "market_state_mode: true\n"
                                                      "backtest:\n"
                                                      "  initial_equity: 100000\n"
                                                      "  max_loss_percent: 0.01\n"
                                                      "composite:\n"
                                                      "  merge_rule: kPriority\n");

    StrategyMainConfig config;
    std::string error;
    EXPECT_FALSE(LoadStrategyMainConfig(main_path.string(), &config, &error));
    EXPECT_NE(error.find("max_loss_percent"), std::string::npos);
    EXPECT_NE(error.find("risk_per_trade_pct"), std::string::npos);
}

TEST(StrategyMainConfigLoaderTest, RejectsLegacyCompositeSections) {
    const std::filesystem::path root = MakeTempDir("quant_hft_strategy_main_v2_legacy");
    const std::filesystem::path main_path = WriteFile(root / "main_strategy.yaml",
                                                      "run_type: backtest\n"
                                                      "market_state_mode: true\n"
                                                      "backtest:\n"
                                                      "  initial_equity: 100000\n"
                                                      "composite:\n"
                                                      "  merge_rule: kPriority\n"
                                                      "  opening_strategies:\n"
                                                      "    - id: old\n"
                                                      "      type: TrendOpening\n");

    StrategyMainConfig config;
    std::string error;
    EXPECT_FALSE(LoadStrategyMainConfig(main_path.string(), &config, &error));
    EXPECT_NE(error.find("opening_strategies"), std::string::npos);
    EXPECT_NE(error.find("sub_strategies"), std::string::npos);
}

TEST(StrategyMainConfigLoaderTest, LoadsJsonMainConfigWithV2SubStrategies) {
    const std::filesystem::path root = MakeTempDir("quant_hft_strategy_main_v2_json");
    WriteFile(root / "sub" / "kama.json",
              "{\n"
              "  \"params\": {\n"
              "    \"id\": \"kama_1\",\n"
              "    \"er_period\": \"10\",\n"
              "    \"fast_period\": \"2\",\n"
              "    \"slow_period\": \"30\"\n"
              "  }\n"
              "}\n");

    const std::filesystem::path main_path =
        WriteFile(root / "main_strategy.json",
                  "{\n"
                  "  \"run_type\": \"backtest\",\n"
                  "  \"market_state_mode\": true,\n"
                  "  \"backtest\": {\n"
                  "    \"initial_equity\": 500000,\n"
                  "    \"symbols\": [\"rb2405\"],\n"
                  "    \"start_date\": \"20240101\",\n"
                  "    \"end_date\": \"20240110\",\n"
                  "    \"product_config_path\": \"./instrument_info.json\"\n"
                  "  },\n"
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

    StrategyMainConfig config;
    std::string error;
    ASSERT_TRUE(LoadStrategyMainConfig(main_path.string(), &config, &error)) << error;
    EXPECT_EQ(config.run_type, "backtest");
    EXPECT_TRUE(config.market_state_mode);
    EXPECT_DOUBLE_EQ(config.backtest.initial_equity, 500000.0);
    ASSERT_EQ(config.backtest.symbols.size(), 1U);
    EXPECT_EQ(config.backtest.symbols[0], "rb2405");
    ASSERT_EQ(config.composite.sub_strategies.size(), 1U);
    EXPECT_EQ(config.composite.sub_strategies[0].id, "kama_1");
    EXPECT_TRUE(config.composite.sub_strategies[0].enabled);
    EXPECT_EQ(config.composite.sub_strategies[0].params.at("er_period"), "10");
}

}  // namespace
}  // namespace quant_hft
