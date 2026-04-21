#include "quant_hft/optim/temp_config_generator.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>

#include "quant_hft/apps/backtest_replay_support.h"
#include "quant_hft/strategy/composite_config_loader.h"

namespace quant_hft::optim {
namespace {

std::filesystem::path MakeTempDir() {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("quant_hft_temp_config_generator_test_" + std::to_string(stamp));
    std::filesystem::create_directories(dir / "sub");
    return dir;
}

void WriteFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    out << content;
    out.close();
}

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream in(path);
    std::ostringstream buffer;
    buffer << in.rdbuf();
    return buffer.str();
}

TEST(TempConfigGeneratorTest, RewritesTargetSubConfigAndAbsolutizesOtherPaths) {
    const std::filesystem::path base_dir = MakeTempDir();
    const std::filesystem::path target_sub = base_dir / "sub" / "kama.yaml";
    const std::filesystem::path other_sub = base_dir / "sub" / "trend.yaml";
    const std::filesystem::path composite = base_dir / "composite.yaml";

    WriteFile(target_sub,
              "params:\n"
              "  id: kama\n"
              "  take_profit_atr_multiplier: 3.0\n"
              "  default_volume: 1\n");
    WriteFile(other_sub,
              "params:\n"
              "  id: trend\n"
              "  default_volume: 1\n");
    WriteFile(composite,
              "run_type: backtest\n"
              "market_state_mode: false\n"
              "backtest:\n"
              "  initial_equity: 200000\n"
              "  product_series_mode: raw\n"
              "  symbols: [c]\n"
              "  start_date: 20240101\n"
              "  end_date: 20240331\n"
              "  product_config_path: ./instrument_info.json\n"
              "  contract_expiry_calendar_path: ./contract_expiry_calendar.yaml\n"
              "composite:\n"
              "  merge_rule: kPriority\n"
              "  enable_non_backtest: false\n"
              "  sub_strategies:\n"
              "    - id: kama\n"
              "      enabled: true\n"
              "      timeframe_minutes: 5\n"
              "      type: KamaTrendStrategy\n"
              "      config_path: ./sub/kama.yaml\n"
              "    - id: trend\n"
              "      enabled: true\n"
              "      timeframe_minutes: 15\n"
              "      type: TrendStrategy\n"
              "      config_path: ./sub/trend.yaml\n");

    TrialConfigRequest request;
    request.composite_config_path = composite;
    request.target_sub_config_path = "./sub/kama.yaml";
    request.param_overrides["take_profit_atr_multiplier"] = 20.0;
    request.trial_id = "trial_1";

    TrialConfigArtifacts artifacts;
    std::string error;
    ASSERT_TRUE(GenerateTrialConfig(request, &artifacts, &error)) << error;

    EXPECT_TRUE(std::filesystem::exists(artifacts.working_dir));
    EXPECT_TRUE(std::filesystem::exists(artifacts.sub_config_path));
    EXPECT_TRUE(std::filesystem::exists(artifacts.composite_config_path));

    std::map<std::string, std::string> params;
    ASSERT_TRUE(quant_hft::apps::detail::LoadYamlScalarMap(artifacts.sub_config_path, &params, &error))
        << error;
    EXPECT_EQ(params["params.take_profit_atr_multiplier"], "20.0");

    CompositeStrategyDefinition generated;
    ASSERT_TRUE(
        LoadCompositeStrategyDefinition(artifacts.composite_config_path.string(), &generated, &error))
        << error;
    EXPECT_EQ(generated.run_type, "backtest");
    EXPECT_FALSE(generated.market_state_mode);
    ASSERT_EQ(generated.sub_strategies.size(), 2U);
    EXPECT_EQ(generated.sub_strategies[0].timeframe_minutes, 5);
    EXPECT_EQ(generated.sub_strategies[1].timeframe_minutes, 15);

    const std::string composite_text = ReadFile(artifacts.composite_config_path);
    const std::string other_abs = std::filesystem::absolute(other_sub).lexically_normal().string();
    EXPECT_EQ(composite_text.rfind("composite:\n", 0), 0U);
    EXPECT_NE(composite_text.find(other_abs), std::string::npos);
    EXPECT_NE(composite_text.find(artifacts.sub_config_path.string()), std::string::npos);
    EXPECT_NE(composite_text.find("market_state_mode: false"), std::string::npos);
    EXPECT_NE(composite_text.find("timeframe_minutes: 5"), std::string::npos);
    EXPECT_EQ(composite_text.find("\nbacktest:\n"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(base_dir, ec);
    std::filesystem::remove_all(artifacts.working_dir, ec);
}

}  // namespace
}  // namespace quant_hft::optim
