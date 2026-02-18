#include "quant_hft/apps/backtest_replay_support.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>

namespace quant_hft::apps {
namespace {

std::filesystem::path WriteTempDetectorConfig(const std::string& content) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_detector_config_test_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}
}  // namespace

TEST(BacktestReplaySupportTest, BuildStateSnapshotFromBarPopulatesBarFields) {
    ReplayTick first;
    first.instrument_id = "SHFE.ag2406";
    first.last_price = 100.0;
    first.volume = 100;
    first.bid_volume_1 = 20;
    first.ask_volume_1 = 18;

    ReplayTick last = first;
    last.last_price = 105.0;
    last.volume = 160;
    last.bid_volume_1 = 25;
    last.ask_volume_1 = 22;
    last.ts_ns = 456;

    const StateSnapshot7D state = BuildStateSnapshotFromBar(first, last, 106.0, 99.0, 60, 456);

    EXPECT_EQ(state.instrument_id, "SHFE.ag2406");
    EXPECT_DOUBLE_EQ(state.bar_open, 100.0);
    EXPECT_DOUBLE_EQ(state.bar_high, 106.0);
    EXPECT_DOUBLE_EQ(state.bar_low, 99.0);
    EXPECT_DOUBLE_EQ(state.bar_close, 105.0);
    EXPECT_DOUBLE_EQ(state.bar_volume, 60.0);
    EXPECT_TRUE(state.has_bar);
    EXPECT_EQ(state.market_regime, MarketRegime::kUnknown);
    EXPECT_EQ(state.ts_ns, 456);
}

TEST(BacktestReplaySupportTest, BuildStateSnapshotFromBarUpdatesMarketRegimeWhenDetectorProvided) {
    MarketStateDetectorConfig config;
    config.adx_period = 3;
    config.atr_period = 3;
    config.kama_er_period = 3;
    config.min_bars_for_flat = 1;
    MarketStateDetector detector(config);

    StateSnapshot7D state;
    for (int i = 0; i < 8; ++i) {
        ReplayTick first;
        first.instrument_id = "SHFE.rb2405";
        first.last_price = 100.0 + static_cast<double>(i);
        first.volume = 100 + i;
        first.bid_volume_1 = 20;
        first.ask_volume_1 = 18;

        ReplayTick last = first;
        last.ts_ns = 100 + i;

        state = BuildStateSnapshotFromBar(first, last, first.last_price + 1.0,
                                          first.last_price - 1.0, 1, last.ts_ns, &detector);
    }

    EXPECT_EQ(state.market_regime, MarketRegime::kStrongTrend);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecLoadsDetectorConfigFile) {
    const auto config_path = WriteTempDetectorConfig(
        "market_state_detector:\n"
        "  adx_period: 7\n"
        "  atr_period: 5\n"
        "  kama_er_period: 6\n"
        "  atr_flat_ratio: 0.002\n"
        "  require_adx_for_trend: false\n");

    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["detector_config"] = config_path.string();

    BacktestCliSpec spec;
    std::string error;
    EXPECT_TRUE(ParseBacktestCliSpec(args, &spec, &error)) << error;
    EXPECT_EQ(spec.detector_config_path, config_path.string());
    EXPECT_EQ(spec.detector_config.adx_period, 7);
    EXPECT_EQ(spec.detector_config.atr_period, 5);
    EXPECT_EQ(spec.detector_config.kama_er_period, 6);
    EXPECT_NEAR(spec.detector_config.atr_flat_ratio, 0.002, 1e-12);
    EXPECT_FALSE(spec.detector_config.require_adx_for_trend);

    std::filesystem::remove(config_path);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecRejectsInvalidDetectorConfigFile) {
    const auto config_path = WriteTempDetectorConfig("adx_period: 0\n");

    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["detector-config"] = config_path.string();

    BacktestCliSpec spec;
    std::string error;
    EXPECT_FALSE(ParseBacktestCliSpec(args, &spec, &error));
    EXPECT_NE(error.find("detector_config"), std::string::npos);

    std::filesystem::remove(config_path);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecLoadsDetectorConfigFromCtpNestedBlock) {
    const auto config_path = WriteTempDetectorConfig(
        "ctp:\n"
        "  market_state_detector:\n"
        "    adx_period: 9\n"
        "    atr_period: 11\n");

    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["detector_config"] = config_path.string();

    BacktestCliSpec spec;
    std::string error;
    EXPECT_TRUE(ParseBacktestCliSpec(args, &spec, &error)) << error;
    EXPECT_EQ(spec.detector_config.adx_period, 9);
    EXPECT_EQ(spec.detector_config.atr_period, 11);

    std::filesystem::remove(config_path);
}

}  // namespace quant_hft::apps
