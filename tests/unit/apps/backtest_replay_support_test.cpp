#include "quant_hft/apps/backtest_replay_support.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <system_error>

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

std::filesystem::path WriteTempReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,ts_ns,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1\n";
    out << "rb2405,1704067200000000000,100,100,99,20,101,18\n";
    out << "rb2405,1704067201000000000,101,101,100,21,102,19\n";
    out << "rb2405,1704067202000000000,102,102,101,22,103,20\n";
    out << "rb2405,1704067203000000000,103,103,102,23,104,21\n";
    out.close();
    return path;
}

std::string UniqueRunId(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    return stem + "-" + std::to_string(stamp);
}

std::filesystem::path WriteTempCompositeConfig() {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_composite_config_test_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << "composite:\n";
    out << "  merge_rule: kPriority\n";
    out << "  opening_strategies:\n";
    out << "    - id: trend_open\n";
    out << "      type: TrendOpening\n";
    out << "      params:\n";
    out << "        id: trend_open\n";
    out << "        instrument_id: rb2405\n";
    out << "        er_period: 2\n";
    out << "        fast_period: 2\n";
    out << "        slow_period: 4\n";
    out << "        volume: 1\n";
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

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecParsesIndicatorTraceFlags) {
    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["emit_indicator_trace"] = "true";
    args["indicator_trace_path"] = "runtime/research/indicator_trace/test.parquet";

    BacktestCliSpec spec;
    std::string error;
    EXPECT_TRUE(ParseBacktestCliSpec(args, &spec, &error)) << error;
    EXPECT_TRUE(spec.emit_indicator_trace);
    EXPECT_EQ(spec.indicator_trace_path, "runtime/research/indicator_trace/test.parquet");
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecRejectsInvalidIndicatorTraceFlag) {
    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["emit-indicator-trace"] = "bad-bool";

    BacktestCliSpec spec;
    std::string error;
    EXPECT_FALSE(ParseBacktestCliSpec(args, &spec, &error));
    EXPECT_NE(error.find("emit_indicator_trace"), std::string::npos);
}

TEST(BacktestReplaySupportTest, BuildInputSignatureChangesWithIndicatorTraceSpec) {
    BacktestCliSpec left;
    left.engine_mode = "csv";
    left.csv_path = "backtest_data/rb.csv";
    left.run_id = "sig-left";
    left.emit_indicator_trace = false;

    BacktestCliSpec right = left;
    right.emit_indicator_trace = true;
    right.indicator_trace_path = "runtime/research/indicator_trace/sig-right.parquet";

    EXPECT_NE(BuildInputSignature(left), BuildInputSignature(right));
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecRequiresCompositeConfigWhenFactoryComposite) {
    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["strategy_factory"] = "composite";

    BacktestCliSpec spec;
    std::string error;
    EXPECT_FALSE(ParseBacktestCliSpec(args, &spec, &error));
    EXPECT_NE(error.find("strategy_composite_config"), std::string::npos);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecAcceptsCompositeFactoryAndConfigPath) {
    const std::filesystem::path config_path = WriteTempCompositeConfig();
    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["strategy-factory"] = "composite";
    args["strategy-composite-config"] = config_path.string();

    BacktestCliSpec spec;
    std::string error;
    EXPECT_TRUE(ParseBacktestCliSpec(args, &spec, &error)) << error;
    EXPECT_EQ(spec.strategy_factory, "composite");
    EXPECT_EQ(spec.strategy_composite_config, config_path.string());

    std::error_code ec;
    std::filesystem::remove(config_path, ec);
}

TEST(BacktestReplaySupportTest, BuildInputSignatureChangesWithSubStrategyTraceSpec) {
    BacktestCliSpec left;
    left.engine_mode = "csv";
    left.csv_path = "backtest_data/rb.csv";
    left.run_id = "sub-trace-left";
    left.strategy_factory = "composite";
    left.strategy_composite_config = "/tmp/a.yaml";
    left.emit_sub_strategy_indicator_trace = false;

    BacktestCliSpec right = left;
    right.emit_sub_strategy_indicator_trace = true;
    right.sub_strategy_indicator_trace_path =
        "runtime/research/sub_strategy_indicator_trace/x.parquet";

    EXPECT_NE(BuildInputSignature(left), BuildInputSignature(right));
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecRejectsInvalidSubStrategyTraceFlag) {
    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["strategy_factory"] = "composite";
    args["strategy_composite_config"] = "/tmp/composite.yaml";
    args["emit_sub_strategy_indicator_trace"] = "not-bool";

    BacktestCliSpec spec;
    std::string error;
    EXPECT_FALSE(ParseBacktestCliSpec(args, &spec, &error));
    EXPECT_NE(error.find("emit_sub_strategy_indicator_trace"), std::string::npos);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecIndicatorTraceFollowsArrowCapability) {
    const std::filesystem::path csv_path = WriteTempReplayCsv("quant_hft_indicator_trace");
    const std::filesystem::path trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_indicator_trace.parquet";
    std::error_code ec;
    std::filesystem::remove(trace_path, ec);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "indicator-trace-test";
    spec.emit_indicator_trace = true;
    spec.indicator_trace_path = trace_path.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
#if QUANT_HFT_ENABLE_ARROW_PARQUET
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    EXPECT_TRUE(result.indicator_trace_enabled);
    EXPECT_EQ(result.indicator_trace_path, trace_path.string());
    EXPECT_GT(result.indicator_trace_rows, 0);
    EXPECT_TRUE(std::filesystem::exists(trace_path));
#else
    EXPECT_FALSE(RunBacktestSpec(spec, &result, &error));
    EXPECT_NE(error.find("QUANT_HFT_ENABLE_ARROW_PARQUET=ON"), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(trace_path));
#endif

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(trace_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecSubStrategyTraceRequiresCompositeFactory) {
    const std::filesystem::path csv_path = WriteTempReplayCsv("quant_hft_sub_strategy_trace");
    const std::filesystem::path trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_sub_strategy_trace.parquet";
    std::error_code ec;
    std::filesystem::remove(trace_path, ec);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "sub-strategy-trace-test";
    spec.strategy_factory = "demo";
    spec.emit_sub_strategy_indicator_trace = true;
    spec.sub_strategy_indicator_trace_path = trace_path.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    EXPECT_FALSE(RunBacktestSpec(spec, &result, &error));
    EXPECT_NE(error.find("strategy_factory=composite"), std::string::npos);

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(trace_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecSubStrategyTraceFollowsArrowCapability) {
    const std::filesystem::path csv_path =
        WriteTempReplayCsv("quant_hft_sub_strategy_trace_composite");
    const std::filesystem::path composite_path = WriteTempCompositeConfig();
    const std::filesystem::path trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_sub_strategy_trace_composite.parquet";
    std::error_code ec;
    std::filesystem::remove(trace_path, ec);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "sub-strategy-trace-composite-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.emit_sub_strategy_indicator_trace = true;
    spec.sub_strategy_indicator_trace_path = trace_path.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
#if QUANT_HFT_ENABLE_ARROW_PARQUET
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    EXPECT_TRUE(result.sub_strategy_indicator_trace_enabled);
    EXPECT_EQ(result.sub_strategy_indicator_trace_path, trace_path.string());
    EXPECT_GT(result.sub_strategy_indicator_trace_rows, 0);
    EXPECT_TRUE(std::filesystem::exists(trace_path));
#else
    EXPECT_FALSE(RunBacktestSpec(spec, &result, &error));
    EXPECT_NE(error.find("QUANT_HFT_ENABLE_ARROW_PARQUET=ON"), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(trace_path));
#endif

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove(trace_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecSubStrategyTraceUsesDefaultPathWhenEnabled) {
#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    GTEST_SKIP() << "Arrow parquet writer is disabled in this build";
#else
    const std::filesystem::path csv_path =
        WriteTempReplayCsv("quant_hft_sub_strategy_trace_default");
    const std::filesystem::path composite_path = WriteTempCompositeConfig();
    const std::string run_id = UniqueRunId("sub-strategy-trace-default");
    const std::filesystem::path expected_path = std::filesystem::path("runtime") / "research" /
                                                "sub_strategy_indicator_trace" /
                                                (run_id + ".parquet");
    std::error_code ec;
    std::filesystem::remove(expected_path, ec);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = run_id;
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.emit_sub_strategy_indicator_trace = true;
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    EXPECT_EQ(result.sub_strategy_indicator_trace_path, expected_path.string());
    EXPECT_TRUE(std::filesystem::exists(expected_path));

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove(expected_path, ec);
#endif
}

TEST(BacktestReplaySupportTest, RunBacktestSpecSubStrategyTraceFailsWhenPathExists) {
#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    GTEST_SKIP() << "Arrow parquet writer is disabled in this build";
#else
    const std::filesystem::path csv_path =
        WriteTempReplayCsv("quant_hft_sub_strategy_trace_exists");
    const std::filesystem::path composite_path = WriteTempCompositeConfig();
    const std::filesystem::path trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_sub_strategy_trace_exists.parquet";
    std::error_code ec;
    std::filesystem::remove(trace_path, ec);
    std::ofstream existing(trace_path);
    existing << "occupied";
    existing.close();

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "sub-strategy-trace-existing-path";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.emit_sub_strategy_indicator_trace = true;
    spec.sub_strategy_indicator_trace_path = trace_path.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    EXPECT_FALSE(RunBacktestSpec(spec, &result, &error));
    EXPECT_NE(error.find("already exists"), std::string::npos);

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove(trace_path, ec);
#endif
}

TEST(BacktestReplaySupportTest, RunBacktestSpecIndicatorTraceUsesDefaultPathWhenEnabled) {
#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    GTEST_SKIP() << "Arrow parquet writer is disabled in this build";
#else
    const std::filesystem::path csv_path = WriteTempReplayCsv("quant_hft_indicator_trace_default");
    const std::string run_id = UniqueRunId("indicator-trace-default");
    const std::filesystem::path expected_path =
        std::filesystem::path("runtime") / "research" / "indicator_trace" / (run_id + ".parquet");

    std::error_code ec;
    std::filesystem::remove(expected_path, ec);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = run_id;
    spec.emit_indicator_trace = true;
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    EXPECT_EQ(result.indicator_trace_path, expected_path.string());
    EXPECT_TRUE(std::filesystem::exists(expected_path));

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(expected_path, ec);
#endif
}

TEST(BacktestReplaySupportTest, RunBacktestSpecIndicatorTraceFailsWhenPathExists) {
#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    GTEST_SKIP() << "Arrow parquet writer is disabled in this build";
#else
    const std::filesystem::path csv_path = WriteTempReplayCsv("quant_hft_indicator_trace_exists");
    const std::filesystem::path trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_indicator_trace_exists.parquet";

    std::error_code ec;
    std::filesystem::remove(trace_path, ec);
    std::ofstream existing(trace_path);
    existing << "occupied";
    existing.close();

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "indicator-trace-existing-path";
    spec.emit_indicator_trace = true;
    spec.indicator_trace_path = trace_path.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    EXPECT_FALSE(RunBacktestSpec(spec, &result, &error));
    EXPECT_NE(error.find("already exists"), std::string::npos);

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(trace_path, ec);
#endif
}

}  // namespace quant_hft::apps
