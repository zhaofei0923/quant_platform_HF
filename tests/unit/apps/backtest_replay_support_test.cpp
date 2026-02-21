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

std::filesystem::path WriteMultiMinuteReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,ts_ns,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1\n";
    out << "rb2405,1704067200000000000,100,100,99,20,101,18\n";
    out << "rb2405,1704067201000000000,101,101,100,21,102,19\n";
    out << "rb2405,1704067260000000000,102,102,101,22,103,20\n";
    out << "rb2405,1704067261000000000,103,103,102,23,104,21\n";
    out << "rb2405,1704067320000000000,104,104,103,24,105,22\n";
    out << "rb2405,1704067321000000000,105,105,104,25,106,23\n";
    out << "rb2405,1704067380000000000,106,106,105,26,107,24\n";
    out << "rb2405,1704067381000000000,107,107,106,27,108,25\n";
    out.close();
    return path;
}

std::filesystem::path WriteFlatReplayCsv(const std::string& stem, double price = 100.0) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,ts_ns,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1\n";
    out << "rb2405,1704067200000000000," << price << ",100," << (price - 1.0) << ",20,"
        << (price + 1.0) << ",18\n";
    out << "rb2405,1704067201000000000," << price << ",101," << (price - 1.0) << ",21,"
        << (price + 1.0) << ",19\n";
    out << "rb2405,1704067202000000000," << price << ",102," << (price - 1.0) << ",22,"
        << (price + 1.0) << ",20\n";
    out << "rb2405,1704067203000000000," << price << ",103," << (price - 1.0) << ",23,"
        << (price + 1.0) << ",21\n";
    out.close();
    return path;
}

std::string UniqueRunId(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    return stem + "-" + std::to_string(stamp);
}

std::filesystem::path WriteTempCompositeConfig(int volume = 1) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_composite_config_test_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << "composite:\n";
    out << "  merge_rule: kPriority\n";
    out << "  sub_strategies:\n";
    out << "    - id: trend_1\n";
    out << "      enabled: true\n";
    out << "      type: TrendStrategy\n";
    out << "      params:\n";
    out << "        id: trend_1\n";
    out << "        er_period: 2\n";
    out << "        fast_period: 2\n";
    out << "        slow_period: 4\n";
    out << "        kama_filter: 0.0\n";
    out << "        risk_per_trade_pct: 0.01\n";
    out << "        default_volume: " << volume << "\n";
    out << "        stop_loss_mode: none\n";
    out << "        take_profit_mode: none\n";
    out.close();
    return path;
}

std::filesystem::path WriteTempMainStrategyConfig(const std::filesystem::path& composite_path,
                                                  const std::string& run_type = "backtest") {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_main_strategy_config_test_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << "run_type: " << run_type << "\n";
    out << "market_state_mode: true\n";
    out << "backtest:\n";
    out << "  initial_equity: 123456\n";
    out << "  symbols: [rb2405]\n";
    out << "  start_date: 20240101\n";
    out << "  end_date: 20240110\n";
    out << "  product_config_path: ./instrument_info.json\n";
    out << "composite:\n";
    out << "  merge_rule: kPriority\n";
    out << "  sub_strategies:\n";
    out << "    - id: trend_1\n";
    out << "      enabled: true\n";
    out << "      type: TrendStrategy\n";
    out << "      config_path: " << composite_path.string() << "\n";
    out.close();
    return path;
}

std::filesystem::path WriteTempAtomicStrategyConfig() {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_atomic_cfg_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << "params:\n";
    out << "  id: trend_1\n";
    out << "  er_period: 2\n";
    out << "  fast_period: 2\n";
    out << "  slow_period: 4\n";
    out << "  kama_filter: 0.0\n";
    out << "  risk_per_trade_pct: 0.01\n";
    out << "  default_volume: 1\n";
    out << "  stop_loss_mode: trailing_atr\n";
    out << "  stop_loss_atr_period: 2\n";
    out << "  stop_loss_atr_multiplier: 2.0\n";
    out << "  take_profit_mode: atr_target\n";
    out << "  take_profit_atr_period: 2\n";
    out << "  take_profit_atr_multiplier: 3.0\n";
    out.close();
    return path;
}

std::filesystem::path WriteTempProductFeeConfig(const std::string& content) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_product_fee_cfg_test_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

std::filesystem::path WriteTempProductFeeJsonConfig(const std::string& content) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_product_fee_cfg_test_" + std::to_string(stamp) + ".json");
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

TEST(BacktestReplaySupportTest, SelectParquetPartitionsForSymbolsSupportsProductAndInstrument) {
    ParquetDataFeed feed;
    ASSERT_TRUE(feed.RegisterPartition(ParquetPartitionMeta{
        .file_path = "runtime/backtest/parquet/source=c/trading_day=20240101/instrument_id=c2405/"
                     "part-0000.parquet",
        .source = "c",
        .trading_day = "20240101",
        .instrument_id = "c2405",
        .min_ts_ns = 100,
        .max_ts_ns = 200,
        .row_count = 10,
    }));
    ASSERT_TRUE(feed.RegisterPartition(ParquetPartitionMeta{
        .file_path = "runtime/backtest/parquet/source=c/trading_day=20240101/instrument_id=c2409/"
                     "part-0000.parquet",
        .source = "c",
        .trading_day = "20240101",
        .instrument_id = "c2409",
        .min_ts_ns = 150,
        .max_ts_ns = 250,
        .row_count = 8,
    }));
    ASSERT_TRUE(feed.RegisterPartition(ParquetPartitionMeta{
        .file_path = "runtime/backtest/parquet/source=rb/trading_day=20240101/instrument_id=rb2405/"
                     "part-0000.parquet",
        .source = "rb",
        .trading_day = "20240101",
        .instrument_id = "rb2405",
        .min_ts_ns = 160,
        .max_ts_ns = 260,
        .row_count = 6,
    }));

    const auto by_product =
        SelectParquetPartitionsForSymbols(&feed, 120, 240, std::vector<std::string>{"c"});
    ASSERT_EQ(by_product.size(), 2U);
    EXPECT_EQ(by_product[0].instrument_id, "c2405");
    EXPECT_EQ(by_product[1].instrument_id, "c2409");

    const auto by_instrument =
        SelectParquetPartitionsForSymbols(&feed, 120, 240, std::vector<std::string>{"rb2405"});
    ASSERT_EQ(by_instrument.size(), 1U);
    EXPECT_EQ(by_instrument.front().instrument_id, "rb2405");

    const auto mixed =
        SelectParquetPartitionsForSymbols(&feed, 120, 240, std::vector<std::string>{"c", "rb2405"});
    ASSERT_EQ(mixed.size(), 3U);
    EXPECT_EQ(mixed[0].instrument_id, "c2405");
    EXPECT_EQ(mixed[1].instrument_id, "c2409");
    EXPECT_EQ(mixed[2].instrument_id, "rb2405");
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

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecParsesDetailEmissionFlags) {
    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["emit_trades"] = "false";
    args["emit_orders"] = "false";
    args["emit_position_history"] = "true";

    BacktestCliSpec spec;
    std::string error;
    EXPECT_TRUE(ParseBacktestCliSpec(args, &spec, &error)) << error;
    EXPECT_FALSE(spec.emit_trades);
    EXPECT_FALSE(spec.emit_orders);
    EXPECT_TRUE(spec.emit_position_history);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecParsesCapitalAndConfigFields) {
    const std::filesystem::path open_cfg = WriteTempAtomicStrategyConfig();
    const std::filesystem::path main_cfg = WriteTempMainStrategyConfig(open_cfg);
    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["initial_equity"] = "1500000";
    args["product_config_path"] = "configs/strategies/products_info.yaml";
    args["strategy_main_config_path"] = main_cfg.string();

    BacktestCliSpec spec;
    std::string error;
    EXPECT_TRUE(ParseBacktestCliSpec(args, &spec, &error)) << error;
    EXPECT_DOUBLE_EQ(spec.initial_equity, 1500000.0);
    EXPECT_EQ(spec.product_config_path, "configs/strategies/products_info.yaml");
    EXPECT_EQ(spec.strategy_main_config_path, main_cfg.string());

    std::error_code ec;
    std::filesystem::remove(open_cfg, ec);
    std::filesystem::remove(main_cfg, ec);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecAllowsCliOverrideOverMainStrategyConfig) {
    const std::filesystem::path open_cfg = WriteTempAtomicStrategyConfig();
    const std::filesystem::path main_cfg = WriteTempMainStrategyConfig(open_cfg);

    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["strategy_main_config_path"] = main_cfg.string();
    args["initial_equity"] = "2000000";

    BacktestCliSpec spec;
    std::string error;
    EXPECT_TRUE(ParseBacktestCliSpec(args, &spec, &error)) << error;
    EXPECT_DOUBLE_EQ(spec.initial_equity, 2000000.0);
    EXPECT_EQ(spec.strategy_factory, "composite");
    EXPECT_EQ(spec.strategy_composite_config, main_cfg.string());
    EXPECT_NE(spec.product_config_path.find("instrument_info.json"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove(open_cfg, ec);
    std::filesystem::remove(main_cfg, ec);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecRejectsRemovedMaxLossPercentFlag) {
    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["max_loss_percent"] = "0.02";

    BacktestCliSpec spec;
    std::string error;
    EXPECT_FALSE(ParseBacktestCliSpec(args, &spec, &error));
    EXPECT_NE(error.find("max_loss_percent"), std::string::npos);
    EXPECT_NE(error.find("risk_per_trade_pct"), std::string::npos);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecRejectsNonBacktestMainRunType) {
    const std::filesystem::path open_cfg = WriteTempAtomicStrategyConfig();
    const std::filesystem::path main_cfg = WriteTempMainStrategyConfig(open_cfg, "sim");

    ArgMap args;
    args["engine_mode"] = "csv";
    args["csv_path"] = "backtest_data/rb.csv";
    args["strategy_main_config_path"] = main_cfg.string();

    BacktestCliSpec spec;
    std::string error;
    EXPECT_FALSE(ParseBacktestCliSpec(args, &spec, &error));
    EXPECT_NE(error.find("run_type"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove(open_cfg, ec);
    std::filesystem::remove(main_cfg, ec);
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

TEST(BacktestReplaySupportTest, BuildInputSignatureChangesWithCapitalSpec) {
    BacktestCliSpec left;
    left.engine_mode = "csv";
    left.csv_path = "backtest_data/rb.csv";
    left.run_id = "sig-cap-left";
    left.initial_equity = 1000000.0;
    left.product_config_path = "a.yaml";

    BacktestCliSpec right = left;
    right.initial_equity = 2000000.0;

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

TEST(BacktestReplaySupportTest, RunBacktestSpecDeterministicFillFeedsOrderEventToComposite) {
    const std::filesystem::path csv_path = WriteMultiMinuteReplayCsv("quant_hft_order_event_feed");
    const std::filesystem::path composite_path = WriteTempCompositeConfig();

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "order-event-feed-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);
    EXPECT_EQ(result.deterministic.intents_processed, 1);
    EXPECT_FALSE(result.trades.empty());
    EXPECT_FALSE(result.orders.empty());
    EXPECT_TRUE(result.position_history.empty());
    EXPECT_EQ(result.parameters.engine_mode, "csv");

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecRespectsDetailEmissionFlags) {
    const std::filesystem::path csv_path = WriteMultiMinuteReplayCsv("quant_hft_detail_flags");
    const std::filesystem::path composite_path = WriteTempCompositeConfig();

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "detail-flags-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.emit_trades = false;
    spec.emit_orders = false;
    spec.emit_position_history = true;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);
    EXPECT_TRUE(result.trades.empty());
    EXPECT_TRUE(result.orders.empty());
    EXPECT_FALSE(result.position_history.empty());

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecAccumulatesCommissionFromProductConfig) {
    const std::filesystem::path csv_path = WriteTempReplayCsv("quant_hft_fee_accum");
    const std::filesystem::path fee_cfg = WriteTempProductFeeConfig(
        "products:\n"
        "  rb2405:\n"
        "    symbol: rb\n"
        "    contract_multiplier: 10\n"
        "    long_margin_ratio: 0.16\n"
        "    short_margin_ratio: 0.16\n"
        "    open_mode: rate\n"
        "    open_value: 0.001\n"
        "    close_mode: per_lot\n"
        "    close_value: 1\n"
        "    close_today_mode: per_lot\n"
        "    close_today_value: 1\n");

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "fee-accum-test";
    spec.product_config_path = fee_cfg.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);
    EXPECT_GT(result.deterministic.performance.total_commission, 0.0);
    EXPECT_NEAR(result.deterministic.performance.total_pnl_after_cost,
                result.deterministic.performance.total_pnl -
                    result.deterministic.performance.total_commission,
                1e-9);

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(fee_cfg, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecSupportsRawInstrumentInfoJsonConfig) {
    const std::filesystem::path csv_path = WriteTempReplayCsv("quant_hft_fee_raw_json");
    const std::filesystem::path fee_cfg = WriteTempProductFeeJsonConfig(
        "{\n"
        "  \"RB\": {\n"
        "    \"product\": \"RB\",\n"
        "    \"volume_multiple\": 10,\n"
        "    \"long_margin_ratio\": 0.16,\n"
        "    \"short_margin_ratio\": 0.16,\n"
        "    \"trading_sessions\": [\"21:00:00-23:00:00\"],\n"
        "    \"commission\": {\n"
        "      \"open_ratio_by_money\": 0.0001,\n"
        "      \"open_ratio_by_volume\": 0,\n"
        "      \"close_ratio_by_money\": 0.0001,\n"
        "      \"close_ratio_by_volume\": 0,\n"
        "      \"close_today_ratio_by_money\": 0.0001,\n"
        "      \"close_today_ratio_by_volume\": 0\n"
        "    }\n"
        "  }\n"
        "}\n");

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "fee-raw-json-test";
    spec.product_config_path = fee_cfg.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);
    EXPECT_GT(result.deterministic.performance.total_commission, 0.0);

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(fee_cfg, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecFailsWhenProductConfigMissingInstrument) {
    const std::filesystem::path csv_path = WriteTempReplayCsv("quant_hft_fee_missing");
    const std::filesystem::path fee_cfg = WriteTempProductFeeConfig(
        "products:\n"
        "  ag2406:\n"
        "    symbol: ag\n"
        "    contract_multiplier: 15\n"
        "    long_margin_ratio: 0.16\n"
        "    short_margin_ratio: 0.16\n"
        "    open_mode: rate\n"
        "    open_value: 0.001\n"
        "    close_mode: rate\n"
        "    close_value: 0.001\n"
        "    close_today_mode: rate\n"
        "    close_today_value: 0.001\n");

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "fee-missing-test";
    spec.product_config_path = fee_cfg.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    EXPECT_FALSE(RunBacktestSpec(spec, &result, &error));
    EXPECT_NE(error.find("rb2405"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(fee_cfg, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecEquityCurveUsesInitialEquityBaseline) {
    const std::filesystem::path csv_path = WriteFlatReplayCsv("quant_hft_equity_baseline");
    const std::filesystem::path fee_cfg = WriteTempProductFeeConfig(
        "products:\n"
        "  rb2405:\n"
        "    symbol: rb\n"
        "    contract_multiplier: 10\n"
        "    long_margin_ratio: 0.16\n"
        "    short_margin_ratio: 0.16\n"
        "    open_mode: rate\n"
        "    open_value: 0.001\n"
        "    close_mode: per_lot\n"
        "    close_value: 1\n"
        "    close_today_mode: per_lot\n"
        "    close_today_value: 1\n");

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "equity-baseline-test";
    spec.initial_equity = 1000.0;
    spec.product_config_path = fee_cfg.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);

    const auto& perf = result.deterministic.performance;
    EXPECT_DOUBLE_EQ(perf.initial_equity, 1000.0);
    EXPECT_NEAR(perf.total_pnl, 0.0, 1e-9);
    EXPECT_GT(perf.total_commission, 0.0);
    EXPECT_NEAR(perf.total_pnl_after_cost, perf.total_pnl - perf.total_commission, 1e-9);
    EXPECT_NEAR(perf.final_equity, perf.initial_equity + perf.total_pnl_after_cost, 1e-9);
    EXPECT_NEAR(perf.max_equity, perf.initial_equity, 1e-9);
    EXPECT_NEAR(perf.min_equity, perf.final_equity, 1e-9);
    EXPECT_NEAR(perf.max_drawdown, perf.max_equity - perf.min_equity, 1e-9);

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(fee_cfg, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecRejectsOpenWhenMarginInsufficient) {
    const std::filesystem::path csv_path = WriteTempReplayCsv("quant_hft_margin_reject");
    const std::filesystem::path fee_cfg = WriteTempProductFeeConfig(
        "products:\n"
        "  rb2405:\n"
        "    symbol: rb\n"
        "    contract_multiplier: 100\n"
        "    long_margin_ratio: 1.0\n"
        "    short_margin_ratio: 1.0\n"
        "    open_mode: rate\n"
        "    open_value: 0.0001\n"
        "    close_mode: rate\n"
        "    close_value: 0.0001\n"
        "    close_today_mode: rate\n"
        "    close_today_value: 0.0001\n");

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "margin-reject-test";
    spec.initial_equity = 1000.0;
    spec.product_config_path = fee_cfg.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);
    EXPECT_EQ(result.deterministic.performance.margin_clipped_orders, 1);
    EXPECT_EQ(result.deterministic.performance.margin_rejected_orders, 1);
    EXPECT_EQ(result.deterministic.performance.final_margin_used, 0.0);
    EXPECT_EQ(result.deterministic.performance.max_margin_used, 0.0);
    const auto it = result.deterministic.performance.order_status_counts.find("REJECTED");
    ASSERT_NE(it, result.deterministic.performance.order_status_counts.end());
    EXPECT_EQ(it->second, 1);

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(fee_cfg, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecClipsVolumeByMarginAndTracksUsage) {
    const std::filesystem::path csv_path = WriteMultiMinuteReplayCsv("quant_hft_margin_clip");
    const std::filesystem::path composite_path = WriteTempCompositeConfig(/*volume=*/10);
    const std::filesystem::path fee_cfg = WriteTempProductFeeConfig(
        "products:\n"
        "  rb2405:\n"
        "    symbol: rb\n"
        "    contract_multiplier: 10\n"
        "    long_margin_ratio: 0.2\n"
        "    short_margin_ratio: 0.2\n"
        "    open_mode: rate\n"
        "    open_value: 0.0001\n"
        "    close_mode: rate\n"
        "    close_value: 0.0001\n"
        "    close_today_mode: rate\n"
        "    close_today_value: 0.0001\n");

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "margin-clip-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.initial_equity = 1500.0;
    spec.product_config_path = fee_cfg.string();

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);
    EXPECT_GT(result.deterministic.performance.margin_clipped_orders, 0);
    EXPECT_EQ(result.deterministic.performance.margin_rejected_orders, 0);
    EXPECT_GT(result.deterministic.performance.max_margin_used, 0.0);
    EXPECT_GT(result.deterministic.performance.final_margin_used, 0.0);
    const auto it = result.deterministic.performance.order_status_counts.find("FILLED");
    ASSERT_NE(it, result.deterministic.performance.order_status_counts.end());
    EXPECT_GT(it->second, 0);

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove(fee_cfg, ec);
}

TEST(BacktestReplaySupportTest, RequireParquetBacktestSpecRejectsUnsupportedEngineMode) {
    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.dataset_root = "backtest_data/parquet_v2";
    std::string error;
    EXPECT_FALSE(RequireParquetBacktestSpec(spec, &error));
    EXPECT_NE(error.find("engine_mode must be parquet"), std::string::npos);
}

TEST(BacktestReplaySupportTest, RequireParquetBacktestSpecHonorsArrowBuildFlag) {
    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = "backtest_data/parquet_v2";
    std::string error;
    const bool ok = RequireParquetBacktestSpec(spec, &error);
#if QUANT_HFT_ENABLE_ARROW_PARQUET
    EXPECT_TRUE(ok) << error;
#else
    EXPECT_FALSE(ok);
    EXPECT_NE(error.find("built without Arrow/Parquet support"), std::string::npos);
#endif
}

}  // namespace quant_hft::apps
