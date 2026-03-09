#include "quant_hft/apps/backtest_replay_support.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#if QUANT_HFT_ENABLE_ARROW_PARQUET
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#endif

#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft::apps {
namespace {

#if QUANT_HFT_ENABLE_ARROW_PARQUET
template <typename ReaderPtr>
auto OpenParquetReaderCompat(const std::shared_ptr<arrow::io::RandomAccessFile>& input,
                             ReaderPtr* reader, int)
    -> decltype(parquet::arrow::OpenFile(input, arrow::default_memory_pool()), bool()) {
    auto reader_result = parquet::arrow::OpenFile(input, arrow::default_memory_pool());
    if (!reader_result.ok()) {
        return false;
    }
    *reader = std::move(reader_result).ValueOrDie();
    return *reader != nullptr;
}

template <typename ReaderPtr>
auto OpenParquetReaderCompat(const std::shared_ptr<arrow::io::RandomAccessFile>& input,
                             ReaderPtr* reader, long)
    -> decltype(parquet::arrow::OpenFile(input, arrow::default_memory_pool(), reader), bool()) {
    auto reader_status = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), reader);
    return reader_status.ok() && *reader != nullptr;
}

std::vector<std::string> ReadStringColumnFromParquet(const std::filesystem::path& path,
                                                     const std::string& column) {
    std::vector<std::string> out;
    auto input_result = arrow::io::ReadableFile::Open(path.string());
    if (!input_result.ok()) {
        return out;
    }
    std::shared_ptr<arrow::io::ReadableFile> input = input_result.ValueOrDie();
    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    if (!OpenParquetReaderCompat(input, &parquet_reader, 0)) {
        return out;
    }

    std::shared_ptr<arrow::Table> table;
    if (!parquet_reader->ReadTable(&table).ok() || table == nullptr) {
        return out;
    }
    const auto column_data = table->GetColumnByName(column);
    if (column_data == nullptr || column_data->num_chunks() == 0) {
        return out;
    }
    const auto values = std::static_pointer_cast<arrow::StringArray>(column_data->chunk(0));
    out.reserve(values->length());
    for (int64_t i = 0; i < values->length(); ++i) {
        out.push_back(values->GetString(i));
    }
    return out;
}

std::vector<std::int32_t> ReadInt32ColumnFromParquet(const std::filesystem::path& path,
                                                     const std::string& column) {
    std::vector<std::int32_t> out;
    auto input_result = arrow::io::ReadableFile::Open(path.string());
    if (!input_result.ok()) {
        return out;
    }
    std::shared_ptr<arrow::io::ReadableFile> input = input_result.ValueOrDie();
    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    if (!OpenParquetReaderCompat(input, &parquet_reader, 0)) {
        return out;
    }

    std::shared_ptr<arrow::Table> table;
    if (!parquet_reader->ReadTable(&table).ok() || table == nullptr) {
        return out;
    }
    const auto column_data = table->GetColumnByName(column);
    if (column_data == nullptr || column_data->num_chunks() == 0) {
        return out;
    }
    const auto values = std::static_pointer_cast<arrow::Int32Array>(column_data->chunk(0));
    out.reserve(values->length());
    for (int64_t i = 0; i < values->length(); ++i) {
        out.push_back(values->Value(i));
    }
    return out;
}
#endif

bool IsMinutePrecisionDateTime(const std::string& value) {
    if (value.size() != 16) {
        return false;
    }
    return value[4] == '-' && value[7] == '-' && value[10] == ' ' && value[13] == ':';
}

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
    out << "rb2405,1704186000000000000,100,100,99,20,101,18\n";
    out << "rb2405,1704186001000000000,101,101,100,21,102,19\n";
    out << "rb2405,1704186060000000000,102,102,101,22,103,20\n";
    out << "rb2405,1704186061000000000,103,103,102,23,104,21\n";
    out.close();
    return path;
}

std::filesystem::path WriteMultiMinuteReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,ts_ns,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1\n";
    out << "rb2405,1704186000000000000,100,100,99,20,101,18\n";
    out << "rb2405,1704186001000000000,101,101,100,21,102,19\n";
    out << "rb2405,1704186060000000000,102,102,101,22,103,20\n";
    out << "rb2405,1704186061000000000,103,103,102,23,104,21\n";
    out << "rb2405,1704186120000000000,104,104,103,24,105,22\n";
    out << "rb2405,1704186121000000000,105,105,104,25,106,23\n";
    out << "rb2405,1704186180000000000,106,106,105,26,107,24\n";
    out << "rb2405,1704186181000000000,107,107,106,27,108,25\n";
    out.close();
    return path;
}

std::filesystem::path WriteFlatReplayCsv(const std::string& stem, double price = 100.0) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,ts_ns,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1\n";
    out << "rb2405,1704186000000000000," << price << ",100," << (price - 1.0) << ",20,"
        << (price + 1.0) << ",18\n";
    out << "rb2405,1704186001000000000," << price << ",101," << (price - 1.0) << ",21,"
        << (price + 1.0) << ",19\n";
    out << "rb2405,1704186060000000000," << price << ",102," << (price - 1.0) << ",22,"
        << (price + 1.0) << ",20\n";
    out << "rb2405,1704186061000000000," << price << ",103," << (price - 1.0) << ",23,"
        << (price + 1.0) << ",21\n";
    out.close();
    return path;
}

std::string UniqueRunId(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    return stem + "-" + std::to_string(stamp);
}

std::filesystem::path WriteTempCompositeConfig(int volume = 1, int timeframe_minutes = 1) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_composite_config_test_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << "composite:\n";
    out << "  merge_rule: kPriority\n";
    out << "  sub_strategies:\n";
    out << "    - id: trend_1\n";
    out << "      enabled: true\n";
    out << "      timeframe_minutes: " << timeframe_minutes << "\n";
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

class AlwaysOpenReplayStrategy final : public ISubStrategy {
   public:
    void Init(const AtomicParams& params) override {
        const auto id_it = params.find("id");
        if (id_it != params.end() && !id_it->second.empty()) {
            id_ = id_it->second;
        }
        const auto volume_it = params.find("volume");
        if (volume_it != params.end() && !volume_it->second.empty()) {
            volume_ = std::stoi(volume_it->second);
        }
        const auto side_it = params.find("open_side");
        if (side_it != params.end() && side_it->second == "sell") {
            side_ = Side::kSell;
        }
    }

    std::string GetId() const override { return id_; }

    void Reset() override {}

    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        (void)ctx;
        SignalIntent signal;
        signal.strategy_id = id_;
        signal.instrument_id = state.instrument_id;
        signal.signal_type = SignalType::kOpen;
        signal.side = side_;
        signal.offset = OffsetFlag::kOpen;
        signal.volume = volume_;
        signal.limit_price = state.bar_close;
        signal.ts_ns = state.ts_ns;
        signal.trace_id = id_ + "-open";
        return {signal};
    }

   private:
    std::string id_{"always_open"};
    std::int32_t volume_{1};
    Side side_{Side::kBuy};
};

std::string UniqueAtomicType(const std::string& stem) {
    static std::atomic<int> seq{0};
    return "backtest_replay_support_test_" + stem + "_" + std::to_string(seq.fetch_add(1));
}

void RegisterAlwaysOpenReplayType(const std::string& type) {
    AtomicFactory& factory = AtomicFactory::Instance();
    if (factory.Has(type)) {
        return;
    }
    std::string error;
    ASSERT_TRUE(factory.Register(type, []() { return std::make_unique<AlwaysOpenReplayStrategy>(); },
                                 &error))
        << error;
}

std::filesystem::path WriteForceCloseWindowCompositeConfig(const std::string& strategy_type,
                                                           const std::string& force_close_windows) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() /
        ("quant_hft_force_close_composite_test_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << "composite:\n";
    out << "  merge_rule: kPriority\n";
    out << "  sub_strategies:\n";
    out << "    - id: always_open\n";
    out << "      enabled: true\n";
    out << "      timeframe_minutes: 1\n";
    out << "      type: " << strategy_type << "\n";
    out << "      params:\n";
    out << "        id: always_open\n";
    out << "        volume: 1\n";
    out << "        force_close_windows: \"" << force_close_windows << "\"\n";
    out << "        window_timezone: UTC\n";
    out.close();
    return path;
}

std::filesystem::path WriteForceCloseWindowReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,ts_ns,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1\n";
    const std::vector<std::pair<std::string, double>> ticks = {
        {"09:00:00", 100.0},
        {"09:00:30", 101.0},
        {"09:01:05", 102.0},
        {"09:01:30", 103.0},
        {"09:02:05", 104.0},
        {"09:02:30", 105.0},
        {"09:03:05", 106.0},
        {"09:03:30", 107.0},
        {"09:04:05", 108.0},
        {"09:04:30", 109.0},
    };
    std::int64_t volume = 100;
    for (const auto& [update_time, last_price] : ticks) {
        const EpochNanos ts_ns = detail::ToEpochNs("20240103", update_time, 0);
        out << "rb2405," << ts_ns << "," << last_price << "," << volume << "," << (last_price - 1.0)
            << ",20," << (last_price + 1.0) << ",18\n";
        ++volume;
    }
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
    EXPECT_EQ(state.timeframe_minutes, 1);
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

        state = BuildStateSnapshotFromBar(first,
                                          last,
                                          first.last_price + 1.0,
                                          first.last_price - 1.0,
                                          1,
                                          last.ts_ns,
                                          1,
                                          &detector);
    }

    EXPECT_EQ(state.market_regime, MarketRegime::kStrongTrend);
}

TEST(BacktestReplaySupportTest, PositionPnlUsesContractMultiplier) {
    PositionState state;

    ApplyTrade(&state, Side::kBuy, 2, 100.0, 10.0);
    EXPECT_EQ(state.net_position, 2);
    EXPECT_DOUBLE_EQ(state.avg_open_price, 100.0);
    EXPECT_DOUBLE_EQ(state.realized_pnl, 0.0);

    ApplyTrade(&state, Side::kSell, 1, 103.0, 10.0);
    EXPECT_EQ(state.net_position, 1);
    EXPECT_DOUBLE_EQ(state.avg_open_price, 100.0);
    EXPECT_DOUBLE_EQ(state.realized_pnl, 30.0);
    EXPECT_DOUBLE_EQ(ComputeUnrealized(state.net_position, state.avg_open_price, 104.0, 10.0),
                     40.0);
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

TEST(BacktestReplaySupportTest,
     ResolveBacktestProductConfigPathFallsBackToYamlWhenInstrumentJsonIsMissing) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto root =
        std::filesystem::temp_directory_path() /
        ("quant_hft_product_cfg_resolve_test_" + std::to_string(stamp));
    std::filesystem::create_directories(root);

    const auto yaml_path = root / "products_info.yaml";
    std::ofstream yaml_out(yaml_path);
    yaml_out << "products:\n"
                "  rb:\n"
                "    product: rb\n"
                "    volume_multiple: 10\n"
                "    long_margin_ratio: 0.16\n"
                "    short_margin_ratio: 0.16\n"
                "    commission:\n"
                "      open_ratio_by_money: 0.0\n"
                "      open_ratio_by_volume: 1.0\n"
                "      close_ratio_by_money: 0.0\n"
                "      close_ratio_by_volume: 1.0\n"
                "      close_today_ratio_by_money: 0.0\n"
                "      close_today_ratio_by_volume: 1.0\n";
    yaml_out.close();

    std::string resolved_path;
    std::string error;
    EXPECT_TRUE(detail::ResolveBacktestProductConfigPath((root / "instrument_info.json").string(),
                                                         "", &resolved_path, &error))
        << error;
    EXPECT_EQ(std::filesystem::path(resolved_path).lexically_normal(),
              yaml_path.lexically_normal());

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST(BacktestReplaySupportTest,
     ResolveBacktestProductConfigPathUsesMainConfigSiblingWhenPathNotSpecified) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto root =
        std::filesystem::temp_directory_path() /
        ("quant_hft_product_cfg_default_test_" + std::to_string(stamp));
    std::filesystem::create_directories(root);

    const auto main_cfg = root / "main_backtest_strategy.yaml";
    std::ofstream main_out(main_cfg);
    main_out << "run_type: backtest\n";
    main_out.close();

    const auto json_path = root / "instrument_info.json";
    std::ofstream json_out(json_path);
    json_out << "{\n"
                "  \"rb\": {\n"
                "    \"product\": \"rb\",\n"
                "    \"volume_multiple\": 10,\n"
                "    \"long_margin_ratio\": 0.16,\n"
                "    \"short_margin_ratio\": 0.16,\n"
                "    \"commission\": {\n"
                "      \"open_ratio_by_money\": 0.0,\n"
                "      \"open_ratio_by_volume\": 1.0,\n"
                "      \"close_ratio_by_money\": 0.0,\n"
                "      \"close_ratio_by_volume\": 1.0,\n"
                "      \"close_today_ratio_by_money\": 0.0,\n"
                "      \"close_today_ratio_by_volume\": 1.0\n"
                "    }\n"
                "  }\n"
                "}\n";
    json_out.close();

    std::string resolved_path;
    std::string error;
    EXPECT_TRUE(detail::ResolveBacktestProductConfigPath("", main_cfg.string(), &resolved_path,
                                                         &error))
        << error;
    const auto resolved_normalized = std::filesystem::path(resolved_path).lexically_normal();
    const auto sibling_normalized = json_path.lexically_normal();
    const auto default_normalized =
        std::filesystem::path("configs/strategies/instrument_info.json").lexically_normal();
    EXPECT_TRUE(resolved_normalized == sibling_normalized ||
                resolved_normalized == default_normalized);

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
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
    const auto dt_values = ReadStringColumnFromParquet(trace_path, "dt_utc");
    ASSERT_FALSE(dt_values.empty());
    EXPECT_TRUE(IsMinutePrecisionDateTime(dt_values.front()));
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
    const auto dt_values = ReadStringColumnFromParquet(trace_path, "dt_utc");
    ASSERT_FALSE(dt_values.empty());
    EXPECT_TRUE(IsMinutePrecisionDateTime(dt_values.front()));
#else
    EXPECT_FALSE(RunBacktestSpec(spec, &result, &error));
    EXPECT_NE(error.find("QUANT_HFT_ENABLE_ARROW_PARQUET=ON"), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(trace_path));
#endif

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove(trace_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecCompositeTraceCarriesSubscribedTimeframe) {
#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    GTEST_SKIP() << "Arrow parquet writer is disabled in this build";
#else
    const std::filesystem::path csv_path =
        WriteMultiMinuteReplayCsv("quant_hft_trace_timeframe_composite");
    const std::filesystem::path composite_path =
        WriteTempCompositeConfig(/*volume=*/1, /*timeframe_minutes=*/5);
    const std::filesystem::path indicator_trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_trace_timeframe_indicator.parquet";
    const std::filesystem::path sub_trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_trace_timeframe_sub.parquet";
    std::error_code ec;
    std::filesystem::remove(indicator_trace_path, ec);
    std::filesystem::remove(sub_trace_path, ec);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "trace-timeframe-composite-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.emit_indicator_trace = true;
    spec.indicator_trace_path = indicator_trace_path.string();
    spec.emit_sub_strategy_indicator_trace = true;
    spec.sub_strategy_indicator_trace_path = sub_trace_path.string();
    spec.max_ticks = 8;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_GT(result.indicator_trace_rows, 0);
    ASSERT_GT(result.sub_strategy_indicator_trace_rows, 0);

    const auto indicator_timeframes =
        ReadInt32ColumnFromParquet(indicator_trace_path, "timeframe_minutes");
    ASSERT_FALSE(indicator_timeframes.empty());
    for (const std::int32_t timeframe : indicator_timeframes) {
        EXPECT_EQ(timeframe, 5);
    }

    const auto sub_timeframes = ReadInt32ColumnFromParquet(sub_trace_path, "timeframe_minutes");
    ASSERT_FALSE(sub_timeframes.empty());
    for (const std::int32_t timeframe : sub_timeframes) {
        EXPECT_EQ(timeframe, 5);
    }

    const auto sub_dt_values = ReadStringColumnFromParquet(sub_trace_path, "dt_utc");
    ASSERT_FALSE(sub_dt_values.empty());
    EXPECT_EQ(sub_dt_values.front(), "2024-01-02 09:00");

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove(indicator_trace_path, ec);
    std::filesystem::remove(sub_trace_path, ec);
#endif
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
    EXPECT_GT(result.monte_carlo.simulations, 0);
    EXPECT_TRUE(std::isfinite(result.monte_carlo.mean_final_capital));
    EXPECT_FALSE(result.factor_exposure.empty());
    for (const FactorExposure& row : result.factor_exposure) {
        EXPECT_FALSE(row.factor.empty());
        EXPECT_TRUE(std::isfinite(row.exposure));
        EXPECT_TRUE(std::isfinite(row.t_stat));
    }

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecForceCloseWindowUsesFirstEligibleTickAndBlocksReopen) {
    const std::string strategy_type = UniqueAtomicType("always_open_force_close");
    RegisterAlwaysOpenReplayType(strategy_type);
    const std::filesystem::path csv_path =
        WriteForceCloseWindowReplayCsv("quant_hft_force_close_window");
    const std::filesystem::path composite_path =
        WriteForceCloseWindowCompositeConfig(strategy_type, "09:01-09:04");

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "force-close-window-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);

    const EpochNanos expected_force_close_ts = detail::ToEpochNs("20240103", "09:01:30", 0);
    const EpochNanos window_end_ts = detail::ToEpochNs("20240103", "09:04:00", 0);

    std::size_t force_close_count = 0;
    bool saw_initial_open = false;
    for (const TradeRecord& trade : result.trades) {
        if (trade.signal_type == "kOpen" &&
            trade.timestamp_ns == detail::ToEpochNs("20240103", "09:00:30", 0)) {
            saw_initial_open = true;
        }
        if (trade.signal_type == "kForceClose") {
            ++force_close_count;
            EXPECT_EQ(trade.timestamp_ns, expected_force_close_ts);
            EXPECT_DOUBLE_EQ(trade.price, 103.0);
            EXPECT_EQ(trade.side, "Sell");
            EXPECT_EQ(trade.offset, "Close");
        }
        if (trade.signal_type == "kOpen" && trade.timestamp_ns > expected_force_close_ts &&
            trade.timestamp_ns < window_end_ts) {
            ADD_FAILURE() << "unexpected reopen inside force close window at "
                          << trade.timestamp_ns;
        }
    }

    EXPECT_TRUE(saw_initial_open);
    EXPECT_EQ(force_close_count, 1U);

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecAssignsMonotonicFillAndOrderSequences) {
    const std::string strategy_type = UniqueAtomicType("always_open_fill_seq");
    RegisterAlwaysOpenReplayType(strategy_type);
    const std::filesystem::path csv_path =
        WriteForceCloseWindowReplayCsv("quant_hft_fill_order_sequence");
    const std::filesystem::path composite_path =
        WriteForceCloseWindowCompositeConfig(strategy_type, "09:01-09:04");

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "fill-order-sequence-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_GE(result.orders.size(), 4U);
    ASSERT_GE(result.trades.size(), 2U);

    for (std::size_t i = 0; i < result.orders.size(); ++i) {
        EXPECT_EQ(result.orders[i].order_seq, static_cast<std::int64_t>(i + 1))
            << "order index=" << i;
    }
    for (std::size_t i = 0; i < result.trades.size(); ++i) {
        EXPECT_EQ(result.trades[i].fill_seq, static_cast<std::int64_t>(i + 1))
            << "trade index=" << i;
    }

    const std::string json = RenderBacktestJson(result);
    const std::size_t order_seq_1 = json.find("\"order_seq\":1");
    const std::size_t order_seq_2 = json.find("\"order_seq\":2");
    const std::size_t fill_seq_1 = json.find("\"fill_seq\":1");
    const std::size_t fill_seq_2 = json.find("\"fill_seq\":2");
    ASSERT_NE(order_seq_1, std::string::npos);
    ASSERT_NE(order_seq_2, std::string::npos);
    ASSERT_NE(fill_seq_1, std::string::npos);
    ASSERT_NE(fill_seq_2, std::string::npos);
    EXPECT_LT(order_seq_1, order_seq_2);
    EXPECT_LT(fill_seq_1, fill_seq_2);

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
        "    commission:\n"
        "      open_ratio_by_money: 0.001\n"
        "      open_ratio_by_volume: 0\n"
        "      close_ratio_by_money: 0\n"
        "      close_ratio_by_volume: 0.1\n"
        "      close_today_ratio_by_money: 0\n"
        "      close_today_ratio_by_volume: 0.1\n");

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
        "    commission:\n"
        "      open_ratio_by_money: 0.001\n"
        "      open_ratio_by_volume: 0\n"
        "      close_ratio_by_money: 0.001\n"
        "      close_ratio_by_volume: 0\n"
        "      close_today_ratio_by_money: 0.001\n"
        "      close_today_ratio_by_volume: 0\n");

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
        "    commission:\n"
        "      open_ratio_by_money: 0.001\n"
        "      open_ratio_by_volume: 0\n"
        "      close_ratio_by_money: 0\n"
        "      close_ratio_by_volume: 0.1\n"
        "      close_today_ratio_by_money: 0\n"
        "      close_today_ratio_by_volume: 0.1\n");

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
        "    commission:\n"
        "      open_ratio_by_money: 0.0001\n"
        "      open_ratio_by_volume: 0\n"
        "      close_ratio_by_money: 0.0001\n"
        "      close_ratio_by_volume: 0\n"
        "      close_today_ratio_by_money: 0.0001\n"
        "      close_today_ratio_by_volume: 0\n");

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
    EXPECT_GE(result.deterministic.performance.margin_clipped_orders, 1);
    EXPECT_GE(result.deterministic.performance.margin_rejected_orders, 1);
    EXPECT_EQ(result.deterministic.performance.final_margin_used, 0.0);
    EXPECT_EQ(result.deterministic.performance.max_margin_used, 0.0);
    const auto it = result.deterministic.performance.order_status_counts.find("REJECTED");
    ASSERT_NE(it, result.deterministic.performance.order_status_counts.end());
    EXPECT_EQ(static_cast<int>(it->second),
              result.deterministic.performance.margin_rejected_orders);

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
        "    commission:\n"
        "      open_ratio_by_money: 0.0001\n"
        "      open_ratio_by_volume: 0\n"
        "      close_ratio_by_money: 0.0001\n"
        "      close_ratio_by_volume: 0\n"
        "      close_today_ratio_by_money: 0.0001\n"
        "      close_today_ratio_by_volume: 0\n");

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

TEST(BacktestReplaySupportTest, ToEpochNsMapsNightSessionToPreviousCalendarDay) {
    const EpochNanos night_ts = detail::ToEpochNs("20240103", "20:59:00", 2);
    const EpochNanos day_ts = detail::ToEpochNs("20240103", "09:00:00", 0);
    ASSERT_GT(night_ts, 0);
    ASSERT_GT(day_ts, 0);

    EXPECT_EQ(detail::TradingDayFromEpochNs(night_ts), "20240102");
    EXPECT_EQ(detail::UpdateTimeFromEpochNs(night_ts), "20:59:00");
    EXPECT_EQ(night_ts % 1'000'000'000LL, 2'000'000LL);
    EXPECT_LT(night_ts, day_ts);
}

TEST(BacktestReplaySupportTest, ToEpochNsKeepsDaySessionOnTradingDay) {
    const EpochNanos day_ts = detail::ToEpochNs("20240103", "09:00:00", 0);
    ASSERT_GT(day_ts, 0);
    EXPECT_EQ(detail::TradingDayFromEpochNs(day_ts), "20240103");
    EXPECT_EQ(detail::UpdateTimeFromEpochNs(day_ts), "09:00:00");
}

TEST(BacktestReplaySupportTest, DeriveActionDayFromTradingDayAndUpdateTimeHandlesNightSession) {
    EXPECT_EQ(detail::DeriveActionDayFromTradingDayAndUpdateTime("20260223", "21:05:00"),
              "20260222");
    EXPECT_EQ(detail::DeriveActionDayFromTradingDayAndUpdateTime("20260223", "20:00:00"),
              "20260222");
    EXPECT_EQ(detail::DeriveActionDayFromTradingDayAndUpdateTime("20260223", "19:59:59"),
              "20260223");
    EXPECT_EQ(detail::DeriveActionDayFromTradingDayAndUpdateTime("20260223", "00:05:00"),
              "20260223");
}

TEST(BacktestReplaySupportTest, ReplayBarContextTracksInterleavedInstrumentsWithoutDuplication) {
    ReplayTick a_first;
    a_first.instrument_id = "ag2406";
    a_first.trading_day = "20260223";
    a_first.update_time = "09:00:01";
    a_first.ts_ns = 100;
    a_first.last_price = 100.0;
    a_first.volume = 10;

    ReplayTick b_first = a_first;
    b_first.instrument_id = "rb2405";
    b_first.ts_ns = 110;
    b_first.last_price = 200.0;

    ReplayTick a_last = a_first;
    a_last.update_time = "09:00:45";
    a_last.ts_ns = 120;
    a_last.last_price = 101.0;
    a_last.volume = 12;

    ReplayTick b_last = b_first;
    b_last.update_time = "09:00:55";
    b_last.ts_ns = 130;
    b_last.last_price = 201.0;
    b_last.volume = 11;

    std::unordered_map<std::string, detail::ReplayBarTickContext> contexts;
    std::string error;
    ASSERT_TRUE(detail::TrackReplayBarTickContext(a_first, &contexts, &error)) << error;
    ASSERT_TRUE(detail::TrackReplayBarTickContext(b_first, &contexts, &error)) << error;
    ASSERT_TRUE(detail::TrackReplayBarTickContext(a_last, &contexts, &error)) << error;
    ASSERT_TRUE(detail::TrackReplayBarTickContext(b_last, &contexts, &error)) << error;
    EXPECT_EQ(contexts.size(), 2U);

    BarSnapshot a_bar;
    a_bar.instrument_id = "ag2406";
    a_bar.minute = "20260223 09:00";
    BarSnapshot b_bar;
    b_bar.instrument_id = "rb2405";
    b_bar.minute = "20260223 09:00";

    detail::ReplayBarTickContext a_context;
    detail::ReplayBarTickContext b_context;
    ASSERT_TRUE(detail::ConsumeReplayBarTickContext(a_bar, &contexts, &a_context, &error)) << error;
    ASSERT_TRUE(detail::ConsumeReplayBarTickContext(b_bar, &contexts, &b_context, &error)) << error;
    EXPECT_EQ(a_context.first_tick.ts_ns, 100);
    EXPECT_EQ(a_context.last_tick.ts_ns, 120);
    EXPECT_EQ(b_context.first_tick.ts_ns, 110);
    EXPECT_EQ(b_context.last_tick.ts_ns, 130);
    EXPECT_TRUE(contexts.empty());

    EXPECT_FALSE(detail::ConsumeReplayBarTickContext(a_bar, &contexts, &a_context, &error));
    EXPECT_NE(error.find("missing replay bar context"), std::string::npos);
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
