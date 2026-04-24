#include "quant_hft/apps/backtest_replay_support.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

#if QUANT_HFT_ENABLE_ARROW_PARQUET
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#endif

#include "../backtest/tick_partition_fixture.h"
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

std::vector<std::string> ReadLines(const std::filesystem::path& path) {
    std::vector<std::string> lines;
    std::ifstream input(path);
    std::string line;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        lines.push_back(line);
    }
    return lines;
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

std::filesystem::path WriteInstrumentSwitchReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,ts_ns,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1\n";
    out << "rb2405,1704186000000000000,100,100,99,20,101,18\n";
    out << "rb2405,1704186001000000000,101,101,100,21,102,19\n";
    out << "rb2405,1704186060000000000,102,102,101,22,103,20\n";
    out << "rb2405,1704186061000000000,103,103,102,23,104,21\n";
    out << "rb2409,1704186300000000000,200,200,199,20,201,18\n";
    out << "rb2409,1704186301000000000,201,201,200,21,202,19\n";
    out << "rb2409,1704186360000000000,202,202,201,22,203,20\n";
    out << "rb2409,1704186361000000000,203,203,202,23,204,21\n";
    out << "rb2409,1704186420000000000,204,204,203,24,205,22\n";
    out << "rb2409,1704186421000000000,205,205,204,25,206,23\n";
    out << "rb2409,1704186480000000000,206,206,205,26,207,24\n";
    out << "rb2409,1704186481000000000,207,207,206,27,208,25\n";
    out << "rb2409,1704186540000000000,208,208,207,28,209,26\n";
    out << "rb2409,1704186541000000000,209,209,208,29,210,27\n";
    out << "rb2409,1704186600000000000,210,210,209,30,211,28\n";
    out << "rb2409,1704186601000000000,211,211,210,31,212,29\n";
    out.close();
    return path;
}

std::filesystem::path WriteNightSessionReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,TradingDay,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,"
           "BidVolume1,AskPrice1,AskVolume1\n";
    out << "rb2405,20240103,21:00:00,0,100,100,99,20,101,18\n";
    out << "rb2405,20240103,21:00:01,0,101,101,100,21,102,19\n";
    out << "rb2405,20240103,21:01:00,0,102,102,101,22,103,20\n";
    out << "rb2405,20240103,21:01:01,0,103,103,102,23,104,21\n";
    out.close();
    return path;
}

std::filesystem::path WriteCarryDailyReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,TradingDay,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,"
           "BidVolume1,AskPrice1,AskVolume1\n";
    out << "c2405,20240102,09:00:00,0,100,100,99,20,101,18\n";
    out << "c2405,20240102,09:00:30,0,101,101,100,21,102,19\n";
    out << "c2405,20240102,09:01:00,0,102,102,101,22,103,20\n";
    out << "c2405,20240102,09:01:30,0,103,103,102,23,104,21\n";
    out << "c2405,20240102,14:59:00,0,110,104,109,24,111,22\n";
    out << "c2405,20240102,14:59:30,0,111,105,110,25,112,23\n";
    out << "c2405,20240103,09:00:00,0,112,106,111,26,113,24\n";
    out << "c2405,20240103,09:00:30,0,113,107,112,27,114,25\n";
    out << "c2405,20240103,14:59:00,0,114,108,113,28,115,26\n";
    out << "c2405,20240103,14:59:30,0,115,109,114,29,116,27\n";
    out.close();
    return path;
}

std::filesystem::path WriteLateOpenReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,TradingDay,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,"
           "BidVolume1,AskPrice1,AskVolume1\n";
    out << "c2405,20240102,14:58:00,0,100,100,99,20,101,18\n";
    out << "c2405,20240102,14:58:30,0,101,101,100,21,102,19\n";
    out << "c2405,20240102,14:59:00,0,102,102,101,22,103,20\n";
    out << "c2405,20240102,14:59:30,0,103,103,102,23,104,21\n";
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

std::vector<std::string> SplitCsvLine(const std::string& line) {
    std::vector<std::string> fields;
    std::stringstream stream(line);
    std::string field;
    while (std::getline(stream, field, ',')) {
        fields.push_back(field);
    }
    return fields;
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

class OpenOnceReplayStrategy final : public ISubStrategy {
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
        const auto pos_it = ctx.net_positions.find(state.instrument_id);
        const std::int32_t position = pos_it == ctx.net_positions.end() ? 0 : pos_it->second;
        if (opened_ || position != 0) {
            return {};
        }

        opened_ = true;
        SignalIntent signal;
        signal.strategy_id = id_;
        signal.instrument_id = state.instrument_id;
        signal.signal_type = SignalType::kOpen;
        signal.side = side_;
        signal.offset = OffsetFlag::kOpen;
        signal.volume = volume_;
        signal.limit_price = state.bar_close;
        signal.ts_ns = state.ts_ns;
        signal.trace_id = id_ + "-open-once";
        return {signal};
    }

   private:
    std::string id_{"open_once"};
    std::int32_t volume_{1};
    Side side_{Side::kBuy};
    bool opened_{false};
};

class OpenOnceThenTickStopReplayStrategy final : public ISubStrategy,
                                                 public IAtomicBacktestTickAware {
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
        const auto stop_it = params.find("stop_trigger_price");
        if (stop_it != params.end() && !stop_it->second.empty()) {
            stop_trigger_price_ = std::stod(stop_it->second);
        }
    }

    std::string GetId() const override { return id_; }

    void Reset() override {
        opened_ = false;
        stop_emitted_ = false;
    }

    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        const auto pos_it = ctx.net_positions.find(state.instrument_id);
        const std::int32_t position = pos_it == ctx.net_positions.end() ? 0 : pos_it->second;
        if (opened_ || position != 0) {
            return {};
        }

        opened_ = true;
        SignalIntent signal;
        signal.strategy_id = id_;
        signal.instrument_id = state.instrument_id;
        signal.signal_type = SignalType::kOpen;
        signal.side = Side::kBuy;
        signal.offset = OffsetFlag::kOpen;
        signal.volume = volume_;
        signal.limit_price = state.bar_close;
        signal.ts_ns = state.ts_ns;
        signal.trace_id = id_ + "-open-once";
        return {signal};
    }

    std::vector<SignalIntent> OnBacktestTick(const AtomicTickSnapshot& tick,
                                             const AtomicStrategyContext& ctx) override {
        const auto pos_it = ctx.net_positions.find(tick.instrument_id);
        const std::int32_t position = pos_it == ctx.net_positions.end() ? 0 : pos_it->second;
        if (stop_emitted_ || position <= 0 || tick.last_price < stop_trigger_price_) {
            return {};
        }

        stop_emitted_ = true;
        SignalIntent signal;
        signal.strategy_id = id_;
        signal.instrument_id = tick.instrument_id;
        signal.signal_type = SignalType::kStopLoss;
        signal.side = Side::kSell;
        signal.offset = OffsetFlag::kClose;
        signal.volume = position;
        signal.limit_price = tick.last_price;
        signal.ts_ns = tick.ts_ns;
        signal.trace_id = id_ + "-tick-stop";
        return {signal};
    }

   private:
    std::string id_{"open_once_tick_stop"};
    std::int32_t volume_{1};
    double stop_trigger_price_{104.0};
    bool opened_{false};
    bool stop_emitted_{false};
};

class OpenOnFirstContractThenCloseOnRolloverStrategy final : public ISubStrategy {
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
    }

    std::string GetId() const override { return id_; }

    void Reset() override {
        initial_instrument_.clear();
        opened_initial_ = false;
        closed_after_rollover_ = false;
    }

    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        if (initial_instrument_.empty()) {
            initial_instrument_ = state.instrument_id;
        }

        const auto pos_it = ctx.net_positions.find(state.instrument_id);
        const std::int32_t position = pos_it == ctx.net_positions.end() ? 0 : pos_it->second;

        SignalIntent signal;
        signal.strategy_id = id_;
        signal.instrument_id = state.instrument_id;
        signal.limit_price = state.bar_close;
        signal.ts_ns = state.ts_ns;

        if (!opened_initial_ && state.instrument_id == initial_instrument_ && position == 0) {
            opened_initial_ = true;
            signal.signal_type = SignalType::kOpen;
            signal.side = Side::kBuy;
            signal.offset = OffsetFlag::kOpen;
            signal.volume = volume_;
            signal.trace_id = id_ + "-open";
            return {signal};
        }

        if (!closed_after_rollover_ && state.instrument_id != initial_instrument_ && position > 0) {
            closed_after_rollover_ = true;
            signal.signal_type = SignalType::kClose;
            signal.side = Side::kSell;
            signal.offset = OffsetFlag::kClose;
            signal.volume = volume_;
            signal.trace_id = id_ + "-close";
            return {signal};
        }

        return {};
    }

   private:
    std::string id_{"rollover_close_after_transfer"};
    std::string initial_instrument_;
    std::int32_t volume_{1};
    bool opened_initial_{false};
    bool closed_after_rollover_{false};
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
    ASSERT_TRUE(factory.Register(
        type, []() { return std::make_unique<AlwaysOpenReplayStrategy>(); }, &error))
        << error;
}

void RegisterOpenOnceReplayType(const std::string& type) {
    AtomicFactory& factory = AtomicFactory::Instance();
    if (factory.Has(type)) {
        return;
    }
    std::string error;
    ASSERT_TRUE(factory.Register(
        type, []() { return std::make_unique<OpenOnceReplayStrategy>(); }, &error))
        << error;
}

void RegisterOpenOnceThenTickStopReplayType(const std::string& type) {
    AtomicFactory& factory = AtomicFactory::Instance();
    if (factory.Has(type)) {
        return;
    }
    std::string error;
    ASSERT_TRUE(factory.Register(
        type, []() { return std::make_unique<OpenOnceThenTickStopReplayStrategy>(); }, &error))
        << error;
}

void RegisterOpenThenCloseReplayType(const std::string& type) {
    AtomicFactory& factory = AtomicFactory::Instance();
    if (factory.Has(type)) {
        return;
    }
    std::string error;
    ASSERT_TRUE(factory.Register(
        type, []() { return std::make_unique<OpenOnFirstContractThenCloseOnRolloverStrategy>(); },
        &error))
        << error;
}

std::filesystem::path WriteForceCloseWindowCompositeConfig(
    const std::string& strategy_type, const std::string& force_close_windows,
    const std::string& window_timezone = "UTC") {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
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
    out << "        window_timezone: " << window_timezone << "\n";
    out.close();
    return path;
}

std::filesystem::path WriteTickStopCompositeConfig(const std::string& strategy_type,
                                                   double stop_trigger_price) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_tick_stop_composite_test_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << "composite:\n";
    out << "  merge_rule: kPriority\n";
    out << "  sub_strategies:\n";
    out << "    - id: tick_stop\n";
    out << "      enabled: true\n";
    out << "      timeframe_minutes: 1\n";
    out << "      type: " << strategy_type << "\n";
    out << "      params:\n";
    out << "        id: tick_stop\n";
    out << "        volume: 1\n";
    out << "        stop_trigger_price: " << stop_trigger_price << "\n";
    out.close();
    return path;
}

std::filesystem::path WriteAlwaysOpenCompositeConfig(const std::string& strategy_type,
                                                     int timeframe_minutes) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_always_open_composite_test_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << "composite:\n";
    out << "  merge_rule: kPriority\n";
    out << "  sub_strategies:\n";
    out << "    - id: always_open\n";
    out << "      enabled: true\n";
    out << "      timeframe_minutes: " << timeframe_minutes << "\n";
    out << "      type: " << strategy_type << "\n";
    out << "      params:\n";
    out << "        id: always_open\n";
    out << "        volume: 1\n";
    out.close();
    return path;
}

std::filesystem::path MakeTempDir(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto dir = std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp));
    std::filesystem::create_directories(dir);
    return dir;
}

Tick MakeParquetTick(const std::string& instrument_id, const std::string& trading_day,
                     const std::string& update_time, double last_price, std::int64_t volume) {
    Tick tick;
    tick.symbol = instrument_id;
    tick.exchange = "DCE";
    tick.ts_ns = detail::ToEpochNs(trading_day, update_time, 0);
    tick.last_price = last_price;
    tick.last_volume = 1;
    tick.bid_price1 = last_price - 1.0;
    tick.bid_volume1 = 10;
    tick.ask_price1 = last_price + 1.0;
    tick.ask_volume1 = 10;
    tick.volume = volume;
    tick.turnover = last_price * static_cast<double>(volume);
    tick.open_interest = volume;
    return tick;
}

std::filesystem::path WriteParquetManifest(
    const std::filesystem::path& dataset_root,
    const std::vector<std::tuple<std::string, std::string, std::string, std::vector<Tick>>>&
        partitions,
    std::string* error) {
    const auto manifest = dataset_root / "_manifest" / "partitions.jsonl";
    std::filesystem::create_directories(manifest.parent_path());

    std::ofstream out(manifest);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "unable to open manifest for write: " + manifest.string();
        }
        return {};
    }

    for (const auto& [source, trading_day, instrument_id, ticks] : partitions) {
        if (ticks.empty()) {
            if (error != nullptr) {
                *error = "parquet partition fixture requires ticks";
            }
            return {};
        }

        const std::filesystem::path relative_path =
            std::filesystem::path("source=" + source) / ("trading_day=" + trading_day) /
            ("instrument_id=" + instrument_id) / "part-0000.parquet";
        const std::filesystem::path parquet_path = dataset_root / relative_path;

        if (!quant_hft::backtest::test::WriteTickPartitionFixture(parquet_path, ticks, error)) {
            return {};
        }

        auto [min_it, max_it] = std::minmax_element(
            ticks.begin(), ticks.end(),
            [](const Tick& left, const Tick& right) { return left.ts_ns < right.ts_ns; });
        {
            std::ofstream meta(parquet_path.string() + ".meta", std::ios::out | std::ios::trunc);
            if (!meta.is_open()) {
                if (error != nullptr) {
                    *error = "unable to open parquet meta for write: " + parquet_path.string();
                }
                return {};
            }
            meta << "min_ts_ns=" << min_it->ts_ns << '\n';
            meta << "max_ts_ns=" << max_it->ts_ns << '\n';
            meta << "row_count=" << ticks.size() << '\n';
            meta << "schema_version=v3\n";
            meta << "source_csv_fingerprint=test-fixture\n";
            if (!meta.good()) {
                if (error != nullptr) {
                    *error = "failed to write parquet meta: " + parquet_path.string();
                }
                return {};
            }
        }
        out << "{\"file_path\":\"" << relative_path.generic_string() << "\","
            << "\"source\":\"" << source << "\","
            << "\"trading_day\":\"" << trading_day << "\","
            << "\"instrument_id\":\"" << instrument_id << "\","
            << "\"min_ts_ns\":" << min_it->ts_ns << ',' << "\"max_ts_ns\":" << max_it->ts_ns << ','
            << "\"row_count\":" << ticks.size() << "}\n";
    }

    if (!out.good()) {
        if (error != nullptr) {
            *error = "failed to write manifest: " + manifest.string();
        }
        return {};
    }
    return manifest;
}

std::int32_t LastNetPosition(const std::vector<PositionSnapshot>& rows, const std::string& symbol) {
    std::int32_t last = 0;
    for (const PositionSnapshot& row : rows) {
        if (row.symbol == symbol) {
            last = row.net_position;
        }
    }
    return last;
}

std::filesystem::path WriteForceCloseWindowReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "InstrumentID,ts_ns,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1\n";
    const std::vector<std::pair<std::string, double>> ticks = {
        {"09:00:00", 100.0}, {"09:00:30", 101.0}, {"09:01:05", 102.0}, {"09:01:30", 103.0},
        {"09:02:05", 104.0}, {"09:02:30", 105.0}, {"09:03:05", 106.0}, {"09:03:30", 107.0},
        {"09:04:05", 108.0}, {"09:04:30", 109.0},
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

std::filesystem::path WriteDeferredBarReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "TradingDay,UpdateTime,InstrumentID,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,"
           "AskVolume1\n";
    const std::vector<std::pair<std::string, double>> ticks = {
        {"09:55:05", 100.0}, {"09:56:05", 101.0}, {"09:57:05", 102.0},
        {"09:58:05", 103.0}, {"09:59:05", 104.0}, {"10:00:05", 105.0},
        {"10:00:30", 106.0}, {"10:01:05", 107.0}, {"10:01:30", 108.0},
    };
    std::int64_t volume = 100;
    for (const auto& [update_time, last_price] : ticks) {
        out << "20240110," << update_time << ",rb2405," << last_price << "," << volume << ","
            << (last_price - 1.0) << ",20," << (last_price + 1.0) << ",18\n";
        ++volume;
    }
    out.close();
    return path;
}

std::filesystem::path WriteCrossSessionPendingReplayCsv(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + ".csv");
    std::ofstream out(path);
    out << "TradingDay,UpdateTime,InstrumentID,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,"
           "AskVolume1\n";
    const std::vector<std::pair<std::string, double>> ticks = {
        {"22:55:05", 100.0}, {"22:56:05", 101.0}, {"22:57:05", 102.0}, {"22:58:05", 103.0},
        {"22:59:05", 104.0}, {"09:00:05", 105.0}, {"09:00:30", 106.0},
    };
    std::int64_t volume = 100;
    for (const auto& [update_time, last_price] : ticks) {
        out << "20240110," << update_time << ",c2405," << last_price << "," << volume << ","
            << (last_price - 1.0) << ",20," << (last_price + 1.0) << ",18\n";
        ++volume;
    }
    out.close();
    return path;
}

std::filesystem::path WriteTempMainStrategyConfig(
    const std::filesystem::path& composite_path, const std::string& run_type = "backtest",
    const std::string& contract_expiry_calendar_path = "", bool risk_management_enabled = false,
    double risk_per_trade_pct = 0.005, double max_risk_per_trade = 2000.0) {
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
    if (!contract_expiry_calendar_path.empty()) {
        out << "  contract_expiry_calendar_path: " << contract_expiry_calendar_path << "\n";
    }
    out << "risk_management:\n";
    out << "  enabled: " << (risk_management_enabled ? "true" : "false") << "\n";
    out << "  risk_per_trade_pct: " << risk_per_trade_pct << "\n";
    out << "  max_risk_per_trade: " << max_risk_per_trade << "\n";
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

std::filesystem::path WriteTempContractExpiryCalendarConfig(const std::string& content) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_contract_expiry_calendar_" + std::to_string(stamp) + ".yaml");
    std::ofstream out(path);
    out << content;
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
    EXPECT_DOUBLE_EQ(state.analysis_bar_open, 100.0);
    EXPECT_DOUBLE_EQ(state.analysis_bar_high, 106.0);
    EXPECT_DOUBLE_EQ(state.analysis_bar_low, 99.0);
    EXPECT_DOUBLE_EQ(state.analysis_bar_close, 105.0);
    EXPECT_DOUBLE_EQ(state.analysis_price_offset, 0.0);
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

        state = BuildStateSnapshotFromBar(first, last, first.last_price + 1.0,
                                          first.last_price - 1.0, 1, last.ts_ns, 1, &detector);
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

TEST(BacktestReplaySupportTest,
     RunBacktestSpecParquetProductSymbolStrictRolloverGeneratesSyntheticTrades) {
    const std::string strategy_type = UniqueAtomicType("always_open_parquet_strict_rollover");
    RegisterAlwaysOpenReplayType(strategy_type);

    const auto dataset_root = MakeTempDir("quant_hft_parquet_strict_rollover");
    const auto composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/1);
    const auto atomic_cfg = WriteTempAtomicStrategyConfig();
    const auto main_cfg = WriteTempMainStrategyConfig(atomic_cfg, "backtest", "",
                                                      /*risk_management_enabled=*/true,
                                                      /*risk_per_trade_pct=*/0.01,
                                                      /*max_risk_per_trade=*/2000.0);

    std::string error;
    const auto manifest =
        WriteParquetManifest(dataset_root,
                             {
                                 {"c",
                                  "20240102",
                                  "c2405",
                                  {
                                      MakeParquetTick("c2405", "20240102", "09:00:00", 100.0, 10),
                                      MakeParquetTick("c2405", "20240102", "09:00:30", 101.0, 11),
                                      MakeParquetTick("c2405", "20240102", "09:01:00", 102.0, 12),
                                      MakeParquetTick("c2405", "20240102", "09:01:30", 103.0, 13),
                                  }},
                                 {"c",
                                  "20240103",
                                  "c2407",
                                  {
                                      MakeParquetTick("c2407", "20240103", "08:59:00", 119.0, 19),
                                      MakeParquetTick("c2407", "20240103", "09:00:00", 120.0, 20),
                                      MakeParquetTick("c2407", "20240103", "09:00:30", 121.0, 21),
                                      MakeParquetTick("c2407", "20240103", "09:01:00", 122.0, 22),
                                      MakeParquetTick("c2407", "20240103", "09:01:30", 123.0, 23),
                                  }},
                             },
                             &error);
    ASSERT_FALSE(manifest.empty()) << error;

    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = dataset_root.string();
    spec.dataset_manifest = manifest.string();
    spec.run_id = UniqueRunId("parquet-strict-rollover");
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.strategy_main_config_path = main_cfg.string();
    spec.symbols = {"c"};
    spec.rollover_mode = "strict";
    spec.emit_trades = true;
    spec.emit_orders = true;
    spec.emit_position_history = true;

    BacktestCliResult result;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);
    EXPECT_EQ(result.deterministic.rollover_events.size(), 1U);
    ASSERT_EQ(result.deterministic.rollover_actions.size(), 2U);
    EXPECT_EQ(result.deterministic.rollover_actions[0].action, "close");
    EXPECT_EQ(result.deterministic.rollover_actions[1].action, "open");

    int rollover_trade_count = 0;
    const TradeRecord* rollover_close_trade = nullptr;
    const TradeRecord* rollover_open_trade = nullptr;
    for (const TradeRecord& trade : result.trades) {
        if (trade.strategy_id == "rollover") {
            ++rollover_trade_count;
            if (trade.signal_type == "rollover_close") {
                rollover_close_trade = &trade;
            } else if (trade.signal_type == "rollover_open") {
                rollover_open_trade = &trade;
            }
        }
    }
    EXPECT_EQ(rollover_trade_count, 2);
    ASSERT_NE(rollover_close_trade, nullptr);
    ASSERT_NE(rollover_open_trade, nullptr);
    EXPECT_EQ(rollover_close_trade->trading_day, "20240103");
    EXPECT_EQ(rollover_close_trade->action_day, "20240103");
    EXPECT_EQ(rollover_close_trade->update_time, "09:00:00");
    EXPECT_EQ(rollover_close_trade->timestamp_dt_local, "2024-01-03 09:00:00");
    EXPECT_EQ(rollover_close_trade->signal_dt_local, "2024-01-03 09:00:00");
    EXPECT_EQ(rollover_open_trade->trading_day, "20240103");
    EXPECT_EQ(rollover_open_trade->action_day, "20240103");
    EXPECT_EQ(rollover_open_trade->update_time, "09:00:00");
    EXPECT_EQ(rollover_open_trade->timestamp_dt_local, "2024-01-03 09:00:00");
    EXPECT_EQ(rollover_open_trade->signal_dt_local, "2024-01-03 09:00:00");
    EXPECT_DOUBLE_EQ(rollover_close_trade->risk_budget_r, 0.0);
    EXPECT_GT(rollover_open_trade->risk_budget_r, 0.0);

    int rollover_order_count = 0;
    for (const OrderRecord& order : result.orders) {
        if (order.strategy_id != "rollover") {
            continue;
        }
        ++rollover_order_count;
        EXPECT_EQ(order.trading_day, "20240103");
        EXPECT_EQ(order.action_day, "20240103");
        EXPECT_EQ(order.update_time, "09:00:00");
        EXPECT_EQ(order.created_at_dt_local, "2024-01-03 09:00:00");
        EXPECT_EQ(order.last_update_dt_local, "2024-01-03 09:00:00");
    }
    EXPECT_EQ(rollover_order_count, 4);
    EXPECT_EQ(LastNetPosition(result.position_history, "c2405"), 0);
    EXPECT_EQ(LastNetPosition(result.position_history, "c2407"), 1);

    int non_rollover_open_count = 0;
    for (const TradeRecord& trade : result.trades) {
        if (trade.symbol == "c2407" && trade.strategy_id != "rollover" &&
            trade.signal_type == "kOpen") {
            ++non_rollover_open_count;
        }
    }
    EXPECT_EQ(non_rollover_open_count, 0);

    std::error_code ec;
    std::filesystem::remove(atomic_cfg, ec);
    std::filesystem::remove(main_cfg, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove_all(dataset_root, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecAppliesRiskBudgetToStrategyOpenTrades) {
    const std::string strategy_type = UniqueAtomicType("open_once_risk_budget");
    RegisterOpenOnceReplayType(strategy_type);

    const std::filesystem::path csv_path = WriteFlatReplayCsv("quant_hft_risk_budget_open");
    const std::filesystem::path composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/1);
    const std::filesystem::path atomic_cfg = WriteTempAtomicStrategyConfig();
    const std::filesystem::path main_cfg =
        WriteTempMainStrategyConfig(atomic_cfg, "backtest", "",
                                    /*risk_management_enabled=*/
                                    true,
                                    /*risk_per_trade_pct=*/0.01,
                                    /*max_risk_per_trade=*/400.0);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "risk-budget-open-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.strategy_main_config_path = main_cfg.string();
    spec.initial_equity = 50000.0;
    spec.emit_trades = true;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_EQ(result.trades.size(), 1U);
    EXPECT_EQ(result.trades.front().signal_type, "kOpen");
    EXPECT_DOUBLE_EQ(result.trades.front().risk_budget_r, 400.0);

    std::error_code ec;
    std::filesystem::remove(atomic_cfg, ec);
    std::filesystem::remove(main_cfg, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove(csv_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecStrictRolloverTransfersOwnerToNewContract) {
    const std::string strategy_type = UniqueAtomicType("open_then_close_after_rollover");
    RegisterOpenThenCloseReplayType(strategy_type);

    const auto dataset_root = MakeTempDir("quant_hft_parquet_rollover_owner_transfer");
    const auto composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/1);

    std::string error;
    const auto manifest =
        WriteParquetManifest(dataset_root,
                             {
                                 {"c",
                                  "20240102",
                                  "c2405",
                                  {
                                      MakeParquetTick("c2405", "20240102", "09:00:00", 100.0, 10),
                                      MakeParquetTick("c2405", "20240102", "09:00:30", 101.0, 11),
                                      MakeParquetTick("c2405", "20240102", "09:01:00", 102.0, 12),
                                      MakeParquetTick("c2405", "20240102", "09:01:30", 103.0, 13),
                                  }},
                                 {"c",
                                  "20240103",
                                  "c2407",
                                  {
                                      MakeParquetTick("c2407", "20240103", "08:59:00", 119.0, 19),
                                      MakeParquetTick("c2407", "20240103", "09:00:00", 120.0, 20),
                                      MakeParquetTick("c2407", "20240103", "09:00:30", 121.0, 21),
                                      MakeParquetTick("c2407", "20240103", "09:01:00", 122.0, 22),
                                      MakeParquetTick("c2407", "20240103", "09:01:30", 123.0, 23),
                                  }},
                             },
                             &error);
    ASSERT_FALSE(manifest.empty()) << error;

    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = dataset_root.string();
    spec.dataset_manifest = manifest.string();
    spec.run_id = UniqueRunId("parquet-rollover-owner-transfer");
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.symbols = {"c"};
    spec.rollover_mode = "strict";
    spec.emit_trades = true;
    spec.emit_orders = true;
    spec.emit_position_history = true;

    BacktestCliResult result;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);

    const TradeRecord* rollover_open_trade = nullptr;
    const TradeRecord* transferred_close_trade = nullptr;
    for (const TradeRecord& trade : result.trades) {
        if (trade.signal_type == "rollover_open") {
            rollover_open_trade = &trade;
        }
        if (trade.symbol == "c2407" && trade.strategy_id == "always_open" &&
            trade.signal_type == "kClose") {
            transferred_close_trade = &trade;
        }
    }

    ASSERT_NE(rollover_open_trade, nullptr);
    ASSERT_NE(transferred_close_trade, nullptr);
    EXPECT_EQ(transferred_close_trade->timestamp_dt_local, "2024-01-03 09:01:30");
    EXPECT_DOUBLE_EQ(transferred_close_trade->realized_pnl, 20.0);
    EXPECT_EQ(LastNetPosition(result.position_history, "c2405"), 0);
    EXPECT_EQ(LastNetPosition(result.position_history, "c2407"), 0);

    std::error_code ec;
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove_all(dataset_root, ec);
}

TEST(BacktestReplaySupportTest,
     RunBacktestSpecParquetProductSymbolCarryRolloverTransfersPositionWithoutSyntheticTrades) {
    const std::string strategy_type = UniqueAtomicType("always_open_parquet_carry_rollover");
    RegisterAlwaysOpenReplayType(strategy_type);

    const auto dataset_root = MakeTempDir("quant_hft_parquet_carry_rollover");
    const auto composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/1);

    std::string error;
    const auto manifest =
        WriteParquetManifest(dataset_root,
                             {
                                 {"c",
                                  "20240102",
                                  "c2405",
                                  {
                                      MakeParquetTick("c2405", "20240102", "09:00:00", 100.0, 10),
                                      MakeParquetTick("c2405", "20240102", "09:00:30", 101.0, 11),
                                      MakeParquetTick("c2405", "20240102", "09:01:00", 102.0, 12),
                                      MakeParquetTick("c2405", "20240102", "09:01:30", 103.0, 13),
                                  }},
                                 {"c",
                                  "20240103",
                                  "c2407",
                                  {
                                      MakeParquetTick("c2407", "20240103", "09:00:00", 120.0, 20),
                                      MakeParquetTick("c2407", "20240103", "09:00:30", 121.0, 21),
                                      MakeParquetTick("c2407", "20240103", "09:01:00", 122.0, 22),
                                      MakeParquetTick("c2407", "20240103", "09:01:30", 123.0, 23),
                                  }},
                             },
                             &error);
    ASSERT_FALSE(manifest.empty()) << error;

    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = dataset_root.string();
    spec.dataset_manifest = manifest.string();
    spec.run_id = UniqueRunId("parquet-carry-rollover");
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.symbols = {"c"};
    spec.rollover_mode = "carry";
    spec.emit_trades = true;
    spec.emit_orders = true;
    spec.emit_position_history = true;

    BacktestCliResult result;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);
    EXPECT_EQ(result.deterministic.rollover_events.size(), 1U);
    ASSERT_EQ(result.deterministic.rollover_actions.size(), 1U);
    EXPECT_EQ(result.deterministic.rollover_actions[0].action, "carry");

    int rollover_trade_count = 0;
    for (const TradeRecord& trade : result.trades) {
        if (trade.strategy_id == "rollover") {
            ++rollover_trade_count;
        }
    }
    EXPECT_EQ(rollover_trade_count, 0);
    EXPECT_EQ(LastNetPosition(result.position_history, "c2405"), 0);
    EXPECT_GT(LastNetPosition(result.position_history, "c2407"), 0);

    std::error_code ec;
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove_all(dataset_root, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecParquetExplicitInstrumentDoesNotAutoRollover) {
    const std::string strategy_type = UniqueAtomicType("always_open_parquet_single_contract");
    RegisterAlwaysOpenReplayType(strategy_type);

    const auto dataset_root = MakeTempDir("quant_hft_parquet_single_contract");
    const auto composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/1);

    std::string error;
    const auto manifest =
        WriteParquetManifest(dataset_root,
                             {
                                 {"c",
                                  "20240102",
                                  "c2405",
                                  {
                                      MakeParquetTick("c2405", "20240102", "09:00:00", 100.0, 10),
                                      MakeParquetTick("c2405", "20240102", "09:00:30", 101.0, 11),
                                      MakeParquetTick("c2405", "20240102", "09:01:00", 102.0, 12),
                                      MakeParquetTick("c2405", "20240102", "09:01:30", 103.0, 13),
                                  }},
                                 {"c",
                                  "20240103",
                                  "c2407",
                                  {
                                      MakeParquetTick("c2407", "20240103", "09:00:00", 120.0, 20),
                                      MakeParquetTick("c2407", "20240103", "09:00:30", 121.0, 21),
                                  }},
                             },
                             &error);
    ASSERT_FALSE(manifest.empty()) << error;

    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = dataset_root.string();
    spec.dataset_manifest = manifest.string();
    spec.run_id = UniqueRunId("parquet-single-contract");
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.symbols = {"c2405"};
    spec.rollover_mode = "strict";
    spec.emit_trades = true;
    spec.emit_orders = true;
    spec.emit_position_history = true;

    BacktestCliResult result;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);
    EXPECT_EQ(result.deterministic.rollover_events.size(), 0U);
    EXPECT_EQ(result.deterministic.rollover_actions.size(), 0U);

    int rollover_trade_count = 0;
    for (const TradeRecord& trade : result.trades) {
        if (trade.strategy_id == "rollover") {
            ++rollover_trade_count;
        }
    }
    EXPECT_EQ(rollover_trade_count, 0);
    EXPECT_GT(LastNetPosition(result.position_history, "c2405"), 0);
    EXPECT_EQ(LastNetPosition(result.position_history, "c2407"), 0);

    std::error_code ec;
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove_all(dataset_root, ec);
}

TEST(BacktestReplaySupportTest,
     RunBacktestSpecParquetExpiryCloseUsesOldContractDayOpenAndDoesNotCarryPosition) {
    const std::string strategy_type = UniqueAtomicType("expiry_close_resets_session");
    RegisterOpenThenCloseReplayType(strategy_type);

    const auto dataset_root = MakeTempDir("quant_hft_parquet_expiry_close");
    const auto composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/1);
    const auto calendar_path = WriteTempContractExpiryCalendarConfig(
        "contracts:\n"
        "  c2405:\n"
        "    last_trading_day: 20240103\n"
        "  c2407:\n"
        "    last_trading_day: 20240131\n");
    const auto atomic_cfg = WriteTempAtomicStrategyConfig();
    const auto main_cfg =
        WriteTempMainStrategyConfig(atomic_cfg, "backtest", calendar_path.string(),
                                    /*risk_management_enabled=*/true,
                                    /*risk_per_trade_pct=*/0.01,
                                    /*max_risk_per_trade=*/2000.0);

    std::string error;
    const auto manifest =
        WriteParquetManifest(dataset_root,
                             {
                                 {"c",
                                  "20240102",
                                  "c2405",
                                  {
                                      MakeParquetTick("c2405", "20240102", "09:00:00", 100.0, 10),
                                      MakeParquetTick("c2405", "20240102", "09:00:30", 101.0, 11),
                                      MakeParquetTick("c2405", "20240102", "09:01:00", 102.0, 12),
                                      MakeParquetTick("c2405", "20240102", "09:01:30", 103.0, 13),
                                  }},
                                 {"c",
                                  "20240103",
                                  "c2407",
                                  {
                                      MakeParquetTick("c2407", "20240103", "21:00:00", 119.0, 19),
                                      MakeParquetTick("c2407", "20240103", "21:00:30", 120.0, 20),
                                      MakeParquetTick("c2407", "20240103", "09:01:00", 121.0, 21),
                                      MakeParquetTick("c2407", "20240103", "09:01:30", 122.0, 22),
                                      MakeParquetTick("c2407", "20240103", "09:02:00", 123.0, 23),
                                      MakeParquetTick("c2407", "20240103", "09:02:30", 124.0, 24),
                                  }},
                                 {"c",
                                  "20240103",
                                  "c2405",
                                  {
                                      MakeParquetTick("c2405", "20240103", "09:00:00", 110.0, 14),
                                      MakeParquetTick("c2405", "20240103", "09:00:30", 111.0, 15),
                                  }},
                             },
                             &error);
    ASSERT_FALSE(manifest.empty()) << error;

    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = dataset_root.string();
    spec.dataset_manifest = manifest.string();
    spec.run_id = UniqueRunId("parquet-expiry-close");
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.strategy_main_config_path = main_cfg.string();
    spec.symbols = {"c"};
    spec.rollover_mode = "expiry_close";
    spec.product_series_mode = "raw";
    spec.contract_expiry_calendar_path = calendar_path.string();
    spec.emit_trades = true;
    spec.emit_orders = true;
    spec.emit_position_history = true;

    BacktestCliResult result;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;

    const TradeRecord* expiry_close_trade = nullptr;
    const TradeRecord* reopened_trade = nullptr;
    for (const TradeRecord& trade : result.trades) {
        if (trade.signal_type == "expiry_close") {
            expiry_close_trade = &trade;
        }
        if (trade.symbol == "c2407" && trade.strategy_id == "always_open" &&
            trade.signal_type == "kOpen") {
            reopened_trade = &trade;
        }
        EXPECT_NE(trade.signal_type, "rollover_open");
    }

    ASSERT_NE(expiry_close_trade, nullptr);
    EXPECT_EQ(expiry_close_trade->symbol, "c2405");
    EXPECT_EQ(expiry_close_trade->strategy_id, "always_open");
    EXPECT_EQ(expiry_close_trade->timestamp_dt_local, "2024-01-03 09:00:00");
    EXPECT_EQ(expiry_close_trade->trading_day, "20240103");
    EXPECT_EQ(expiry_close_trade->update_time, "09:00:00");
    EXPECT_DOUBLE_EQ(expiry_close_trade->price, 109.0);
    EXPECT_DOUBLE_EQ(expiry_close_trade->commission, 2.0);
    EXPECT_DOUBLE_EQ(expiry_close_trade->risk_budget_r, 0.0);

    ASSERT_NE(reopened_trade, nullptr);
    EXPECT_EQ(reopened_trade->timestamp_dt_local, "2024-01-03 09:02:30");
    EXPECT_GT(reopened_trade->risk_budget_r, 0.0);

    for (const TradeRecord& trade : result.trades) {
        EXPECT_FALSE(trade.symbol == "c2407" && trade.strategy_id == "always_open" &&
                     trade.signal_type == "kClose");
    }

    EXPECT_EQ(LastNetPosition(result.position_history, "c2405"), 0);
    EXPECT_EQ(LastNetPosition(result.position_history, "c2407"), 1);

    std::error_code ec;
    std::filesystem::remove(atomic_cfg, ec);
    std::filesystem::remove(main_cfg, ec);
    std::filesystem::remove(calendar_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove_all(dataset_root, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecDailyRowsKeepPositionValueAcrossCarryDay) {
    const std::string strategy_type = UniqueAtomicType("open_once_daily_carry");
    RegisterOpenOnceReplayType(strategy_type);
    const std::filesystem::path csv_path = WriteCarryDailyReplayCsv("quant_hft_daily_carry");
    const std::filesystem::path composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/1);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "daily-carry-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.emit_trades = true;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_EQ(result.trades.size(), 1U);

    const DailyPerformance* first_day = nullptr;
    const DailyPerformance* second_day = nullptr;
    for (const DailyPerformance& row : result.daily) {
        if (row.date == "20240102") {
            first_day = &row;
        }
        if (row.date == "20240103") {
            second_day = &row;
        }
    }

    ASSERT_NE(first_day, nullptr);
    ASSERT_NE(second_day, nullptr);
    EXPECT_GT(first_day->position_value, 0.0);
    EXPECT_GT(second_day->position_value, 0.0);

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecDailyRowsCaptureLateOpenAfterFinalBar) {
    const std::string strategy_type = UniqueAtomicType("open_once_late_daily");
    RegisterOpenOnceReplayType(strategy_type);
    const std::filesystem::path csv_path = WriteLateOpenReplayCsv("quant_hft_daily_late_open");
    const std::filesystem::path composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/1);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "daily-late-open-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.emit_trades = true;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_EQ(result.trades.size(), 1U);

    const DailyPerformance* day = nullptr;
    for (const DailyPerformance& row : result.daily) {
        if (row.date == "20240102") {
            day = &row;
            break;
        }
    }

    ASSERT_NE(day, nullptr);
    EXPECT_GT(day->position_value, 0.0);

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecDailyRowsClearPositionValueAfterExpiryClose) {
    const std::string strategy_type = UniqueAtomicType("open_once_expiry_close_daily");
    RegisterOpenOnceReplayType(strategy_type);

    const auto dataset_root = MakeTempDir("quant_hft_expiry_close_daily_row");
    const auto composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/1);
    const auto calendar_path = WriteTempContractExpiryCalendarConfig(
        "contracts:\n"
        "  c2405:\n"
        "    last_trading_day: 20240103\n");

    std::string error;
    const auto manifest =
        WriteParquetManifest(dataset_root,
                             {
                                 {"c",
                                  "20240102",
                                  "c2405",
                                  {
                                      MakeParquetTick("c2405", "20240102", "09:00:00", 100.0, 10),
                                      MakeParquetTick("c2405", "20240102", "09:00:30", 101.0, 11),
                                      MakeParquetTick("c2405", "20240102", "09:01:00", 102.0, 12),
                                      MakeParquetTick("c2405", "20240102", "09:01:30", 103.0, 13),
                                  }},
                                 {"c",
                                  "20240103",
                                  "c2405",
                                  {
                                      MakeParquetTick("c2405", "20240103", "21:00:00", 109.0, 14),
                                      MakeParquetTick("c2405", "20240103", "21:00:30", 110.0, 15),
                                      MakeParquetTick("c2405", "20240103", "09:00:00", 111.0, 16),
                                      MakeParquetTick("c2405", "20240103", "09:00:30", 112.0, 17),
                                  }},
                             },
                             &error);
    ASSERT_FALSE(manifest.empty()) << error;

    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = dataset_root.string();
    spec.dataset_manifest = manifest.string();
    spec.run_id = UniqueRunId("expiry-close-daily-row");
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.symbols = {"c"};
    spec.rollover_mode = "expiry_close";
    spec.product_series_mode = "raw";
    spec.contract_expiry_calendar_path = calendar_path.string();
    spec.emit_trades = true;

    BacktestCliResult result;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;

    const DailyPerformance* expiry_day = nullptr;
    for (const DailyPerformance& row : result.daily) {
        if (row.date == "20240103") {
            expiry_day = &row;
            break;
        }
    }

    ASSERT_NE(expiry_day, nullptr);
    EXPECT_DOUBLE_EQ(expiry_day->position_value, 0.0);

    bool saw_expiry_close = false;
    for (const TradeRecord& trade : result.trades) {
        if (trade.signal_type == "expiry_close") {
            saw_expiry_close = true;
            break;
        }
    }
    EXPECT_TRUE(saw_expiry_close);

    std::error_code ec;
    std::filesystem::remove(calendar_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove_all(dataset_root, ec);
}

TEST(BacktestReplaySupportTest,
     RunBacktestSpecParquetContinuousAdjustedSubTraceKeepsAnalysisSeriesContinuousAcrossRoll) {
    const auto dataset_root = MakeTempDir("quant_hft_parquet_continuous_adjusted_trace");
    const auto composite_path = WriteTempCompositeConfig(/*volume=*/1, /*timeframe_minutes=*/5);
    const auto trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_continuous_adjusted_trace.csv";

    std::error_code ec;
    std::filesystem::remove(trace_path, ec);

    std::string error;
    const auto manifest =
        WriteParquetManifest(dataset_root,
                             {
                                 {"c",
                                  "20240102",
                                  "c2405",
                                  {
                                      MakeParquetTick("c2405", "20240102", "09:00:00", 100.0, 10),
                                      MakeParquetTick("c2405", "20240102", "09:01:00", 101.0, 11),
                                      MakeParquetTick("c2405", "20240102", "09:02:00", 102.0, 12),
                                      MakeParquetTick("c2405", "20240102", "09:03:00", 103.0, 13),
                                      MakeParquetTick("c2405", "20240102", "09:04:00", 104.0, 14),
                                  }},
                                 {"c",
                                  "20240103",
                                  "c2407",
                                  {
                                      MakeParquetTick("c2407", "20240103", "09:00:00", 200.0, 20),
                                      MakeParquetTick("c2407", "20240103", "09:01:00", 201.0, 21),
                                      MakeParquetTick("c2407", "20240103", "09:02:00", 202.0, 22),
                                      MakeParquetTick("c2407", "20240103", "09:03:00", 203.0, 23),
                                      MakeParquetTick("c2407", "20240103", "09:04:00", 204.0, 24),
                                  }},
                             },
                             &error);
    ASSERT_FALSE(manifest.empty()) << error;

    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = dataset_root.string();
    spec.dataset_manifest = manifest.string();
    spec.run_id = UniqueRunId("parquet-continuous-adjusted-trace");
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.symbols = {"c"};
    spec.product_series_mode = "continuous_adjusted";
    spec.emit_sub_strategy_indicator_trace = true;
    spec.sub_strategy_indicator_trace_path = trace_path.string();

    BacktestCliResult result;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(std::filesystem::exists(trace_path));

    const std::vector<std::string> lines = ReadLines(trace_path);
    ASSERT_EQ(lines.size(), 3U);

    const std::vector<std::string> header = SplitCsvLine(lines[0]);
    const auto find_column = [&](const std::string& name) -> std::size_t {
        const auto it = std::find(header.begin(), header.end(), name);
        EXPECT_NE(it, header.end()) << name;
        return static_cast<std::size_t>(std::distance(header.begin(), it));
    };

    const std::size_t instrument_index = find_column("instrument_id");
    const std::size_t bar_close_index = find_column("bar_close");
    const std::size_t analysis_bar_close_index = find_column("analysis_bar_close");
    const std::size_t analysis_offset_index = find_column("analysis_price_offset");

    const std::vector<std::string> first_row = SplitCsvLine(lines[1]);
    const std::vector<std::string> second_row = SplitCsvLine(lines[2]);

    ASSERT_GT(first_row.size(), analysis_offset_index);
    ASSERT_GT(second_row.size(), analysis_offset_index);
    EXPECT_EQ(first_row[instrument_index], "c2405");
    EXPECT_EQ(second_row[instrument_index], "c2407");
    EXPECT_EQ(first_row[bar_close_index], "104");
    EXPECT_EQ(second_row[bar_close_index], "204");
    EXPECT_EQ(first_row[analysis_bar_close_index], "104");
    EXPECT_EQ(second_row[analysis_bar_close_index], "108");
    EXPECT_EQ(first_row[analysis_offset_index], "0");
    EXPECT_EQ(second_row[analysis_offset_index], "-96");

    std::filesystem::remove(trace_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove_all(dataset_root, ec);
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

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecLoadsContractExpiryCalendarFromMainConfig) {
    const std::filesystem::path open_cfg = WriteTempAtomicStrategyConfig();
    const std::filesystem::path calendar_cfg = WriteTempContractExpiryCalendarConfig(
        "contracts:\n  rb2405:\n    last_trading_day: 20240110\n");
    const std::filesystem::path main_cfg =
        WriteTempMainStrategyConfig(open_cfg, "backtest", calendar_cfg.string());

    ArgMap args;
    args["engine_mode"] = "parquet";
    args["dataset_root"] = "backtest_data/parquet_v2";
    args["dataset_manifest"] = "backtest_data/parquet_v2/_manifest/partitions.jsonl";
    args["rollover_mode"] = "expiry_close";
    args["strategy_main_config_path"] = main_cfg.string();
    args["symbols"] = "rb";

    BacktestCliSpec spec;
    std::string error;
    EXPECT_TRUE(ParseBacktestCliSpec(args, &spec, &error)) << error;
    EXPECT_EQ(spec.contract_expiry_calendar_path, calendar_cfg.string());

    std::error_code ec;
    std::filesystem::remove(open_cfg, ec);
    std::filesystem::remove(calendar_cfg, ec);
    std::filesystem::remove(main_cfg, ec);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecAcceptsStep4AliasesAndDefaultsStreamingOff) {
    const std::filesystem::path open_cfg = WriteTempAtomicStrategyConfig();
    const std::filesystem::path calendar_cfg = WriteTempContractExpiryCalendarConfig(
        "contracts:\n  c2405:\n    last_trading_day: 20240110\n");
    const std::filesystem::path main_cfg =
        WriteTempMainStrategyConfig(open_cfg, "backtest", calendar_cfg.string());

    ArgMap args;
    args["engine-mode"] = "parquet";
    args["dataset-root"] = "backtest_data/parquet_v2";
    args["composite-config"] = main_cfg.string();
    args["rollover_mode"] = "expiry_close";
    args["symbols"] = "c";
    args["start"] = "2024-01-01";
    args["end"] = "2024-12-31";

    BacktestCliSpec spec;
    std::string error;
    EXPECT_TRUE(ParseBacktestCliSpec(args, &spec, &error)) << error;
    EXPECT_EQ(spec.start_date, "20240101");
    EXPECT_EQ(spec.end_date, "20241231");
    EXPECT_EQ(spec.strategy_main_config_path, main_cfg.string());
    EXPECT_EQ(spec.strategy_factory, "composite");
    EXPECT_EQ(spec.strategy_composite_config, main_cfg.string());
    EXPECT_EQ(spec.contract_expiry_calendar_path, calendar_cfg.string());
    EXPECT_FALSE(spec.streaming);

    std::error_code ec;
    std::filesystem::remove(open_cfg, ec);
    std::filesystem::remove(calendar_cfg, ec);
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

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecRejectsExpiryCloseWithoutCalendarPath) {
    ArgMap args;
    args["engine_mode"] = "parquet";
    args["dataset_root"] = "backtest_data/parquet_v2";
    args["dataset_manifest"] = "backtest_data/parquet_v2/_manifest/partitions.jsonl";
    args["rollover_mode"] = "expiry_close";
    args["symbols"] = "c";

    BacktestCliSpec spec;
    std::string error;
    EXPECT_FALSE(ParseBacktestCliSpec(args, &spec, &error));
    EXPECT_NE(error.find("contract_expiry_calendar_path"), std::string::npos);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecRejectsExpiryCloseWithContinuousAdjusted) {
    const std::filesystem::path calendar_cfg = WriteTempContractExpiryCalendarConfig(
        "contracts:\n  c2405:\n    last_trading_day: 20240103\n");

    ArgMap args;
    args["engine_mode"] = "parquet";
    args["dataset_root"] = "backtest_data/parquet_v2";
    args["dataset_manifest"] = "backtest_data/parquet_v2/_manifest/partitions.jsonl";
    args["rollover_mode"] = "expiry_close";
    args["symbols"] = "c";
    args["product_series_mode"] = "continuous_adjusted";
    args["contract_expiry_calendar_path"] = calendar_cfg.string();

    BacktestCliSpec spec;
    std::string error;
    EXPECT_FALSE(ParseBacktestCliSpec(args, &spec, &error));
    EXPECT_NE(error.find("product_series_mode"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove(calendar_cfg, ec);
}

TEST(BacktestReplaySupportTest, ParseBacktestCliSpecRejectsExpiryCloseForExplicitInstrumentInput) {
    const std::filesystem::path calendar_cfg = WriteTempContractExpiryCalendarConfig(
        "contracts:\n  c2405:\n    last_trading_day: 20240103\n");

    ArgMap args;
    args["engine_mode"] = "parquet";
    args["dataset_root"] = "backtest_data/parquet_v2";
    args["dataset_manifest"] = "backtest_data/parquet_v2/_manifest/partitions.jsonl";
    args["rollover_mode"] = "expiry_close";
    args["symbols"] = "c2405";
    args["contract_expiry_calendar_path"] = calendar_cfg.string();

    BacktestCliSpec spec;
    std::string error;
    EXPECT_FALSE(ParseBacktestCliSpec(args, &spec, &error));
    EXPECT_NE(error.find("single product symbol"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove(calendar_cfg, ec);
}

TEST(BacktestReplaySupportTest,
     ResolveBacktestProductConfigPathFallsBackToYamlWhenInstrumentJsonIsMissing) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto root = std::filesystem::temp_directory_path() /
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
    const auto root = std::filesystem::temp_directory_path() /
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
    EXPECT_TRUE(
        detail::ResolveBacktestProductConfigPath("", main_cfg.string(), &resolved_path, &error))
        << error;
    const auto resolved_normalized = std::filesystem::path(resolved_path).lexically_normal();
    const auto sibling_normalized = json_path.lexically_normal();
    const auto default_normalized =
        std::filesystem::path("configs/strategies/instrument_info.json").lexically_normal();
    const auto repo_default_normalized =
        (std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path() /
         "configs/strategies/instrument_info.json")
            .lexically_normal();
    EXPECT_TRUE(resolved_normalized == sibling_normalized ||
                resolved_normalized == default_normalized ||
                resolved_normalized == repo_default_normalized);

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST(BacktestReplaySupportTest,
     ResolveBacktestProductConfigPathFindsRepoDefaultFromBuildLikeWorkingDirectory) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto temp_root = std::filesystem::temp_directory_path() /
                           ("quant_hft_product_cfg_cwd_test_" + std::to_string(stamp));
    const auto build_like_dir = temp_root / "nested" / "build";
    std::filesystem::create_directories(build_like_dir);

    const std::filesystem::path original_cwd = std::filesystem::current_path();
    const std::filesystem::path repo_root =
        std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path();
    const std::filesystem::path expected_json =
        (repo_root / "configs/strategies/instrument_info.json").lexically_normal();
    const std::filesystem::path expected_yaml =
        (repo_root / "configs/strategies/products_info.yaml").lexically_normal();

    std::error_code ec;
    std::filesystem::current_path(build_like_dir, ec);
    ASSERT_FALSE(ec) << ec.message();

    std::string resolved_path;
    std::string error;
    EXPECT_TRUE(detail::ResolveBacktestProductConfigPath("", "", &resolved_path, &error)) << error;
    const auto resolved_normalized = std::filesystem::path(resolved_path).lexically_normal();
    EXPECT_TRUE(resolved_normalized == expected_json || resolved_normalized == expected_yaml)
        << "resolved_path=" << resolved_path;

    std::filesystem::current_path(original_cwd, ec);
    EXPECT_FALSE(ec) << ec.message();
    std::filesystem::remove_all(temp_root, ec);
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

TEST(BacktestReplaySupportTest, BuildInputSignatureChangesWithContractExpiryCalendarSpec) {
    BacktestCliSpec left;
    left.engine_mode = "parquet";
    left.dataset_root = "backtest_data/parquet_v2";
    left.dataset_manifest = "backtest_data/parquet_v2/_manifest/partitions.jsonl";
    left.run_id = "sig-expiry-left";
    left.rollover_mode = "expiry_close";
    left.contract_expiry_calendar_path = "configs/strategies/contract_expiry_calendar_a.yaml";

    BacktestCliSpec right = left;
    right.contract_expiry_calendar_path = "configs/strategies/contract_expiry_calendar_b.yaml";

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
    spec.trace_output_format = "parquet";
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
    spec.trace_output_format = "parquet";
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

TEST(BacktestReplaySupportTest, RunBacktestSpecIndicatorTraceWritesCsvWhenPathEndsWithCsv) {
    const std::filesystem::path csv_path = WriteTempReplayCsv("quant_hft_indicator_trace_csv");
    const std::filesystem::path trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_indicator_trace.csv";
    std::error_code ec;
    std::filesystem::remove(trace_path, ec);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "indicator-trace-csv-test";
    spec.emit_indicator_trace = true;
    spec.indicator_trace_path = trace_path.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    EXPECT_TRUE(result.indicator_trace_enabled);
    EXPECT_EQ(result.indicator_trace_path, trace_path.string());
    EXPECT_GT(result.indicator_trace_rows, 0);
    ASSERT_TRUE(std::filesystem::exists(trace_path));

    const std::vector<std::string> lines = ReadLines(trace_path);
    ASSERT_GE(lines.size(), 2U);
    EXPECT_EQ(lines.front(),
              "instrument_id,ts_ns,dt_utc,timeframe_minutes,bar_open,bar_high,bar_low,bar_close,"
              "bar_volume,analysis_bar_open,analysis_bar_high,analysis_bar_low,"
              "analysis_bar_close,analysis_price_offset,kama,atr,adx,er,market_regime");

    std::filesystem::remove(csv_path, ec);
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
    spec.trace_output_format = "parquet";
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

TEST(BacktestReplaySupportTest,
     RunBacktestSpecSubStrategyTraceFlushesExpiredInstrumentBucketsBeforeFutureBars) {
    const std::filesystem::path csv_path =
        WriteInstrumentSwitchReplayCsv("quant_hft_sub_trace_instrument_switch");
    const std::filesystem::path composite_path =
        WriteTempCompositeConfig(/*volume=*/1, /*timeframe_minutes=*/5);
    const std::filesystem::path trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_sub_trace_instrument_switch.csv";
    std::error_code ec;
    std::filesystem::remove(trace_path, ec);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "sub-trace-instrument-switch";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.emit_sub_strategy_indicator_trace = true;
    spec.sub_strategy_indicator_trace_path = trace_path.string();
    spec.max_ticks = 16;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(std::filesystem::exists(trace_path));

    const std::vector<std::string> lines = ReadLines(trace_path);
    ASSERT_EQ(lines.size(), 4U);

    std::vector<std::string> dt_values;
    std::vector<std::string> instrument_ids;
    dt_values.reserve(lines.size() - 1U);
    instrument_ids.reserve(lines.size() - 1U);
    for (std::size_t index = 1; index < lines.size(); ++index) {
        const std::vector<std::string> fields = SplitCsvLine(lines[index]);
        ASSERT_GE(fields.size(), 3U);
        instrument_ids.push_back(fields[0]);
        dt_values.push_back(fields[2]);
    }

    EXPECT_EQ(dt_values[0], "2024-01-02 09:00");
    EXPECT_EQ(dt_values[1], "2024-01-02 09:05");
    EXPECT_EQ(dt_values[2], "2024-01-02 09:10");
    EXPECT_EQ(instrument_ids[0], "rb2405");
    EXPECT_EQ(instrument_ids[1], "rb2409");
    EXPECT_EQ(instrument_ids[2], "rb2409");
    EXPECT_LE(dt_values[0], dt_values[1]);
    EXPECT_LE(dt_values[1], dt_values[2]);

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove(trace_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecSubStrategyTraceUsesDefaultPathWhenEnabled) {
    const std::filesystem::path csv_path =
        WriteTempReplayCsv("quant_hft_sub_strategy_trace_default");
    const std::filesystem::path composite_path = WriteTempCompositeConfig();
    const std::string run_id = UniqueRunId("sub-strategy-trace-default");
    const std::filesystem::path expected_path = std::filesystem::path("runtime") / "research" /
                                                "sub_strategy_indicator_trace" / (run_id + ".csv");
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
    spec.trace_output_format = "parquet";
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
    const std::filesystem::path csv_path = WriteTempReplayCsv("quant_hft_indicator_trace_default");
    const std::string run_id = UniqueRunId("indicator-trace-default");
    const std::filesystem::path expected_path =
        std::filesystem::path("runtime") / "research" / "indicator_trace" / (run_id + ".csv");

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
}

TEST(BacktestReplaySupportTest, RunBacktestSpecSubStrategyTraceWritesCsvWhenPathEndsWithCsv) {
    const std::filesystem::path csv_path =
        WriteTempReplayCsv("quant_hft_sub_strategy_trace_composite_csv");
    const std::filesystem::path composite_path = WriteTempCompositeConfig();
    const std::filesystem::path trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_sub_strategy_trace.csv";
    std::error_code ec;
    std::filesystem::remove(trace_path, ec);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "sub-strategy-trace-csv-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.emit_sub_strategy_indicator_trace = true;
    spec.sub_strategy_indicator_trace_path = trace_path.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    EXPECT_TRUE(result.sub_strategy_indicator_trace_enabled);
    EXPECT_EQ(result.sub_strategy_indicator_trace_path, trace_path.string());
    EXPECT_GT(result.sub_strategy_indicator_trace_rows, 0);
    ASSERT_TRUE(std::filesystem::exists(trace_path));

    const std::vector<std::string> lines = ReadLines(trace_path);
    ASSERT_GE(lines.size(), 2U);
    EXPECT_EQ(lines.front(),
              "instrument_id,ts_ns,dt_utc,trading_day,action_day,timeframe_minutes,strategy_id,"
              "strategy_type,bar_open,bar_high,bar_low,bar_close,bar_volume,"
              "analysis_bar_open,analysis_bar_high,analysis_bar_low,analysis_bar_close,"
              "analysis_price_offset,kama,atr,adx,er,stop_loss_price,take_profit_price,"
              "market_regime");

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove(trace_path, ec);
}

TEST(BacktestReplaySupportTest,
     RunBacktestSpecSubStrategyTraceUsesActionDayForNightSessionDisplayTime) {
    const std::filesystem::path csv_path =
        WriteNightSessionReplayCsv("quant_hft_sub_strategy_trace_night_session");
    const std::filesystem::path composite_path = WriteTempCompositeConfig();
    const std::filesystem::path trace_path =
        std::filesystem::temp_directory_path() / "quant_hft_sub_strategy_trace_night_session.csv";
    std::error_code ec;
    std::filesystem::remove(trace_path, ec);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "sub-strategy-trace-night-session-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();
    spec.emit_sub_strategy_indicator_trace = true;
    spec.sub_strategy_indicator_trace_path = trace_path.string();
    spec.max_ticks = 4;

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(std::filesystem::exists(trace_path));

    const std::vector<std::string> lines = ReadLines(trace_path);
    ASSERT_GE(lines.size(), 2U);
    const std::vector<std::string> fields = SplitCsvLine(lines[1]);
    ASSERT_GE(fields.size(), 6U);
    EXPECT_EQ(fields[2], "2024-01-02 21:00");
    EXPECT_EQ(fields[3], "20240103");
    EXPECT_EQ(fields[4], "20240102");

    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
    std::filesystem::remove(trace_path, ec);
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
    spec.trace_output_format = "parquet";
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

TEST(BacktestReplaySupportTest,
     RunBacktestSpecForceCloseWindowUsesFirstEligibleTickAndBlocksReopen) {
    const std::string strategy_type = UniqueAtomicType("always_open_force_close");
    RegisterAlwaysOpenReplayType(strategy_type);
    const std::filesystem::path csv_path =
        WriteForceCloseWindowReplayCsv("quant_hft_force_close_window");
    const std::filesystem::path composite_path =
        WriteForceCloseWindowCompositeConfig(strategy_type, "09:01-09:04", "Asia/Shanghai");

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
            trade.timestamp_ns == detail::ToEpochNs("20240103", "09:01:30", 0)) {
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

TEST(BacktestReplaySupportTest,
     RunBacktestSpecForceCloseWindowBlocksNightSessionOpensInAsiaShanghai) {
    const std::string strategy_type = UniqueAtomicType("always_open_force_close_night");
    RegisterAlwaysOpenReplayType(strategy_type);
    const std::filesystem::path csv_path =
        WriteNightSessionReplayCsv("quant_hft_force_close_window_night");
    const std::filesystem::path composite_path =
        WriteForceCloseWindowCompositeConfig(strategy_type, "21:00-21:02", "Asia/Shanghai");

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "force-close-window-night-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_TRUE(result.has_deterministic);
    EXPECT_TRUE(result.trades.empty());

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
        WriteForceCloseWindowCompositeConfig(strategy_type, "09:01-09:04", "Asia/Shanghai");

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

TEST(BacktestReplaySupportTest, RunBacktestSpecDefersFiveMinuteBarSignalUntilLaterTick) {
    const std::string strategy_type = UniqueAtomicType("always_open_deferred_bar");
    RegisterAlwaysOpenReplayType(strategy_type);
    const std::filesystem::path csv_path = WriteDeferredBarReplayCsv("quant_hft_deferred_bar");
    const std::filesystem::path composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/5);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "deferred-bar-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    ASSERT_EQ(result.trades.size(), 1U);

    const TradeRecord& trade = result.trades.front();
    EXPECT_EQ(trade.signal_type, "kOpen");
    EXPECT_EQ(trade.signal_ts_ns, detail::ToEpochNs("20240110", "09:59:05", 0));
    EXPECT_EQ(trade.timestamp_ns, detail::ToEpochNs("20240110", "10:01:30", 0));
    EXPECT_GT(trade.timestamp_ns, trade.signal_ts_ns);
    EXPECT_EQ(trade.trading_day, "20240110");
    EXPECT_EQ(trade.update_time, "10:01:30");
    EXPECT_EQ(trade.timestamp_dt_local, "2024-01-10 10:01:30");

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecDefersTickStopLossUntilNextTick) {
    const std::string strategy_type = UniqueAtomicType("open_once_tick_stop");
    RegisterOpenOnceThenTickStopReplayType(strategy_type);
    const std::filesystem::path csv_path =
        WriteForceCloseWindowReplayCsv("quant_hft_tick_stop_next_tick");
    const std::filesystem::path composite_path =
        WriteTickStopCompositeConfig(strategy_type, /*stop_trigger_price=*/104.0);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "tick-stop-next-tick-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;

    const auto stop_it =
        std::find_if(result.trades.begin(), result.trades.end(),
                     [](const TradeRecord& trade) { return trade.signal_type == "kStopLoss"; });
    ASSERT_NE(stop_it, result.trades.end());
    EXPECT_EQ(stop_it->signal_ts_ns, detail::ToEpochNs("20240103", "09:02:05", 0));
    EXPECT_EQ(stop_it->timestamp_ns, detail::ToEpochNs("20240103", "09:02:30", 0));
    EXPECT_GT(stop_it->timestamp_ns, stop_it->signal_ts_ns);
    EXPECT_DOUBLE_EQ(stop_it->price, 105.0);

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
    std::filesystem::remove(composite_path, ec);
}

TEST(BacktestReplaySupportTest, RunBacktestSpecExpiresPendingBarSignalAcrossSessionBoundary) {
    const std::string strategy_type = UniqueAtomicType("always_open_session_expiry");
    RegisterAlwaysOpenReplayType(strategy_type);
    const std::filesystem::path csv_path =
        WriteCrossSessionPendingReplayCsv("quant_hft_pending_session_expiry");
    const std::filesystem::path composite_path =
        WriteAlwaysOpenCompositeConfig(strategy_type, /*timeframe_minutes=*/5);

    BacktestCliSpec spec;
    spec.engine_mode = "csv";
    spec.csv_path = csv_path.string();
    spec.run_id = "pending-session-expiry-test";
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = composite_path.string();

    BacktestCliResult result;
    std::string error;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    EXPECT_TRUE(result.trades.empty());

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
