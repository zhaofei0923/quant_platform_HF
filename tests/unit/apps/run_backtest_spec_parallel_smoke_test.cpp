#include "quant_hft/apps/backtest_replay_support.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <string>
#include <vector>

namespace quant_hft::apps {
namespace {

std::filesystem::path WriteTempReplayCsv() {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_parallel_backtest_" + std::to_string(stamp) + ".csv");

    std::ofstream out(path);
    out << "InstrumentID,ts_ns,LastPrice,Volume,BidPrice1,BidVolume1,AskPrice1,AskVolume1\n";
    for (int i = 0; i < 10; ++i) {
        const long long ts_ns = 1704067200000000000LL + static_cast<long long>(i) * 1000000000LL;
        out << "rb2405," << ts_ns << ',' << (100 + i) << ',' << (1000 + i) << ',' << (99 + i)
            << ",20," << (101 + i) << ",18\n";
    }
    out.close();
    return path;
}

TEST(RunBacktestSpecParallelSmokeTest, SupportsConcurrentRuns) {
    const auto csv_path = WriteTempReplayCsv();

    std::vector<std::future<bool>> futures;
    futures.reserve(4);

    for (int i = 0; i < 4; ++i) {
        futures.push_back(std::async(std::launch::async, [csv_path, i]() {
            BacktestCliSpec spec;
            spec.engine_mode = "csv";
            spec.csv_path = csv_path.string();
            spec.strategy_factory = "demo";
            spec.run_id = "parallel-smoke-" + std::to_string(i);
            spec.emit_trades = false;
            spec.emit_orders = false;
            spec.emit_position_history = false;

            BacktestCliResult result;
            std::string error;
            if (!RunBacktestSpec(spec, &result, &error)) {
                return false;
            }
            return !result.run_id.empty() && result.engine_mode == "csv";
        }));
    }

    for (auto& future : futures) {
        EXPECT_TRUE(future.get());
    }

    std::error_code ec;
    std::filesystem::remove(csv_path, ec);
}

}  // namespace
}  // namespace quant_hft::apps

