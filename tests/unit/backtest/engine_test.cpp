#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

#include "quant_hft/backtest/backtest_data_feed.h"
#include "quant_hft/backtest/broker.h"
#include "quant_hft/backtest/engine.h"
#include "quant_hft/strategy/base_strategy.h"

namespace fs = std::filesystem;

namespace quant_hft::backtest {

namespace {

class TestStrategy final : public Strategy {
public:
    void OnTick(const Tick& tick) override {
        if (!ordered_ && tick.last_price > 0.0) {
            Buy(tick.symbol, tick.ask_price1 > 0 ? tick.ask_price1 : tick.last_price, 1);
            ordered_ = true;
        }
    }

private:
    bool ordered_{false};
};

}  // namespace

TEST(EngineTest, RunWithSimpleStrategyGeneratesTrades) {
    const fs::path root = fs::temp_directory_path() / "engine_test_data";
    fs::remove_all(root);
    const fs::path partition = root / "source=rb" / "trading_day=2024-01-01" / "instrument_id=rb2405";
    fs::create_directories(partition);

    const fs::path parquet_file = partition / "part-0000.parquet";
    std::ofstream(parquet_file) << "PAR1";
    std::ofstream(parquet_file.string() + ".meta")
        << "min_ts_ns=1704067200000000000\nmax_ts_ns=1704067201000000000\nrow_count=2\n";
    std::ofstream(parquet_file.string() + ".ticks.csv")
        << "symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,ask_volume1,volume,turnover,open_interest\n"
        << "rb2405,SHFE,1704067200000000000,3500.0,10,3499.0,5,3501.0,5,100,350000.0,1200000\n"
        << "rb2405,SHFE,1704067201000000000,3502.0,10,3501.0,5,3503.0,5,110,385220.0,1200010\n";

    auto feed = std::make_unique<BacktestDataFeed>(root.string(), Timestamp::FromSql("2024-01-01"), Timestamp::FromSql("2024-01-02"));
    auto broker = std::make_unique<SimulatedBroker>(BrokerConfig{});
    auto strategy = std::make_shared<TestStrategy>();

    BacktestEngine engine(std::move(feed), std::move(broker), std::move(strategy));
    engine.Run();

    const auto result = engine.GetResult();
    EXPECT_FALSE(result.orders.empty());
    EXPECT_FALSE(result.trades.empty());
    EXPECT_FALSE(result.equity_curve.empty());

    fs::remove_all(root);
}

}  // namespace quant_hft::backtest
