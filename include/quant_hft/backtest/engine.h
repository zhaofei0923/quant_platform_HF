#pragma once

#include <memory>
#include <vector>

#include "quant_hft/backtest/events.h"
#include "quant_hft/common/timestamp.h"

namespace quant_hft {

class DataFeed;
class Strategy;

namespace backtest {

class SimulatedBroker;

struct EquityPoint {
    Timestamp time;
    double balance{0.0};
};

struct BacktestResult {
    std::vector<Order> orders;
    std::vector<Trade> trades;
    std::vector<EquityPoint> equity_curve;
};

class BacktestEngine {
public:
    BacktestEngine(std::unique_ptr<DataFeed> data_feed,
                   std::unique_ptr<SimulatedBroker> broker,
                   std::shared_ptr<Strategy> strategy);

    void Run();

    BacktestResult GetResult() const;

    void SetTimeRange(const Timestamp& start, const Timestamp& end);

private:
    void ProcessEvent(const Event& event);
    void OnMarketData(const Tick& tick);
    void OnOrderUpdate(const Order& order);
    void OnFill(const Trade& trade);

    std::unique_ptr<DataFeed> data_feed_;
    std::unique_ptr<SimulatedBroker> broker_;
    std::shared_ptr<Strategy> strategy_;

    Timestamp start_time_;
    Timestamp end_time_;
    BacktestResult result_;
};

}  // namespace backtest
}  // namespace quant_hft
