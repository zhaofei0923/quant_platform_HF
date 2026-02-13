#pragma once

#include <cstdint>
#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class DataFeed;

namespace backtest {
class BacktestEngine;
class SimulatedBroker;
}

class Strategy {
public:
    virtual ~Strategy() = default;

    virtual void Initialize() {}
    virtual void OnTick(const Tick& tick) { (void)tick; }
    virtual void OnBar(const Bar& bar) { (void)bar; }
    virtual void OnOrder(const Order& order) { (void)order; }
    virtual void OnTrade(const Trade& trade) { (void)trade; }

    void Buy(const std::string& symbol, double price, std::int32_t volume);
    void Sell(const std::string& symbol, double price, std::int32_t volume);
    void CancelOrder(const std::string& client_order_id);

protected:
    DataFeed* data() const { return data_feed_; }
    backtest::SimulatedBroker* broker() const { return broker_; }

private:
    friend class backtest::BacktestEngine;

    void BindContext(DataFeed* data_feed, backtest::SimulatedBroker* broker);

    DataFeed* data_feed_{nullptr};
    backtest::SimulatedBroker* broker_{nullptr};
    std::int64_t order_seed_{0};
};

}  // namespace quant_hft
