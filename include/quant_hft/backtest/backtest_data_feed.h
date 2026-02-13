#pragma once

#include <memory>
#include <string>

#include "quant_hft/backtest/parquet_data_feed.h"
#include "quant_hft/interfaces/data_feed.h"

namespace quant_hft {
namespace backtest {

class BacktestDataFeed : public DataFeed {
public:
    BacktestDataFeed(const std::string& parquet_root,
                     const Timestamp& start,
                     const Timestamp& end);
    ~BacktestDataFeed() override;

    BacktestDataFeed(const BacktestDataFeed&) = delete;
    BacktestDataFeed& operator=(const BacktestDataFeed&) = delete;

    BacktestDataFeed(BacktestDataFeed&&) noexcept;
    BacktestDataFeed& operator=(BacktestDataFeed&&) noexcept;

    void Subscribe(const std::vector<std::string>& symbols,
                   std::function<void(const Tick&)> on_tick,
                   std::function<void(const Bar&)> on_bar) override;

    std::vector<Bar> GetHistoryBars(const std::string& symbol,
                                    const Timestamp& start,
                                    const Timestamp& end,
                                    const std::string& timeframe) override;

    std::vector<Tick> GetHistoryTicks(const std::string& symbol,
                                      const Timestamp& start,
                                      const Timestamp& end) override;

    void Run() override;
    void Stop() override;
    Timestamp CurrentTime() const override;
    bool IsLive() const override { return false; }

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace backtest
}  // namespace quant_hft
