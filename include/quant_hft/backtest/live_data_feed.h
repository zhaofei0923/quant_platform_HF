#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "quant_hft/interfaces/data_feed.h"

namespace quant_hft {
namespace backtest {

class LiveDataFeed : public DataFeed {
public:
    LiveDataFeed();
    ~LiveDataFeed() override;

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
    bool IsLive() const override { return true; }

private:
    std::function<void(const Tick&)> tick_cb_;
    std::function<void(const Bar&)> bar_cb_;
    std::atomic<bool> running_{true};
    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

}  // namespace backtest
}  // namespace quant_hft
