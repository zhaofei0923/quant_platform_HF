#include "quant_hft/backtest/live_data_feed.h"

namespace quant_hft {
namespace backtest {

LiveDataFeed::LiveDataFeed() = default;

LiveDataFeed::~LiveDataFeed() {
    Stop();
}

void LiveDataFeed::Subscribe(const std::vector<std::string>& symbols,
                             std::function<void(const Tick&)> on_tick,
                             std::function<void(const Bar&)> on_bar) {
    (void)symbols;
    tick_cb_ = std::move(on_tick);
    bar_cb_ = std::move(on_bar);
}

std::vector<Bar> LiveDataFeed::GetHistoryBars(const std::string& symbol,
                                              const Timestamp& start,
                                              const Timestamp& end,
                                              const std::string& timeframe) {
    (void)symbol;
    (void)start;
    (void)end;
    (void)timeframe;
    return {};
}

std::vector<Tick> LiveDataFeed::GetHistoryTicks(const std::string& symbol,
                                                const Timestamp& start,
                                                const Timestamp& end) {
    (void)symbol;
    (void)start;
    (void)end;
    return {};
}

void LiveDataFeed::Run() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return !running_.load(); });
}

void LiveDataFeed::Stop() {
    running_ = false;
    cv_.notify_all();
}

Timestamp LiveDataFeed::CurrentTime() const {
    return Timestamp::Now();
}

}  // namespace backtest
}  // namespace quant_hft
