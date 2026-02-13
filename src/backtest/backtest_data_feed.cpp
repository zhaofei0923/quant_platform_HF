#include "quant_hft/backtest/backtest_data_feed.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace quant_hft {
namespace backtest {

struct BacktestDataFeed::Impl {
    ParquetDataFeed parquet_feed;
    Timestamp start_time;
    Timestamp end_time;
    Timestamp current_time;

    std::vector<std::string> subscribed_symbols;
    std::function<void(const Tick&)> tick_callback;
    std::function<void(const Bar&)> bar_callback;

    enum class EventType { kTick, kBar };

    struct Event {
        Timestamp time;
        EventType type;
        std::variant<Tick, Bar> data;

        bool operator>(const Event& other) const {
            return time > other.time;
        }
    };

    std::priority_queue<Event, std::vector<Event>, std::greater<Event>> event_queue;

    std::atomic<bool> running{false};
    std::mutex mutex;
    std::condition_variable cv;

    Impl(const std::string& parquet_root, const Timestamp& start, const Timestamp& end)
        : parquet_feed(parquet_root), start_time(start), end_time(end), current_time(start) {}

    void LoadAllTicks() {
        while (!event_queue.empty()) {
            event_queue.pop();
        }
        if (subscribed_symbols.empty()) {
            auto ticks = parquet_feed.LoadTicks("", start_time, end_time);
            for (const auto& tick : ticks) {
                event_queue.push({Timestamp(tick.ts_ns), EventType::kTick, tick});
            }
            return;
        }
        for (const auto& symbol : subscribed_symbols) {
            auto ticks = parquet_feed.LoadTicks(symbol, start_time, end_time);
            for (const auto& tick : ticks) {
                event_queue.push({Timestamp(tick.ts_ns), EventType::kTick, tick});
            }
        }
    }
};

BacktestDataFeed::BacktestDataFeed(const std::string& parquet_root,
                                   const Timestamp& start,
                                   const Timestamp& end)
    : impl_(std::make_unique<Impl>(parquet_root, start, end)) {}

BacktestDataFeed::~BacktestDataFeed() = default;

BacktestDataFeed::BacktestDataFeed(BacktestDataFeed&&) noexcept = default;
BacktestDataFeed& BacktestDataFeed::operator=(BacktestDataFeed&&) noexcept = default;

void BacktestDataFeed::Subscribe(const std::vector<std::string>& symbols,
                                 std::function<void(const Tick&)> on_tick,
                                 std::function<void(const Bar&)> on_bar) {
    impl_->subscribed_symbols = symbols;
    impl_->tick_callback = std::move(on_tick);
    impl_->bar_callback = std::move(on_bar);
    impl_->LoadAllTicks();
}

std::vector<Bar> BacktestDataFeed::GetHistoryBars(const std::string& symbol,
                                                  const Timestamp& start,
                                                  const Timestamp& end,
                                                  const std::string& timeframe) {
    (void)symbol;
    (void)start;
    (void)end;
    (void)timeframe;
    return {};
}

std::vector<Tick> BacktestDataFeed::GetHistoryTicks(const std::string& symbol,
                                                    const Timestamp& start,
                                                    const Timestamp& end) {
    return impl_->parquet_feed.LoadTicks(symbol, start, end);
}

void BacktestDataFeed::Run() {
    impl_->running = true;
    while (impl_->running && !impl_->event_queue.empty()) {
        auto event = impl_->event_queue.top();
        impl_->event_queue.pop();

        impl_->current_time = event.time;
        if (event.type == Impl::EventType::kTick && impl_->tick_callback) {
            impl_->tick_callback(std::get<Tick>(event.data));
            continue;
        }
        if (event.type == Impl::EventType::kBar && impl_->bar_callback) {
            impl_->bar_callback(std::get<Bar>(event.data));
        }
    }
}

void BacktestDataFeed::Stop() {
    impl_->running = false;
    impl_->cv.notify_all();
}

Timestamp BacktestDataFeed::CurrentTime() const {
    return impl_->current_time;
}

}  // namespace backtest
}  // namespace quant_hft
