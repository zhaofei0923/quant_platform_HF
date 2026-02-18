#include "quant_hft/services/rule_market_state_engine.h"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <utility>

namespace quant_hft {

RuleMarketStateEngine::RuleMarketStateEngine(std::size_t lookback_window)
    : lookback_window_(lookback_window) {}

void RuleMarketStateEngine::OnMarketSnapshot(const MarketSnapshot& snapshot) {
    StateCallback callback_copy;
    StateSnapshot7D state;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& buffer = buffers_[snapshot.instrument_id];
        buffer.prices.push_back(snapshot.last_price);
        buffer.volumes.push_back(snapshot.volume);

        while (buffer.prices.size() > lookback_window_) {
            buffer.prices.pop_front();
        }
        while (buffer.volumes.size() > lookback_window_) {
            buffer.volumes.pop_front();
        }

        buffer.latest = BuildState(snapshot.instrument_id, buffer, snapshot);
        state = buffer.latest;
        callback_copy = callback_;
    }

    if (callback_copy) {
        callback_copy(state);
    }
}

StateSnapshot7D RuleMarketStateEngine::GetCurrentState(const std::string& instrument_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = buffers_.find(instrument_id);
    if (it == buffers_.end()) {
        StateSnapshot7D empty;
        empty.instrument_id = instrument_id;
        empty.ts_ns = NowEpochNanos();
        return empty;
    }
    return it->second.latest;
}

void RuleMarketStateEngine::RegisterStateCallback(StateCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = std::move(callback);
}

StateSnapshot7D RuleMarketStateEngine::BuildState(const std::string& instrument_id,
                                                  const InstrumentBuffer& buffer,
                                                  const MarketSnapshot& snapshot) const {
    StateSnapshot7D out;
    out.instrument_id = instrument_id;
    out.ts_ns = snapshot.recv_ts_ns == 0 ? NowEpochNanos() : snapshot.recv_ts_ns;
    out.bar_open = snapshot.last_price;
    out.bar_high = snapshot.last_price;
    out.bar_low = snapshot.last_price;
    out.bar_close = snapshot.last_price;
    out.bar_volume = 0.0;
    out.has_bar = false;

    if (!buffer.prices.empty()) {
        out.bar_open = buffer.prices.front();
        out.bar_close = buffer.prices.back();
        out.bar_high = *std::max_element(buffer.prices.begin(), buffer.prices.end());
        out.bar_low = *std::min_element(buffer.prices.begin(), buffer.prices.end());
    }
    if (buffer.volumes.size() >= 2) {
        const std::int64_t volume_delta =
            std::max<std::int64_t>(0, buffer.volumes.back() - buffer.volumes.front());
        out.bar_volume = static_cast<double>(volume_delta);
        out.has_bar = true;
    }

    if (buffer.prices.size() < 2) {
        out.trend = {0.0, 0.0};
        out.volatility = {0.0, 0.0};
        out.liquidity = {0.0, 0.0};
        out.sentiment = {0.0, 0.0};
        out.seasonality = {0.0, 0.0};
        out.pattern = {0.0, 0.0};
        out.event_drive = {0.0, 0.0};
        return out;
    }

    const auto first = buffer.prices.front();
    const auto last = buffer.prices.back();
    const auto trend_raw = first == 0.0 ? 0.0 : (last - first) / first;
    const auto trend = std::clamp(trend_raw, -1.0, 1.0);

    double mean = std::accumulate(buffer.prices.begin(), buffer.prices.end(), 0.0) /
                  static_cast<double>(buffer.prices.size());
    double variance = 0.0;
    for (const auto p : buffer.prices) {
        const auto d = p - mean;
        variance += d * d;
    }
    variance /= static_cast<double>(buffer.prices.size());
    const auto volatility = std::clamp(std::sqrt(variance) / std::max(1.0, mean), 0.0, 1.0);

    const auto spread = std::max(0.0, snapshot.ask_price_1 - snapshot.bid_price_1);
    const auto liquidity = std::clamp(1.0 / (1.0 + spread), 0.0, 1.0);

    const auto imbalance_num = static_cast<double>(snapshot.bid_volume_1 - snapshot.ask_volume_1);
    const auto imbalance_den = static_cast<double>(snapshot.bid_volume_1 + snapshot.ask_volume_1 + 1);
    const auto sentiment = std::clamp(imbalance_num / imbalance_den, -1.0, 1.0);

    const auto seasonality = 0.0;   // Placeholder: daily/weekly seasonality model in phase 2.
    const auto pattern = trend;     // Placeholder: chart pattern score.
    const auto event_drive = 0.0;   // Placeholder: macro/news impact score.

    out.trend = {trend, 0.8};
    out.volatility = {volatility, 0.7};
    out.liquidity = {liquidity, 0.7};
    out.sentiment = {sentiment, 0.6};
    out.seasonality = {seasonality, 0.2};
    out.pattern = {pattern, 0.3};
    out.event_drive = {event_drive, 0.2};

    return out;
}

}  // namespace quant_hft
