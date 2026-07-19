#include "quant_hft/services/execution_planner.h"

#include <algorithm>
#include <cmath>
#include <optional>

namespace quant_hft {

namespace {

std::vector<std::int32_t> BuildUniformSlices(std::int32_t total_volume, int slice_size) {
    std::vector<std::int32_t> slices;
    if (total_volume <= 0) {
        return slices;
    }
    const int normalized_size = std::max(1, slice_size);
    int remaining = total_volume;
    while (remaining > 0) {
        const int this_slice = std::min(remaining, normalized_size);
        slices.push_back(this_slice);
        remaining -= this_slice;
    }
    return slices;
}

std::vector<std::int32_t> BuildVwapSlices(std::int32_t total_volume, int max_slices,
                                          const std::vector<MarketSnapshot>& recent_market) {
    std::vector<std::int32_t> slices;
    if (total_volume <= 0 || max_slices <= 0 || recent_market.empty()) {
        return slices;
    }
    const std::size_t usable =
        std::min<std::size_t>(recent_market.size(), static_cast<std::size_t>(max_slices));
    double weight_sum = 0.0;
    std::vector<double> weights(usable, 1.0);
    for (std::size_t i = 0; i < usable; ++i) {
        const auto volume = static_cast<double>(std::max<std::int64_t>(1, recent_market[i].volume));
        weights[i] = volume;
        weight_sum += volume;
    }
    if (weight_sum <= 0.0) {
        return slices;
    }

    int assigned = 0;
    slices.resize(usable, 0);
    for (std::size_t i = 0; i < usable; ++i) {
        const double raw = static_cast<double>(total_volume) * (weights[i] / weight_sum);
        const int value = std::max(0, static_cast<int>(std::floor(raw)));
        slices[i] = value;
        assigned += value;
    }
    int remaining = total_volume - assigned;
    for (std::size_t i = 0; i < usable && remaining > 0; ++i) {
        ++slices[i];
        --remaining;
    }

    slices.erase(std::remove(slices.begin(), slices.end(), 0), slices.end());
    if (slices.empty()) {
        slices.push_back(total_volume);
    }
    return slices;
}

std::optional<double> ResolveExecutionPrice(const SignalIntent& signal,
                                            const ExecutionConfig& config,
                                            const std::vector<MarketSnapshot>& recent_market) {
    if (config.price_mode == ExecutionPriceMode::kSignalLimit) {
        return signal.limit_price;
    }
    if (recent_market.empty()) {
        return std::nullopt;
    }
    const MarketSnapshot& snapshot = recent_market.back();
    const double price = signal.side == Side::kBuy ? snapshot.ask_price_1 : snapshot.bid_price_1;
    if (!std::isfinite(price) || price <= 0.0) {
        return std::nullopt;
    }
    return price;
}

}  // namespace

ExecutionPlanner::ExecutionPlanner(std::size_t throttle_window_size)
    : throttle_window_size_(std::max<std::size_t>(5, throttle_window_size)) {}

std::vector<PlannedOrder> ExecutionPlanner::BuildPlan(
    const SignalIntent& signal, const std::string& account_id, const ExecutionConfig& config,
    const std::vector<MarketSnapshot>& recent_market) const {
    std::vector<PlannedOrder> out;
    if (signal.volume <= 0 || signal.trace_id.empty()) {
        return out;
    }
    const auto volume_plan = BuildVolumePlan(signal, config, recent_market);
    if (volume_plan.empty()) {
        return out;
    }
    const auto order_price = ResolveExecutionPrice(signal, config, recent_market);
    if (!order_price.has_value()) {
        return out;
    }
    const auto base_ts = signal.generated_ts_ns > 0
                             ? signal.generated_ts_ns
                             : (signal.ts_ns == 0 ? NowEpochNanos() : signal.ts_ns);
    const MarketSnapshot* latest_market = recent_market.empty() ? nullptr : &recent_market.back();
    const auto algo_id = AlgoToId(config.algo);
    const std::int32_t total = static_cast<std::int32_t>(volume_plan.size());
    out.reserve(volume_plan.size());
    for (std::size_t idx = 0; idx < volume_plan.size(); ++idx) {
        PlannedOrder planned;
        planned.intent.account_id = account_id;
        planned.intent.instrument_id = signal.instrument_id;
        planned.intent.strategy_id = signal.strategy_id;
        planned.intent.side = signal.side;
        planned.intent.offset = signal.offset;
        planned.intent.type = OrderType::kLimit;
        planned.intent.volume = volume_plan[idx];
        planned.intent.price = *order_price;
        planned.intent.ts_ns = base_ts + static_cast<EpochNanos>(idx + 1);
        planned.intent.signal_ts_ns = base_ts;
        planned.intent.product_id = signal.product_id;
        planned.intent.contract_generation = signal.contract_generation;
        if (latest_market != nullptr) {
            planned.intent.exchange_id = latest_market->exchange_id;
            planned.intent.trading_day = latest_market->trading_day.empty()
                                             ? latest_market->action_day
                                             : latest_market->trading_day;
            planned.intent.market_recv_ts_ns = latest_market->recv_ts_ns;
        }
        if (planned.intent.exchange_id.empty()) {
            const std::size_t separator = signal.instrument_id.find('.');
            if (separator != std::string::npos) {
                planned.intent.exchange_id = signal.instrument_id.substr(0, separator);
            }
        }
        planned.slice_index = static_cast<std::int32_t>(idx + 1);
        planned.slice_total = total;
        planned.execution_algo_id = algo_id;
        if (total > 1) {
            planned.intent.client_order_id = signal.trace_id + "#slice-" + std::to_string(idx + 1);
            planned.intent.trace_id = planned.intent.client_order_id;
        } else {
            planned.intent.client_order_id = signal.trace_id;
            planned.intent.trace_id = signal.trace_id;
        }
        out.push_back(std::move(planned));
    }
    return out;
}

void ExecutionPlanner::RecordOrderResult(bool rejected) {
    reject_history_.push_back(rejected);
    while (reject_history_.size() > throttle_window_size_) {
        reject_history_.pop_front();
    }
}

bool ExecutionPlanner::ShouldThrottle(double reject_ratio_threshold) const {
    if (reject_ratio_threshold <= 0.0 || reject_history_.size() < 5) {
        return false;
    }
    return CurrentRejectRatio() >= reject_ratio_threshold;
}

double ExecutionPlanner::CurrentRejectRatio() const {
    if (reject_history_.empty()) {
        return 0.0;
    }
    std::size_t rejected = 0;
    for (bool item : reject_history_) {
        if (item) {
            ++rejected;
        }
    }
    return static_cast<double>(rejected) / static_cast<double>(reject_history_.size());
}

std::string ExecutionPlanner::AlgoToId(ExecutionAlgo algo) {
    switch (algo) {
        case ExecutionAlgo::kDirect:
            return "direct";
        case ExecutionAlgo::kSliced:
            return "sliced";
        case ExecutionAlgo::kTwap:
            return "twap";
        case ExecutionAlgo::kVwapLite:
            return "vwap_lite";
    }
    return "direct";
}

std::vector<std::int32_t> ExecutionPlanner::BuildVolumePlan(
    const SignalIntent& signal, const ExecutionConfig& config,
    const std::vector<MarketSnapshot>& recent_market) {
    switch (config.algo) {
        case ExecutionAlgo::kDirect:
            return {signal.volume};
        case ExecutionAlgo::kSliced:
            return BuildUniformSlices(signal.volume, config.slice_size);
        case ExecutionAlgo::kTwap: {
            int slices = config.slice_size > 0 ? 0 : 1;
            if (config.slice_size <= 0 && config.twap_duration_ms > 0 &&
                config.slice_interval_ms > 0) {
                slices = std::max(1, config.twap_duration_ms / config.slice_interval_ms);
            }
            if (config.slice_size > 0) {
                return BuildUniformSlices(signal.volume, config.slice_size);
            }
            const int dynamic_slice = std::max(1, signal.volume / std::max(1, slices));
            return BuildUniformSlices(signal.volume, dynamic_slice);
        }
        case ExecutionAlgo::kVwapLite: {
            auto vwap = BuildVwapSlices(signal.volume, std::max(1, config.vwap_lookback_bars),
                                        recent_market);
            if (!vwap.empty()) {
                return vwap;
            }
            return BuildUniformSlices(signal.volume, std::max(1, config.slice_size));
        }
    }
    return {signal.volume};
}

}  // namespace quant_hft
