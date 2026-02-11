#include "quant_hft/services/in_memory_portfolio_ledger.h"

#include <functional>
#include <sstream>

namespace quant_hft {

std::size_t InMemoryPortfolioLedger::PositionKeyHasher::operator()(const PositionKey& key) const {
    const auto h1 = std::hash<std::string>{}(key.account_id);
    const auto h2 = std::hash<std::string>{}(key.instrument_id);
    const auto h3 = std::hash<int>{}(static_cast<int>(key.direction));
    return h1 ^ (h2 << 1) ^ (h3 << 2);
}

void InMemoryPortfolioLedger::OnOrderEvent(const OrderEvent& event) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto event_key = BuildEventKey(event);
    if (!applied_event_keys_.insert(event_key).second) {
        return;
    }

    if (event.filled_volume <= 0 || event.avg_fill_price <= 0.0) {
        return;
    }

    auto& last_filled = order_last_filled_[event.client_order_id];
    if (event.filled_volume <= last_filled) {
        return;
    }

    const auto delta_volume = event.filled_volume - last_filled;
    last_filled = event.filled_volume;

    PositionDirection direction = PositionDirection::kLong;
    if (event.reason == "short") {
        direction = PositionDirection::kShort;
    }

    PositionKey key{event.account_id, event.instrument_id, direction};
    auto& pos = positions_[key];
    pos.account_id = event.account_id;
    pos.instrument_id = event.instrument_id;
    pos.direction = direction;
    const auto total_cost = pos.avg_price * static_cast<double>(pos.volume) +
                            event.avg_fill_price * static_cast<double>(delta_volume);
    pos.volume += delta_volume;
    if (pos.volume > 0) {
        pos.avg_price = total_cost / static_cast<double>(pos.volume);
    }
    pos.margin = pos.avg_price * static_cast<double>(pos.volume) * 0.1;
    pos.ts_ns = event.ts_ns;
}

PositionSnapshot InMemoryPortfolioLedger::GetPositionSnapshot(const std::string& account_id,
                                                              const std::string& instrument_id,
                                                              PositionDirection direction) const {
    const PositionKey key{account_id, instrument_id, direction};
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = positions_.find(key);
    if (it == positions_.end()) {
        PositionSnapshot empty;
        empty.account_id = account_id;
        empty.instrument_id = instrument_id;
        empty.direction = direction;
        empty.ts_ns = NowEpochNanos();
        return empty;
    }
    return it->second;
}

std::string InMemoryPortfolioLedger::BuildEventKey(const OrderEvent& event) {
    std::ostringstream oss;
    oss << event.client_order_id << '|'
        << static_cast<int>(event.status) << '|'
        << event.filled_volume << '|'
        << event.avg_fill_price << '|'
        << event.ts_ns << '|'
        << event.trace_id << '|'
        << event.reason;
    return oss.str();
}

}  // namespace quant_hft
