#include "quant_hft/services/in_memory_portfolio_ledger.h"

#include <algorithm>
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

    if (event.filled_volume <= 0) {
        return;
    }

    auto& progress = order_fill_progress_[BuildOrderKey(event)];
    if (event.filled_volume <= progress.filled_volume) {
        return;
    }

    const auto delta_volume = event.filled_volume - progress.filled_volume;
    const auto cumulative_notional =
        event.avg_fill_price > 0.0 ? event.avg_fill_price * static_cast<double>(event.filled_volume)
                                   : progress.cumulative_notional;
    double delta_notional = cumulative_notional - progress.cumulative_notional;
    if (delta_notional < 0.0) {
        // A malformed/replayed cumulative average must not reduce the local cost basis.
        delta_notional = event.avg_fill_price > 0.0
                             ? event.avg_fill_price * static_cast<double>(delta_volume)
                             : 0.0;
    }
    progress.filled_volume = event.filled_volume;
    progress.cumulative_notional = cumulative_notional;

    const bool is_open = event.offset == OffsetFlag::kOpen;
    // Buy-open creates long, sell-open creates short.  A close has the inverse
    // ownership: sell-close reduces long and buy-close reduces short.
    const PositionDirection direction =
        is_open
            ? (event.side == Side::kBuy ? PositionDirection::kLong : PositionDirection::kShort)
            : (event.side == Side::kSell ? PositionDirection::kLong : PositionDirection::kShort);

    PositionKey key{event.account_id, event.instrument_id, direction};
    auto& pos = positions_[key];
    pos.account_id = event.account_id;
    pos.instrument_id = event.instrument_id;
    pos.direction = direction;
    if (is_open) {
        const auto total_cost = pos.avg_price * static_cast<double>(pos.volume) + delta_notional;
        pos.volume += delta_volume;
        if (pos.volume > 0 && total_cost > 0.0) {
            pos.avg_price = total_cost / static_cast<double>(pos.volume);
        }
    } else {
        pos.volume = std::max<std::int32_t>(0, pos.volume - delta_volume);
        if (pos.volume == 0) {
            pos.avg_price = 0.0;
        }
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
    oss << event.client_order_id << '|' << static_cast<int>(event.status) << '|'
        << event.filled_volume << '|' << event.avg_fill_price << '|' << event.ts_ns << '|'
        << event.trace_id << '|' << event.reason;
    return oss.str();
}

std::string InMemoryPortfolioLedger::BuildOrderKey(const OrderEvent& event) {
    std::ostringstream oss;
    oss << event.account_id << '|' << event.trading_day << '|';
    if (!event.client_order_id.empty()) {
        oss << "client:" << event.client_order_id;
    } else if (!event.exchange_order_id.empty()) {
        oss << "exchange:" << event.exchange_id << ':' << event.exchange_order_id;
    } else {
        oss << "legacy:" << event.instrument_id << ':' << event.order_ref << ':' << event.front_id
            << ':' << event.session_id;
    }
    return oss.str();
}

}  // namespace quant_hft
