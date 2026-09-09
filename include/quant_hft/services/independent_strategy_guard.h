#pragma once
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <string>
#include <vector>

#include "quant_hft/config/execution_profile.h"
#include "quant_hft/contracts/types.h"
namespace quant_hft {
inline bool IndependentOpenWindowAllows(const std::string& windows, EpochNanos utc_ns) {
    const int minute =
        static_cast<int>(((utc_ns / 1000000000LL + 8 * 3600) % 86400 + 86400) % 86400 / 60);
    std::size_t start = 0;
    while (start < windows.size()) {
        const auto comma = windows.find(',', start);
        const auto token =
            windows.substr(start, comma == std::string::npos ? comma : comma - start);
        int h1 = 0, m1 = 0, h2 = 0, m2 = 0, n = 0;
        if (std::sscanf(token.c_str(), "%d:%d-%d:%d%n", &h1, &m1, &h2, &m2, &n) != 4 ||
            n != static_cast<int>(token.size()) || h1 < 0 || h1 > 23 || h2 < 0 || h2 > 23 ||
            m1 < 0 || m1 > 59 || m2 < 0 || m2 > 59)
            return false;
        const int a = h1 * 60 + m1, b = h2 * 60 + m2;
        if (a == b || (a < b ? minute >= a && minute < b : minute >= a || minute < b)) return false;
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return true;
}
inline std::string CheckIndependentStrategyOrder(const OrderIntent& intent,
                                                 const std::vector<Position>& owned,
                                                 const std::vector<Order>& account_orders,
                                                 const StrategyExecutionProfile& limits,
                                                 double equity, double current_and_frozen_margin,
                                                 double new_margin_and_fee, double multiplier,
                                                 EpochNanos utc_ns) {
    int own_quantity = 0, reserved = 0;
    for (const auto& p : owned) {
        if (p.account_id != intent.account_id || p.strategy_id != intent.strategy_id)
            return "foreign_strategy_position";
        if (p.symbol == intent.instrument_id && p.hedge_flag == intent.hedge_flag)
            own_quantity += intent.side == Side::kSell ? p.long_qty : p.short_qty;
    }
    for (const auto& o : account_orders) {
        if (o.account_id != intent.account_id || o.symbol != intent.instrument_id ||
            o.order_id == intent.client_order_id || o.quantity <= o.filled_quantity)
            continue;
        if (o.side != intent.side &&
            (intent.side == Side::kBuy ? intent.price >= o.price : intent.price <= o.price))
            return "self_trade_reject_new_order";
        if (o.strategy_id == intent.strategy_id && o.offset != OffsetFlag::kOpen &&
            o.side == intent.side && o.hedge_flag == intent.hedge_flag)
            reserved += std::max(0, o.quantity - o.filled_quantity);
    }
    if (intent.volume <= 0 || limits.max_order_volume <= 0 ||
        intent.volume > limits.max_order_volume)
        return "strategy_max_order_volume";
    if (!std::isfinite(limits.max_order_notional) || limits.max_order_notional <= 0 ||
        !std::isfinite(multiplier) || multiplier <= 0 || !std::isfinite(intent.price) ||
        intent.price <= 0 || intent.price * multiplier * intent.volume > limits.max_order_notional)
        return "strategy_max_order_notional";
    if (intent.offset != OffsetFlag::kOpen)
        return intent.volume <= own_quantity - reserved ? ""
                                                        : "strategy_close_ownership_or_reservation";
    if (!std::isfinite(equity) || equity <= 0) return "strategy_equity_unavailable";
    if (!IndependentOpenWindowAllows(limits.forbid_open_windows, utc_ns))
        return "strategy_forbid_open_window";
    if (!std::isfinite(limits.max_margin_to_equity_ratio) ||
        limits.max_margin_to_equity_ratio <= 0 || limits.max_margin_to_equity_ratio > 1)
        return "strategy_margin_budget";
    if (!std::isfinite(current_and_frozen_margin) || current_and_frozen_margin < 0 ||
        !std::isfinite(new_margin_and_fee) || new_margin_and_fee <= 0 ||
        current_and_frozen_margin + new_margin_and_fee > equity * limits.max_margin_to_equity_ratio)
        return "strategy_margin_budget";
    return "";
}
}  // namespace quant_hft
