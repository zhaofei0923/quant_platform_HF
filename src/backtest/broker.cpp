#include "quant_hft/backtest/broker.h"

#include <algorithm>
#include <cmath>
#include <deque>
#include <utility>

namespace quant_hft::backtest {

SimulatedBroker::SimulatedBroker(BrokerConfig config)
    : config_(config), account_balance_(config.initial_capital) {}

void SimulatedBroker::OnTick(const Tick& tick) {
    last_tick_by_symbol_[tick.symbol] = tick;

    auto process_side = [&](std::vector<PendingOrder>* orders) {
        for (auto* pending = orders->data(); pending != orders->data() + orders->size(); ++pending) {
            if (pending->remaining_volume <= 0 || pending->order.symbol != tick.symbol) {
                continue;
            }
            TryMatchOrder(pending, tick);
        }

        orders->erase(
            std::remove_if(orders->begin(), orders->end(), [](const PendingOrder& pending) {
                return pending.remaining_volume <= 0 || pending.order.status == OrderStatus::kCanceled;
            }),
            orders->end());
    };

    process_side(&buy_orders_);
    process_side(&sell_orders_);
}

std::string SimulatedBroker::PlaceOrder(const OrderIntent& intent) {
    PendingOrder pending;
    pending.order.order_id = "ord-" + std::to_string(++id_seed_);
    pending.order.account_id = intent.account_id;
    pending.order.strategy_id = intent.strategy_id;
    pending.order.symbol = intent.instrument_id;
    pending.order.exchange = "";
    pending.order.side = intent.side;
    pending.order.offset = intent.offset;
    pending.order.order_type = intent.type;
    pending.order.price = intent.price;
    pending.order.quantity = intent.volume;
    pending.order.filled_quantity = 0;
    pending.order.status = OrderStatus::kNew;
    pending.order.created_at_ns = intent.ts_ns;
    pending.order.updated_at_ns = intent.ts_ns;
    pending.offset = intent.offset;
    pending.remaining_volume = intent.volume;
    pending.is_market = intent.type == OrderType::kMarket;

    if (order_callback_) {
        order_callback_(pending.order);
    }

    if (pending.order.side == Side::kBuy) {
        buy_orders_.push_back(pending);
    } else {
        sell_orders_.push_back(pending);
    }

    const auto tick_it = last_tick_by_symbol_.find(pending.order.symbol);
    if (tick_it != last_tick_by_symbol_.end()) {
        OnTick(tick_it->second);
    }

    return pending.order.order_id;
}

bool SimulatedBroker::CancelOrder(const std::string& client_order_id) {
    auto cancel_in = [&](std::vector<PendingOrder>* orders) {
        for (auto& pending : *orders) {
            if (pending.order.order_id != client_order_id) {
                continue;
            }
            pending.order.status = OrderStatus::kCanceled;
            pending.remaining_volume = 0;
            if (order_callback_) {
                order_callback_(pending.order);
            }
            return true;
        }
        return false;
    };

    return cancel_in(&buy_orders_) || cancel_in(&sell_orders_);
}

std::vector<Position> SimulatedBroker::GetPositions(const std::string& symbol) const {
    std::vector<Position> result;
    for (const auto& [instrument, lots] : lots_by_symbol_) {
        if (!symbol.empty() && instrument != symbol) {
            continue;
        }

        Position position;
        position.symbol = instrument;
        for (const auto& lot : lots) {
            if (lot.direction == PositionDirection::kLong) {
                position.long_qty += lot.volume;
            } else {
                position.short_qty += lot.volume;
            }
        }
        result.push_back(position);
    }
    return result;
}

double SimulatedBroker::GetAccountBalance() const {
    return account_balance_;
}

void SimulatedBroker::SetFillCallback(std::function<void(const Trade&)> callback) {
    fill_callback_ = std::move(callback);
}

void SimulatedBroker::SetOrderCallback(std::function<void(const Order&)> callback) {
    order_callback_ = std::move(callback);
}

void SimulatedBroker::TryMatchOrder(PendingOrder* pending, const Tick& tick) {
    if (pending->remaining_volume <= 0) {
        return;
    }

    const double bid = tick.bid_price1 > 0.0 ? tick.bid_price1 : tick.last_price;
    const double ask = tick.ask_price1 > 0.0 ? tick.ask_price1 : tick.last_price;

    bool should_fill = pending->is_market;
    double match_price = pending->order.side == Side::kBuy ? ask : bid;

    if (!pending->is_market) {
        if (pending->order.side == Side::kBuy) {
            should_fill = pending->order.price >= ask;
            match_price = pending->order.price;
        } else {
            should_fill = pending->order.price <= bid;
            match_price = pending->order.price;
        }
    }

    if (!should_fill) {
        return;
    }

    const std::int32_t available_liquidity = tick.last_volume > 0 ? tick.last_volume : pending->remaining_volume;
    std::int32_t fill_qty = pending->remaining_volume;
    if (config_.partial_fill_enabled) {
        fill_qty = std::max(1, std::min(pending->remaining_volume, available_liquidity));
    }

    const double filled_price = ApplySlippage(match_price, pending->order.side);
    const double commission = ComputeCommission(*pending, fill_qty, filled_price);

    pending->remaining_volume -= fill_qty;
    pending->order.filled_quantity += fill_qty;
    pending->order.avg_fill_price = filled_price;
    pending->order.updated_at_ns = tick.ts_ns;
    pending->order.status = pending->remaining_volume == 0 ? OrderStatus::kFilled
                                                           : OrderStatus::kPartiallyFilled;

    Trade trade;
    trade.trade_id = "trd-" + std::to_string(++id_seed_);
    trade.order_id = pending->order.order_id;
    trade.account_id = pending->order.account_id;
    trade.strategy_id = pending->order.strategy_id;
    trade.symbol = pending->order.symbol;
    trade.exchange = pending->order.exchange;
    trade.side = pending->order.side;
    trade.offset = pending->offset;
    trade.price = filled_price;
    trade.quantity = fill_qty;
    trade.trade_ts_ns = tick.ts_ns;
    trade.commission = commission;

    ApplyTradeToPosition(trade);
    account_balance_ -= commission;

    if (order_callback_) {
        order_callback_(pending->order);
    }
    if (fill_callback_) {
        fill_callback_(trade);
    }
}

double SimulatedBroker::ComputeCommission(const PendingOrder& pending,
                                          std::int32_t fill_qty,
                                          double fill_price) const {
    const double amount = fill_price * static_cast<double>(fill_qty);
    const bool is_close = pending.offset != OffsetFlag::kOpen;
    const double rate = is_close ? config_.close_today_commission_rate : config_.commission_rate;
    return amount * rate;
}

double SimulatedBroker::ApplySlippage(double raw_price, Side side) const {
    if (config_.slippage <= 0.0) {
        return raw_price;
    }
    return side == Side::kBuy ? raw_price + config_.slippage : raw_price - config_.slippage;
}

void SimulatedBroker::ApplyTradeToPosition(const Trade& trade) {
    auto& lots = lots_by_symbol_[trade.symbol];

    auto consume_lots = [&](PositionDirection direction_to_consume, std::int32_t qty_to_close) {
        double realized = 0.0;
        for (auto lot_it = lots.begin(); lot_it != lots.end() && qty_to_close > 0;) {
            if (lot_it->direction != direction_to_consume || lot_it->volume <= 0) {
                ++lot_it;
                continue;
            }

            const std::int32_t matched = std::min(lot_it->volume, qty_to_close);
            if (direction_to_consume == PositionDirection::kLong) {
                realized += (trade.price - lot_it->open_price) * matched;
            } else {
                realized += (lot_it->open_price - trade.price) * matched;
            }
            lot_it->volume -= matched;
            qty_to_close -= matched;
            if (lot_it->volume == 0) {
                lot_it = lots.erase(lot_it);
            } else {
                ++lot_it;
            }
        }
        account_balance_ += realized;
    };

    if (trade.offset == OffsetFlag::kOpen) {
        PositionLot lot;
        lot.direction = trade.side == Side::kBuy ? PositionDirection::kLong : PositionDirection::kShort;
        lot.volume = trade.quantity;
        lot.open_price = trade.price;
        lots.push_back(lot);
        return;
    }

    if (trade.side == Side::kSell) {
        consume_lots(PositionDirection::kLong, trade.quantity);
    } else {
        consume_lots(PositionDirection::kShort, trade.quantity);
    }
}

}  // namespace quant_hft::backtest
