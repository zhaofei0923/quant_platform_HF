#include "quant_hft/services/ctp_account_ledger.h"

#include <algorithm>
#include <array>
#include <cmath>

namespace quant_hft {

namespace {

bool IsCancelActionFeedback(const OrderEvent& event) {
    return event.event_source == "OnRspOrderAction" ||
           event.event_source == "OnErrRtnOrderAction";
}

}  // namespace

double CtpAccountLedger::ResolveMarginPrice(char margin_price_type,
                                            const CtpMarginPriceInputs& prices) {
    const auto pre_settlement = NormalizePrice(prices.pre_settlement_price);
    const auto settlement = NormalizePrice(prices.settlement_price);
    const auto average = NormalizePrice(prices.average_price);
    const auto open = NormalizePrice(prices.open_price);

    double selected = 0.0;
    switch (margin_price_type) {
        case '1':
            selected = pre_settlement;
            break;
        case '2':
            selected = settlement;
            break;
        case '3':
            selected = average;
            break;
        case '4':
            selected = open;
            break;
        default:
            break;
    }
    if (selected > 0.0) {
        return selected;
    }

    const std::array<double, 4> fallbacks{settlement, pre_settlement, average, open};
    for (const double candidate : fallbacks) {
        if (candidate > 0.0) {
            return candidate;
        }
    }
    return 0.0;
}

double CtpAccountLedger::ComputePositionMargin(char margin_price_type,
                                               const CtpMarginPriceInputs& prices,
                                               std::int32_t position_volume,
                                               std::int32_t volume_multiple,
                                               double margin_rate) {
    const auto base_price = ResolveMarginPrice(margin_price_type, prices);
    const auto effective_rate = std::max(0.0, margin_rate);
    const auto effective_volume_multiple = std::max(0, volume_multiple);
    const auto effective_position = std::abs(position_volume);
    return base_price * static_cast<double>(effective_position) *
           static_cast<double>(effective_volume_multiple) * effective_rate;
}

double CtpAccountLedger::ComputeOrderMargin(const CtpOrderFundInputs& inputs) {
    const auto price = NormalizePrice(inputs.price);
    const auto volume = std::max(0, inputs.volume);
    const auto volume_multiple = std::max(0, inputs.volume_multiple);
    const auto margin_by_money = std::max(0.0, inputs.margin_ratio_by_money);
    const auto margin_by_volume = std::max(0.0, inputs.margin_ratio_by_volume);
    return price * static_cast<double>(volume) * static_cast<double>(volume_multiple) *
               margin_by_money +
           static_cast<double>(volume) * margin_by_volume;
}

double CtpAccountLedger::ComputeOrderCommission(const CtpOrderFundInputs& inputs) {
    const auto price = NormalizePrice(inputs.price);
    const auto volume = std::max(0, inputs.volume);
    const auto volume_multiple = std::max(0, inputs.volume_multiple);
    const auto commission_by_money = std::max(0.0, inputs.commission_ratio_by_money);
    const auto commission_by_volume = std::max(0.0, inputs.commission_ratio_by_volume);
    return price * static_cast<double>(volume) * static_cast<double>(volume_multiple) *
               commission_by_money +
           static_cast<double>(volume) * commission_by_volume;
}

void CtpAccountLedger::SetMarginPriceType(char margin_price_type) {
    const bool supported = margin_price_type == '1' || margin_price_type == '2' ||
                           margin_price_type == '3' || margin_price_type == '4';
    std::lock_guard<std::mutex> lock(mutex_);
    margin_price_type_ = supported ? margin_price_type : '1';
}

char CtpAccountLedger::margin_price_type() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return margin_price_type_;
}

void CtpAccountLedger::ApplyTradingAccountSnapshot(
    const TradingAccountSnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);
    balance_ = snapshot.balance;
    available_ = snapshot.available;
    current_margin_ = snapshot.curr_margin;
    frozen_margin_ = snapshot.frozen_margin;
    frozen_commission_ = snapshot.frozen_commission;
    commission_ = snapshot.commission;
    trading_day_ = snapshot.trading_day;
}

bool CtpAccountLedger::ReserveOrderFunds(const CtpOrderFundInputs& inputs, std::string* error) {
    if (inputs.client_order_id.empty() || inputs.volume <= 0) {
        if (error != nullptr) {
            *error = "invalid order fund inputs";
        }
        return false;
    }

    const auto margin = ComputeOrderMargin(inputs);
    const auto commission = ComputeOrderCommission(inputs);
    const auto required = margin + commission;

    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_order_funds_.find(inputs.client_order_id) != pending_order_funds_.end()) {
        if (error != nullptr) {
            *error = "duplicate fund reservation";
        }
        return false;
    }
    if (available_ + 1e-9 < required) {
        if (error != nullptr) {
            *error = "insufficient available funds";
        }
        return false;
    }

    PendingOrderFunds pending;
    pending.requested_volume = inputs.volume;
    pending.margin_per_volume = margin / static_cast<double>(inputs.volume);
    pending.commission_per_volume = commission / static_cast<double>(inputs.volume);
    pending.frozen_margin_remaining = margin;
    pending.frozen_commission_remaining = commission;
    available_ -= required;
    frozen_margin_ += margin;
    frozen_commission_ += commission;
    pending_order_funds_[inputs.client_order_id] = pending;
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

bool CtpAccountLedger::ApplyOrderEvent(const OrderEvent& event, std::string* error) {
    if (IsCancelActionFeedback(event)) {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }
    if (event.client_order_id.empty()) {
        if (error != nullptr) {
            *error = "event.client_order_id is required";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = pending_order_funds_.find(event.client_order_id);
    if (it == pending_order_funds_.end()) {
        if (error != nullptr) {
            *error = "order funds not reserved";
        }
        return false;
    }

    auto& pending = it->second;
    if (event.filled_volume < pending.last_filled_volume) {
        if (error != nullptr) {
            *error = "filled_volume cannot decrease";
        }
        return false;
    }

    const auto delta_filled = event.filled_volume - pending.last_filled_volume;
    if (delta_filled > 0) {
        const auto margin_to_use = std::min(pending.frozen_margin_remaining,
                                            pending.margin_per_volume * delta_filled);
        const auto commission_to_use = std::min(pending.frozen_commission_remaining,
                                                pending.commission_per_volume * delta_filled);
        pending.frozen_margin_remaining -= margin_to_use;
        pending.frozen_commission_remaining -= commission_to_use;
        frozen_margin_ = std::max(0.0, frozen_margin_ - margin_to_use);
        frozen_commission_ = std::max(0.0, frozen_commission_ - commission_to_use);
        current_margin_ += margin_to_use;
        commission_ += commission_to_use;
        balance_ -= commission_to_use;
        pending.last_filled_volume = event.filled_volume;
    }

    if (IsTerminalStatus(event.status)) {
        const auto release = pending.frozen_margin_remaining + pending.frozen_commission_remaining;
        available_ += release;
        frozen_margin_ = std::max(0.0, frozen_margin_ - pending.frozen_margin_remaining);
        frozen_commission_ =
            std::max(0.0, frozen_commission_ - pending.frozen_commission_remaining);
        pending_order_funds_.erase(it);
    }

    if (error != nullptr) {
        error->clear();
    }
    return true;
}

void CtpAccountLedger::ApplyDailySettlement(double previous_settlement_price,
                                            double new_settlement_price,
                                            std::int32_t net_position,
                                            std::int32_t volume_multiple) {
    const auto prev = NormalizePrice(previous_settlement_price);
    const auto current = NormalizePrice(new_settlement_price);
    const auto effective_volume_multiple = std::max(0, volume_multiple);
    const auto delta = (current - prev) * static_cast<double>(net_position) *
                       static_cast<double>(effective_volume_multiple);
    std::lock_guard<std::mutex> lock(mutex_);
    balance_ += delta;
    available_ += delta;
    daily_settlement_pnl_ += delta;
}

void CtpAccountLedger::RollTradingDay(const std::string& trading_day) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (trading_day_ != trading_day) {
        trading_day_ = trading_day;
        daily_settlement_pnl_ = 0.0;
    }
}

double CtpAccountLedger::balance() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return balance_;
}

double CtpAccountLedger::available() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return available_;
}

double CtpAccountLedger::current_margin() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_margin_;
}

double CtpAccountLedger::frozen_margin() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return frozen_margin_;
}

double CtpAccountLedger::frozen_commission() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return frozen_commission_;
}

double CtpAccountLedger::commission() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return commission_;
}

double CtpAccountLedger::daily_settlement_pnl() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return daily_settlement_pnl_;
}

std::string CtpAccountLedger::trading_day() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return trading_day_;
}

double CtpAccountLedger::NormalizePrice(double price) {
    if (!std::isfinite(price) || price <= 0.0) {
        return 0.0;
    }
    return price;
}

bool CtpAccountLedger::IsTerminalStatus(OrderStatus status) {
    return status == OrderStatus::kFilled || status == OrderStatus::kCanceled ||
           status == OrderStatus::kRejected;
}

}  // namespace quant_hft
