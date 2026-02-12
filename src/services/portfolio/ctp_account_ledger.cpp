#include "quant_hft/services/ctp_account_ledger.h"

#include <algorithm>
#include <array>
#include <cmath>

namespace quant_hft {

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
    trading_day_ = snapshot.trading_day;
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

}  // namespace quant_hft
