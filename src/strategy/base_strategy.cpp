#include "quant_hft/strategy/base_strategy.h"

#include <stdexcept>

#include "quant_hft/backtest/broker.h"
#include "quant_hft/common/timestamp.h"

namespace quant_hft {

void Strategy::Buy(const std::string& symbol, double price, std::int32_t volume) {
    if (broker_ == nullptr) {
        throw std::runtime_error("strategy broker context is not bound");
    }

    OrderIntent intent;
    intent.account_id = "sim-account";
    intent.client_order_id = "py-buy-" + std::to_string(++order_seed_);
    intent.strategy_id = "strategy";
    intent.instrument_id = symbol;
    intent.side = Side::kBuy;
    intent.offset = OffsetFlag::kOpen;
    intent.type = OrderType::kLimit;
    intent.volume = volume;
    intent.price = price;
    intent.ts_ns = Timestamp::Now().ToEpochNanos();
    intent.trace_id = intent.client_order_id;

    broker_->PlaceOrder(intent);
}

void Strategy::Sell(const std::string& symbol, double price, std::int32_t volume) {
    if (broker_ == nullptr) {
        throw std::runtime_error("strategy broker context is not bound");
    }

    OrderIntent intent;
    intent.account_id = "sim-account";
    intent.client_order_id = "py-sell-" + std::to_string(++order_seed_);
    intent.strategy_id = "strategy";
    intent.instrument_id = symbol;
    intent.side = Side::kSell;
    intent.offset = OffsetFlag::kClose;
    intent.type = OrderType::kLimit;
    intent.volume = volume;
    intent.price = price;
    intent.ts_ns = Timestamp::Now().ToEpochNanos();
    intent.trace_id = intent.client_order_id;

    broker_->PlaceOrder(intent);
}

void Strategy::CancelOrder(const std::string& client_order_id) {
    if (broker_ == nullptr) {
        throw std::runtime_error("strategy broker context is not bound");
    }
    broker_->CancelOrder(client_order_id);
}

void Strategy::BindContext(DataFeed* data_feed, backtest::SimulatedBroker* broker) {
    data_feed_ = data_feed;
    broker_ = broker;
}

}  // namespace quant_hft
