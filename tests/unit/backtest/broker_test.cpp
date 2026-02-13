#include <gtest/gtest.h>

#include "quant_hft/backtest/broker.h"

namespace quant_hft::backtest {

namespace {

Tick BuildTick(double bid, double ask, std::int32_t last_volume = 1) {
    Tick tick;
    tick.symbol = "rb2405";
    tick.bid_price1 = bid;
    tick.ask_price1 = ask;
    tick.last_price = (bid + ask) / 2.0;
    tick.last_volume = last_volume;
    tick.ts_ns = 1'700'000'000'000'000'000;
    return tick;
}

OrderIntent BuildIntent(Side side, OrderType type, double price, std::int32_t volume, OffsetFlag offset = OffsetFlag::kOpen) {
    OrderIntent intent;
    intent.account_id = "sim-account";
    intent.client_order_id = "cid-1";
    intent.strategy_id = "strategy";
    intent.instrument_id = "rb2405";
    intent.side = side;
    intent.offset = offset;
    intent.type = type;
    intent.volume = volume;
    intent.price = price;
    intent.ts_ns = 1'700'000'000'000'000'000;
    intent.trace_id = "trace";
    return intent;
}

}  // namespace

TEST(BrokerTest, MarketOrderFilledAtCurrentPrice) {
    SimulatedBroker broker;
    broker.OnTick(BuildTick(3499.0, 3501.0));

    int fills = 0;
    broker.SetFillCallback([&fills](const Trade&) { ++fills; });

    broker.PlaceOrder(BuildIntent(Side::kBuy, OrderType::kMarket, 0.0, 1));
    EXPECT_EQ(fills, 1);
}

TEST(BrokerTest, LimitOrderRespectsPrice) {
    SimulatedBroker broker;

    int fills = 0;
    broker.SetFillCallback([&fills](const Trade&) { ++fills; });

    broker.PlaceOrder(BuildIntent(Side::kBuy, OrderType::kLimit, 3500.0, 1));
    broker.OnTick(BuildTick(3500.0, 3502.0));
    EXPECT_EQ(fills, 0);

    broker.OnTick(BuildTick(3499.0, 3500.0));
    EXPECT_EQ(fills, 1);
}

TEST(BrokerTest, CommissionDeductedFromBalance) {
    BrokerConfig config;
    config.initial_capital = 1000.0;
    config.commission_rate = 0.001;
    SimulatedBroker broker(config);

    broker.OnTick(BuildTick(99.0, 101.0));
    broker.PlaceOrder(BuildIntent(Side::kBuy, OrderType::kMarket, 0.0, 1));

    EXPECT_LT(broker.GetAccountBalance(), 1000.0);
}

TEST(BrokerTest, PartialFillLeavesPending) {
    BrokerConfig config;
    config.partial_fill_enabled = true;
    SimulatedBroker broker(config);

    int fills = 0;
    broker.SetFillCallback([&fills](const Trade&) { ++fills; });

    broker.PlaceOrder(BuildIntent(Side::kBuy, OrderType::kLimit, 3501.0, 5));
    broker.OnTick(BuildTick(3499.0, 3501.0, 1));

    EXPECT_EQ(fills, 1);
}

}  // namespace quant_hft::backtest
