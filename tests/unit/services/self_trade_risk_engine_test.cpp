#include <gtest/gtest.h>

#include "quant_hft/services/self_trade_risk_engine.h"

namespace quant_hft {
namespace {

OrderIntent MakeIntent(const std::string& client_order_id,
                       Side side,
                       double price,
                       std::int32_t volume = 1) {
    OrderIntent intent;
    intent.account_id = "acc-1";
    intent.client_order_id = client_order_id;
    intent.instrument_id = "SHFE.ag2406";
    intent.side = side;
    intent.offset = OffsetFlag::kOpen;
    intent.type = OrderType::kLimit;
    intent.volume = volume;
    intent.price = price;
    intent.ts_ns = 1;
    intent.trace_id = "trace-1";
    return intent;
}

OrderEvent MakeOrderEvent(const std::string& client_order_id,
                          OrderStatus status,
                          std::int32_t total_volume,
                          std::int32_t filled_volume) {
    OrderEvent event;
    event.client_order_id = client_order_id;
    event.account_id = "acc-1";
    event.instrument_id = "SHFE.ag2406";
    event.status = status;
    event.total_volume = total_volume;
    event.filled_volume = filled_volume;
    event.ts_ns = 2;
    return event;
}

}  // namespace

TEST(SelfTradeRiskEngineTest, StrictModeRejectsCrossingOrder) {
    SelfTradeRiskConfig config;
    config.enabled = true;
    config.strict_mode = true;
    config.strict_mode_trigger_hits = 1;
    SelfTradeRiskEngine engine(config);

    engine.RecordAcceptedOrder(MakeIntent("sell-1", Side::kSell, 100.0, 1));
    const auto decision = engine.PreCheck(MakeIntent("buy-cross", Side::kBuy, 101.0, 1));
    EXPECT_EQ(decision.action, RiskAction::kReject);
    EXPECT_NE(decision.reason.find("self_trade"), std::string::npos);
}

TEST(SelfTradeRiskEngineTest, EscalatesFromWarnOnlyToStrictModeAfterThreshold) {
    SelfTradeRiskConfig config;
    config.enabled = true;
    config.strict_mode = false;
    config.strict_mode_trigger_hits = 2;
    SelfTradeRiskEngine engine(config);

    engine.RecordAcceptedOrder(MakeIntent("sell-1", Side::kSell, 100.0, 1));
    const auto first = engine.PreCheck(MakeIntent("buy-cross-1", Side::kBuy, 100.0, 1));
    EXPECT_EQ(first.action, RiskAction::kAllow);
    EXPECT_FALSE(engine.strict_mode());

    const auto second = engine.PreCheck(MakeIntent("buy-cross-2", Side::kBuy, 101.0, 1));
    EXPECT_EQ(second.action, RiskAction::kReject);
    EXPECT_TRUE(engine.strict_mode());
    EXPECT_EQ(engine.conflict_hits(), 2);
}

TEST(SelfTradeRiskEngineTest, NonCrossingOrderPasses) {
    SelfTradeRiskConfig config;
    config.enabled = true;
    config.strict_mode = true;
    config.strict_mode_trigger_hits = 1;
    SelfTradeRiskEngine engine(config);

    engine.RecordAcceptedOrder(MakeIntent("sell-1", Side::kSell, 105.0, 1));
    const auto decision = engine.PreCheck(MakeIntent("buy-pass", Side::kBuy, 100.0, 1));
    EXPECT_EQ(decision.action, RiskAction::kAllow);
}

TEST(SelfTradeRiskEngineTest, TerminalOrderEventRemovesActiveOrder) {
    SelfTradeRiskConfig config;
    config.enabled = true;
    config.strict_mode = true;
    config.strict_mode_trigger_hits = 1;
    SelfTradeRiskEngine engine(config);

    engine.RecordAcceptedOrder(MakeIntent("sell-1", Side::kSell, 100.0, 1));
    engine.OnOrderEvent(MakeOrderEvent("sell-1", OrderStatus::kCanceled, 1, 0));

    const auto decision = engine.PreCheck(MakeIntent("buy-after-cancel", Side::kBuy, 101.0, 1));
    EXPECT_EQ(decision.action, RiskAction::kAllow);
}

}  // namespace quant_hft
