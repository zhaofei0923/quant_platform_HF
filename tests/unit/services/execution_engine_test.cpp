#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "quant_hft/core/circuit_breaker.h"
#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/flow_controller.h"
#include "quant_hft/services/execution_engine.h"

namespace quant_hft {

namespace {

MarketDataConnectConfig BuildSimConfig() {
    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "pwd";
    cfg.is_production_mode = false;
    return cfg;
}

OrderIntent BuildOrder(const std::string& client_order_id) {
    OrderIntent intent;
    intent.account_id = "acc1";
    intent.client_order_id = client_order_id;
    intent.strategy_id = "strat1";
    intent.instrument_id = "SHFE.ag2406";
    intent.volume = 1;
    intent.price = 4000.0;
    intent.trace_id = client_order_id;
    return intent;
}

std::shared_ptr<CTPTraderAdapter> BuildConnectedAdapter() {
    auto adapter = std::make_shared<CTPTraderAdapter>(10, 1);
    EXPECT_TRUE(adapter->Connect(BuildSimConfig()));
    EXPECT_TRUE(adapter->ConfirmSettlement());
    return adapter;
}

std::shared_ptr<CircuitBreakerManager> BuildBreakerManager() {
    auto breaker = std::make_shared<CircuitBreakerManager>();
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 3;
    cfg.timeout_ms = 1000;
    cfg.half_open_timeout_ms = 1000;
    breaker->Configure(BreakerScope::kStrategy, cfg, true);
    breaker->Configure(BreakerScope::kAccount, cfg, true);
    breaker->Configure(BreakerScope::kSystem, cfg, true);
    return breaker;
}

}  // namespace

TEST(ExecutionEngineTest, PlaceOrderSuccessPath) {
    auto adapter = BuildConnectedAdapter();
    auto flow = std::make_shared<FlowController>();
    FlowRule insert_rule;
    insert_rule.account_id = "acc1";
    insert_rule.type = OperationType::kOrderInsert;
    insert_rule.rate_per_second = 100.0;
    insert_rule.capacity = 10;
    flow->AddRule(insert_rule);
    auto breaker = BuildBreakerManager();

    ExecutionEngine engine(adapter, flow, breaker, 0);
    EXPECT_TRUE(engine.PlaceOrder(BuildOrder("ord-1")));
}

TEST(ExecutionEngineTest, OpenBreakerRejectsOrderBeforeFlowAndSend) {
    auto adapter = BuildConnectedAdapter();
    auto flow = std::make_shared<FlowController>();
    FlowRule insert_rule;
    insert_rule.account_id = "acc1";
    insert_rule.type = OperationType::kOrderInsert;
    insert_rule.rate_per_second = 100.0;
    insert_rule.capacity = 10;
    flow->AddRule(insert_rule);

    auto breaker = BuildBreakerManager();
    CircuitBreakerConfig strict;
    strict.failure_threshold = 1;
    strict.timeout_ms = 1000;
    strict.half_open_timeout_ms = 1000;
    breaker->Configure(BreakerScope::kStrategy, strict, true);
    breaker->RecordFailure(BreakerScope::kStrategy, "strat1");

    ExecutionEngine engine(adapter, flow, breaker, 0);
    EXPECT_FALSE(engine.PlaceOrder(BuildOrder("ord-2")));
}

TEST(ExecutionEngineTest, FlowLimitCanRejectSecondOrder) {
    auto adapter = BuildConnectedAdapter();
    auto flow = std::make_shared<FlowController>();
    FlowRule insert_rule;
    insert_rule.account_id = "acc1";
    insert_rule.type = OperationType::kOrderInsert;
    insert_rule.rate_per_second = 0.5;
    insert_rule.capacity = 1;
    flow->AddRule(insert_rule);
    auto breaker = BuildBreakerManager();

    ExecutionEngine engine(adapter, flow, breaker, 0);
    EXPECT_TRUE(engine.PlaceOrder(BuildOrder("ord-3")));
    EXPECT_FALSE(engine.PlaceOrder(BuildOrder("ord-4")));
}

TEST(ExecutionEngineTest, QueryRespectsQueryFlowRule) {
    auto adapter = BuildConnectedAdapter();
    auto flow = std::make_shared<FlowController>();
    FlowRule query_rule;
    query_rule.account_id = "acc1";
    query_rule.type = OperationType::kQuery;
    query_rule.rate_per_second = 0.5;
    query_rule.capacity = 1;
    flow->AddRule(query_rule);
    auto breaker = BuildBreakerManager();

    ExecutionEngine engine(adapter, flow, breaker, 0);
    EXPECT_TRUE(engine.QueryTradingAccount(1, "acc1"));
    EXPECT_FALSE(engine.QueryTradingAccount(2, "acc1"));
}

}  // namespace quant_hft
