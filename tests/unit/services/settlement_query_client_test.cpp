#include <memory>
#include <string>
#include <vector>
#include <chrono>

#include <gtest/gtest.h>

#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/flow_controller.h"
#include "quant_hft/services/settlement_query_client.h"

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

std::shared_ptr<CTPTraderAdapter> BuildConnectedAdapter() {
    auto adapter = std::make_shared<CTPTraderAdapter>(10, 1);
    EXPECT_TRUE(adapter->Connect(BuildSimConfig()));
    EXPECT_TRUE(adapter->ConfirmSettlement());
    return adapter;
}

}  // namespace

TEST(SettlementQueryClientTest, QueryRequestsSucceedWithFlowPermit) {
    auto trader = BuildConnectedAdapter();
    auto flow = std::make_shared<FlowController>();
    FlowRule query_rule;
    query_rule.account_id = "acc1";
    query_rule.type = OperationType::kSettlementQuery;
    query_rule.rate_per_second = 10.0;
    query_rule.capacity = 5;
    flow->AddRule(query_rule);

    SettlementQueryClientConfig cfg;
    cfg.account_id = "acc1";
    cfg.retry_max = 3;
    cfg.backoff_initial_ms = 1;
    cfg.backoff_max_ms = 2;
    cfg.acquire_timeout_ms = 10;
    SettlementQueryClient client(trader, flow, cfg);

    std::string error;
    EXPECT_TRUE(client.QueryTradingAccountWithRetry(1, &error)) << error;
    EXPECT_TRUE(client.QueryInvestorPositionWithRetry(10, &error)) << error;
    EXPECT_TRUE(client.QueryInstrumentWithRetry(20, &error)) << error;
    trader->Disconnect();
}

TEST(SettlementQueryClientTest, OrderTradeBackfillQueriesSucceedWithFlowPermit) {
    auto trader = BuildConnectedAdapter();
    auto flow = std::make_shared<FlowController>();
    FlowRule query_rule;
    query_rule.account_id = "acc1";
    query_rule.type = OperationType::kSettlementQuery;
    query_rule.rate_per_second = 10.0;
    query_rule.capacity = 5;
    flow->AddRule(query_rule);

    SettlementQueryClientConfig cfg;
    cfg.account_id = "acc1";
    cfg.retry_max = 2;
    cfg.backoff_initial_ms = 1;
    cfg.backoff_max_ms = 2;
    cfg.acquire_timeout_ms = 10;
    SettlementQueryClient client(trader, flow, cfg);

    std::string error;
    std::vector<OrderEvent> events;
    EXPECT_TRUE(client.QueryOrderTradeBackfill(&events, &error)) << error;
    trader->Disconnect();
}

TEST(SettlementQueryClientTest, QueryFailsWhenDependenciesAreMissing) {
    SettlementQueryClientConfig cfg;
    cfg.account_id = "acc1";
    SettlementQueryClient client(nullptr, nullptr, cfg);

    std::string error;
    EXPECT_FALSE(client.QueryTradingAccountWithRetry(1, &error));
    EXPECT_NE(error.find("dependencies"), std::string::npos);
}

TEST(SettlementQueryClientTest, QueryRetriesWhenSenderFailsAndReturnsLastAttemptError) {
    auto trader = std::make_shared<CTPTraderAdapter>(10, 1);
    auto flow = std::make_shared<FlowController>();
    FlowRule query_rule;
    query_rule.account_id = "acc1";
    query_rule.type = OperationType::kSettlementQuery;
    query_rule.rate_per_second = 50.0;
    query_rule.capacity = 10;
    flow->AddRule(query_rule);

    SettlementQueryClientConfig cfg;
    cfg.account_id = "acc1";
    cfg.retry_max = 3;
    cfg.backoff_initial_ms = 1;
    cfg.backoff_max_ms = 5;
    cfg.acquire_timeout_ms = 1;
    SettlementQueryClient client(trader, flow, cfg);

    std::string error;
    const auto started = std::chrono::steady_clock::now();
    EXPECT_FALSE(client.QueryTradingAccountWithRetry(100, &error));
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - started)
                                .count();

    EXPECT_NE(error.find("attempt=3"), std::string::npos);
    EXPECT_GE(elapsed_ms, 2);
}

TEST(SettlementQueryClientTest, QueryRetriesWhenFlowRejectedAndFailsClosed) {
    auto trader = BuildConnectedAdapter();
    auto flow = std::make_shared<FlowController>();
    FlowRule query_rule;
    query_rule.account_id = "acc1";
    query_rule.type = OperationType::kSettlementQuery;
    query_rule.rate_per_second = 0.1;
    query_rule.capacity = 1;
    flow->AddRule(query_rule);

    Operation op;
    op.account_id = "acc1";
    op.type = OperationType::kSettlementQuery;
    op.instrument_id = "";
    const auto first_acquire = flow->Acquire(op, 10);
    ASSERT_TRUE(first_acquire.allowed);

    SettlementQueryClientConfig cfg;
    cfg.account_id = "acc1";
    cfg.retry_max = 3;
    cfg.backoff_initial_ms = 1;
    cfg.backoff_max_ms = 5;
    cfg.acquire_timeout_ms = 1;
    SettlementQueryClient client(trader, flow, cfg);

    std::string error;
    EXPECT_FALSE(client.QueryTradingAccountWithRetry(200, &error));
    EXPECT_NE(error.find("flow control rejected"), std::string::npos);
    trader->Disconnect();
}

}  // namespace quant_hft
