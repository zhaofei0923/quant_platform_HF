#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/core/circuit_breaker.h"
#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/flow_controller.h"
#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/core/trading_domain_store_client_adapter.h"
#include "quant_hft/core/timescale_sql_client.h"
#include "quant_hft/risk/risk_manager.h"
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
    intent.ts_ns = 100;
    return intent;
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

struct EngineBundle {
    std::shared_ptr<CTPTraderAdapter> adapter;
    std::shared_ptr<FlowController> flow;
    std::shared_ptr<CircuitBreakerManager> breaker;
    std::shared_ptr<TradingDomainStoreClientAdapter> store;
    std::shared_ptr<OrderManager> order_manager;
    std::shared_ptr<PositionManager> position_manager;
    std::shared_ptr<InMemoryRedisHashClient> redis;
    std::shared_ptr<InMemoryTimescaleSqlClient> sql;
    std::unique_ptr<ExecutionEngine> engine;
};

class RejectAllRiskManager final : public RiskManager {
public:
    bool Initialize(const RiskManagerConfig& config) override {
        (void)config;
        return true;
    }

    RiskCheckResult CheckOrder(const OrderIntent& intent, const OrderContext& context) override {
        (void)intent;
        (void)context;
        RiskCheckResult result;
        result.allowed = false;
        result.violated_rule = RiskRuleType::MAX_ORDER_VOLUME;
        result.reason = "forced reject";
        return result;
    }

    RiskCheckResult CheckCancel(const std::string& client_order_id,
                                const OrderContext& context) override {
        (void)client_order_id;
        (void)context;
        RiskCheckResult result;
        result.allowed = true;
        return result;
    }

    void OnTrade(const Trade& trade) override {
        (void)trade;
    }

    void OnOrderRejected(const Order& order, const std::string& reason) override {
        (void)order;
        (void)reason;
    }

    bool ReloadRules(const std::vector<RiskRule>& rules) override {
        (void)rules;
        return true;
    }

    std::vector<RiskRule> GetActiveRules() const override {
        return {};
    }

    void ResetDailyStats() override {}

    void RegisterRiskEventCallback(RiskEventCallback callback) override {
        (void)callback;
    }
};

EngineBundle BuildEngineBundle() {
    EngineBundle bundle;
    bundle.adapter = std::make_shared<CTPTraderAdapter>(10, 1);
    EXPECT_TRUE(bundle.adapter->Connect(BuildSimConfig()));
    EXPECT_TRUE(bundle.adapter->ConfirmSettlement());

    bundle.flow = std::make_shared<FlowController>();
    FlowRule insert_rule;
    insert_rule.account_id = "acc1";
    insert_rule.type = OperationType::kOrderInsert;
    insert_rule.rate_per_second = 100.0;
    insert_rule.capacity = 10;
    bundle.flow->AddRule(insert_rule);

    FlowRule cancel_rule = insert_rule;
    cancel_rule.type = OperationType::kOrderCancel;
    bundle.flow->AddRule(cancel_rule);

    FlowRule query_rule = insert_rule;
    query_rule.type = OperationType::kQuery;
    bundle.flow->AddRule(query_rule);

    bundle.breaker = BuildBreakerManager();
    bundle.sql = std::make_shared<InMemoryTimescaleSqlClient>();
    bundle.store = std::make_shared<TradingDomainStoreClientAdapter>(
        bundle.sql, StorageRetryPolicy{}, "trading_core");
    bundle.redis = std::make_shared<InMemoryRedisHashClient>();
    bundle.order_manager = std::make_shared<OrderManager>(bundle.store);
    bundle.position_manager = std::make_shared<PositionManager>(bundle.store, bundle.redis);
    bundle.engine = std::make_unique<ExecutionEngine>(bundle.adapter,
                                                      bundle.flow,
                                                      bundle.breaker,
                                                      bundle.order_manager,
                                                      bundle.position_manager,
                                                      bundle.store,
                                                      0);
    return bundle;
}

TEST(ExecutionEngineTest, PlaceOrderAsyncReturnsOrderRef) {
    auto bundle = BuildEngineBundle();
    auto result = bundle.engine->PlaceOrderAsync(BuildOrder("ord-1")).get();
    EXPECT_TRUE(result.success);
    EXPECT_FALSE(result.client_order_id.empty());
}

TEST(ExecutionEngineTest, PlaceOrderRiskRejectReturnsFailedResult) {
    auto bundle = BuildEngineBundle();
    bundle.engine->SetRiskManager(std::make_shared<RejectAllRiskManager>());
    const auto result = bundle.engine->PlaceOrderAsync(BuildOrder("ord-risk-reject")).get();
    EXPECT_FALSE(result.success);
    EXPECT_NE(result.message.find("risk reject"), std::string::npos);
}

TEST(ExecutionEngineTest, CircuitBreakerOpenBlocksNewOrder) {
    auto bundle = BuildEngineBundle();
    for (int i = 0; i < 3; ++i) {
        bundle.breaker->RecordFailure(BreakerScope::kStrategy, "strat1");
        bundle.breaker->RecordFailure(BreakerScope::kAccount, "acc1");
        bundle.breaker->RecordFailure(BreakerScope::kSystem, "__system__");
    }

    const auto result = bundle.engine->PlaceOrderAsync(BuildOrder("ord-breaker-open")).get();
    EXPECT_FALSE(result.success);
    EXPECT_NE(result.message.find("blocked by circuit breaker"), std::string::npos);
}

TEST(ExecutionEngineTest, OrderStateTransitionValidSequence) {
    auto bundle = BuildEngineBundle();
    auto submit = bundle.engine->PlaceOrderAsync(BuildOrder("ord-2")).get();
    ASSERT_TRUE(submit.success);

    OrderEvent accepted;
    accepted.account_id = "acc1";
    accepted.client_order_id = submit.client_order_id;
    accepted.order_ref = submit.client_order_id;
    accepted.instrument_id = "SHFE.ag2406";
    accepted.front_id = 1;
    accepted.session_id = 1;
    accepted.status = OrderStatus::kAccepted;
    accepted.total_volume = 1;
    accepted.filled_volume = 0;
    accepted.event_source = "OnRtnOrder";
    accepted.exchange_ts_ns = 100;
    accepted.ts_ns = 100;
    bundle.engine->HandleOrderEvent(accepted);

    OrderEvent filled = accepted;
    filled.status = OrderStatus::kFilled;
    filled.filled_volume = 1;
    filled.event_source = "OnRtnTrade";
    filled.trade_id = "t-1";
    filled.ts_ns = 101;
    filled.exchange_ts_ns = 101;
    bundle.engine->HandleOrderEvent(filled);

    const auto order = bundle.order_manager->GetOrder(submit.client_order_id);
    ASSERT_TRUE(order.has_value());
    EXPECT_EQ(order->status, OrderStatus::kFilled);
}

TEST(ExecutionEngineTest, OrderStateTransitionInvalidSequenceRejected) {
    auto bundle = BuildEngineBundle();
    auto submit = bundle.engine->PlaceOrderAsync(BuildOrder("ord-3")).get();
    ASSERT_TRUE(submit.success);

    OrderEvent canceled;
    canceled.account_id = "acc1";
    canceled.client_order_id = submit.client_order_id;
    canceled.order_ref = submit.client_order_id;
    canceled.instrument_id = "SHFE.ag2406";
    canceled.front_id = 1;
    canceled.session_id = 1;
    canceled.status = OrderStatus::kCanceled;
    canceled.total_volume = 1;
    canceled.filled_volume = 0;
    canceled.event_source = "OnRtnOrder";
    canceled.exchange_ts_ns = 200;
    canceled.ts_ns = 200;
    bundle.engine->HandleOrderEvent(canceled);

    OrderEvent late_fill = canceled;
    late_fill.status = OrderStatus::kFilled;
    late_fill.filled_volume = 1;
    late_fill.event_source = "OnRtnTrade";
    late_fill.trade_id = "late-t";
    late_fill.ts_ns = 201;
    late_fill.exchange_ts_ns = 201;
    bundle.engine->HandleOrderEvent(late_fill);

    const auto order = bundle.order_manager->GetOrder(submit.client_order_id);
    ASSERT_TRUE(order.has_value());
    EXPECT_EQ(order->status, OrderStatus::kCanceled);
}

TEST(ExecutionEngineTest, DuplicateOrderEventIgnored) {
    auto bundle = BuildEngineBundle();
    auto submit = bundle.engine->PlaceOrderAsync(BuildOrder("ord-dup")).get();
    ASSERT_TRUE(submit.success);

    OrderEvent accepted;
    accepted.account_id = "acc1";
    accepted.client_order_id = submit.client_order_id;
    accepted.order_ref = submit.client_order_id;
    accepted.instrument_id = "SHFE.ag2406";
    accepted.front_id = 1;
    accepted.session_id = 1;
    accepted.status = OrderStatus::kAccepted;
    accepted.total_volume = 1;
    accepted.filled_volume = 0;
    accepted.event_source = "OnRtnOrder";
    accepted.exchange_ts_ns = 300;
    accepted.ts_ns = 300;

    bundle.engine->HandleOrderEvent(accepted);
    bundle.engine->HandleOrderEvent(accepted);

    const auto rows =
        bundle.sql->QueryRows("ops.processed_order_events", "event_key",
                              OrderManager::BuildOrderEventKey(accepted), nullptr);
    EXPECT_EQ(rows.size(), 1U);
}

TEST(ExecutionEngineTest, CancelOrderAsyncSuccessReturnsTrue) {
    auto bundle = BuildEngineBundle();
    auto submit = bundle.engine->PlaceOrderAsync(BuildOrder("ord-cancel-ok")).get();
    ASSERT_TRUE(submit.success);
    std::thread ack_thread([&bundle, order_id = submit.client_order_id]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        OrderEvent canceled;
        canceled.account_id = "acc1";
        canceled.client_order_id = order_id;
        canceled.order_ref = order_id;
        canceled.instrument_id = "SHFE.ag2406";
        canceled.front_id = 1;
        canceled.session_id = 1;
        canceled.status = OrderStatus::kCanceled;
        canceled.total_volume = 1;
        canceled.filled_volume = 0;
        canceled.event_source = "OnRtnOrder";
        canceled.exchange_ts_ns = 500;
        canceled.ts_ns = 500;
        bundle.engine->HandleOrderEvent(canceled);
    });
    EXPECT_TRUE(bundle.engine->CancelOrderAsync(submit.client_order_id).get());
    ack_thread.join();
}

TEST(ExecutionEngineTest, CancelOrderAsyncRetryOnFailureEventuallyReturnsFalse) {
    auto bundle = BuildEngineBundle();
    EXPECT_FALSE(bundle.engine->CancelOrderAsync("missing-order").get());
}

TEST(ExecutionEngineTest, PositionUpdateAfterTradeRedisAndPgConsistent) {
    auto bundle = BuildEngineBundle();
    auto submit = bundle.engine->PlaceOrderAsync(BuildOrder("ord-pos")).get();
    ASSERT_TRUE(submit.success);

    OrderEvent filled;
    filled.account_id = "acc1";
    filled.client_order_id = submit.client_order_id;
    filled.order_ref = submit.client_order_id;
    filled.instrument_id = "SHFE.ag2406";
    filled.exchange_id = "SHFE";
    filled.front_id = 1;
    filled.session_id = 1;
    filled.status = OrderStatus::kFilled;
    filled.total_volume = 1;
    filled.filled_volume = 1;
    filled.avg_fill_price = 4001.0;
    filled.side = Side::kBuy;
    filled.offset = OffsetFlag::kOpen;
    filled.trade_id = "tp-1";
    filled.event_source = "OnRtnTrade";
    filled.ts_ns = 400;
    filled.exchange_ts_ns = 400;
    bundle.engine->HandleOrderEvent(filled);

    const auto rows = bundle.sql->QueryRows("trading_core.position_summary", "account_id", "acc1", nullptr);
    ASSERT_FALSE(rows.empty());
    std::unordered_map<std::string, std::string> hash;
    std::string error;
    ASSERT_TRUE(bundle.redis->HGetAll("position:acc1:SHFE.ag2406", &hash, &error)) << error;
    EXPECT_EQ(hash["long_volume"], "1");
}

TEST(ExecutionEngineTest, QueryTradingAccountAsyncReturnsSnapshot) {
    auto bundle = BuildEngineBundle();
    auto snapshot = bundle.engine->QueryTradingAccountAsync().get();
    EXPECT_FALSE(snapshot.account_id.empty());
}

}  // namespace
}  // namespace quant_hft
