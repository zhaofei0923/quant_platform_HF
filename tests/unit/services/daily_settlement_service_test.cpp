#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/flow_controller.h"
#include "quant_hft/interfaces/settlement_store.h"
#include "quant_hft/services/daily_settlement_service.h"
#include "quant_hft/services/settlement_query_client.h"

namespace quant_hft {
namespace {

class FakeSettlementStore : public ISettlementStore {
public:
    SettlementRunRecord existing_run;
    std::vector<SettlementRunRecord> upserted_runs;
    std::vector<SettlementSummaryRecord> summaries;
    std::vector<SettlementReconcileDiffRecord> diffs;

    bool GetRun(const std::string& trading_day,
                SettlementRunRecord* out,
                std::string* error) const override {
        (void)error;
        if (out == nullptr) {
            return false;
        }
        if (!existing_run.trading_day.empty() && existing_run.trading_day == trading_day) {
            *out = existing_run;
        } else {
            *out = SettlementRunRecord{};
        }
        return true;
    }

    bool UpsertRun(const SettlementRunRecord& run, std::string* error) override {
        (void)error;
        upserted_runs.push_back(run);
        return true;
    }

    bool AppendSummary(const SettlementSummaryRecord& summary, std::string* error) override {
        (void)error;
        summaries.push_back(summary);
        return true;
    }

    bool AppendDetail(const SettlementDetailRecord& detail, std::string* error) override {
        (void)detail;
        (void)error;
        return true;
    }

    bool AppendPrice(const SettlementPriceRecord& price, std::string* error) override {
        (void)price;
        (void)error;
        return true;
    }

    bool AppendReconcileDiff(const SettlementReconcileDiffRecord& diff,
                             std::string* error) override {
        (void)error;
        diffs.push_back(diff);
        return true;
    }
};

class FakeTradingDomainStore : public ITradingDomainStore {
public:
    bool UpsertOrder(const Order& order, std::string* error) override {
        (void)error;
        orders.push_back(order);
        return true;
    }

    bool AppendTrade(const Trade& trade, std::string* error) override {
        (void)error;
        trades.push_back(trade);
        return true;
    }

    bool UpsertPosition(const Position& position, std::string* error) override {
        (void)position;
        (void)error;
        return true;
    }

    bool UpsertAccount(const Account& account, std::string* error) override {
        (void)account;
        (void)error;
        return true;
    }

    bool AppendRiskEvent(const RiskEventRecord& risk_event, std::string* error) override {
        (void)risk_event;
        (void)error;
        return true;
    }

    std::vector<Order> orders;
    std::vector<Trade> trades;
};

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

TEST(DailySettlementServiceTest, ReturnsNoopWhenRunAlreadyCompletedAndForceDisabled) {
    auto store = std::make_shared<FakeSettlementStore>();
    store->existing_run.trading_day = "2026-02-12";
    store->existing_run.status = "COMPLETED";

    SettlementQueryClientConfig query_cfg;
    query_cfg.account_id = "acc1";
    auto query_client = std::make_shared<SettlementQueryClient>(nullptr, nullptr, query_cfg);
    DailySettlementService service(store, query_client);

    DailySettlementConfig cfg;
    cfg.account_id = "acc1";
    cfg.trading_day = "2026-02-12";

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(cfg, &result, &error)) << error;
    EXPECT_TRUE(result.success);
    EXPECT_TRUE(result.noop);
    EXPECT_TRUE(store->upserted_runs.empty());
}

TEST(DailySettlementServiceTest, ReturnsNoopWhenRunAlreadyRunningAndNotStale) {
    auto store = std::make_shared<FakeSettlementStore>();
    store->existing_run.trading_day = "2026-02-12";
    store->existing_run.status = "RUNNING";
    store->existing_run.heartbeat_ts_ns = NowEpochNanos();

    SettlementQueryClientConfig query_cfg;
    query_cfg.account_id = "acc1";
    auto query_client = std::make_shared<SettlementQueryClient>(nullptr, nullptr, query_cfg);
    DailySettlementService service(store, query_client);

    DailySettlementConfig cfg;
    cfg.account_id = "acc1";
    cfg.trading_day = "2026-02-12";
    cfg.running_stale_timeout_ms = 10'000;

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(cfg, &result, &error)) << error;
    EXPECT_TRUE(result.success);
    EXPECT_TRUE(result.noop);
    EXPECT_FALSE(result.blocked);
    EXPECT_EQ(result.status, "RUNNING");
    EXPECT_TRUE(store->upserted_runs.empty());
}

TEST(DailySettlementServiceTest, RetriesWhenExistingRunningRunIsStale) {
    auto store = std::make_shared<FakeSettlementStore>();
    store->existing_run.trading_day = "2026-02-16";
    store->existing_run.status = "RUNNING";
    store->existing_run.heartbeat_ts_ns = NowEpochNanos() - 10'000'000'000LL;

    auto trader = BuildConnectedAdapter();
    auto flow = std::make_shared<FlowController>();
    FlowRule query_rule;
    query_rule.account_id = "acc1";
    query_rule.type = OperationType::kSettlementQuery;
    query_rule.rate_per_second = 10.0;
    query_rule.capacity = 5;
    flow->AddRule(query_rule);

    SettlementQueryClientConfig query_cfg;
    query_cfg.account_id = "acc1";
    query_cfg.retry_max = 1;
    query_cfg.backoff_initial_ms = 1;
    query_cfg.backoff_max_ms = 1;
    query_cfg.acquire_timeout_ms = 10;
    auto query_client = std::make_shared<SettlementQueryClient>(trader, flow, query_cfg);
    DailySettlementService service(store, query_client);

    DailySettlementConfig cfg;
    cfg.account_id = "acc1";
    cfg.trading_day = "2026-02-16";
    cfg.running_stale_timeout_ms = 100;

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(cfg, &result, &error)) << error;
    EXPECT_TRUE(result.success);
    EXPECT_FALSE(result.noop);
    ASSERT_EQ(store->upserted_runs.size(), 3U);
    EXPECT_EQ(store->upserted_runs[0].status, "RECONCILING");
    EXPECT_EQ(store->upserted_runs.back().status, "COMPLETED");
    trader->Disconnect();
}

TEST(DailySettlementServiceTest, BlocksWhenQueriesFailAndWritesDiff) {
    auto store = std::make_shared<FakeSettlementStore>();
    SettlementQueryClientConfig query_cfg;
    query_cfg.account_id = "acc1";
    auto query_client = std::make_shared<SettlementQueryClient>(nullptr, nullptr, query_cfg);
    DailySettlementService service(store, query_client);

    DailySettlementConfig cfg;
    cfg.account_id = "acc1";
    cfg.trading_day = "2026-02-13";

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(cfg, &result, &error)) << error;
    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.blocked);
    ASSERT_EQ(store->upserted_runs.size(), 2U);
    EXPECT_EQ(store->upserted_runs.front().status, "RECONCILING");
    EXPECT_EQ(store->upserted_runs.back().status, "BLOCKED");
    ASSERT_EQ(store->diffs.size(), 1U);
    EXPECT_EQ(store->diffs.front().diff_type, "QUERY_ERROR");
}

TEST(DailySettlementServiceTest, CompletesAndWritesSummaryWhenQueriesPass) {
    auto store = std::make_shared<FakeSettlementStore>();
    auto trader = BuildConnectedAdapter();
    auto flow = std::make_shared<FlowController>();
    FlowRule query_rule;
    query_rule.account_id = "acc1";
    query_rule.type = OperationType::kSettlementQuery;
    query_rule.rate_per_second = 10.0;
    query_rule.capacity = 5;
    flow->AddRule(query_rule);

    SettlementQueryClientConfig query_cfg;
    query_cfg.account_id = "acc1";
    query_cfg.retry_max = 1;
    query_cfg.backoff_initial_ms = 1;
    query_cfg.backoff_max_ms = 1;
    query_cfg.acquire_timeout_ms = 10;
    auto query_client = std::make_shared<SettlementQueryClient>(trader, flow, query_cfg);
    DailySettlementService service(store, query_client);

    DailySettlementConfig cfg;
    cfg.account_id = "acc1";
    cfg.trading_day = "2026-02-14";
    cfg.strict_order_trade_backfill = false;

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(cfg, &result, &error)) << error;
    EXPECT_TRUE(result.success);
    EXPECT_FALSE(result.noop);
    EXPECT_FALSE(result.blocked);
    ASSERT_EQ(store->summaries.size(), 1U);
    ASSERT_EQ(store->upserted_runs.size(), 3U);
    EXPECT_EQ(store->upserted_runs[0].status, "RECONCILING");
    EXPECT_EQ(store->upserted_runs[1].status, "CALCULATED");
    EXPECT_EQ(store->upserted_runs[2].status, "COMPLETED");
    EXPECT_LE(store->upserted_runs[0].started_ts_ns, store->upserted_runs[2].completed_ts_ns);
    trader->Disconnect();
}

TEST(DailySettlementServiceTest, KeepsDomainBackfillPathNonBlockingWhenNoBackfillEvents) {
    auto store = std::make_shared<FakeSettlementStore>();
    auto domain_store = std::make_shared<FakeTradingDomainStore>();
    auto trader = BuildConnectedAdapter();
    auto flow = std::make_shared<FlowController>();
    FlowRule query_rule;
    query_rule.account_id = "acc1";
    query_rule.type = OperationType::kSettlementQuery;
    query_rule.rate_per_second = 10.0;
    query_rule.capacity = 5;
    flow->AddRule(query_rule);

    SettlementQueryClientConfig query_cfg;
    query_cfg.account_id = "acc1";
    query_cfg.retry_max = 1;
    query_cfg.backoff_initial_ms = 1;
    query_cfg.backoff_max_ms = 1;
    query_cfg.acquire_timeout_ms = 10;
    auto query_client = std::make_shared<SettlementQueryClient>(trader, flow, query_cfg);
    DailySettlementService service(store, query_client, domain_store);

    DailySettlementConfig cfg;
    cfg.account_id = "acc1";
    cfg.trading_day = "2026-02-15";
    cfg.strict_order_trade_backfill = false;

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(cfg, &result, &error)) << error;
    EXPECT_TRUE(result.success);
    EXPECT_TRUE(domain_store->orders.empty());
    EXPECT_TRUE(domain_store->trades.empty());
    ASSERT_EQ(store->upserted_runs.size(), 3U);
    EXPECT_EQ(store->upserted_runs.back().status, "COMPLETED");
    trader->Disconnect();
}

}  // namespace quant_hft
