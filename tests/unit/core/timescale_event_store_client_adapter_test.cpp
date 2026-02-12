#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/timescale_event_store_client_adapter.h"
#include "quant_hft/core/timescale_sql_client.h"

namespace quant_hft {

namespace {

class FlakyTimescaleClient : public ITimescaleSqlClient {
public:
    explicit FlakyTimescaleClient(int fail_times) : fail_times_(fail_times) {}

    bool InsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   std::string* error) override {
        ++insert_calls_;
        if (insert_calls_ <= fail_times_) {
            if (error != nullptr) {
                *error = "transient";
            }
            return false;
        }
        tables_[table].push_back(row);
        return true;
    }

    std::vector<std::unordered_map<std::string, std::string>> QueryRows(
        const std::string& table,
        const std::string& key,
        const std::string& value,
        std::string* error) const override {
        (void)error;
        const auto table_it = tables_.find(table);
        if (table_it == tables_.end()) {
            return {};
        }
        std::vector<std::unordered_map<std::string, std::string>> out;
        for (const auto& row : table_it->second) {
            const auto it = row.find(key);
            if (it != row.end() && it->second == value) {
                out.push_back(row);
            }
        }
        return out;
    }

    std::vector<std::unordered_map<std::string, std::string>> QueryAllRows(
        const std::string& table,
        std::string* error) const override {
        (void)error;
        const auto table_it = tables_.find(table);
        if (table_it == tables_.end()) {
            return {};
        }
        return table_it->second;
    }

    bool Ping(std::string* error) const override {
        (void)error;
        return true;
    }

    int insert_calls() const { return insert_calls_; }

private:
    int fail_times_{0};
    int insert_calls_{0};
    std::unordered_map<
        std::string,
        std::vector<std::unordered_map<std::string, std::string>>>
        tables_;
};

}  // namespace

TEST(TimescaleEventStoreClientAdapterTest, RoundTripsRowsByKey) {
    auto client = std::make_shared<InMemoryTimescaleSqlClient>();
    TimescaleEventStoreClientAdapter store(client, StorageRetryPolicy{});

    MarketSnapshot market;
    market.instrument_id = "SHFE.ag2406";
    market.last_price = 4512.0;
    market.recv_ts_ns = 10;
    store.AppendMarketSnapshot(market);

    OrderEvent order;
    order.account_id = "acc-1";
    order.client_order_id = "ord-1";
    order.instrument_id = "SHFE.ag2406";
    order.status = OrderStatus::kAccepted;
    order.total_volume = 1;
    order.ts_ns = 20;
    order.execution_algo_id = "sliced";
    order.slice_index = 1;
    order.slice_total = 2;
    order.throttle_applied = true;
    order.venue = "SIM";
    order.route_id = "route-sim-1";
    order.slippage_bps = 1.25;
    order.impact_cost = 8.5;
    store.AppendOrderEvent(order);

    RiskDecision decision;
    decision.action = RiskAction::kAllow;
    decision.rule_id = "BASIC_LIMIT";
    decision.rule_group = "default";
    decision.rule_version = "v1";
    decision.policy_id = "policy.max_notional";
    decision.policy_scope = "instrument";
    decision.observed_value = 120001.0;
    decision.threshold_value = 120000.0;
    decision.decision_tags = "risk,notional";
    decision.decision_ts_ns = 25;
    decision.reason = "ok";
    OrderIntent intent;
    intent.account_id = "acc-1";
    intent.client_order_id = "ord-1";
    intent.instrument_id = "SHFE.ag2406";
    intent.volume = 1;
    intent.ts_ns = 19;
    store.AppendRiskDecision(intent, decision);

    EXPECT_EQ(store.GetMarketSnapshots("SHFE.ag2406").size(), 1U);
    EXPECT_EQ(store.GetOrderEvents("ord-1").size(), 1U);
    const auto orders = store.GetOrderEvents("ord-1");
    ASSERT_EQ(orders.size(), 1U);
    EXPECT_EQ(orders[0].execution_algo_id, "sliced");
    EXPECT_EQ(orders[0].slice_index, 1);
    EXPECT_EQ(orders[0].slice_total, 2);
    EXPECT_TRUE(orders[0].throttle_applied);
    EXPECT_EQ(orders[0].venue, "SIM");
    EXPECT_EQ(orders[0].route_id, "route-sim-1");
    EXPECT_DOUBLE_EQ(orders[0].slippage_bps, 1.25);
    EXPECT_DOUBLE_EQ(orders[0].impact_cost, 8.5);
    const auto rows = store.GetRiskDecisionRows();
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0].decision.rule_group, "default");
    EXPECT_EQ(rows[0].decision.rule_version, "v1");
    EXPECT_EQ(rows[0].decision.policy_id, "policy.max_notional");
    EXPECT_EQ(rows[0].decision.policy_scope, "instrument");
    EXPECT_DOUBLE_EQ(rows[0].decision.observed_value, 120001.0);
    EXPECT_DOUBLE_EQ(rows[0].decision.threshold_value, 120000.0);
    EXPECT_EQ(rows[0].decision.decision_tags, "risk,notional");
    EXPECT_EQ(rows[0].decision.decision_ts_ns, 25);
}

TEST(TimescaleEventStoreClientAdapterTest, RetriesTransientInsertFailure) {
    auto client = std::make_shared<FlakyTimescaleClient>(1);
    StorageRetryPolicy policy;
    policy.max_attempts = 2;
    policy.initial_backoff_ms = 0;
    policy.max_backoff_ms = 0;
    TimescaleEventStoreClientAdapter store(client, policy);

    MarketSnapshot market;
    market.instrument_id = "SHFE.ag2406";
    market.last_price = 4512.0;
    market.recv_ts_ns = 10;
    store.AppendMarketSnapshot(market);

    EXPECT_EQ(client->insert_calls(), 2);
}

TEST(TimescaleEventStoreClientAdapterTest, StopsAtMaxAttemptsOnFailure) {
    auto client = std::make_shared<FlakyTimescaleClient>(10);
    StorageRetryPolicy policy;
    policy.max_attempts = 3;
    policy.initial_backoff_ms = 0;
    policy.max_backoff_ms = 0;
    TimescaleEventStoreClientAdapter store(client, policy);

    MarketSnapshot market;
    market.instrument_id = "SHFE.ag2406";
    market.last_price = 4512.0;
    market.recv_ts_ns = 10;
    store.AppendMarketSnapshot(market);

    EXPECT_EQ(client->insert_calls(), 3);
}

TEST(TimescaleEventStoreClientAdapterTest, StoresAndLoadsCtpQuerySnapshots) {
    auto client = std::make_shared<InMemoryTimescaleSqlClient>();
    TimescaleEventStoreClientAdapter store(client, StorageRetryPolicy{});

    TradingAccountSnapshot account;
    account.account_id = "acc-1";
    account.investor_id = "191202";
    account.balance = 1'000'000.0;
    account.available = 950'000.0;
    account.curr_margin = 50'000.0;
    account.trading_day = "20260211";
    account.ts_ns = 123;
    account.source = "ctp";
    store.AppendTradingAccountSnapshot(account);

    InvestorPositionSnapshot position;
    position.account_id = "acc-1";
    position.investor_id = "191202";
    position.instrument_id = "SHFE.ag2406";
    position.exchange_id = "SHFE";
    position.posi_direction = "long";
    position.position = 3;
    position.today_position = 1;
    position.yd_position = 2;
    position.use_margin = 12'345.0;
    position.ts_ns = 124;
    position.source = "ctp";
    store.AppendInvestorPositionSnapshot(position);

    BrokerTradingParamsSnapshot broker_params;
    broker_params.account_id = "acc-1";
    broker_params.investor_id = "191202";
    broker_params.margin_price_type = "1";
    broker_params.algorithm = "THOST_FTDC_MPT_PreSettlementPrice";
    broker_params.ts_ns = 125;
    broker_params.source = "ctp";
    store.AppendBrokerTradingParamsSnapshot(broker_params);

    InstrumentMetaSnapshot instrument_meta;
    instrument_meta.instrument_id = "SHFE.ag2406";
    instrument_meta.exchange_id = "SHFE";
    instrument_meta.product_id = "ag";
    instrument_meta.volume_multiple = 15;
    instrument_meta.price_tick = 1.0;
    instrument_meta.max_margin_side_algorithm = true;
    instrument_meta.ts_ns = 126;
    instrument_meta.source = "ctp";
    store.AppendInstrumentMetaSnapshot(instrument_meta);

    const auto accounts = store.GetTradingAccountSnapshots("acc-1");
    ASSERT_EQ(accounts.size(), 1U);
    EXPECT_DOUBLE_EQ(accounts[0].balance, 1'000'000.0);

    const auto positions = store.GetInvestorPositionSnapshots("acc-1", "SHFE.ag2406");
    ASSERT_EQ(positions.size(), 1U);
    EXPECT_EQ(positions[0].position, 3);

    const auto params = store.GetBrokerTradingParamsSnapshots("acc-1");
    ASSERT_EQ(params.size(), 1U);
    EXPECT_EQ(params[0].margin_price_type, "1");

    const auto metas = store.GetInstrumentMetaSnapshots("SHFE.ag2406");
    ASSERT_EQ(metas.size(), 1U);
    EXPECT_EQ(metas[0].exchange_id, "SHFE");
}

}  // namespace quant_hft
