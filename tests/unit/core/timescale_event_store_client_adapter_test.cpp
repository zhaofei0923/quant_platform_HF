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
    store.AppendOrderEvent(order);

    RiskDecision decision;
    decision.action = RiskAction::kAllow;
    decision.rule_id = "BASIC_LIMIT";
    decision.rule_group = "default";
    decision.rule_version = "v1";
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
    const auto rows = store.GetRiskDecisionRows();
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0].decision.rule_group, "default");
    EXPECT_EQ(rows[0].decision.rule_version, "v1");
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

}  // namespace quant_hft
