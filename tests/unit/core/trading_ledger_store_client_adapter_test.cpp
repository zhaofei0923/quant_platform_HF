#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/trading_ledger_store_client_adapter.h"
#include "quant_hft/core/timescale_sql_client.h"

namespace quant_hft {

namespace {

class FakeTimescaleSqlClient : public ITimescaleSqlClient {
public:
    explicit FakeTimescaleSqlClient(int transient_fail_times)
        : transient_fail_times_(transient_fail_times) {}

    bool InsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   std::string* error) override {
        ++insert_calls_;
        if (insert_calls_ <= transient_fail_times_) {
            if (error != nullptr) {
                *error = "transient error";
            }
            return false;
        }

        auto idempotency_it = row.find("idempotency_key");
        if (idempotency_it != row.end()) {
            const std::string dedupe_key = table + "|" + idempotency_it->second;
            if (idempotency_keys_.find(dedupe_key) != idempotency_keys_.end()) {
                if (error != nullptr) {
                    *error = "duplicate key value violates unique constraint";
                }
                return false;
            }
            idempotency_keys_.insert(dedupe_key);
        }

        auto stream_it = row.find("stream_name");
        if (stream_it != row.end()) {
            if (replay_streams_.find(stream_it->second) != replay_streams_.end()) {
                if (error != nullptr) {
                    *error = "duplicate key value violates unique constraint";
                }
                return false;
            }
            replay_streams_.insert(stream_it->second);
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
        std::vector<std::unordered_map<std::string, std::string>> rows;
        for (const auto& row : table_it->second) {
            const auto it = row.find(key);
            if (it != row.end() && it->second == value) {
                rows.push_back(row);
            }
        }
        return rows;
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

    std::size_t table_row_count(const std::string& table) const {
        const auto it = tables_.find(table);
        if (it == tables_.end()) {
            return 0U;
        }
        return it->second.size();
    }

private:
    int transient_fail_times_{0};
    int insert_calls_{0};
    std::unordered_map<std::string,
                       std::vector<std::unordered_map<std::string, std::string>>>
        tables_;
    std::unordered_set<std::string> idempotency_keys_;
    std::unordered_set<std::string> replay_streams_;
};

OrderEvent BuildOrderEvent() {
    OrderEvent event;
    event.account_id = "acc-1";
    event.client_order_id = "ord-1";
    event.exchange_order_id = "ex-1";
    event.instrument_id = "SHFE.ag2406";
    event.exchange_id = "SHFE";
    event.status = OrderStatus::kAccepted;
    event.total_volume = 5;
    event.filled_volume = 2;
    event.avg_fill_price = 4500.5;
    event.reason = "ok";
    event.event_source = "OnRtnOrder";
    event.exchange_ts_ns = 100;
    event.recv_ts_ns = 110;
    event.ts_ns = 120;
    event.trace_id = "trace-1";
    return event;
}

}  // namespace

TEST(TradingLedgerStoreClientAdapterTest, RetriesTransientFailureAndSucceeds) {
    auto client = std::make_shared<FakeTimescaleSqlClient>(1);
    StorageRetryPolicy retry;
    retry.max_attempts = 2;
    retry.initial_backoff_ms = 0;
    retry.max_backoff_ms = 0;
    TradingLedgerStoreClientAdapter adapter(client, retry, "trading_core");

    std::string error;
    EXPECT_TRUE(adapter.AppendOrderEvent(BuildOrderEvent(), &error)) << error;
    EXPECT_EQ(client->insert_calls(), 2);
    EXPECT_EQ(client->table_row_count("trading_core.order_events"), 1U);
}

TEST(TradingLedgerStoreClientAdapterTest, TreatsDuplicateOrderInsertAsIdempotentSuccess) {
    auto client = std::make_shared<FakeTimescaleSqlClient>(0);
    StorageRetryPolicy retry;
    retry.max_attempts = 2;
    retry.initial_backoff_ms = 0;
    retry.max_backoff_ms = 0;
    TradingLedgerStoreClientAdapter adapter(client, retry, "trading_core");

    OrderEvent event = BuildOrderEvent();
    std::string error;
    EXPECT_TRUE(adapter.AppendOrderEvent(event, &error)) << error;
    EXPECT_TRUE(adapter.AppendOrderEvent(event, &error)) << error;
    EXPECT_EQ(client->table_row_count("trading_core.order_events"), 1U);
}

TEST(TradingLedgerStoreClientAdapterTest, WritesTradeRowsIntoConfiguredSchema) {
    auto client = std::make_shared<FakeTimescaleSqlClient>(0);
    TradingLedgerStoreClientAdapter adapter(client, StorageRetryPolicy{}, "trading_core");

    OrderEvent event = BuildOrderEvent();
    event.status = OrderStatus::kFilled;
    event.trade_id = "trade-1";
    std::string error;
    EXPECT_TRUE(adapter.AppendTradeEvent(event, &error)) << error;
    EXPECT_EQ(client->table_row_count("trading_core.trade_events"), 1U);
}

TEST(TradingLedgerStoreClientAdapterTest, ReplayOffsetDuplicateWithHigherStoredSeqIsAccepted) {
    auto client = std::make_shared<FakeTimescaleSqlClient>(0);
    TradingLedgerStoreClientAdapter adapter(client, StorageRetryPolicy{}, "trading_core");

    std::string error;
    EXPECT_TRUE(adapter.UpsertReplayOffset("runtime_events.wal", 10, 1000, &error)) << error;
    EXPECT_TRUE(adapter.UpsertReplayOffset("runtime_events.wal", 9, 1001, &error)) << error;
    EXPECT_EQ(client->table_row_count("trading_core.replay_offsets"), 1U);
}

}  // namespace quant_hft
