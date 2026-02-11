#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/timescale_buffered_event_store.h"
#include "quant_hft/core/timescale_sql_client.h"

namespace quant_hft {

namespace {

class FlakyDelegatingTimescaleClient : public ITimescaleSqlClient {
public:
    FlakyDelegatingTimescaleClient(std::shared_ptr<ITimescaleSqlClient> delegate,
                                   int fail_insert_times)
        : delegate_(std::move(delegate)), fail_insert_times_(fail_insert_times) {}

    bool InsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   std::string* error) override {
        ++insert_calls_;
        if (insert_calls_ <= fail_insert_times_) {
            if (error != nullptr) {
                *error = "transient";
            }
            return false;
        }
        return delegate_->InsertRow(table, row, error);
    }

    std::vector<std::unordered_map<std::string, std::string>> QueryRows(
        const std::string& table,
        const std::string& key,
        const std::string& value,
        std::string* error) const override {
        return delegate_->QueryRows(table, key, value, error);
    }

    std::vector<std::unordered_map<std::string, std::string>> QueryAllRows(
        const std::string& table,
        std::string* error) const override {
        return delegate_->QueryAllRows(table, error);
    }

    bool Ping(std::string* error) const override { return delegate_->Ping(error); }

    int insert_calls() const { return insert_calls_; }

private:
    std::shared_ptr<ITimescaleSqlClient> delegate_;
    int fail_insert_times_{0};
    mutable int insert_calls_{0};
};

OrderEvent MakeOrderEvent(const std::string& id, EpochNanos ts_ns) {
    OrderEvent event;
    event.account_id = "acc-1";
    event.client_order_id = id;
    event.instrument_id = "SHFE.ag2406";
    event.status = OrderStatus::kAccepted;
    event.total_volume = 1;
    event.filled_volume = 0;
    event.avg_fill_price = 0.0;
    event.ts_ns = ts_ns;
    return event;
}

}  // namespace

TEST(TimescaleBufferedEventStoreTest, FlushesQueuedRowsToUnderlyingStore) {
    auto client = std::make_shared<InMemoryTimescaleSqlClient>();
    StorageRetryPolicy retry;
    retry.max_attempts = 2;
    retry.initial_backoff_ms = 0;
    retry.max_backoff_ms = 0;
    TimescaleBufferedStoreOptions opts;
    opts.batch_size = 4;
    opts.flush_interval_ms = 10;
    TimescaleBufferedEventStore store(client, retry, opts);

    store.AppendOrderEvent(MakeOrderEvent("ord-1", 100));
    store.AppendOrderEvent(MakeOrderEvent("ord-1", 101));
    store.Flush();

    const auto rows = store.GetOrderEvents("ord-1");
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows[0].client_order_id, "ord-1");
}

TEST(TimescaleBufferedEventStoreTest, RetriesTransientInsertFailuresInsideWorker) {
    auto base = std::make_shared<InMemoryTimescaleSqlClient>();
    auto flaky = std::make_shared<FlakyDelegatingTimescaleClient>(base, 2);
    StorageRetryPolicy retry;
    retry.max_attempts = 3;
    retry.initial_backoff_ms = 0;
    retry.max_backoff_ms = 0;
    TimescaleBufferedStoreOptions opts;
    opts.batch_size = 1;
    opts.flush_interval_ms = 5;
    TimescaleBufferedEventStore store(flaky, retry, opts);

    store.AppendOrderEvent(MakeOrderEvent("ord-2", 200));
    store.Flush();

    const auto rows = store.GetOrderEvents("ord-2");
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(flaky->insert_calls(), 3);
}

}  // namespace quant_hft
