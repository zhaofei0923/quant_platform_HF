#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/core/storage_client_pool.h"
#include "quant_hft/core/timescale_sql_client.h"

namespace quant_hft {

namespace {

class RecordingRedisClient : public IRedisHashClient {
public:
    RecordingRedisClient(bool healthy, bool write_ok)
        : healthy_(healthy), write_ok_(write_ok) {}

    bool HSet(const std::string& key,
              const std::unordered_map<std::string, std::string>& fields,
              std::string* error) override {
        ++hset_calls_;
        if (!write_ok_) {
            if (error != nullptr) {
                *error = "write fail";
            }
            return false;
        }
        store_[key] = fields;
        return true;
    }

    bool HGetAll(const std::string& key,
                 std::unordered_map<std::string, std::string>* out,
                 std::string* error) const override {
        ++hget_calls_;
        if (out == nullptr) {
            return false;
        }
        const auto it = store_.find(key);
        if (it == store_.end()) {
            if (error != nullptr) {
                *error = "not found";
            }
            return false;
        }
        *out = it->second;
        return true;
    }

    bool Expire(const std::string& key, int ttl_seconds, std::string* error) override {
        ++expire_calls_;
        if (ttl_seconds <= 0) {
            if (error != nullptr) {
                *error = "invalid ttl";
            }
            return false;
        }
        const auto it = store_.find(key);
        if (it == store_.end()) {
            if (error != nullptr) {
                *error = "not found";
            }
            return false;
        }
        return true;
    }

    bool Ping(std::string* error) const override {
        if (!healthy_ && error != nullptr) {
            *error = "unhealthy";
        }
        return healthy_;
    }

    int hset_calls() const { return hset_calls_; }
    int hget_calls() const { return hget_calls_; }
    int expire_calls() const { return expire_calls_; }

private:
    bool healthy_{true};
    bool write_ok_{true};
    mutable int hset_calls_{0};
    mutable int hget_calls_{0};
    mutable int expire_calls_{0};
    std::unordered_map<std::string,
                       std::unordered_map<std::string, std::string>>
        store_;
};

class RecordingTimescaleClient : public ITimescaleSqlClient {
public:
    RecordingTimescaleClient(bool healthy, bool insert_ok)
        : healthy_(healthy), insert_ok_(insert_ok) {}

    bool InsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   std::string* error) override {
        ++insert_calls_;
        if (!insert_ok_) {
            if (error != nullptr) {
                *error = "insert fail";
            }
            return false;
        }
        tables_[table].push_back(row);
        return true;
    }

    bool UpsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   const std::vector<std::string>& conflict_keys,
                   const std::vector<std::string>& update_keys,
                   std::string* error) override {
        (void)conflict_keys;
        (void)update_keys;
        return InsertRow(table, row, error);
    }

    std::vector<std::unordered_map<std::string, std::string>> QueryRows(
        const std::string& table,
        const std::string& key,
        const std::string& value,
        std::string* error) const override {
        ++query_calls_;
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
        ++query_calls_;
        const auto table_it = tables_.find(table);
        if (table_it == tables_.end()) {
            return {};
        }
        return table_it->second;
    }

    bool Ping(std::string* error) const override {
        if (!healthy_ && error != nullptr) {
            *error = "unhealthy";
        }
        return healthy_;
    }

    int insert_calls() const { return insert_calls_; }
    int query_calls() const { return query_calls_; }

private:
    bool healthy_{true};
    bool insert_ok_{true};
    mutable int insert_calls_{0};
    mutable int query_calls_{0};
    std::unordered_map<
        std::string,
        std::vector<std::unordered_map<std::string, std::string>>>
        tables_;
};

}  // namespace

TEST(StorageClientPoolTest, RedisPoolFallsBackWhenPrimaryUnhealthy) {
    auto bad = std::make_shared<RecordingRedisClient>(false, true);
    auto ok = std::make_shared<RecordingRedisClient>(true, true);
    PooledRedisHashClient pooled({bad, ok});

    std::unordered_map<std::string, std::string> fields{{"k", "v"}};
    std::string error;
    ASSERT_TRUE(pooled.HSet("trade:order:1:info", fields, &error));
    EXPECT_EQ(bad->hset_calls(), 0);
    EXPECT_EQ(ok->hset_calls(), 1);
}

TEST(StorageClientPoolTest, RedisPoolReadsBackWrittenHash) {
    auto c1 = std::make_shared<RecordingRedisClient>(true, true);
    auto c2 = std::make_shared<RecordingRedisClient>(true, true);
    PooledRedisHashClient pooled({c1, c2});

    std::unordered_map<std::string, std::string> fields{{"last_price", "1"}};
    std::string error;
    ASSERT_TRUE(pooled.HSet("market:tick:ag:latest", fields, &error));

    std::unordered_map<std::string, std::string> out;
    ASSERT_TRUE(pooled.HGetAll("market:tick:ag:latest", &out, &error));
    EXPECT_EQ(out["last_price"], "1");
}

TEST(StorageClientPoolTest, TimescalePoolRoundRobinAndFallback) {
    auto bad = std::make_shared<RecordingTimescaleClient>(true, false);
    auto ok = std::make_shared<RecordingTimescaleClient>(true, true);
    PooledTimescaleSqlClient pooled({bad, ok});

    std::unordered_map<std::string, std::string> row{{"instrument_id", "ag"}};
    std::string error;
    ASSERT_TRUE(pooled.InsertRow("market_snapshots", row, &error));
    EXPECT_GE(bad->insert_calls(), 1);
    EXPECT_EQ(ok->insert_calls(), 1);
}

TEST(StorageClientPoolTest, PoolHealthCountReflectsAvailableClients) {
    auto c1 = std::make_shared<RecordingRedisClient>(true, true);
    auto c2 = std::make_shared<RecordingRedisClient>(false, true);
    RedisHashClientPool pool({c1, c2});
    EXPECT_EQ(pool.Size(), 2U);
    EXPECT_EQ(pool.HealthyClientCount(), 1U);
}

TEST(StorageClientPoolTest, InMemoryTimescaleClientUpsertUpdatesExistingRow) {
    InMemoryTimescaleSqlClient client;
    std::unordered_map<std::string, std::string> first{
        {"trading_day", "2026-02-12"},
        {"status", "RUNNING"},
    };
    std::string error;
    ASSERT_TRUE(client.UpsertRow("ops.settlement_runs",
                                 first,
                                 {"trading_day"},
                                 {"status"},
                                 &error))
        << error;

    std::unordered_map<std::string, std::string> second{
        {"trading_day", "2026-02-12"},
        {"status", "COMPLETED"},
    };
    ASSERT_TRUE(client.UpsertRow("ops.settlement_runs",
                                 second,
                                 {"trading_day"},
                                 {"status"},
                                 &error))
        << error;

    const auto rows = client.QueryRows("ops.settlement_runs", "trading_day", "2026-02-12", &error);
    ASSERT_EQ(rows.size(), 1U);
    ASSERT_TRUE(rows.front().find("status") != rows.front().end());
    EXPECT_EQ(rows.front().at("status"), "COMPLETED");
}

TEST(StorageClientPoolTest, TimescalePoolUpsertFallsBackToHealthyReplica) {
    auto bad = std::make_shared<RecordingTimescaleClient>(true, false);
    auto ok = std::make_shared<RecordingTimescaleClient>(true, true);
    PooledTimescaleSqlClient pooled({bad, ok});

    std::unordered_map<std::string, std::string> row{
        {"trading_day", "2026-02-12"},
        {"status", "RUNNING"},
    };
    std::string error;
    ASSERT_TRUE(pooled.UpsertRow("ops.settlement_runs",
                                 row,
                                 {"trading_day"},
                                 {"status"},
                                 &error))
        << error;
    EXPECT_GE(bad->insert_calls(), 1);
    EXPECT_EQ(ok->insert_calls(), 1);
}

}  // namespace quant_hft
