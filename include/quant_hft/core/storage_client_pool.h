#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/core/timescale_sql_client.h"

namespace quant_hft {

class RedisHashClientPool {
public:
    explicit RedisHashClientPool(std::vector<std::shared_ptr<IRedisHashClient>> clients);

    std::size_t Size() const;
    std::size_t HealthyClientCount() const;
    std::shared_ptr<IRedisHashClient> ClientAt(std::size_t index) const;

private:
    std::vector<std::shared_ptr<IRedisHashClient>> clients_;
};

class TimescaleSqlClientPool {
public:
    explicit TimescaleSqlClientPool(std::vector<std::shared_ptr<ITimescaleSqlClient>> clients);

    std::size_t Size() const;
    std::size_t HealthyClientCount() const;
    std::shared_ptr<ITimescaleSqlClient> ClientAt(std::size_t index) const;

private:
    std::vector<std::shared_ptr<ITimescaleSqlClient>> clients_;
};

class PooledRedisHashClient : public IRedisHashClient {
public:
    explicit PooledRedisHashClient(std::vector<std::shared_ptr<IRedisHashClient>> clients);

    bool HSet(const std::string& key,
              const std::unordered_map<std::string, std::string>& fields,
              std::string* error) override;
    bool HGetAll(const std::string& key,
                 std::unordered_map<std::string, std::string>* out,
                 std::string* error) const override;
    bool HIncrBy(const std::string& key,
                 const std::string& field,
                 std::int64_t delta,
                 std::string* error) override;
    bool Expire(const std::string& key, int ttl_seconds, std::string* error) override;
    bool Ping(std::string* error) const override;

private:
    RedisHashClientPool pool_;
};

class PooledTimescaleSqlClient : public ITimescaleSqlClient {
public:
    explicit PooledTimescaleSqlClient(std::vector<std::shared_ptr<ITimescaleSqlClient>> clients);

    bool InsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   std::string* error) override;
    bool UpsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   const std::vector<std::string>& conflict_keys,
                   const std::vector<std::string>& update_keys,
                   std::string* error) override;
    std::vector<std::unordered_map<std::string, std::string>> QueryRows(
        const std::string& table,
        const std::string& key,
        const std::string& value,
        std::string* error) const override;
    std::vector<std::unordered_map<std::string, std::string>> QueryAllRows(
        const std::string& table,
        std::string* error) const override;
    bool Ping(std::string* error) const override;

private:
    mutable std::atomic<std::size_t> next_index_{0};
    TimescaleSqlClientPool pool_;
};

}  // namespace quant_hft
