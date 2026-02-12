#include "quant_hft/core/storage_client_factory.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "quant_hft/core/libpq_timescale_sql_client.h"
#include "quant_hft/core/tcp_redis_hash_client.h"

namespace quant_hft {

namespace {

class UnavailableRedisHashClient : public IRedisHashClient {
public:
    explicit UnavailableRedisHashClient(std::string reason) : reason_(std::move(reason)) {}

    bool HSet(const std::string& key,
              const std::unordered_map<std::string, std::string>& fields,
              std::string* error) override {
        (void)key;
        (void)fields;
        if (error != nullptr) {
            *error = reason_;
        }
        return false;
    }

    bool HGetAll(const std::string& key,
                 std::unordered_map<std::string, std::string>* out,
                 std::string* error) const override {
        (void)key;
        (void)out;
        if (error != nullptr) {
            *error = reason_;
        }
        return false;
    }

    bool HIncrBy(const std::string& key,
                 const std::string& field,
                 std::int64_t delta,
                 std::string* error) override {
        (void)key;
        (void)field;
        (void)delta;
        if (error != nullptr) {
            *error = reason_;
        }
        return false;
    }

    bool Expire(const std::string& key, int ttl_seconds, std::string* error) override {
        (void)key;
        (void)ttl_seconds;
        if (error != nullptr) {
            *error = reason_;
        }
        return false;
    }

    bool Ping(std::string* error) const override {
        if (error != nullptr) {
            *error = reason_;
        }
        return false;
    }

private:
    std::string reason_;
};

class UnavailableTimescaleSqlClient : public ITimescaleSqlClient {
public:
    explicit UnavailableTimescaleSqlClient(std::string reason) : reason_(std::move(reason)) {}

    bool InsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   std::string* error) override {
        (void)table;
        (void)row;
        if (error != nullptr) {
            *error = reason_;
        }
        return false;
    }

    bool UpsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   const std::vector<std::string>& conflict_keys,
                   const std::vector<std::string>& update_keys,
                   std::string* error) override {
        (void)table;
        (void)row;
        (void)conflict_keys;
        (void)update_keys;
        if (error != nullptr) {
            *error = reason_;
        }
        return false;
    }

    std::vector<std::unordered_map<std::string, std::string>> QueryRows(
        const std::string& table,
        const std::string& key,
        const std::string& value,
        std::string* error) const override {
        (void)table;
        (void)key;
        (void)value;
        if (error != nullptr) {
            *error = reason_;
        }
        return {};
    }

    std::vector<std::unordered_map<std::string, std::string>> QueryAllRows(
        const std::string& table,
        std::string* error) const override {
        (void)table;
        if (error != nullptr) {
            *error = reason_;
        }
        return {};
    }

    bool Ping(std::string* error) const override {
        if (error != nullptr) {
            *error = reason_;
        }
        return false;
    }

private:
    std::string reason_;
};

std::string BuildExternalDisabledMessage(const char* component) {
    return std::string("external ") + component +
           " driver not enabled in current build";
}

}  // namespace

std::shared_ptr<IRedisHashClient> StorageClientFactory::CreateRedisClient(
    const StorageConnectionConfig& config,
    std::string* error) {
    if (config.redis.mode == StorageBackendMode::kInMemory) {
        return std::make_shared<InMemoryRedisHashClient>();
    }

#if defined(QUANT_HFT_ENABLE_REDIS_EXTERNAL) && QUANT_HFT_ENABLE_REDIS_EXTERNAL
    auto external_client = std::make_shared<TcpRedisHashClient>(config.redis);
    std::string ping_error;
    if (external_client->Ping(&ping_error)) {
        return external_client;
    }
    const std::string reason = std::string("external redis unavailable: ") + ping_error;
    if (error != nullptr) {
        *error = reason;
    }
    if (config.allow_inmemory_fallback) {
        return std::make_shared<InMemoryRedisHashClient>();
    }
    return std::make_shared<UnavailableRedisHashClient>(reason);
#else
    const auto reason = BuildExternalDisabledMessage("redis");
    if (error != nullptr) {
        *error = reason;
    }
    if (config.allow_inmemory_fallback) {
        return std::make_shared<InMemoryRedisHashClient>();
    }
    return std::make_shared<UnavailableRedisHashClient>(reason);
#endif
}

std::shared_ptr<ITimescaleSqlClient> StorageClientFactory::CreateTimescaleClient(
    const StorageConnectionConfig& config,
    std::string* error) {
    if (config.timescale.mode == StorageBackendMode::kInMemory) {
        return std::make_shared<InMemoryTimescaleSqlClient>();
    }

#if defined(QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL) && QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL
    auto external_client = std::make_shared<LibpqTimescaleSqlClient>(config.timescale);
    std::string ping_error;
    if (external_client->Ping(&ping_error)) {
        return external_client;
    }
    const std::string reason =
        std::string("external timescaledb unavailable: ") + ping_error;
    if (error != nullptr) {
        *error = reason;
    }
    if (config.allow_inmemory_fallback) {
        return std::make_shared<InMemoryTimescaleSqlClient>();
    }
    return std::make_shared<UnavailableTimescaleSqlClient>(reason);
#else
    const auto reason = BuildExternalDisabledMessage("timescaledb");
    if (error != nullptr) {
        *error = reason;
    }
    if (config.allow_inmemory_fallback) {
        return std::make_shared<InMemoryTimescaleSqlClient>();
    }
    return std::make_shared<UnavailableTimescaleSqlClient>(reason);
#endif
}

}  // namespace quant_hft
