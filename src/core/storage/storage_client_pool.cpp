#include "quant_hft/core/storage_client_pool.h"

#include <functional>

namespace quant_hft {

RedisHashClientPool::RedisHashClientPool(
    std::vector<std::shared_ptr<IRedisHashClient>> clients)
    : clients_(std::move(clients)) {}

std::size_t RedisHashClientPool::Size() const { return clients_.size(); }

std::size_t RedisHashClientPool::HealthyClientCount() const {
    std::size_t healthy = 0;
    for (const auto& client : clients_) {
        if (client == nullptr) {
            continue;
        }
        std::string error;
        if (client->Ping(&error)) {
            ++healthy;
        }
    }
    return healthy;
}

std::shared_ptr<IRedisHashClient> RedisHashClientPool::ClientAt(std::size_t index) const {
    if (clients_.empty()) {
        return nullptr;
    }
    return clients_[index % clients_.size()];
}

TimescaleSqlClientPool::TimescaleSqlClientPool(
    std::vector<std::shared_ptr<ITimescaleSqlClient>> clients)
    : clients_(std::move(clients)) {}

std::size_t TimescaleSqlClientPool::Size() const { return clients_.size(); }

std::size_t TimescaleSqlClientPool::HealthyClientCount() const {
    std::size_t healthy = 0;
    for (const auto& client : clients_) {
        if (client == nullptr) {
            continue;
        }
        std::string error;
        if (client->Ping(&error)) {
            ++healthy;
        }
    }
    return healthy;
}

std::shared_ptr<ITimescaleSqlClient> TimescaleSqlClientPool::ClientAt(
    std::size_t index) const {
    if (clients_.empty()) {
        return nullptr;
    }
    return clients_[index % clients_.size()];
}

PooledRedisHashClient::PooledRedisHashClient(
    std::vector<std::shared_ptr<IRedisHashClient>> clients)
    : pool_(std::move(clients)) {}

bool PooledRedisHashClient::HSet(
    const std::string& key,
    const std::unordered_map<std::string, std::string>& fields,
    std::string* error) {
    const auto total = pool_.Size();
    if (total == 0 || key.empty()) {
        if (error != nullptr) {
            *error = "redis pool empty or key empty";
        }
        return false;
    }

    const std::size_t start = std::hash<std::string>{}(key) % total;
    for (std::size_t i = 0; i < total; ++i) {
        const auto client = pool_.ClientAt(start + i);
        if (client == nullptr) {
            continue;
        }

        std::string ping_error;
        if (!client->Ping(&ping_error)) {
            continue;
        }
        if (client->HSet(key, fields, error)) {
            return true;
        }
    }
    if (error != nullptr && error->empty()) {
        *error = "all redis clients failed";
    }
    return false;
}

bool PooledRedisHashClient::HGetAll(
    const std::string& key,
    std::unordered_map<std::string, std::string>* out,
    std::string* error) const {
    const auto total = pool_.Size();
    if (total == 0 || key.empty() || out == nullptr) {
        if (error != nullptr) {
            *error = "redis pool empty or invalid input";
        }
        return false;
    }

    const std::size_t start = std::hash<std::string>{}(key) % total;
    for (std::size_t i = 0; i < total; ++i) {
        const auto client = pool_.ClientAt(start + i);
        if (client == nullptr) {
            continue;
        }

        std::string ping_error;
        if (!client->Ping(&ping_error)) {
            continue;
        }
        if (client->HGetAll(key, out, error)) {
            return true;
        }
    }
    if (error != nullptr && error->empty()) {
        *error = "all redis clients failed";
    }
    return false;
}

bool PooledRedisHashClient::Expire(const std::string& key,
                                   int ttl_seconds,
                                   std::string* error) {
    const auto total = pool_.Size();
    if (total == 0 || key.empty() || ttl_seconds <= 0) {
        if (error != nullptr) {
            *error = "redis pool empty or invalid input";
        }
        return false;
    }

    const std::size_t start = std::hash<std::string>{}(key) % total;
    for (std::size_t i = 0; i < total; ++i) {
        const auto client = pool_.ClientAt(start + i);
        if (client == nullptr) {
            continue;
        }

        std::string ping_error;
        if (!client->Ping(&ping_error)) {
            continue;
        }
        if (client->Expire(key, ttl_seconds, error)) {
            return true;
        }
    }
    if (error != nullptr && error->empty()) {
        *error = "all redis clients failed";
    }
    return false;
}

bool PooledRedisHashClient::Ping(std::string* error) const {
    if (pool_.HealthyClientCount() > 0) {
        return true;
    }
    if (error != nullptr) {
        *error = "no healthy redis client";
    }
    return false;
}

PooledTimescaleSqlClient::PooledTimescaleSqlClient(
    std::vector<std::shared_ptr<ITimescaleSqlClient>> clients)
    : pool_(std::move(clients)) {}

bool PooledTimescaleSqlClient::InsertRow(
    const std::string& table,
    const std::unordered_map<std::string, std::string>& row,
    std::string* error) {
    const auto total = pool_.Size();
    if (total == 0 || table.empty()) {
        if (error != nullptr) {
            *error = "timescale pool empty or table empty";
        }
        return false;
    }

    const auto start = next_index_.fetch_add(1) % total;
    for (std::size_t i = 0; i < total; ++i) {
        const auto client = pool_.ClientAt(start + i);
        if (client == nullptr) {
            continue;
        }
        std::string ping_error;
        if (!client->Ping(&ping_error)) {
            continue;
        }
        if (client->InsertRow(table, row, error)) {
            return true;
        }
    }
    if (error != nullptr && error->empty()) {
        *error = "all timescale clients failed";
    }
    return false;
}

bool PooledTimescaleSqlClient::UpsertRow(
    const std::string& table,
    const std::unordered_map<std::string, std::string>& row,
    const std::vector<std::string>& conflict_keys,
    const std::vector<std::string>& update_keys,
    std::string* error) {
    const auto total = pool_.Size();
    if (total == 0 || table.empty()) {
        if (error != nullptr) {
            *error = "timescale pool empty or table empty";
        }
        return false;
    }

    const auto start = next_index_.fetch_add(1) % total;
    for (std::size_t i = 0; i < total; ++i) {
        const auto client = pool_.ClientAt(start + i);
        if (client == nullptr) {
            continue;
        }
        std::string ping_error;
        if (!client->Ping(&ping_error)) {
            continue;
        }
        if (client->UpsertRow(table, row, conflict_keys, update_keys, error)) {
            return true;
        }
    }
    if (error != nullptr && error->empty()) {
        *error = "all timescale clients failed";
    }
    return false;
}

std::vector<std::unordered_map<std::string, std::string>>
PooledTimescaleSqlClient::QueryRows(const std::string& table,
                                    const std::string& key,
                                    const std::string& value,
                                    std::string* error) const {
    const auto total = pool_.Size();
    if (total == 0 || table.empty()) {
        if (error != nullptr) {
            *error = "timescale pool empty or table empty";
        }
        return {};
    }

    const auto start = next_index_.fetch_add(1) % total;
    for (std::size_t i = 0; i < total; ++i) {
        const auto client = pool_.ClientAt(start + i);
        if (client == nullptr) {
            continue;
        }
        std::string ping_error;
        if (!client->Ping(&ping_error)) {
            continue;
        }
        std::string query_error;
        const auto rows = client->QueryRows(table, key, value, &query_error);
        if (query_error.empty()) {
            return rows;
        }
    }
    if (error != nullptr && error->empty()) {
        *error = "all timescale clients failed";
    }
    return {};
}

std::vector<std::unordered_map<std::string, std::string>>
PooledTimescaleSqlClient::QueryAllRows(const std::string& table,
                                       std::string* error) const {
    const auto total = pool_.Size();
    if (total == 0 || table.empty()) {
        if (error != nullptr) {
            *error = "timescale pool empty or table empty";
        }
        return {};
    }

    const auto start = next_index_.fetch_add(1) % total;
    for (std::size_t i = 0; i < total; ++i) {
        const auto client = pool_.ClientAt(start + i);
        if (client == nullptr) {
            continue;
        }
        std::string ping_error;
        if (!client->Ping(&ping_error)) {
            continue;
        }
        std::string query_error;
        const auto rows = client->QueryAllRows(table, &query_error);
        if (query_error.empty()) {
            return rows;
        }
    }
    if (error != nullptr && error->empty()) {
        *error = "all timescale clients failed";
    }
    return {};
}

bool PooledTimescaleSqlClient::Ping(std::string* error) const {
    if (pool_.HealthyClientCount() > 0) {
        return true;
    }
    if (error != nullptr) {
        *error = "no healthy timescale client";
    }
    return false;
}

}  // namespace quant_hft
