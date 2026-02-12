#include "quant_hft/core/storage_client_factory.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <memory>
#include <netdb.h>
#include <fcntl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "quant_hft/core/kafka_market_bus_producer.h"
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

class DisabledMarketBusProducer : public IMarketBusProducer {
public:
    bool PublishMarketSnapshot(const MarketSnapshot& snapshot, std::string* error) override {
        (void)snapshot;
        (void)error;
        return true;
    }

    bool Flush(std::string* error) override {
        (void)error;
        return true;
    }
};

class UnavailableMarketBusProducer : public IMarketBusProducer {
public:
    explicit UnavailableMarketBusProducer(std::string reason) : reason_(std::move(reason)) {}

    bool PublishMarketSnapshot(const MarketSnapshot& snapshot, std::string* error) override {
        (void)snapshot;
        if (error != nullptr) {
            *error = reason_;
        }
        return false;
    }

    bool Flush(std::string* error) override {
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

bool ProbeTcpEndpoint(const std::string& host, int port, int timeout_ms, std::string* error) {
    if (host.empty()) {
        if (error != nullptr) {
            *error = "clickhouse host is empty";
        }
        return false;
    }
    if (port <= 0 || port > 65535) {
        if (error != nullptr) {
            *error = "clickhouse port is out of range";
        }
        return false;
    }

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* results = nullptr;
    const std::string port_text = std::to_string(port);
    const int getaddr_rc = getaddrinfo(host.c_str(), port_text.c_str(), &hints, &results);
    if (getaddr_rc != 0 || results == nullptr) {
        if (error != nullptr) {
            *error = std::string("failed to resolve clickhouse host: ") +
                     gai_strerror(getaddr_rc);
        }
        if (results != nullptr) {
            freeaddrinfo(results);
        }
        return false;
    }

    const int bounded_timeout_ms = std::max(1, timeout_ms);
    bool connected = false;
    for (addrinfo* addr = results; addr != nullptr && !connected; addr = addr->ai_next) {
        const int fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
        if (fd < 0) {
            continue;
        }

        const int flags = fcntl(fd, F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        }

        if (connect(fd, addr->ai_addr, addr->ai_addrlen) == 0) {
            connected = true;
            close(fd);
            break;
        }

        if (errno == EINPROGRESS) {
            fd_set write_fds;
            FD_ZERO(&write_fds);
            FD_SET(fd, &write_fds);

            timeval timeout{};
            timeout.tv_sec = bounded_timeout_ms / 1000;
            timeout.tv_usec = (bounded_timeout_ms % 1000) * 1000;

            const int select_rc = select(fd + 1, nullptr, &write_fds, nullptr, &timeout);
            if (select_rc > 0 && FD_ISSET(fd, &write_fds)) {
                int socket_error = 0;
                socklen_t socket_error_len = sizeof(socket_error);
                if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len) == 0 &&
                    socket_error == 0) {
                    connected = true;
                }
            }
        }
        close(fd);
    }
    freeaddrinfo(results);

    if (!connected && error != nullptr) {
        *error = "unable to connect to clickhouse endpoint";
    }
    return connected;
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

std::shared_ptr<IMarketBusProducer> StorageClientFactory::CreateMarketBusProducer(
    const StorageConnectionConfig& config,
    std::string* error) {
    if (config.kafka.mode == MarketBusMode::kDisabled) {
        return std::make_shared<DisabledMarketBusProducer>();
    }
    if (config.kafka.mode != MarketBusMode::kKafka) {
        const std::string reason = "unsupported market bus mode";
        if (error != nullptr) {
            *error = reason;
        }
        return std::make_shared<UnavailableMarketBusProducer>(reason);
    }

#if defined(QUANT_HFT_ENABLE_KAFKA_EXTERNAL) && QUANT_HFT_ENABLE_KAFKA_EXTERNAL
    return std::make_shared<KafkaMarketBusProducer>(config.kafka);
#else
    const auto reason = BuildExternalDisabledMessage("kafka");
    if (error != nullptr) {
        *error = reason;
    }
    return std::make_shared<UnavailableMarketBusProducer>(reason);
#endif
}

bool StorageClientFactory::CheckClickHouseHealth(const StorageConnectionConfig& config,
                                                 std::string* error) {
    if (config.clickhouse.mode == StorageBackendMode::kInMemory) {
        return true;
    }
    return ProbeTcpEndpoint(config.clickhouse.host,
                            config.clickhouse.port,
                            config.clickhouse.connect_timeout_ms,
                            error);
}

}  // namespace quant_hft
