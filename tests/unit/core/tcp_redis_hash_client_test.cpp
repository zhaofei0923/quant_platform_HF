#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/core/storage_connection_config.h"
#include "quant_hft/core/tcp_redis_hash_client.h"

namespace quant_hft {

namespace {

struct Expectation {
    std::vector<std::string> expected_args;
    std::string response;
};

bool WriteAll(int fd, const std::string& payload) {
    std::size_t offset = 0;
    while (offset < payload.size()) {
        const ssize_t wrote = ::send(fd,
                                     payload.data() + offset,
                                     payload.size() - offset,
                                     0);
        if (wrote <= 0) {
            return false;
        }
        offset += static_cast<std::size_t>(wrote);
    }
    return true;
}

bool ReadLine(int fd, std::string* out) {
    if (out == nullptr) {
        return false;
    }
    out->clear();
    char ch = '\0';
    while (true) {
        const ssize_t got = ::recv(fd, &ch, 1, 0);
        if (got <= 0) {
            return false;
        }
        if (ch == '\r') {
            const ssize_t got_lf = ::recv(fd, &ch, 1, 0);
            if (got_lf <= 0 || ch != '\n') {
                return false;
            }
            return true;
        }
        out->push_back(ch);
    }
}

bool ReadExactly(int fd, std::size_t count, std::string* out) {
    if (out == nullptr) {
        return false;
    }
    out->clear();
    out->resize(count);
    std::size_t offset = 0;
    while (offset < count) {
        const ssize_t got = ::recv(fd, out->data() + offset, count - offset, 0);
        if (got <= 0) {
            return false;
        }
        offset += static_cast<std::size_t>(got);
    }
    return true;
}

bool ReadCommand(int fd, std::vector<std::string>* args) {
    if (args == nullptr) {
        return false;
    }
    args->clear();

    std::string line;
    if (!ReadLine(fd, &line) || line.empty() || line[0] != '*') {
        return false;
    }
    const int argc = std::stoi(line.substr(1));
    if (argc < 0) {
        return false;
    }

    args->reserve(static_cast<std::size_t>(argc));
    for (int i = 0; i < argc; ++i) {
        if (!ReadLine(fd, &line) || line.empty() || line[0] != '$') {
            return false;
        }
        const int len = std::stoi(line.substr(1));
        if (len < 0) {
            return false;
        }

        std::string data;
        if (!ReadExactly(fd, static_cast<std::size_t>(len), &data)) {
            return false;
        }
        args->push_back(std::move(data));

        char crlf[2] = {0, 0};
        if (::recv(fd, &crlf, 2, 0) != 2 || crlf[0] != '\r' || crlf[1] != '\n') {
            return false;
        }
    }
    return true;
}

class FakeRedisServer {
public:
    explicit FakeRedisServer(std::vector<Expectation> expectations)
        : expectations_(std::move(expectations)) {
        worker_ = std::thread(&FakeRedisServer::Run, this);
        WaitUntilReady();
    }

    ~FakeRedisServer() {
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    int port() const { return port_.load(); }

    bool passed() const { return passed_.load(); }

    std::string error() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return error_;
    }

private:
    void WaitUntilReady() {
        for (int i = 0; i < 100; ++i) {
            if (ready_.load()) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    void Fail(const std::string& message) {
        passed_.store(false);
        std::lock_guard<std::mutex> lock(mutex_);
        if (error_.empty()) {
            error_ = message;
        }
    }

    void Run() {
        const int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) {
            Fail("failed to create listen socket");
            ready_.store(true);
            return;
        }

        int on = 1;
        (void)::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        if (::bind(listen_fd,
                   reinterpret_cast<sockaddr*>(&addr),
                   sizeof(addr)) != 0) {
            ::close(listen_fd);
            Fail("failed to bind");
            ready_.store(true);
            return;
        }
        if (::listen(listen_fd, 8) != 0) {
            ::close(listen_fd);
            Fail("failed to listen");
            ready_.store(true);
            return;
        }

        sockaddr_in bound{};
        socklen_t bound_len = sizeof(bound);
        if (::getsockname(listen_fd,
                          reinterpret_cast<sockaddr*>(&bound),
                          &bound_len) != 0) {
            ::close(listen_fd);
            Fail("failed to query bound port");
            ready_.store(true);
            return;
        }
        port_.store(ntohs(bound.sin_port));
        ready_.store(true);

        for (const auto& expectation : expectations_) {
            const int conn_fd = ::accept(listen_fd, nullptr, nullptr);
            if (conn_fd < 0) {
                Fail("failed to accept client connection");
                break;
            }

            std::vector<std::string> args;
            if (!ReadCommand(conn_fd, &args)) {
                Fail("failed to read redis command");
                ::close(conn_fd);
                break;
            }
            if (args != expectation.expected_args) {
                std::string message = "unexpected command: ";
                for (const auto& arg : args) {
                    message += "[" + arg + "]";
                }
                Fail(message);
                ::close(conn_fd);
                break;
            }
            if (!WriteAll(conn_fd, expectation.response)) {
                Fail("failed to write redis response");
                ::close(conn_fd);
                break;
            }
            ::close(conn_fd);
        }
        ::close(listen_fd);
    }

    std::vector<Expectation> expectations_;
    std::thread worker_;
    std::atomic<bool> ready_{false};
    std::atomic<int> port_{0};
    std::atomic<bool> passed_{true};
    mutable std::mutex mutex_;
    std::string error_;
};

RedisConnectionConfig BuildConfig(int port) {
    RedisConnectionConfig config;
    config.mode = StorageBackendMode::kExternal;
    config.host = "127.0.0.1";
    config.port = port;
    config.connect_timeout_ms = 300;
    config.read_timeout_ms = 300;
    return config;
}

}  // namespace

TEST(TcpRedisHashClientTest, PingReturnsTrueOnPong) {
    FakeRedisServer server({
        Expectation{{"PING"}, "+PONG\r\n"},
    });

    TcpRedisHashClient client(BuildConfig(server.port()));
    std::string error;
    EXPECT_TRUE(client.Ping(&error)) << error;
    EXPECT_TRUE(server.passed()) << server.error();
}

TEST(TcpRedisHashClientTest, SupportsHSetAndHGetAll) {
    FakeRedisServer server({
        Expectation{{"HSET", "quant:rt:order:ord-1", "filled_volume", "2", "status", "FILLED"},
                    ":2\r\n"},
        Expectation{{"HGETALL", "quant:rt:order:ord-1"},
                    "*4\r\n$6\r\nstatus\r\n$6\r\nFILLED\r\n$13\r\nfilled_volume\r\n$1\r\n2\r\n"},
    });

    TcpRedisHashClient client(BuildConfig(server.port()));
    std::unordered_map<std::string, std::string> fields{
        {"status", "FILLED"},
        {"filled_volume", "2"},
    };
    std::string error;
    EXPECT_TRUE(client.HSet("quant:rt:order:ord-1", fields, &error)) << error;

    std::unordered_map<std::string, std::string> out;
    EXPECT_TRUE(client.HGetAll("quant:rt:order:ord-1", &out, &error)) << error;
    EXPECT_EQ(out["status"], "FILLED");
    EXPECT_EQ(out["filled_volume"], "2");
    EXPECT_TRUE(server.passed()) << server.error();
}

TEST(TcpRedisHashClientTest, SupportsExpire) {
    FakeRedisServer server({
        Expectation{{"HSET", "market:tick:SHFE.ag2406:latest", "last_price", "4501.5"}, ":1\r\n"},
        Expectation{{"EXPIRE", "market:tick:SHFE.ag2406:latest", "259200"}, ":1\r\n"},
    });

    TcpRedisHashClient client(BuildConfig(server.port()));
    std::string error;
    EXPECT_TRUE(client.HSet("market:tick:SHFE.ag2406:latest",
                            std::unordered_map<std::string, std::string>{{"last_price", "4501.5"}},
                            &error))
        << error;
    EXPECT_TRUE(client.Expire("market:tick:SHFE.ag2406:latest", 259200, &error)) << error;
    EXPECT_TRUE(server.passed()) << server.error();
}

}  // namespace quant_hft
