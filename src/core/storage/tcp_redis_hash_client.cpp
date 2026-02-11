#include "quant_hft/core/tcp_redis_hash_client.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

namespace quant_hft {

struct TcpRedisHashClient::RespValue {
    enum class Type {
        kSimpleString,
        kError,
        kInteger,
        kBulkString,
        kArray,
        kNull,
    };

    Type type{Type::kNull};
    std::string text;
    std::int64_t integer{0};
    std::vector<RespValue> elements;
};

namespace {

class SocketGuard {
public:
    explicit SocketGuard(int fd) : fd_(fd) {}
    ~SocketGuard() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    SocketGuard(const SocketGuard&) = delete;
    SocketGuard& operator=(const SocketGuard&) = delete;

    int get() const { return fd_; }

private:
    int fd_{-1};
};

int NormalizeTimeoutMs(int value, int fallback) {
    if (value <= 0) {
        return fallback;
    }
    return value;
}

bool SetSocketIoTimeout(int fd, int timeout_ms, std::string* error) {
    const int bounded = std::max(1, timeout_ms);
    timeval tv{};
    tv.tv_sec = bounded / 1000;
    tv.tv_usec = (bounded % 1000) * 1000;

    if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0) {
        if (error != nullptr) {
            *error = "setsockopt SO_RCVTIMEO failed";
        }
        return false;
    }
    if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0) {
        if (error != nullptr) {
            *error = "setsockopt SO_SNDTIMEO failed";
        }
        return false;
    }
    return true;
}

bool ConnectWithTimeout(int fd,
                        const sockaddr* addr,
                        socklen_t addr_len,
                        int timeout_ms,
                        std::string* error) {
    const int original_flags = ::fcntl(fd, F_GETFL, 0);
    if (original_flags < 0) {
        if (error != nullptr) {
            *error = "fcntl(F_GETFL) failed";
        }
        return false;
    }
    if (::fcntl(fd, F_SETFL, original_flags | O_NONBLOCK) != 0) {
        if (error != nullptr) {
            *error = "fcntl(F_SETFL) failed";
        }
        return false;
    }

    const int connect_result = ::connect(fd, addr, addr_len);
    if (connect_result == 0) {
        (void)::fcntl(fd, F_SETFL, original_flags);
        return true;
    }
    if (errno != EINPROGRESS) {
        if (error != nullptr) {
            *error = std::string("connect failed: ") + std::strerror(errno);
        }
        (void)::fcntl(fd, F_SETFL, original_flags);
        return false;
    }

    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(fd, &write_fds);

    timeval timeout{};
    timeout.tv_sec = std::max(1, timeout_ms) / 1000;
    timeout.tv_usec = (std::max(1, timeout_ms) % 1000) * 1000;
    const int select_result = ::select(fd + 1, nullptr, &write_fds, nullptr, &timeout);
    if (select_result <= 0) {
        if (error != nullptr) {
            if (select_result == 0) {
                *error = "connect timeout";
            } else {
                *error = std::string("select failed: ") + std::strerror(errno);
            }
        }
        (void)::fcntl(fd, F_SETFL, original_flags);
        return false;
    }

    int socket_error = 0;
    socklen_t error_len = sizeof(socket_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_len) != 0) {
        if (error != nullptr) {
            *error = "getsockopt(SO_ERROR) failed";
        }
        (void)::fcntl(fd, F_SETFL, original_flags);
        return false;
    }
    if (socket_error != 0) {
        if (error != nullptr) {
            *error = std::string("connect failed: ") + std::strerror(socket_error);
        }
        (void)::fcntl(fd, F_SETFL, original_flags);
        return false;
    }

    if (::fcntl(fd, F_SETFL, original_flags) != 0) {
        if (error != nullptr) {
            *error = "fcntl(restore flags) failed";
        }
        return false;
    }
    return true;
}

int ConnectSocket(const RedisConnectionConfig& config, std::string* error) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* result = nullptr;
    const std::string port = std::to_string(config.port);
    const int rc = ::getaddrinfo(config.host.c_str(), port.c_str(), &hints, &result);
    if (rc != 0) {
        if (error != nullptr) {
            *error = std::string("getaddrinfo failed: ") + ::gai_strerror(rc);
        }
        return -1;
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard(result, ::freeaddrinfo);

    std::string last_error = "no address available";
    for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
        const int fd = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) {
            last_error = "socket create failed";
            continue;
        }

        const int connect_timeout_ms =
            NormalizeTimeoutMs(config.connect_timeout_ms, 1000);
        if (!ConnectWithTimeout(fd, it->ai_addr, it->ai_addrlen, connect_timeout_ms, &last_error)) {
            ::close(fd);
            continue;
        }

        const int io_timeout_ms = NormalizeTimeoutMs(config.read_timeout_ms, 1000);
        if (!SetSocketIoTimeout(fd, io_timeout_ms, &last_error)) {
            ::close(fd);
            continue;
        }

        return fd;
    }

    if (error != nullptr) {
        *error = last_error;
    }
    return -1;
}

std::string BuildRespCommand(const std::vector<std::string>& args) {
    std::ostringstream stream;
    stream << '*' << args.size() << "\r\n";
    for (const auto& arg : args) {
        stream << '$' << arg.size() << "\r\n" << arg << "\r\n";
    }
    return stream.str();
}

bool SendAll(int fd, const std::string& data, std::string* error) {
    std::size_t offset = 0;
    while (offset < data.size()) {
        const ssize_t wrote = ::send(fd, data.data() + offset, data.size() - offset, 0);
        if (wrote < 0 && errno == EINTR) {
            continue;
        }
        if (wrote <= 0) {
            if (error != nullptr) {
                *error = std::string("send failed: ") + std::strerror(errno);
            }
            return false;
        }
        offset += static_cast<std::size_t>(wrote);
    }
    return true;
}

bool ReadExact(int fd, std::size_t count, std::string* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "out is null";
        }
        return false;
    }
    out->clear();
    out->resize(count);
    std::size_t offset = 0;
    while (offset < count) {
        const ssize_t got = ::recv(fd, out->data() + offset, count - offset, 0);
        if (got < 0 && errno == EINTR) {
            continue;
        }
        if (got == 0) {
            if (error != nullptr) {
                *error = "connection closed by peer";
            }
            return false;
        }
        if (got < 0) {
            if (error != nullptr) {
                *error = std::string("recv failed: ") + std::strerror(errno);
            }
            return false;
        }
        offset += static_cast<std::size_t>(got);
    }
    return true;
}

bool ReadLine(int fd, std::string* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "out is null";
        }
        return false;
    }
    out->clear();

    char ch = '\0';
    while (true) {
        const ssize_t got = ::recv(fd, &ch, 1, 0);
        if (got < 0 && errno == EINTR) {
            continue;
        }
        if (got == 0) {
            if (error != nullptr) {
                *error = "connection closed by peer";
            }
            return false;
        }
        if (got < 0) {
            if (error != nullptr) {
                *error = std::string("recv failed: ") + std::strerror(errno);
            }
            return false;
        }

        if (ch == '\r') {
            const ssize_t got_lf = ::recv(fd, &ch, 1, 0);
            if (got_lf <= 0 || ch != '\n') {
                if (error != nullptr) {
                    *error = "invalid line ending";
                }
                return false;
            }
            return true;
        }
        out->push_back(ch);
    }
}

bool ParseRespReply(int fd,
                    int depth,
                    TcpRedisHashClient::RespValue* out,
                    std::string* error);

bool ParseArrayReply(int fd,
                     int depth,
                     int count,
                     TcpRedisHashClient::RespValue* out,
                     std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "out is null";
        }
        return false;
    }
    out->type = TcpRedisHashClient::RespValue::Type::kArray;
    out->elements.clear();
    out->elements.reserve(static_cast<std::size_t>(count));

    for (int i = 0; i < count; ++i) {
        TcpRedisHashClient::RespValue item;
        if (!ParseRespReply(fd, depth + 1, &item, error)) {
            return false;
        }
        out->elements.push_back(std::move(item));
    }
    return true;
}

bool ParseRespReply(int fd,
                    int depth,
                    TcpRedisHashClient::RespValue* out,
                    std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "out is null";
        }
        return false;
    }
    if (depth > 32) {
        if (error != nullptr) {
            *error = "redis reply nested too deep";
        }
        return false;
    }

    char kind = '\0';
    ssize_t got = 0;
    do {
        got = ::recv(fd, &kind, 1, 0);
    } while (got < 0 && errno == EINTR);
    if (got == 0) {
        if (error != nullptr) {
            *error = "connection closed by peer";
        }
        return false;
    }
    if (got < 0) {
        if (error != nullptr) {
            *error = std::string("recv reply type failed: ") + std::strerror(errno);
        }
        return false;
    }

    std::string line;
    if (!ReadLine(fd, &line, error)) {
        return false;
    }

    switch (kind) {
        case '+':
            out->type = TcpRedisHashClient::RespValue::Type::kSimpleString;
            out->text = line;
            return true;
        case '-':
            out->type = TcpRedisHashClient::RespValue::Type::kError;
            out->text = line;
            return true;
        case ':':
            try {
                out->type = TcpRedisHashClient::RespValue::Type::kInteger;
                out->integer = std::stoll(line);
                return true;
            } catch (...) {
                if (error != nullptr) {
                    *error = "invalid integer response";
                }
                return false;
            }
        case '$': {
            int len = 0;
            try {
                len = std::stoi(line);
            } catch (...) {
                if (error != nullptr) {
                    *error = "invalid bulk string length";
                }
                return false;
            }
            if (len < 0) {
                out->type = TcpRedisHashClient::RespValue::Type::kNull;
                out->text.clear();
                return true;
            }

            std::string payload;
            if (!ReadExact(fd, static_cast<std::size_t>(len), &payload, error)) {
                return false;
            }
            char crlf[2] = {0, 0};
            if (::recv(fd, &crlf, 2, 0) != 2 || crlf[0] != '\r' || crlf[1] != '\n') {
                if (error != nullptr) {
                    *error = "bulk string missing CRLF";
                }
                return false;
            }
            out->type = TcpRedisHashClient::RespValue::Type::kBulkString;
            out->text = std::move(payload);
            return true;
        }
        case '*': {
            int count = 0;
            try {
                count = std::stoi(line);
            } catch (...) {
                if (error != nullptr) {
                    *error = "invalid array length";
                }
                return false;
            }
            if (count < 0) {
                out->type = TcpRedisHashClient::RespValue::Type::kNull;
                out->elements.clear();
                return true;
            }
            return ParseArrayReply(fd, depth, count, out, error);
        }
        default:
            if (error != nullptr) {
                *error = "unsupported redis reply type";
            }
            return false;
    }
}

bool RespString(const TcpRedisHashClient::RespValue& value, std::string* out) {
    if (out == nullptr) {
        return false;
    }
    if (value.type == TcpRedisHashClient::RespValue::Type::kSimpleString ||
        value.type == TcpRedisHashClient::RespValue::Type::kBulkString) {
        *out = value.text;
        return true;
    }
    return false;
}

}  // namespace

TcpRedisHashClient::TcpRedisHashClient(RedisConnectionConfig config)
    : config_(std::move(config)) {}

bool TcpRedisHashClient::HSet(const std::string& key,
                              const std::unordered_map<std::string, std::string>& fields,
                              std::string* error) {
    if (key.empty()) {
        if (error != nullptr) {
            *error = "empty key";
        }
        return false;
    }
    if (fields.empty()) {
        if (error != nullptr) {
            *error = "fields is empty";
        }
        return false;
    }

    std::vector<std::pair<std::string, std::string>> ordered_fields(fields.begin(),
                                                                     fields.end());
    std::sort(ordered_fields.begin(),
              ordered_fields.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    std::vector<std::string> args;
    args.reserve(2 + ordered_fields.size() * 2);
    args.push_back("HSET");
    args.push_back(key);
    for (const auto& [field, value] : ordered_fields) {
        args.push_back(field);
        args.push_back(value);
    }

    RespValue reply;
    if (!ExecuteCommand(args, &reply, error)) {
        return false;
    }
    if (reply.type == RespValue::Type::kInteger) {
        return true;
    }
    std::string text;
    if (RespString(reply, &text) && text == "OK") {
        return true;
    }
    if (error != nullptr) {
        *error = "unexpected HSET reply";
    }
    return false;
}

bool TcpRedisHashClient::HGetAll(const std::string& key,
                                 std::unordered_map<std::string, std::string>* out,
                                 std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "out is null";
        }
        return false;
    }
    if (key.empty()) {
        if (error != nullptr) {
            *error = "empty key";
        }
        return false;
    }

    RespValue reply;
    if (!ExecuteCommand({"HGETALL", key}, &reply, error)) {
        return false;
    }
    if (reply.type == RespValue::Type::kNull) {
        if (error != nullptr) {
            *error = "not found";
        }
        return false;
    }
    if (reply.type != RespValue::Type::kArray) {
        if (error != nullptr) {
            *error = "unexpected HGETALL reply";
        }
        return false;
    }
    if (reply.elements.size() % 2 != 0) {
        if (error != nullptr) {
            *error = "invalid HGETALL reply size";
        }
        return false;
    }

    out->clear();
    for (std::size_t i = 0; i < reply.elements.size(); i += 2) {
        std::string field;
        std::string value;
        if (!RespString(reply.elements[i], &field) ||
            !RespString(reply.elements[i + 1], &value)) {
            if (error != nullptr) {
                *error = "invalid HGETALL field/value type";
            }
            return false;
        }
        (*out)[field] = value;
    }
    return true;
}

bool TcpRedisHashClient::Ping(std::string* error) const {
    RespValue reply;
    if (!ExecuteCommand({"PING"}, &reply, error)) {
        return false;
    }
    std::string text;
    if (!RespString(reply, &text)) {
        if (error != nullptr) {
            *error = "unexpected PING reply";
        }
        return false;
    }
    if (text != "PONG") {
        if (error != nullptr) {
            *error = "PING response is not PONG";
        }
        return false;
    }
    return true;
}

bool TcpRedisHashClient::ExecuteCommand(const std::vector<std::string>& args,
                                        RespValue* reply,
                                        std::string* error) const {
    if (args.empty()) {
        if (error != nullptr) {
            *error = "empty command";
        }
        return false;
    }

    std::string connect_error;
    const int fd = ConnectSocket(config_, &connect_error);
    if (fd < 0) {
        if (error != nullptr) {
            *error = connect_error;
        }
        return false;
    }
    SocketGuard guard(fd);

    if (!Authenticate(fd, error)) {
        return false;
    }
    if (!SendCommand(fd, args, error)) {
        return false;
    }
    if (!ReadReply(fd, reply, error)) {
        return false;
    }
    if (reply != nullptr && reply->type == RespValue::Type::kError) {
        if (error != nullptr) {
            *error = reply->text;
        }
        return false;
    }
    return true;
}

bool TcpRedisHashClient::Authenticate(int fd, std::string* error) const {
    if (config_.password.empty()) {
        return true;
    }

    std::vector<std::string> auth;
    if (config_.username.empty()) {
        auth = {"AUTH", config_.password};
    } else {
        auth = {"AUTH", config_.username, config_.password};
    }

    if (!SendCommand(fd, auth, error)) {
        return false;
    }
    RespValue reply;
    if (!ReadReply(fd, &reply, error)) {
        return false;
    }
    if (reply.type == RespValue::Type::kError) {
        if (error != nullptr) {
            *error = "AUTH failed: " + reply.text;
        }
        return false;
    }

    std::string text;
    if (!RespString(reply, &text) || text != "OK") {
        if (error != nullptr) {
            *error = "AUTH unexpected response";
        }
        return false;
    }
    return true;
}

bool TcpRedisHashClient::SendCommand(int fd,
                                     const std::vector<std::string>& args,
                                     std::string* error) const {
    const auto payload = BuildRespCommand(args);
    return SendAll(fd, payload, error);
}

bool TcpRedisHashClient::ReadReply(int fd, RespValue* out, std::string* error) const {
    return ParseRespReply(fd, 0, out, error);
}

}  // namespace quant_hft
