#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/core/storage_connection_config.h"

namespace quant_hft {

class TcpRedisHashClient : public IRedisHashClient {
public:
    struct RespValue;

    explicit TcpRedisHashClient(RedisConnectionConfig config);

    bool HSet(const std::string& key,
              const std::unordered_map<std::string, std::string>& fields,
              std::string* error) override;
    bool HGetAll(const std::string& key,
                 std::unordered_map<std::string, std::string>* out,
                 std::string* error) const override;
    bool Expire(const std::string& key, int ttl_seconds, std::string* error) override;
    bool Ping(std::string* error) const override;

private:
    bool ExecuteCommand(const std::vector<std::string>& args,
                        RespValue* reply,
                        std::string* error) const;
    bool Authenticate(int fd, std::string* error) const;
    bool SendCommand(int fd,
                     const std::vector<std::string>& args,
                     std::string* error) const;
    bool ReadReply(int fd, RespValue* out, std::string* error) const;

    RedisConnectionConfig config_;
};

}  // namespace quant_hft
