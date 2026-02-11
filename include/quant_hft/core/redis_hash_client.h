#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

namespace quant_hft {

class IRedisHashClient {
public:
    virtual ~IRedisHashClient() = default;

    virtual bool HSet(const std::string& key,
                      const std::unordered_map<std::string, std::string>& fields,
                      std::string* error) = 0;
    virtual bool HGetAll(const std::string& key,
                         std::unordered_map<std::string, std::string>* out,
                         std::string* error) const = 0;
    virtual bool Ping(std::string* error) const = 0;
};

class InMemoryRedisHashClient : public IRedisHashClient {
public:
    bool HSet(const std::string& key,
              const std::unordered_map<std::string, std::string>& fields,
              std::string* error) override;
    bool HGetAll(const std::string& key,
                 std::unordered_map<std::string, std::string>* out,
                 std::string* error) const override;
    bool Ping(std::string* error) const override;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> storage_;
};

}  // namespace quant_hft
