#pragma once

#include <memory>
#include <utility>

#include "quant_hft/core/redis_hash_client.h"

namespace quant_hft {

// All cache projections, including strategy checkpoints, share the deployment namespace.
class ScopedRedisClient final : public IRedisHashClient {
   public:
    ScopedRedisClient(std::shared_ptr<IRedisHashClient> client, std::string scope)
        : client_(std::move(client)), prefix_(std::move(scope) + ":") {}

    bool HSet(const std::string& key, const std::unordered_map<std::string, std::string>& fields,
              std::string* error) override {
        return client_->HSet(prefix_ + key, fields, error);
    }
    bool HGetAll(const std::string& key, std::unordered_map<std::string, std::string>* out,
                 std::string* error) const override {
        return client_->HGetAll(prefix_ + key, out, error);
    }
    bool HIncrBy(const std::string& key, const std::string& field, std::int64_t delta,
                 std::string* error) override {
        return client_->HIncrBy(prefix_ + key, field, delta, error);
    }
    bool HSetVersioned(const std::string& key,
                       const std::unordered_map<std::string, std::string>& fields,
                       std::uint64_t version, std::string* error) override {
        return client_->HSetVersioned(prefix_ + key, fields, version, error);
    }
    bool Expire(const std::string& key, int ttl_seconds, std::string* error) override {
        return client_->Expire(prefix_ + key, ttl_seconds, error);
    }
    bool Ping(std::string* error) const override { return client_->Ping(error); }

   private:
    std::shared_ptr<IRedisHashClient> client_;
    std::string prefix_;
};

}  // namespace quant_hft
