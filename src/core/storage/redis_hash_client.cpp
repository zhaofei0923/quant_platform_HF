#include "quant_hft/core/redis_hash_client.h"

namespace quant_hft {

bool InMemoryRedisHashClient::HSet(
    const std::string& key,
    const std::unordered_map<std::string, std::string>& fields,
    std::string* error) {
    if (key.empty()) {
        if (error != nullptr) {
            *error = "empty key";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    storage_[key] = fields;
    return true;
}

bool InMemoryRedisHashClient::HGetAll(
    const std::string& key,
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

    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = storage_.find(key);
    if (it == storage_.end()) {
        if (error != nullptr) {
            *error = "not found";
        }
        return false;
    }
    *out = it->second;
    return true;
}

bool InMemoryRedisHashClient::Ping(std::string* error) const {
    (void)error;
    return true;
}

}  // namespace quant_hft
