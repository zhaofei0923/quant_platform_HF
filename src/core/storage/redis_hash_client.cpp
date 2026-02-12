#include "quant_hft/core/redis_hash_client.h"

#include <chrono>

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
    if (IsExpiredLocked(key)) {
        storage_.erase(key);
        expiry_epoch_seconds_.erase(key);
    }
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
    if (IsExpiredLocked(key)) {
        if (error != nullptr) {
            *error = "not found";
        }
        return false;
    }
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

bool InMemoryRedisHashClient::HIncrBy(const std::string& key,
                                      const std::string& field,
                                      std::int64_t delta,
                                      std::string* error) {
    if (key.empty() || field.empty()) {
        if (error != nullptr) {
            *error = "key and field must be non-empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (IsExpiredLocked(key)) {
        storage_.erase(key);
        expiry_epoch_seconds_.erase(key);
    }
    auto& hash = storage_[key];
    std::int64_t current = 0;
    const auto it = hash.find(field);
    if (it != hash.end() && !it->second.empty()) {
        try {
            current = std::stoll(it->second);
        } catch (...) {
            if (error != nullptr) {
                *error = "field value is not integer";
            }
            return false;
        }
    }
    hash[field] = std::to_string(current + delta);
    return true;
}

bool InMemoryRedisHashClient::Expire(const std::string& key,
                                     int ttl_seconds,
                                     std::string* error) {
    if (key.empty()) {
        if (error != nullptr) {
            *error = "empty key";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (IsExpiredLocked(key)) {
        storage_.erase(key);
        expiry_epoch_seconds_.erase(key);
    }
    const auto it = storage_.find(key);
    if (it == storage_.end()) {
        if (error != nullptr) {
            *error = "not found";
        }
        return false;
    }
    if (ttl_seconds <= 0) {
        storage_.erase(it);
        expiry_epoch_seconds_.erase(key);
        return true;
    }
    expiry_epoch_seconds_[key] = NowEpochSeconds() + ttl_seconds;
    return true;
}

bool InMemoryRedisHashClient::Ping(std::string* error) const {
    (void)error;
    return true;
}

bool InMemoryRedisHashClient::IsExpiredLocked(const std::string& key) const {
    const auto it = expiry_epoch_seconds_.find(key);
    if (it == expiry_epoch_seconds_.end()) {
        return false;
    }
    return NowEpochSeconds() >= it->second;
}

std::int64_t InMemoryRedisHashClient::NowEpochSeconds() {
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    return now.time_since_epoch().count();
}

}  // namespace quant_hft
