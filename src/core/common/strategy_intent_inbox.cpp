#include "quant_hft/core/strategy_intent_inbox.h"

#include "quant_hft/core/strategy_intent_codec.h"

namespace quant_hft {

StrategyIntentInbox::StrategyIntentInbox(std::shared_ptr<IRedisHashClient> client)
    : client_(std::move(client)) {}

bool StrategyIntentInbox::ReadLatest(const std::string& strategy_id,
                                     StrategyIntentBatch* out,
                                     std::string* error) {
    if (out == nullptr || strategy_id.empty() || client_ == nullptr) {
        if (error != nullptr) {
            *error = "output pointer, strategy_id, or client is invalid";
        }
        return false;
    }

    std::unordered_map<std::string, std::string> hash;
    std::string redis_error;
    if (!client_->HGetAll(BuildKey(strategy_id), &hash, &redis_error)) {
        if (error != nullptr) {
            *error = redis_error.empty() ? "redis hgetall failed" : redis_error;
        }
        return false;
    }

    StrategyIntentBatch batch;
    if (!ParseInt64(hash, "seq", &batch.seq)) {
        if (error != nullptr) {
            *error = "missing or invalid seq";
        }
        return false;
    }
    std::int64_t count64 = 0;
    if (!ParseInt64(hash, "count", &count64) || count64 < 0) {
        if (error != nullptr) {
            *error = "missing or invalid count";
        }
        return false;
    }
    if (count64 > 0) {
        batch.intents.reserve(static_cast<std::size_t>(count64));
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = last_seq_by_strategy_.find(strategy_id);
        if (it != last_seq_by_strategy_.end() && batch.seq <= it->second) {
            batch.ts_ns = 0;
            batch.intents.clear();
            *out = std::move(batch);
            return true;
        }
    }

    for (std::int64_t i = 0; i < count64; ++i) {
        const auto field = "intent_" + std::to_string(i);
        const auto encoded = GetOrEmpty(hash, field);
        if (encoded.empty()) {
            if (error != nullptr) {
                *error = "missing field: " + field;
            }
            return false;
        }

        SignalIntent intent;
        std::string decode_error;
        if (!StrategyIntentCodec::DecodeSignalIntent(strategy_id,
                                                     encoded,
                                                     &intent,
                                                     &decode_error)) {
            if (error != nullptr) {
                *error = "decode " + field + " failed: " + decode_error;
            }
            return false;
        }
        batch.intents.push_back(std::move(intent));
    }

    // ts_ns is optional; treat absence as 0.
    (void)ParseInt64(hash, "ts_ns", &batch.ts_ns);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        last_seq_by_strategy_[strategy_id] = batch.seq;
    }
    *out = std::move(batch);
    return true;
}

std::string StrategyIntentInbox::BuildKey(const std::string& strategy_id) {
    return "strategy:intent:" + strategy_id + ":latest";
}

bool StrategyIntentInbox::ParseInt64(const std::unordered_map<std::string, std::string>& hash,
                                     const std::string& key,
                                     std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    const auto it = hash.find(key);
    if (it == hash.end()) {
        return false;
    }
    try {
        *out = std::stoll(it->second);
        return true;
    } catch (...) {
        return false;
    }
}

std::string StrategyIntentInbox::GetOrEmpty(
    const std::unordered_map<std::string, std::string>& hash,
    const std::string& key) {
    const auto it = hash.find(key);
    if (it == hash.end()) {
        return "";
    }
    return it->second;
}

}  // namespace quant_hft
