#include "quant_hft/core/flow_controller.h"

#include <algorithm>
#include <thread>

namespace quant_hft {

TokenBucket::TokenBucket()
    : last_refill_(std::chrono::steady_clock::now()) {}

TokenBucket::TokenBucket(double rate_per_second, int capacity)
    : rate_per_second_(std::max(0.1, rate_per_second)),
      capacity_(std::max(1, capacity)),
      tokens_(static_cast<double>(std::max(1, capacity))),
      last_refill_(std::chrono::steady_clock::now()) {}

bool TokenBucket::TryAcquire() {
    std::lock_guard<std::mutex> lock(mutex_);
    RefillLocked(std::chrono::steady_clock::now());
    if (tokens_ < 1.0) {
        return false;
    }
    tokens_ -= 1.0;
    return true;
}

bool TokenBucket::Acquire(int timeout_ms) {
    const auto started = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::milliseconds(std::max(0, timeout_ms));
    while (true) {
        if (TryAcquire()) {
            return true;
        }
        if (std::chrono::steady_clock::now() - started >= timeout) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void TokenBucket::SetRate(double rate_per_second) {
    std::lock_guard<std::mutex> lock(mutex_);
    RefillLocked(std::chrono::steady_clock::now());
    rate_per_second_ = std::max(0.1, rate_per_second);
}

void TokenBucket::RefillLocked(std::chrono::steady_clock::time_point now) {
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - last_refill_);
    if (elapsed.count() <= 0) {
        return;
    }
    const double refill = (static_cast<double>(elapsed.count()) / 1000.0) * rate_per_second_;
    tokens_ = std::min(static_cast<double>(capacity_), tokens_ + refill);
    last_refill_ = now;
}

std::size_t FlowController::KeyHash::operator()(const Key& key) const {
    const auto h1 = std::hash<std::string>{}(key.account_id);
    const auto h2 = std::hash<int>{}(static_cast<int>(key.type));
    const auto h3 = std::hash<std::string>{}(key.instrument_id);
    return h1 ^ (h2 << 1U) ^ (h3 << 2U);
}

void FlowController::AddRule(const FlowRule& rule) {
    const Key key{rule.account_id, rule.type, rule.instrument_id};
    std::lock_guard<std::mutex> lock(mutex_);
    buckets_[key] = std::make_shared<TokenBucket>(std::max(0.1, rule.rate_per_second),
                                                  std::max(1, rule.capacity));
}

FlowResult FlowController::Check(const Operation& operation) {
    std::shared_ptr<TokenBucket> bucket;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        bucket = FindBucketLocked(operation);
    }
    if (bucket == nullptr) {
        return FlowResult{true, "", 0};
    }
    if (bucket->TryAcquire()) {
        return FlowResult{true, "", 0};
    }
    return FlowResult{false, "rate_limited", 0};
}

FlowResult FlowController::Acquire(const Operation& operation, int timeout_ms) {
    std::shared_ptr<TokenBucket> bucket;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        bucket = FindBucketLocked(operation);
    }
    if (bucket == nullptr) {
        return FlowResult{true, "", 0};
    }
    if (bucket->Acquire(timeout_ms)) {
        return FlowResult{true, "", 0};
    }
    return FlowResult{false, "rate_limited_timeout", std::max(0, timeout_ms)};
}

std::shared_ptr<TokenBucket> FlowController::FindBucketLocked(const Operation& operation) {
    Key key_with_instrument{operation.account_id, operation.type, operation.instrument_id};
    if (const auto it = buckets_.find(key_with_instrument); it != buckets_.end()) {
        return it->second;
    }
    Key key_account_scope{operation.account_id, operation.type, ""};
    if (const auto it = buckets_.find(key_account_scope); it != buckets_.end()) {
        return it->second;
    }
    return {};
}

}  // namespace quant_hft
