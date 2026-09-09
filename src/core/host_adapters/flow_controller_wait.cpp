#include "quant_hft/core/flow_controller.h"

#include <algorithm>
#include <thread>

namespace quant_hft {

bool TokenBucket::Acquire(int timeout_ms) {
    const auto started = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::milliseconds(std::max(0, timeout_ms));
    while (true) {
        if (TryAcquire()) return true;
        if (std::chrono::steady_clock::now() - started >= timeout) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

FlowResult FlowController::Acquire(const Operation& operation, int timeout_ms) {
    std::shared_ptr<TokenBucket> bucket;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        bucket = FindBucketLocked(operation);
    }
    if (bucket == nullptr) return FlowResult{true, "", 0};
    if (bucket->Acquire(timeout_ms)) return FlowResult{true, "", 0};
    return FlowResult{false, "rate_limited_timeout", std::max(0, timeout_ms)};
}

}  // namespace quant_hft
