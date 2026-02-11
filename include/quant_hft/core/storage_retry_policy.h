#pragma once

namespace quant_hft {

struct StorageRetryPolicy {
    int max_attempts{3};
    int initial_backoff_ms{5};
    int max_backoff_ms{100};
};

}  // namespace quant_hft
