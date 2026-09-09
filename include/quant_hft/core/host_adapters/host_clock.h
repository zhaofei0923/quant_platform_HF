#pragma once

#include <chrono>

#include "quant_hft/contracts/clock.h"

namespace quant_hft {
// Bind once at the online process boundary, before creating worker threads.
inline void BindOnlineHostClocks() {
    SetHostEpochNanosClock([]() -> EpochNanos {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    });
    SetHostMonotonicNanosClock([]() -> EpochNanos {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    });
}
}  // namespace quant_hft
