#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

enum class StrategyStopKind : std::uint8_t {
    kNone = 0,
    kInitial = 1,
    kTrailing = 2,
};

// A read-only, point-in-time projection of the risk levels that actually own a
// strategy position. Unknown prices stay absent; consumers must never infer
// risk levels from unrelated sub-strategies.
struct StrategyRiskSnapshot {
    std::string account_id;
    std::string strategy_id;
    std::string owner_strategy_id;
    std::string instrument_id;
    std::int32_t net{0};
    std::optional<double> avg_open;
    std::optional<double> initial_stop;
    std::optional<double> trailing_stop;
    std::optional<double> effective_stop;
    StrategyStopKind stop_kind{StrategyStopKind::kNone};
    std::optional<double> take_profit;
    EpochNanos as_of_ns{0};
};

}  // namespace quant_hft
