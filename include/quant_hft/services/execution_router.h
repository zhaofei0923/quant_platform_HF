#pragma once

#include <cstdint>
#include <string>

#include "quant_hft/core/ctp_config.h"
#include "quant_hft/services/execution_planner.h"

namespace quant_hft {

struct ExecutionRoute {
    std::string venue;
    std::string route_id;
    double slippage_bps{0.0};
    double impact_cost{0.0};
};

class ExecutionRouter {
public:
    ExecutionRoute Route(const PlannedOrder& planned,
                         const ExecutionConfig& config,
                         std::int64_t observed_market_volume) const;
};

}  // namespace quant_hft
