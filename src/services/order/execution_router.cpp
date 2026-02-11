#include "quant_hft/services/execution_router.h"

#include <algorithm>
#include <cmath>

namespace quant_hft {

ExecutionRoute ExecutionRouter::Route(const PlannedOrder& planned,
                                      const ExecutionConfig& config,
                                      std::int64_t observed_market_volume) const {
    ExecutionRoute route;
    route.venue = config.preferred_venue.empty() ? "SIM" : config.preferred_venue;
    route.route_id = route.venue + ":" + planned.execution_algo_id + ":" +
                     std::to_string(std::max(1, planned.slice_index)) + "/" +
                     std::to_string(std::max(1, planned.slice_total));
    if (observed_market_volume <= 0 || planned.intent.volume <= 0) {
        return route;
    }

    const double participation = static_cast<double>(planned.intent.volume) /
                                 static_cast<double>(observed_market_volume);
    const double threshold = std::max(1e-9, config.participation_rate_limit);
    const double overload = std::max(0.0, participation - threshold);
    route.slippage_bps = overload * 10000.0;
    route.impact_cost = std::max(0.0, config.impact_cost_bps) * participation;
    return route;
}

}  // namespace quant_hft
