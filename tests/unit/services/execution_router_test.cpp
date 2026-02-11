#include <gtest/gtest.h>

#include "quant_hft/core/ctp_config.h"
#include "quant_hft/services/execution_planner.h"
#include "quant_hft/services/execution_router.h"

namespace quant_hft {
namespace {

TEST(ExecutionRouterTest, BuildsRouteWithImpactAndParticipationGuard) {
    ExecutionConfig config;
    config.preferred_venue = "SIM";
    config.participation_rate_limit = 0.2;
    config.impact_cost_bps = 6.0;

    PlannedOrder planned;
    planned.intent.client_order_id = "trace-1#slice-1";
    planned.intent.instrument_id = "SHFE.ag2406";
    planned.intent.volume = 20;
    planned.execution_algo_id = "twap";
    planned.slice_index = 1;
    planned.slice_total = 4;

    ExecutionRouter router;
    const auto route = router.Route(planned, config, /*observed_market_volume=*/50);

    EXPECT_EQ(route.venue, "SIM");
    EXPECT_EQ(route.route_id, "SIM:twap:1/4");
    EXPECT_GT(route.slippage_bps, 0.0);
    EXPECT_GT(route.impact_cost, 0.0);
}

TEST(ExecutionRouterTest, KeepsDefaultsWhenMarketVolumeIsUnavailable) {
    ExecutionConfig config;
    config.preferred_venue = "SIM";
    config.participation_rate_limit = 0.5;
    config.impact_cost_bps = 0.0;

    PlannedOrder planned;
    planned.intent.client_order_id = "trace-2";
    planned.intent.instrument_id = "SHFE.rb2405";
    planned.intent.volume = 5;
    planned.execution_algo_id = "direct";
    planned.slice_index = 1;
    planned.slice_total = 1;

    ExecutionRouter router;
    const auto route = router.Route(planned, config, /*observed_market_volume=*/0);

    EXPECT_EQ(route.route_id, "SIM:direct:1/1");
    EXPECT_DOUBLE_EQ(route.impact_cost, 0.0);
    EXPECT_DOUBLE_EQ(route.slippage_bps, 0.0);
}

}  // namespace
}  // namespace quant_hft
