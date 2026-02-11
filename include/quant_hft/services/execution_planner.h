#pragma once

#include <deque>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/ctp_config.h"

namespace quant_hft {

struct PlannedOrder {
    OrderIntent intent;
    std::string execution_algo_id;
    std::int32_t slice_index{1};
    std::int32_t slice_total{1};
};

class ExecutionPlanner {
public:
    explicit ExecutionPlanner(std::size_t throttle_window_size = 20);

    std::vector<PlannedOrder> BuildPlan(const SignalIntent& signal,
                                        const std::string& account_id,
                                        const ExecutionConfig& config,
                                        const std::vector<MarketSnapshot>& recent_market) const;

    void RecordOrderResult(bool rejected);
    bool ShouldThrottle(double reject_ratio_threshold) const;
    double CurrentRejectRatio() const;

private:
    static std::string AlgoToId(ExecutionAlgo algo);
    static std::vector<std::int32_t> BuildVolumePlan(const SignalIntent& signal,
                                                     const ExecutionConfig& config,
                                                     const std::vector<MarketSnapshot>& recent_market);

    std::size_t throttle_window_size_{20};
    std::deque<bool> reject_history_;
};

}  // namespace quant_hft
