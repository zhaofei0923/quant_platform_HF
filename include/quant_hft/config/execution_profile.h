#pragma once

#include <string>

namespace quant_hft {

struct StrategyExecutionProfile {
    int max_order_volume{1};
    double max_order_notional{1'000'000.0};
    double max_margin_to_equity_ratio{1.0};
    // Asia/Shanghai, HH:MM-HH:MM comma-separated; closes remain permitted.
    std::string forbid_open_windows;
};

}  // namespace quant_hft
