#pragma once

#include "quant_hft/backtest/engine.h"

namespace quant_hft::backtest {

struct BacktestPerformanceSummary {
    double initial_balance{0.0};
    double final_balance{0.0};
    double net_profit{0.0};
    double total_return{0.0};
    double max_drawdown{0.0};
    double max_drawdown_ratio{0.0};
    double return_volatility{0.0};
    double sharpe_ratio{0.0};
    std::size_t order_count{0};
    std::size_t trade_count{0};
    double commission_paid{0.0};
};

BacktestPerformanceSummary AnalyzePerformance(const BacktestResult& result);

}  // namespace quant_hft::backtest
