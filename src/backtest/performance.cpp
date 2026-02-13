#include "quant_hft/backtest/performance.h"

#include <cmath>
#include <numeric>
#include <vector>

namespace quant_hft::backtest {

BacktestPerformanceSummary AnalyzePerformance(const BacktestResult& result) {
    BacktestPerformanceSummary summary;
    summary.order_count = result.orders.size();
    summary.trade_count = result.trades.size();

    for (const auto& trade : result.trades) {
        summary.commission_paid += trade.commission;
    }

    if (result.equity_curve.empty()) {
        return summary;
    }

    summary.initial_balance = result.equity_curve.front().balance;
    summary.final_balance = result.equity_curve.back().balance;
    summary.net_profit = summary.final_balance - summary.initial_balance;
    if (std::abs(summary.initial_balance) > 1e-12) {
        summary.total_return = summary.net_profit / summary.initial_balance;
    }

    double peak = result.equity_curve.front().balance;
    std::vector<double> returns;
    returns.reserve(result.equity_curve.size() > 1 ? result.equity_curve.size() - 1 : 0);

    for (std::size_t index = 0; index < result.equity_curve.size(); ++index) {
        const double balance = result.equity_curve[index].balance;
        if (balance > peak) {
            peak = balance;
        }

        const double drawdown = peak - balance;
        if (drawdown > summary.max_drawdown) {
            summary.max_drawdown = drawdown;
        }
        if (peak > 1e-12) {
            const double drawdown_ratio = drawdown / peak;
            if (drawdown_ratio > summary.max_drawdown_ratio) {
                summary.max_drawdown_ratio = drawdown_ratio;
            }
        }

        if (index == 0) {
            continue;
        }
        const double previous = result.equity_curve[index - 1].balance;
        if (std::abs(previous) > 1e-12) {
            returns.push_back((balance - previous) / previous);
        } else {
            returns.push_back(0.0);
        }
    }

    if (!returns.empty()) {
        const double mean = std::accumulate(returns.begin(), returns.end(), 0.0) /
                            static_cast<double>(returns.size());

        double variance = 0.0;
        for (const double value : returns) {
            const double diff = value - mean;
            variance += diff * diff;
        }
        variance /= static_cast<double>(returns.size());
        summary.return_volatility = std::sqrt(variance);

        if (summary.return_volatility > 1e-12) {
            summary.sharpe_ratio = (mean / summary.return_volatility) *
                                   std::sqrt(static_cast<double>(returns.size()));
        }
    }

    return summary;
}

}  // namespace quant_hft::backtest
