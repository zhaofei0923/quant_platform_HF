#include "quant_hft/apps/backtest_metrics.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <ctime>
#include <iomanip>
#include <map>
#include <numeric>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace quant_hft::apps {

namespace {

std::string NormalizeTradingDay(std::string day) {
    std::string digits;
    digits.reserve(day.size());
    for (const unsigned char ch : day) {
        if (std::isdigit(ch) != 0) {
            digits.push_back(static_cast<char>(ch));
        }
    }
    if (digits.size() == 8) {
        return digits;
    }
    return "";
}

std::string TradingDayFromEpochNs(EpochNanos ts_ns) {
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / 1'000'000'000LL);
    std::tm tm = *gmtime(&seconds);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d");
    return oss.str();
}

std::string NormalizeStatus(std::string status) {
    std::transform(status.begin(), status.end(), status.begin(),
                   [](const unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    return status;
}

double PercentileFromSorted(const std::vector<double>& sorted_values, double p) {
    if (sorted_values.empty()) {
        return 0.0;
    }
    if (sorted_values.size() == 1) {
        return sorted_values.front();
    }
    const double clamped_p = std::clamp(p, 0.0, 1.0);
    const double index = clamped_p * static_cast<double>(sorted_values.size() - 1);
    const std::size_t lo = static_cast<std::size_t>(std::floor(index));
    const std::size_t hi = static_cast<std::size_t>(std::ceil(index));
    if (lo == hi) {
        return sorted_values[lo];
    }
    const double w = index - static_cast<double>(lo);
    return sorted_values[lo] * (1.0 - w) + sorted_values[hi] * w;
}

double Mean(const std::vector<double>& values) {
    if (values.empty()) {
        return 0.0;
    }
    const double total = std::accumulate(values.begin(), values.end(), 0.0);
    return total / static_cast<double>(values.size());
}

double StdDev(const std::vector<double>& values, double mean) {
    if (values.empty()) {
        return 0.0;
    }
    double sum = 0.0;
    for (const double value : values) {
        const double diff = value - mean;
        sum += diff * diff;
    }
    return std::sqrt(sum / static_cast<double>(values.size()));
}

double MaxDrawdownPct(const std::vector<double>& values) {
    if (values.empty()) {
        return 0.0;
    }
    double peak = values.front();
    double max_drawdown = 0.0;
    for (const double value : values) {
        peak = std::max(peak, value);
        if (peak > 0.0) {
            max_drawdown = std::max(max_drawdown, (peak - value) / peak * 100.0);
        }
    }
    return max_drawdown;
}

}  // namespace

std::vector<DailyPerformance> ComputeDailyMetrics(const std::vector<EquitySample>& equity_history,
                                                  const std::vector<TradeRecord>& trades,
                                                  double initial_capital) {
    struct DayAggregate {
        EpochNanos ts_ns{0};
        double capital{0.0};
        double position_value{0.0};
        std::string regime;
        bool seen{false};
    };

    std::map<std::string, DayAggregate> per_day;
    for (const EquitySample& sample : equity_history) {
        std::string day = NormalizeTradingDay(sample.trading_day);
        if (day.empty()) {
            day = TradingDayFromEpochNs(sample.ts_ns);
        }
        if (day.empty()) {
            continue;
        }
        DayAggregate& agg = per_day[day];
        if (!agg.seen || sample.ts_ns >= agg.ts_ns) {
            agg.ts_ns = sample.ts_ns;
            agg.capital = sample.equity;
            agg.position_value = sample.position_value;
            agg.regime = sample.market_regime;
            agg.seen = true;
        }
    }

    std::map<std::string, int> day_trade_count;
    std::map<std::string, double> day_turnover;
    for (const TradeRecord& trade : trades) {
        const std::string day = TradingDayFromEpochNs(trade.timestamp_ns);
        day_trade_count[day] += 1;
        day_turnover[day] += std::fabs(trade.price * static_cast<double>(trade.volume));
    }

    std::vector<DailyPerformance> daily;
    daily.reserve(per_day.size());
    if (initial_capital <= 0.0 && !per_day.empty()) {
        initial_capital = per_day.begin()->second.capital;
    }

    double previous_capital = initial_capital;
    double running_peak = initial_capital;
    for (const auto& [day, agg] : per_day) {
        DailyPerformance perf;
        perf.date = day;
        perf.capital = agg.capital;
        perf.position_value = agg.position_value;
        perf.market_regime = agg.regime.empty() ? "kUnknown" : agg.regime;

        const auto trade_count_it = day_trade_count.find(day);
        if (trade_count_it != day_trade_count.end()) {
            perf.trades_count = trade_count_it->second;
        }
        const auto turnover_it = day_turnover.find(day);
        if (turnover_it != day_turnover.end()) {
            perf.turnover = turnover_it->second;
        }

        if (std::fabs(previous_capital) > 1e-12) {
            perf.daily_return_pct = (perf.capital - previous_capital) / previous_capital * 100.0;
        }
        if (std::fabs(initial_capital) > 1e-12) {
            perf.cumulative_return_pct = (perf.capital - initial_capital) / initial_capital * 100.0;
        }
        running_peak = std::max(running_peak, perf.capital);
        if (running_peak > 0.0) {
            perf.drawdown_pct = (running_peak - perf.capital) / running_peak * 100.0;
        }
        previous_capital = perf.capital;
        daily.push_back(std::move(perf));
    }

    return daily;
}

RiskMetrics ComputeRiskMetrics(const std::vector<DailyPerformance>& daily) {
    RiskMetrics metrics;
    if (daily.empty()) {
        return metrics;
    }

    std::vector<double> returns;
    returns.reserve(daily.size());
    for (const DailyPerformance& row : daily) {
        returns.push_back(row.daily_return_pct / 100.0);
    }

    std::vector<double> sorted_returns = returns;
    std::sort(sorted_returns.begin(), sorted_returns.end());
    const double q05 = PercentileFromSorted(sorted_returns, 0.05);
    metrics.var_95 = std::max(0.0, -q05 * 100.0);

    double es_sum = 0.0;
    int es_count = 0;
    for (const double value : sorted_returns) {
        if (value <= q05) {
            es_sum += value;
            ++es_count;
        }
    }
    if (es_count > 0) {
        metrics.expected_shortfall_95 = std::max(0.0, -(es_sum / static_cast<double>(es_count)) * 100.0);
    }

    double dd_sq_sum = 0.0;
    double max_dd = 0.0;
    for (const DailyPerformance& row : daily) {
        max_dd = std::max(max_dd, row.drawdown_pct);
        dd_sq_sum += row.drawdown_pct * row.drawdown_pct;
    }
    metrics.ulcer_index = std::sqrt(dd_sq_sum / static_cast<double>(daily.size()));

    const double cumulative_return_pct = daily.back().cumulative_return_pct;
    if (max_dd > 1e-12) {
        metrics.recovery_factor = cumulative_return_pct / max_dd;
    }

    const double min_return = *std::min_element(returns.begin(), returns.end());
    metrics.tail_loss = std::max(0.0, -min_return * 100.0);

    return metrics;
}

ExecutionQuality ComputeExecutionQuality(const std::vector<OrderRecord>& orders,
                                         const std::vector<TradeRecord>& trades) {
    ExecutionQuality quality;
    if (orders.empty() && trades.empty()) {
        quality.slippage_percentiles = {0.0, 0.0, 0.0};
        return quality;
    }

    int filled = 0;
    int canceled = 0;
    double wait_sum_ms = 0.0;
    int waited = 0;
    for (const OrderRecord& order : orders) {
        const std::string status = NormalizeStatus(order.status);
        if (status == "FILLED") {
            ++filled;
        }
        if (status == "CANCELED") {
            ++canceled;
        }
        if (order.last_update_ns >= order.created_at_ns) {
            wait_sum_ms += static_cast<double>(order.last_update_ns - order.created_at_ns) / 1'000'000.0;
            ++waited;
        }
    }

    if (!orders.empty()) {
        quality.limit_order_fill_rate = static_cast<double>(filled) / static_cast<double>(orders.size());
        quality.cancel_rate = static_cast<double>(canceled) / static_cast<double>(orders.size());
    }
    if (waited > 0) {
        quality.avg_wait_time_ms = wait_sum_ms / static_cast<double>(waited);
    }

    std::vector<double> slippages;
    slippages.reserve(trades.size());
    for (const TradeRecord& trade : trades) {
        slippages.push_back(trade.slippage);
    }
    if (slippages.empty()) {
        quality.slippage_percentiles = {0.0, 0.0, 0.0};
        return quality;
    }

    quality.slippage_mean = Mean(slippages);
    quality.slippage_std = StdDev(slippages, quality.slippage_mean);

    std::sort(slippages.begin(), slippages.end());
    quality.slippage_percentiles = {
        PercentileFromSorted(slippages, 0.25),
        PercentileFromSorted(slippages, 0.50),
        PercentileFromSorted(slippages, 0.75),
    };
    return quality;
}

RollingMetrics ComputeRollingMetrics(const std::vector<DailyPerformance>& daily, int window_days) {
    RollingMetrics metrics;
    if (daily.empty()) {
        return metrics;
    }

    const int window = std::max(2, window_days);
    std::vector<double> returns;
    std::vector<double> capitals;
    returns.reserve(daily.size());
    capitals.reserve(daily.size());
    for (const DailyPerformance& row : daily) {
        returns.push_back(row.daily_return_pct / 100.0);
        capitals.push_back(row.capital);
    }

    metrics.rolling_sharpe_3m.reserve(daily.size());
    metrics.rolling_max_dd_3m.reserve(daily.size());
    for (std::size_t end = 0; end < daily.size(); ++end) {
        if (static_cast<int>(end + 1) < window) {
            metrics.rolling_sharpe_3m.push_back(0.0);
            metrics.rolling_max_dd_3m.push_back(0.0);
            continue;
        }
        const std::size_t begin = end + 1 - static_cast<std::size_t>(window);

        std::vector<double> window_returns(returns.begin() + static_cast<std::ptrdiff_t>(begin),
                                           returns.begin() + static_cast<std::ptrdiff_t>(end + 1));
        const double mean = Mean(window_returns);
        const double stdev = StdDev(window_returns, mean);
        double sharpe = 0.0;
        if (stdev > 1e-12) {
            sharpe = (mean / stdev) * std::sqrt(252.0);
        }
        metrics.rolling_sharpe_3m.push_back(sharpe);

        std::vector<double> window_capitals(capitals.begin() + static_cast<std::ptrdiff_t>(begin),
                                            capitals.begin() + static_cast<std::ptrdiff_t>(end + 1));
        metrics.rolling_max_dd_3m.push_back(MaxDrawdownPct(window_capitals));
    }

    return metrics;
}

std::vector<RegimePerformance> ComputeRegimePerformance(const std::vector<TradeRecord>& trades) {
    struct RegimeAggregate {
        int trades_count{0};
        int wins{0};
        double total_pnl{0.0};
        double return_sum_pct{0.0};
        std::vector<double> trade_returns_pct;
        std::set<std::string> days;
        std::vector<double> cumulative_pnl;
    };

    std::map<std::string, RegimeAggregate> grouped;
    for (const TradeRecord& trade : trades) {
        const std::string key = trade.regime_at_entry.empty() ? "kUnknown" : trade.regime_at_entry;
        RegimeAggregate& agg = grouped[key];
        agg.trades_count += 1;
        if (trade.realized_pnl > 0.0) {
            agg.wins += 1;
        }
        agg.total_pnl += trade.realized_pnl;
        const double notional = std::fabs(trade.price * static_cast<double>(trade.volume));
        const double return_pct = notional > 1e-12 ? trade.realized_pnl / notional * 100.0 : 0.0;
        agg.return_sum_pct += return_pct;
        agg.trade_returns_pct.push_back(return_pct);
        agg.days.insert(TradingDayFromEpochNs(trade.timestamp_ns));
        const double cumulative = agg.cumulative_pnl.empty() ? trade.realized_pnl
                                                             : agg.cumulative_pnl.back() + trade.realized_pnl;
        agg.cumulative_pnl.push_back(cumulative);
    }

    std::vector<RegimePerformance> out;
    out.reserve(grouped.size());
    for (auto& [regime, agg] : grouped) {
        RegimePerformance perf;
        perf.regime = regime;
        perf.total_days = static_cast<int>(agg.days.size());
        perf.trades_count = agg.trades_count;
        if (agg.trades_count > 0) {
            perf.win_rate = static_cast<double>(agg.wins) / static_cast<double>(agg.trades_count);
            perf.average_return_pct = agg.return_sum_pct / static_cast<double>(agg.trades_count);
        }
        perf.total_pnl = agg.total_pnl;
        if (!agg.trade_returns_pct.empty()) {
            const double mean = Mean(agg.trade_returns_pct);
            const double stdev = StdDev(agg.trade_returns_pct, mean);
            if (stdev > 1e-12) {
                perf.sharpe =
                    (mean / stdev) * std::sqrt(static_cast<double>(agg.trade_returns_pct.size()));
            }
        }
        perf.max_drawdown_pct = MaxDrawdownPct(agg.cumulative_pnl);
        out.push_back(std::move(perf));
    }

    return out;
}

AdvancedSummary ComputeAdvancedSummary(const std::vector<DailyPerformance>& daily,
                                       const std::vector<TradeRecord>& trades,
                                       const RiskMetrics& /*risk_metrics*/) {
    AdvancedSummary summary;
    if (daily.empty()) {
        return summary;
    }

    const RollingMetrics rolling = ComputeRollingMetrics(daily, 63);
    if (!rolling.rolling_sharpe_3m.empty()) {
        summary.rolling_sharpe_3m_last = rolling.rolling_sharpe_3m.back();
    }
    if (!rolling.rolling_max_dd_3m.empty()) {
        summary.rolling_max_dd_3m_last = rolling.rolling_max_dd_3m.back();
    }

    std::vector<double> returns;
    returns.reserve(daily.size());
    double sum_positive = 0.0;
    double sum_negative = 0.0;
    for (const DailyPerformance& row : daily) {
        const double value = row.daily_return_pct / 100.0;
        returns.push_back(value);
        if (value > 0.0) {
            sum_positive += value;
        } else if (value < 0.0) {
            sum_negative += value;
        }
    }

    std::vector<double> sorted = returns;
    std::sort(sorted.begin(), sorted.end());
    const double q95 = PercentileFromSorted(sorted, 0.95);
    const double q05 = PercentileFromSorted(sorted, 0.05);
    if (std::fabs(q05) > 1e-12) {
        summary.tail_ratio = std::fabs(q95 / q05);
    }
    if (std::fabs(sum_negative) > 1e-12) {
        summary.gain_to_pain_ratio = sum_positive / std::fabs(sum_negative);
    }

    double win_pnl = 0.0;
    double loss_pnl = 0.0;
    for (const TradeRecord& trade : trades) {
        if (trade.realized_pnl > 0.0) {
            win_pnl += trade.realized_pnl;
        } else if (trade.realized_pnl < 0.0) {
            loss_pnl += trade.realized_pnl;
        }
    }
    if (std::fabs(loss_pnl) > 1e-12) {
        summary.profit_factor = win_pnl / std::fabs(loss_pnl);
    }

    return summary;
}

}  // namespace quant_hft::apps
