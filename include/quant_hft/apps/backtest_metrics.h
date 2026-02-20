#pragma once

#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft::apps {

struct TradeRecord {
    std::string trade_id;
    std::string order_id;
    std::string symbol;
    std::string exchange;
    std::string side;
    std::string offset;
    int volume{0};
    double price{0.0};
    EpochNanos timestamp_ns{0};
    double commission{0.0};
    double slippage{0.0};
    double realized_pnl{0.0};
    std::string strategy_id;
    std::string signal_type;
    std::string regime_at_entry;
};

struct OrderRecord {
    std::string order_id;
    std::string client_order_id;
    std::string symbol;
    std::string type;
    std::string side;
    std::string offset;
    double price{0.0};
    int volume{0};
    std::string status;
    int filled_volume{0};
    double avg_fill_price{0.0};
    EpochNanos created_at_ns{0};
    EpochNanos last_update_ns{0};
    std::string strategy_id;
    std::string cancel_reason;
};

struct DailyPerformance {
    std::string date;
    double capital{0.0};
    double daily_return_pct{0.0};
    double cumulative_return_pct{0.0};
    double drawdown_pct{0.0};
    double position_value{0.0};
    int trades_count{0};
    double turnover{0.0};
    std::string market_regime;
};

struct PositionSnapshot {
    EpochNanos timestamp_ns{0};
    std::string symbol;
    int net_position{0};
    double avg_price{0.0};
    double unrealized_pnl{0.0};
};

struct ExecutionQuality {
    double limit_order_fill_rate{0.0};
    double avg_wait_time_ms{0.0};
    double cancel_rate{0.0};
    double slippage_mean{0.0};
    double slippage_std{0.0};
    std::vector<double> slippage_percentiles;
};

struct RiskMetrics {
    double var_95{0.0};
    double expected_shortfall_95{0.0};
    double ulcer_index{0.0};
    double recovery_factor{0.0};
    double tail_loss{0.0};
};

struct RegimePerformance {
    std::string regime;
    int total_days{0};
    int trades_count{0};
    double win_rate{0.0};
    double average_return_pct{0.0};
    double total_pnl{0.0};
    double sharpe{0.0};
    double max_drawdown_pct{0.0};
};

struct RollingMetrics {
    std::vector<double> rolling_sharpe_3m;
    std::vector<double> rolling_max_dd_3m;
};

struct FactorExposure {
    std::string factor;
    double exposure{0.0};
    double t_stat{0.0};
};

struct MonteCarloResult {
    int simulations{0};
    double mean_final_capital{0.0};
    double ci_95_lower{0.0};
    double ci_95_upper{0.0};
    double prob_loss{0.0};
    double max_drawdown_95{0.0};
};

struct Parameters {
    std::string start_date;
    std::string end_date;
    double initial_capital{0.0};
    std::string engine_mode;
    std::string rollover_mode;
    std::string strategy_factory;
};

struct AdvancedSummary {
    double rolling_sharpe_3m_last{0.0};
    double rolling_max_dd_3m_last{0.0};
    double information_ratio{0.0};
    double beta{0.0};
    double alpha{0.0};
    double tail_ratio{0.0};
    double gain_to_pain_ratio{0.0};
    double avg_win_loss_duration_ratio{0.0};
    double profit_factor{0.0};
};

struct EquitySample {
    EpochNanos ts_ns{0};
    std::string trading_day;
    double equity{0.0};
    double position_value{0.0};
    std::string market_regime;
};

std::vector<DailyPerformance> ComputeDailyMetrics(const std::vector<EquitySample>& equity_history,
                                                  const std::vector<TradeRecord>& trades,
                                                  double initial_capital);

RiskMetrics ComputeRiskMetrics(const std::vector<DailyPerformance>& daily);

ExecutionQuality ComputeExecutionQuality(const std::vector<OrderRecord>& orders,
                                         const std::vector<TradeRecord>& trades);

RollingMetrics ComputeRollingMetrics(const std::vector<DailyPerformance>& daily,
                                     int window_days = 63);

std::vector<RegimePerformance> ComputeRegimePerformance(const std::vector<TradeRecord>& trades);

AdvancedSummary ComputeAdvancedSummary(const std::vector<DailyPerformance>& daily,
                                       const std::vector<TradeRecord>& trades,
                                       const RiskMetrics& risk_metrics);

}  // namespace quant_hft::apps
