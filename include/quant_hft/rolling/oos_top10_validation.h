#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "quant_hft/apps/backtest_replay_support.h"
#include "quant_hft/optim/optimization_algorithm.h"

namespace quant_hft::rolling {

using OosBacktestRunFn = std::function<bool(const quant_hft::apps::BacktestCliSpec&,
                                            quant_hft::apps::BacktestCliResult*,
                                            std::string*)>;

struct OosTop10ValidationRequest {
    std::string train_report_json;
    std::string oos_start_date;
    std::string oos_end_date;
    int top_n{10};
    bool overwrite{false};
    std::string output_dir;
};

struct OosTop10ValidationRow {
    int rank{0};
    std::string trial_id;
    optim::ParamValueMap params;
    std::optional<double> in_sample_calmar;
    std::optional<double> in_sample_max_drawdown_pct;
    std::optional<double> in_sample_sharpe;
    std::optional<double> oos_calmar;
    std::optional<double> oos_max_drawdown_pct;
    std::optional<double> oos_sharpe;
    std::optional<double> oos_profit_factor;
    std::optional<double> oos_total_pnl;
    std::optional<double> oos_win_rate_pct;
    std::optional<int> oos_trades;
    bool success{false};
    bool reused_existing{false};
    std::string status;
    std::string error_msg;
    std::string result_json_path;
};

struct OosTop10ValidationReport {
    std::string train_report_json;
    std::string top_trials_dir;
    std::string output_dir;
    std::string output_csv;
    std::string final_recommended_params_yaml;
    std::string recommended_trial_id;
    int requested_top_n{0};
    int selected_count{0};
    int success_count{0};
    int failed_count{0};
    std::vector<OosTop10ValidationRow> rows;
};

bool RunOosTop10Validation(const OosTop10ValidationRequest& request,
                           OosTop10ValidationReport* report,
                           std::string* error,
                           OosBacktestRunFn run_fn = OosBacktestRunFn{});

}  // namespace quant_hft::rolling