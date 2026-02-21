#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/apps/backtest_replay_support.h"
#include "quant_hft/rolling/rolling_config.h"
#include "quant_hft/rolling/window_generator.h"

namespace quant_hft::rolling {

struct WindowResult {
    int index{0};
    std::string train_start;
    std::string train_end;
    std::string test_start;
    std::string test_end;
    bool success{false};
    double objective{0.0};
    std::unordered_map<std::string, double> metrics;
    std::string best_params_yaml;
    std::string error_msg;
};

struct RollingReport {
    std::string mode;
    std::vector<WindowResult> windows;
    double mean_objective{0.0};
    double std_objective{0.0};
    double max_objective{0.0};
    double min_objective{0.0};
    int success_count{0};
    int failed_count{0};
    bool interrupted{false};
    std::vector<double> objectives;
};

using BacktestRunFn = std::function<bool(const quant_hft::apps::BacktestCliSpec&,
                                         quant_hft::apps::BacktestCliResult*,
                                         std::string*)>;

bool RunRollingBacktest(const RollingConfig& config,
                        RollingReport* report,
                        std::string* error,
                        BacktestRunFn run_fn = BacktestRunFn{});

}  // namespace quant_hft::rolling

