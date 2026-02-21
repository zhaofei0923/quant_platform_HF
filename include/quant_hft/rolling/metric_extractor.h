#pragma once

#include <string>

#include "quant_hft/apps/backtest_replay_support.h"

namespace quant_hft::rolling {

bool ExtractMetricFromResult(const quant_hft::apps::BacktestCliResult& result,
                             const std::string& metric_path,
                             double* out,
                             std::string* error);

}  // namespace quant_hft::rolling

