#pragma once

#include <string>

#include "quant_hft/backtest/replay_runtime.h"

namespace quant_hft::rolling {

bool ExtractMetricFromResult(const quant_hft::backtest::BacktestCliResult& result,
                             const std::string& metric_path, double* out, std::string* error);

}  // namespace quant_hft::rolling
