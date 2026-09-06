#pragma once

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/backtest/replay_runtime.h"

// Compatibility for existing CLI consumers. Research code uses backtest/replay_runtime.h.
namespace quant_hft::apps {
using namespace quant_hft::backtest;
using quant_hft::backtest::PositionSnapshot;
}  // namespace quant_hft::apps
