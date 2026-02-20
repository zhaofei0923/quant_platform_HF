#pragma once

#include <string>
#include <vector>

#include "quant_hft/strategy/composite_strategy.h"

namespace quant_hft {

struct StrategyMainBacktestConfig {
    double initial_equity{0.0};
    std::vector<std::string> symbols;
    std::string start_date;
    std::string end_date;
    std::string product_config_path;
};

struct StrategyMainConfig {
    std::string run_type{"backtest"};
    bool market_state_mode{true};
    StrategyMainBacktestConfig backtest;
    CompositeStrategyDefinition composite;
};

bool LoadStrategyMainConfig(const std::string& path, StrategyMainConfig* out, std::string* error);

}  // namespace quant_hft
