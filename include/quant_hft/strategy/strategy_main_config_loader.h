#pragma once

#include <string>
#include <vector>

#include "quant_hft/strategy/composite_strategy.h"

namespace quant_hft {

struct StrategyMainBacktestConfig {
    double initial_equity{0.0};
    std::string product_series_mode{"raw"};
    std::string contract_expiry_calendar_path;
    std::vector<std::string> symbols;
    std::string start_date;
    std::string end_date;
    std::string product_config_path;
};

struct RiskManagementConfig {
    bool enabled{false};
    double risk_per_trade_pct{0.005};
    double max_risk_per_trade{2000.0};
};

struct StrategyMainConfig {
    std::string run_type{"backtest"};
    bool market_state_mode{true};
    StrategyMainBacktestConfig backtest;
    RiskManagementConfig risk_management;
    CompositeStrategyDefinition composite;
};

bool LoadStrategyMainConfig(const std::string& path, StrategyMainConfig* out, std::string* error);

}  // namespace quant_hft
