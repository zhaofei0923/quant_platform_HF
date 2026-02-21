#pragma once

#include <string>
#include <vector>

#include "quant_hft/rolling/rolling_config.h"

namespace quant_hft::rolling {

struct Window {
    int index{0};
    std::string train_start;
    std::string train_end;
    std::string test_start;
    std::string test_end;
};

bool BuildTradingDayCalendar(const RollingConfig& config,
                             std::vector<std::string>* trading_days,
                             std::string* error);

bool GenerateWindows(const RollingConfig& config,
                     const std::vector<std::string>& trading_days,
                     std::vector<Window>* windows,
                     std::string* error);

}  // namespace quant_hft::rolling

